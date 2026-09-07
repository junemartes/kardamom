//! Full DA round trip against a real L1 (anvil): deploy
//! `KardamomL2Settlement`, post batches as real EIP-4844 blob transactions
//! (blobs recorded in a DA store), then discard the original blocks, read
//! the `BatchPosted` event log back, fetch the blobs from the DA store by
//! the versioned hashes L1 committed to, decode and re-execute them, and
//! check that the reconstructed state root equals the canonical
//! (directly-executed) root.
//!
//! This is the "rebuild-from-L1" data-loss recovery path, proven end to
//! end through actual L1. It skips gracefully if anvil is unavailable
//! (the same convention as `anvil_e2e.rs` and the deployer's
//! `deploy_e2e.rs`).

use alloy_network::EthereumWallet;
use alloy_node_bindings::AnvilInstance;
use alloy_primitives::{Address, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;

use kardamom_batcher::batch::ClosedBlock;
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_batcher::da_store::FsBlobStore;
use kardamom_batcher::l1::{post_batch, read_posted_batches, recover_blocks};
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_arg};
use kardamom_engine::{ReplayBlock, replay_blocks};
use kardamom_reconstruct::reconstruct_state;
use kardamom_reconstruct::test_support::{CHAIN_ID as L2_CHAIN_ID, genesis, two_transfer_blocks};
use kardamom_state::{Durability, StateEnvBuilder};

const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
const DEPLOY_CHAIN_ID: u64 = 42;

/// Spawn anvil, fund the dev owner and batcher accounts, and deploy
/// `KardamomL2Settlement`. Returns `None` (after an `eprintln!("SKIP: ...")`)
/// when anvil is unavailable or an anvil-only setup call fails — the same
/// convention as `anvil_e2e.rs` and the deployer's `deploy_e2e.rs`. The
/// returned [`AnvilInstance`] must stay alive for the rest of the test: L1
/// shuts down when it drops.
async fn setup_l1_settlement() -> Option<(AnvilInstance, Address, PrivateKeySigner)> {
    let Ok(anvil) = alloy_node_bindings::Anvil::new().try_spawn() else {
        eprintln!("SKIP: anvil unavailable");
        return None;
    };

    // The batcher EOA is a real funded anvil key so it can sign 4844 txs.
    let batcher_signer: PrivateKeySigner = anvil.keys()[1].clone().into();
    let batcher_addr = batcher_signer.address();

    // Deploy provider: an impersonated DEV_OWNER drives the factory (no
    // fillers, matching the deployer's e2e convention).
    let deploy_provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(anvil.endpoint_url());
    for setup in [
        (
            "anvil_setCode",
            serde_json::json!([ERC7955_FACTORY, format!("0x{ERC7955_RUNTIME_HEX}")]),
        ),
        (
            "anvil_setBalance",
            serde_json::json!([DEV_OWNER, "0x3635c9adc5dea00000"]),
        ),
        (
            "anvil_setBalance",
            serde_json::json!([batcher_addr, "0x3635c9adc5dea00000"]),
        ),
        ("anvil_impersonateAccount", serde_json::json!([DEV_OWNER])),
    ] {
        let ok: Option<serde_json::Value> = deploy_provider
            .raw_request(setup.0.to_string().into(), setup.1)
            .await
            .ok();
        if ok.is_none() {
            eprintln!("SKIP: anvil setup call {} failed", setup.0);
            return None;
        }
    }

    let deployer = Deployer::new(deploy_provider.clone(), DEV_OWNER);
    deployer.ensure_factory(DEV_OWNER).await.unwrap();
    deployer
        .apply(
            &[Op::Deploy {
                l2_chain_id: DEPLOY_CHAIN_ID,
                id: ContractId::KardamomL2Settlement,
                init_args: encode_address_arg(batcher_addr),
            }],
            DEV_OWNER,
        )
        .await
        .expect("deploy KardamomL2Settlement");
    let settlement = deployer.addresses(Some(DEPLOY_CHAIN_ID)).await.unwrap()[0].proxy;

    Some((anvil, settlement, batcher_signer))
}

/// Post both blocks as real 4844 blob txs (one batch each) to `settlement`,
/// then rebuild the frames purely from the on-chain event log and the DA
/// store: read `BatchPosted`, fetch blobs by the versioned hashes L1
/// committed to, and decode.
async fn post_and_recover_frames(
    post_provider: &impl Provider,
    settlement: Address,
    blocks: [ClosedBlock; 2],
) -> Vec<kardamom_batcher::BlockFrame> {
    let da_dir = tempfile::tempdir().unwrap();
    let da_store = FsBlobStore::open(da_dir.path()).unwrap();
    let cfg = BatcherConfig::default();
    let mut prev_index = 0u64;
    for block in blocks {
        let batch = pack_blocks(&cfg, &[block]).unwrap();
        prev_index = post_batch(post_provider, settlement, prev_index, &batch, &da_store)
            .await
            .expect("post batch to L1");
    }
    assert_eq!(prev_index, 2, "two batches posted");
    assert!(da_store.len() >= 2, "DA store holds the posted blobs");

    let descriptors = read_posted_batches(post_provider, settlement, 0)
        .await
        .unwrap();
    assert_eq!(descriptors.len(), 2);
    assert_eq!(descriptors[0].index, 1);
    assert_eq!(descriptors[1].index, 2);

    let frames = recover_blocks(&descriptors, &da_store).unwrap();
    assert_eq!(frames.len(), 2);
    frames
}

/// Reconstruct state from the recovered frames, replay the original
/// blocks directly as the oracle, and check the two roots agree.
fn assert_recovered_matches_oracle(
    from: Address,
    frames: &[kardamom_batcher::BlockFrame],
    block1: &ClosedBlock,
    block2: &ClosedBlock,
) {
    let recon_dir = tempfile::tempdir().unwrap();
    let recovered =
        reconstruct_state(recon_dir.path(), L2_CHAIN_ID, &genesis(from), &[], frames).unwrap();

    let oracle_dir = tempfile::tempdir().unwrap();
    let oracle_env = StateEnvBuilder::new(oracle_dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let oracle_blocks = vec![
        ReplayBlock {
            block_number: 1,
            l2_timestamp: 1_700_000_000,
            remote_epochs: vec![],
            txs: block1.txs.iter().map(|t| t.envelope.clone()).collect(),
        },
        ReplayBlock {
            block_number: 2,
            l2_timestamp: 1_700_000_001,
            remote_epochs: vec![],
            txs: block2.txs.iter().map(|t| t.envelope.clone()).collect(),
        },
    ];
    let oracle =
        replay_blocks(oracle_env, L2_CHAIN_ID, &genesis(from), &[], oracle_blocks).unwrap();

    assert_eq!(recovered.head_block, 2);
    assert_eq!(recovered.txs_applied, 3);
    assert_eq!(
        recovered.state_root, oracle.state_root,
        "state rebuilt purely from L1 data must match the canonical root"
    );
}

/// The rebuild-from-L1 scenario, sequenced from its four steps: deploy
/// the settlement contract on a real L1 ([`setup_l1_settlement`]), build
/// two blocks of transfers ([`two_transfer_blocks`]), post them as blob
/// txs and recover the frames purely from L1 ([`post_and_recover_frames`]),
/// then check the recovered root against the directly-executed one
/// ([`assert_recovered_matches_oracle`]).
#[tokio::test]
async fn rebuild_from_l1_reconstructs_canonical_state_root() {
    let Some((anvil, settlement, batcher_signer)) = setup_l1_settlement().await else {
        return;
    };

    // Posting provider: wallet-filled with the batcher key (fills nonce,
    // gas, and blob fields).
    let post_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(batcher_signer))
        .connect_http(anvil.endpoint_url());

    let user = PrivateKeySigner::random();
    let from = user.address();
    let to1 = address!("00000000000000000000000000000000000D0001");
    let to2 = address!("00000000000000000000000000000000000D0002");
    let (block1, block2) = two_transfer_blocks(&user, to1, to2);

    let frames =
        post_and_recover_frames(&post_provider, settlement, [block1.clone(), block2.clone()]).await;

    assert_recovered_matches_oracle(from, &frames, &block1, &block2);
}
