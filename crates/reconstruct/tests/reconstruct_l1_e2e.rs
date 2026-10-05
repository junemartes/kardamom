//! Full DA round trip against a real L1 (anvil): deploy
//! `KardamomL2Settlement`, disperse batches through a fake EigenDA proxy
//! and post their certificates, then discard the original blocks, read
//! the `BatchPosted` event log back, fetch the payloads from the proxy by
//! the certificates L1 committed to, decode and re-execute them, and
//! check that the reconstructed state root equals the canonical
//! (directly-executed) root.
//!
//! This is the "rebuild-from-L1" data-loss recovery path, proven end to
//! end through actual L1. A second case adds a lockbox deposit on L1, and
//! the rebuild derives it from the lockbox logs. Both skip gracefully if
//! anvil is unavailable (the same convention as `anvil_e2e.rs` and the
//! deployer's `deploy_e2e.rs`).

use alloy_network::EthereumWallet;
use alloy_node_bindings::AnvilInstance;
use alloy_primitives::{Address, B256, Bytes, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;

use kardamom_batcher::batch::ClosedBlock;
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_batcher::da::DaProxy;
use kardamom_batcher::l1::{post_batch, read_posted_batches, recover_blocks};
use kardamom_batcher::testkit_da::FakeDaProxy;
use kardamom_da_watcher::RpcL1Source;
use kardamom_deployer::abi::ETHLockbox;
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_arg, encode_address_pair};
use kardamom_engine::{ReplayBlock, ReplayOutcome};
use kardamom_reconstruct::test_support::{
    genesis, oracle_replay, test_genesis, two_transfer_blocks,
};
use kardamom_reconstruct::{L1Epochs, Reconstruction, block_frame_to_replay, reconstruct_state};
use kardamom_state::{Durability, StateEnvBuilder, StateSnapshot};
use kardamom_types::epoch::{DepositLog, LockboxLog, derive_epoch};
use kardamom_types::{BPosition, StateDatabase};

const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
const DEPLOY_CHAIN_ID: u64 = 42;
/// The gas limit of the lockbox deposit's L2 call.
const DEPOSIT_GAS_LIMIT: u64 = 200_000;
/// The wei the lockbox deposit mints on L2.
const MINT: u64 = 3_000_000;

/// A running anvil L1 with the settlement contract and the lockbox. The
/// anvil must stay alive for the rest of the test: L1 shuts down when it
/// drops.
struct L1Rig {
    anvil: AnvilInstance,
    settlement: Address,
    lockbox: Address,
    batcher_signer: PrivateKeySigner,
}

/// Spawn anvil, fund the dev owner and batcher accounts, and deploy
/// `KardamomL2Settlement` and `ETHLockbox`. Returns `None` (after an
/// `eprintln!("SKIP: ...")`) when anvil is unavailable or an anvil-only
/// setup call fails — the same convention as `anvil_e2e.rs` and the
/// deployer's `deploy_e2e.rs`.
async fn setup_l1() -> Option<L1Rig> {
    let Ok(anvil) = alloy_node_bindings::Anvil::new().try_spawn() else {
        eprintln!("SKIP: anvil unavailable");
        return None;
    };

    // The batcher EOA is a real funded anvil key so it can sign the
    // `postBatch` txs.
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
            &[
                Op::Deploy {
                    l2_chain_id: DEPLOY_CHAIN_ID,
                    id: ContractId::KardamomL2Settlement,
                    init_args: encode_address_arg(batcher_addr),
                },
                // A deposit reads neither initializer address.
                Op::Deploy {
                    l2_chain_id: DEPLOY_CHAIN_ID,
                    id: ContractId::EthLockbox,
                    init_args: encode_address_pair(DEV_OWNER, DEV_OWNER),
                },
            ],
            DEV_OWNER,
        )
        .await
        .expect("deploy KardamomL2Settlement and ETHLockbox");
    let entries = deployer.addresses(Some(DEPLOY_CHAIN_ID)).await.unwrap();
    let proxy = |id: ContractId| {
        entries
            .iter()
            .find(|e| e.id == id.id())
            .map(|e| e.proxy)
            .expect("deployed")
    };

    Some(L1Rig {
        settlement: proxy(ContractId::KardamomL2Settlement),
        lockbox: proxy(ContractId::EthLockbox),
        anvil,
        batcher_signer,
    })
}

impl L1Rig {
    /// A provider that signs with the batcher key, for `postBatch`.
    fn batcher(&self) -> impl Provider {
        ProviderBuilder::new()
            .wallet(EthereumWallet::from(self.batcher_signer.clone()))
            .connect_http(self.anvil.endpoint_url())
    }

    /// Deposit [`MINT`] wei for `to` through the lockbox, from an anvil
    /// key. Returns the epoch L1 holds for the deposit's block, built from
    /// the values this call sent, not read back from the logs.
    async fn deposit(&self, to: Address) -> kardamom_types::EpochRecord {
        let depositor: PrivateKeySigner = self.anvil.keys()[2].clone().into();
        let from = depositor.address();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(depositor))
            .connect_http(self.anvil.endpoint_url());
        let receipt = ETHLockbox::new(self.lockbox, &provider)
            .depositETH(to, DEPOSIT_GAS_LIMIT, Bytes::new())
            .value(U256::from(MINT))
            .send()
            .await
            .expect("send depositETH")
            .get_receipt()
            .await
            .expect("depositETH receipt");
        assert!(receipt.status(), "depositETH reverted");
        let log = receipt
            .inner
            .logs()
            .iter()
            .find(|l| l.address() == self.lockbox)
            .expect("a DepositInitiated log");
        let (block_number, block_hash) = (log.block_number.unwrap(), log.block_hash.unwrap());
        let sent = LockboxLog::Deposit(DepositLog {
            block_number,
            block_hash,
            log_index: log.log_index.unwrap(),
            from,
            to,
            mint: u128::from(MINT),
            gas_limit: DEPOSIT_GAS_LIMIT,
            data: Bytes::new(),
        });
        derive_epoch(block_number, block_hash, &[sent]).expect("one log of one block")
    }
}

/// Post both blocks (one batch each) to `settlement` through the fake
/// proxy `fake`, then rebuild the frames purely from the on-chain event
/// log and the proxy: read `BatchPosted`, fetch the payloads by the
/// certificates L1 committed to, and decode.
async fn post_and_recover_frames(
    post_provider: &impl Provider,
    settlement: Address,
    fake: &FakeDaProxy,
    blocks: [ClosedBlock; 2],
) -> Vec<kardamom_batcher::BlockFrame> {
    let da = DaProxy::new(fake.url()).unwrap();
    let cfg = BatcherConfig::default();
    let mut prev_index = 0u64;
    for block in blocks {
        let batch = pack_blocks(&cfg, &[block]).unwrap();
        prev_index = post_batch(post_provider, settlement, prev_index, &batch, &da)
            .await
            .expect("post batch to L1");
    }
    assert_eq!(prev_index, 2, "two batches posted");
    assert_eq!(
        fake.stored().len(),
        2,
        "the proxy holds the posted payloads"
    );

    let descriptors = read_posted_batches(post_provider, settlement, 0)
        .await
        .unwrap();
    assert_eq!(descriptors.len(), 2);
    assert_eq!(descriptors[0].index, 1);
    assert_eq!(descriptors[1].index, 2);

    let frames = recover_blocks(&descriptors, &da).unwrap();
    assert_eq!(frames.len(), 2);
    frames
}

/// The original blocks as the oracle replays them: no DA round trip, and
/// `epoch`, when given, leading block 1.
fn oracle_blocks(
    block1: &ClosedBlock,
    block2: &ClosedBlock,
    epoch: Option<kardamom_types::EpochRecord>,
) -> Vec<ReplayBlock> {
    [(block1, epoch), (block2, None)]
        .into_iter()
        .map(|(block, epoch)| ReplayBlock {
            block_number: block.block_number,
            l2_timestamp: block.l2_timestamp,
            canonical_end: None,
            l1_epochs: epoch.into_iter().collect(),
            remote_epochs: vec![],
            txs: block.txs.iter().map(|t| t.envelope.clone()).collect(),
        })
        .collect()
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
        reconstruct_state(recon_dir.path(), &test_genesis(&genesis(from), &[]), frames).unwrap();
    let oracle = oracle_replay(&genesis(from), &[], oracle_blocks(block1, block2, None));

    assert_eq!(recovered.head_block, 2);
    assert_eq!(recovered.txs_applied, 3);
    // The batcher posted each block's canonical end, so the state rebuilt
    // from L1 carries the cursor a consumer resumes from: block 2 ends at
    // index 3. The oracle replayed bare blocks and has none.
    assert_eq!(recovered.head_end_tx_idx, Some(3));
    assert_eq!(oracle.head_end_tx_idx, None);
    assert_eq!(
        recovered.state_root, oracle.state_root,
        "state rebuilt purely from L1 data must match the canonical root"
    );
}

/// The rebuild-from-L1 scenario, sequenced from its four steps: deploy
/// the settlement contract on a real L1 ([`setup_l1`]), build two blocks
/// of transfers ([`two_transfer_blocks`]), post them through the fake
/// proxy and recover the frames purely from L1 and the proxy
/// ([`post_and_recover_frames`]), then check the recovered root against
/// the directly-executed one ([`assert_recovered_matches_oracle`]).
///
/// The proxy client blocks in place on the read path, so the runtime is
/// multi-thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_from_l1_reconstructs_canonical_state_root() {
    let Some(l1) = setup_l1().await else {
        return;
    };
    let fake = FakeDaProxy::start();

    let user = PrivateKeySigner::random();
    let from = user.address();
    let to1 = address!("00000000000000000000000000000000000D0001");
    let to2 = address!("00000000000000000000000000000000000D0002");
    let (block1, block2) = two_transfer_blocks(&user, to1, to2);

    let frames = post_and_recover_frames(
        &l1.batcher(),
        l1.settlement,
        &fake,
        [block1.clone(), block2.clone()],
    )
    .await;

    assert_recovered_matches_oracle(from, &frames, &block1, &block2);
}

/// A lockbox deposit on L1, rebuilt from L1 alone. The batcher payload
/// carries only each block's L1 origin; the rebuild reads the lockbox
/// logs of that L1 block and applies the deposit at the head of block 1.
/// The root must equal a direct replay that has the deposit, and must
/// differ from a rebuild that leaves deposits out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_from_l1_derives_lockbox_deposits() {
    let Some(l1) = setup_l1().await else {
        return;
    };
    let fake = FakeDaProxy::start();

    let user = PrivateKeySigner::random();
    let from = user.address();
    let to1 = address!("00000000000000000000000000000000000F0001");
    let to2 = address!("00000000000000000000000000000000000F0002");
    let recipient = address!("00000000000000000000000000000000000F0003");
    let epoch = l1.deposit(recipient).await;
    let deposit = epoch.deposits[0].source_hash;

    // Block 1 is the first block of epoch `origin`: its marker and its
    // deposit take slots 0 and 1, then the two transfers. Block 2 stays
    // in the same epoch.
    let origin = epoch.l1_number;
    let (block1, block2) = two_transfer_blocks(&user, to1, to2);
    let block1 = ClosedBlock {
        end_tx_idx: BPosition::from_index(4),
        l1_origin: origin,
        ..block1
    };
    let block2 = ClosedBlock {
        end_tx_idx: BPosition::from_index(5),
        l1_origin: origin,
        ..block2
    };
    let frames = post_and_recover_frames(
        &l1.batcher(),
        l1.settlement,
        &fake,
        [block1.clone(), block2.clone()],
    )
    .await;

    let source = RpcL1Source::new(ProviderBuilder::new().connect_http(l1.anvil.endpoint_url()));
    let replay = L1Epochs::new(source, l1.lockbox)
        .attach(frames.iter().map(block_frame_to_replay).collect())
        .await
        .expect("derive the L1 epochs");
    assert_eq!(replay[0].l1_epochs, std::slice::from_ref(&epoch));
    assert_eq!(replay[1].l1_epochs, []);

    let recon_dir = tempfile::tempdir().unwrap();
    let recovered = Reconstruction {
        state_dir: recon_dir.path(),
        durability: Durability::SafeNoSync,
    }
    .run(&test_genesis(&genesis(from), &[]), replay)
    .unwrap();
    let oracle = oracle_replay(
        &genesis(from),
        &[],
        oracle_blocks(&block1, &block2, Some(epoch)),
    );
    assert_rebuilt_deposit(&recovered, &oracle, recon_dir.path(), (deposit, recipient));

    let without_dir = tempfile::tempdir().unwrap();
    let without = reconstruct_state(
        without_dir.path(),
        &test_genesis(&genesis(from), &[]),
        &frames,
    )
    .unwrap();
    assert_ne!(
        without.state_root, recovered.state_root,
        "a rebuild without the deposit must not reach the same root"
    );
}

/// The rebuilt state holds the deposit: the oracle's root, the live
/// cursor, the minted balance, and the deposit's receipt in slot 1.
fn assert_rebuilt_deposit(
    recovered: &ReplayOutcome,
    oracle: &ReplayOutcome,
    state_dir: &std::path::Path,
    (deposit, recipient): (B256, Address),
) {
    assert_eq!(recovered.head_end_tx_idx, Some(5));
    assert_eq!(recovered.txs_applied, 4, "one deposit and three transfers");
    assert_eq!(
        recovered.state_root, oracle.state_root,
        "the state rebuilt from L1 must hold the lockbox deposit"
    );
    let env = StateEnvBuilder::new(state_dir)
        .read_only(true)
        .open()
        .unwrap();
    let snap = StateSnapshot::open(&env).unwrap();
    assert_eq!(
        snap.basic(recipient).unwrap().expect("minted").1,
        U256::from(MINT)
    );
    assert_eq!(
        snap.get_tx_position(deposit).unwrap(),
        Some(BPosition::from_index(1))
    );
}
