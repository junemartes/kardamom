//! Full DA round-trip against a real L1 (anvil): deploy `KardamomL2Settlement`,
//! post batches as real EIP-4844 blob transactions (blobs recorded in a DA
//! store), then — with the original blocks thrown away — read the `BatchPosted`
//! event log back, fetch the blobs from the DA store by the versioned hashes L1
//! committed to, decode + re-execute them, and assert the reconstructed state
//! root equals the canonical (directly-executed) root.
//!
//! This is the "rebuild-from-L1" data-loss recovery path proven end-to-end
//! through actual L1. Skips gracefully if anvil is unavailable (same convention
//! as `anvil_e2e.rs` / the deployer's `deploy_e2e.rs`).

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::{EthereumWallet, TxSignerSync};
use alloy_primitives::{Address, Bytes as AlloyBytes, TxKind, U256, address, keccak256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;

use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_batcher::da_store::FsBlobStore;
use kardamom_batcher::l1::{post_batch, read_posted_batches, recover_blocks};
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_arg};
use kardamom_engine::{ReplayBlock, replay_blocks};
use kardamom_reconstruct::reconstruct_state;
use kardamom_state::{Durability, StateEnvBuilder};
use kardamom_types::{BPosition, TxEnvelope};

const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
const L2_CHAIN_ID: u64 = 412346;
const DEPLOY_CHAIN_ID: u64 = 42;

fn transfer(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64) -> TxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(L2_CHAIN_ID),
        nonce,
        gas_price: 0,
        gas_limit: 21_000,
        to: TxKind::Call(to),
        value: U256::from(value),
        input: AlloyBytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    TxEnvelope {
        correlation_id: nonce,
        raw_tx,
        sender: signer.address(),
        tx_hash,
    }
}

fn genesis(from: Address) -> Vec<kardamom_types::AccountChange> {
    vec![kardamom_types::AccountChange {
        address: from,
        nonce: 0,
        balance: U256::from(1_000_000_000_000_000_000u128),
        code_hash: keccak256(b""),
    }]
}

#[tokio::test]
async fn rebuild_from_l1_reconstructs_canonical_state_root() {
    let anvil = match alloy_node_bindings::Anvil::new().try_spawn() {
        Ok(a) => a,
        Err(_) => {
            eprintln!("SKIP: anvil unavailable");
            return;
        }
    };

    // The batcher EOA is a real funded anvil key so it can sign 4844 txs.
    let batcher_signer: PrivateKeySigner = anvil.keys()[1].clone().into();
    let batcher_addr = batcher_signer.address();

    // Deploy provider: impersonated DEV_OWNER drives the factory (no fillers,
    // matching the deployer's e2e convention).
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
            return;
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

    // Posting provider: wallet-filled with the batcher key (fills nonce/gas/blob).
    let post_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(batcher_signer))
        .connect_http(anvil.endpoint_url());

    // ---- Build the chain: two blocks of transfers. ----
    let user = PrivateKeySigner::random();
    let from = user.address();
    let to1 = address!("00000000000000000000000000000000000D0001");
    let to2 = address!("00000000000000000000000000000000000D0002");
    let block1 = ClosedBlock {
        block_number: 1,
        l2_timestamp: 1_700_000_000,
        end_tx_idx: BPosition::from_index(2),
        txs: vec![
            RecordedTx {
                position: BPosition::from_index(0),
                envelope: transfer(&user, to1, 0, 100),
            },
            RecordedTx {
                position: BPosition::from_index(1),
                envelope: transfer(&user, to2, 1, 50),
            },
        ],
    };
    let block2 = ClosedBlock {
        block_number: 2,
        l2_timestamp: 1_700_000_001,
        end_tx_idx: BPosition::from_index(3),
        txs: vec![RecordedTx {
            position: BPosition::from_index(2),
            envelope: transfer(&user, to1, 2, 25),
        }],
    };

    // ---- Post each block as its own batch (real 4844 tx + DA store). ----
    let da_dir = tempfile::tempdir().unwrap();
    let da_store = FsBlobStore::open(da_dir.path()).unwrap();
    let cfg = BatcherConfig::default();
    let mut prev_index = 0u64;
    for block in [block1.clone(), block2.clone()] {
        let batch = pack_blocks(&cfg, &[block]).unwrap();
        prev_index = post_batch(&post_provider, settlement, prev_index, &batch, &da_store)
            .await
            .expect("post batch to L1");
    }
    assert_eq!(prev_index, 2, "two batches posted");
    assert!(da_store.len() >= 2, "DA store holds the posted blobs");

    // ---- Rebuild from L1: read the event log, fetch blobs, reconstruct. ----
    let descriptors = read_posted_batches(&post_provider, settlement, 0)
        .await
        .unwrap();
    assert_eq!(descriptors.len(), 2);
    assert_eq!(descriptors[0].index, 1);
    assert_eq!(descriptors[1].index, 2);

    let frames = recover_blocks(&descriptors, &da_store).unwrap();
    assert_eq!(frames.len(), 2);

    let recon_dir = tempfile::tempdir().unwrap();
    let recovered =
        reconstruct_state(recon_dir.path(), L2_CHAIN_ID, &genesis(from), &[], &frames).unwrap();

    // ---- Oracle: directly execute the original envelopes. ----
    let oracle_dir = tempfile::tempdir().unwrap();
    let oracle_env = StateEnvBuilder::new(oracle_dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let oracle_blocks = vec![
        ReplayBlock {
            block_number: 1,
            l2_timestamp: 1_700_000_000,
            txs: block1.txs.iter().map(|t| t.envelope.clone()).collect(),
        },
        ReplayBlock {
            block_number: 2,
            l2_timestamp: 1_700_000_001,
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
