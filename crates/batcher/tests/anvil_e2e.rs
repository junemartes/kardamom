//! Anvil end-to-end: deploy `KardamomL2Settlement` through the kardamom
//! factory, call `postBatch` from the batcher EOA, and check the on-chain
//! `BatchPosted` event was emitted with the correct fields.
//!
//! Skips gracefully if anvil is not available (the same convention as the
//! deployer's `deploy_e2e.rs`).

use alloy_primitives::{Address, B256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::SolEvent;

use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_deployer::Deployer;
use kardamom_deployer::testkit::{AnvilRig, Funding};

const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
const BATCHER: Address = address!("0000000000000000000000000000000000000BA7");
const L2_CHAIN_ID: u64 = 42;

/// A deployed `KardamomL2Settlement` on a live anvil, plus the provider to
/// call it with. Both e2e tests in this file build one, differing only in
/// which key signs `provider` (an impersonated dev account for the
/// calldata-path test, a real wallet signer for the live-sender test,
/// which needs to sign blob transactions).
struct Scenario<P: Provider + Clone> {
    _anvil: alloy_node_bindings::AnvilInstance,
    provider: P,
    settlement: IKardamomL2Settlement::IKardamomL2SettlementInstance<P>,
    settlement_addr: Address,
}

impl<P: Provider + Clone> Scenario<P> {
    /// Post one batch's calldata (no real blob bytes; this only checks the
    /// calldata path) and return the receipt.
    async fn post_batch(
        &self,
        prev_index: u64,
        blob_hashes: Vec<B256>,
        l2_block_start: u64,
        l2_block_end: u64,
    ) -> alloy_rpc_types_eth::TransactionReceipt {
        self.settlement
            .postBatch(
                prev_index,
                blob_hashes,
                l2_block_start,
                l2_block_end,
                B256::repeat_byte(0x4C),
            )
            .from(BATCHER)
            .send()
            .await
            .expect("send postBatch tx")
            .get_receipt()
            .await
            .expect("await receipt")
    }

    /// Find the `BatchPosted` log for this scenario's settlement and
    /// assert its indexed `batchIndex` equals `want_index`.
    fn assert_batch_posted_index(
        &self,
        receipt: &alloy_rpc_types_eth::TransactionReceipt,
        want_index: u64,
    ) {
        let topic0 = IKardamomL2Settlement::BatchPosted::SIGNATURE_HASH;
        let log = receipt
            .logs()
            .iter()
            .find(|log| {
                log.address() == self.settlement_addr
                    && log.topic0().copied().unwrap_or(B256::ZERO) == topic0
            })
            .expect("BatchPosted event must be emitted");
        // batchIndex is indexed (topics[1]); other fields are in data.
        let log_topics = log.topics();
        assert_eq!(log_topics.len(), 2);
        // indexed uint64 left-padded
        let idx_bytes = log_topics[1].as_slice();
        assert_eq!(&idx_bytes[..24], &[0u8; 24]);
        let idx = u64::from_be_bytes(idx_bytes[24..32].try_into().unwrap());
        assert_eq!(idx, want_index);
    }
}

/// Deploy a `KardamomL2Settlement` with `BATCHER` (an impersonated dev
/// account) as its `l1Batcher`.
async fn setup() -> Option<Scenario<impl Provider + Clone>> {
    let rig = AnvilRig::spawn(
        alloy_node_bindings::Anvil::new(),
        &[
            (DEV_OWNER, Funding::FundAndImpersonate),
            (BATCHER, Funding::FundAndImpersonate),
        ],
    )
    .await?;
    let deployer = Deployer::new(rig.provider.clone(), DEV_OWNER);
    let settlement_addr = deployer
        .deploy_settlement(DEV_OWNER, L2_CHAIN_ID, BATCHER)
        .await;
    let settlement = IKardamomL2Settlement::new(settlement_addr, rig.provider.clone());
    Some(Scenario {
        _anvil: rig.anvil,
        provider: rig.provider,
        settlement,
        settlement_addr,
    })
}

#[tokio::test]
async fn deploy_settlement_and_post_batch_emits_event() {
    // ----- setup -----
    let Some(s) = setup().await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };

    // ----- act -----
    // Sanity: initializer recorded the batcher.
    let on_chain_batcher = s.settlement.l1Batcher().call().await.unwrap();
    let last_idx_before = s.settlement.lastBatchIndex().call().await.unwrap();
    let blob_hashes = vec![B256::repeat_byte(0xA1), B256::repeat_byte(0xA2)];
    let receipt = s.post_batch(0, blob_hashes.clone(), 100, 105).await;
    let last_idx_after = s.settlement.lastBatchIndex().call().await.unwrap();
    // Replay protection: same prev index rejected.
    let replay = s
        .settlement
        .postBatch(0, blob_hashes, 106, 110, B256::repeat_byte(0x4C))
        .from(BATCHER)
        .send()
        .await;
    let replay_receipt = match replay {
        Ok(p) => Some(p.get_receipt().await.unwrap()),
        Err(_) => None,
    };

    // ----- assert -----
    assert_eq!(on_chain_batcher, BATCHER);
    assert_eq!(last_idx_before, 0);
    assert!(receipt.status(), "postBatch must succeed");
    assert_eq!(last_idx_after, 1);
    s.assert_batch_posted_index(&receipt, 1);
    if let Some(r) = replay_receipt {
        assert!(!r.status(), "stale prev_index must revert");
    }
}

/// A `ClosedBlock` with no transactions, the smallest legal batch. Dense
/// coverage posts empty blocks too.
fn empty_block(block_number: u64, l2_timestamp: u64) -> kardamom_batcher::batch::ClosedBlock {
    kardamom_batcher::batch::ClosedBlock {
        block_number,
        l2_timestamp,
        end_tx_idx: kardamom_types::BPosition::from_index(0),
        remote_epochs: vec![],
        txs: vec![],
    }
}

/// Deploy a `KardamomL2Settlement` with a real wallet-signing key (not an
/// impersonated account) as its `l1Batcher` — blob transactions need a
/// real signer.
async fn setup_wallet_and_settlement() -> Option<Scenario<impl Provider + Clone>> {
    use alloy_network::EthereumWallet;
    use alloy_signer_local::PrivateKeySigner;

    let rig = AnvilRig::spawn(
        alloy_node_bindings::Anvil::new(),
        &[(DEV_OWNER, Funding::FundAndImpersonate)],
    )
    .await?;
    let batcher_signer: PrivateKeySigner = rig.anvil.keys()[2].clone().into();
    let batcher_addr = batcher_signer.address();
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(batcher_signer))
        .connect_http(rig.anvil.endpoint_url());
    let deployer = Deployer::new(rig.provider, DEV_OWNER);
    let settlement_addr = deployer
        .deploy_settlement(DEV_OWNER, L2_CHAIN_ID, batcher_addr)
        .await;
    let settlement = IKardamomL2Settlement::new(settlement_addr, provider.clone());
    Some(Scenario {
        _anvil: rig.anvil,
        provider,
        settlement,
        settlement_addr,
    })
}

/// The live-sender loop against real anvil: a confirmed post advances the
/// CAS and persists the cursor. A foreign advance of `lastBatchIndex` is a
/// fail-stop, not a silent retry.
#[tokio::test]
async fn live_sender_confirms_and_rejects_foreign_writer() {
    use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
    use kardamom_batcher::da_store::FsBlobStore;
    use kardamom_batcher::live::{BatchCursor, LiveSender};

    // ----- setup -----
    let Some(s) = setup_wallet_and_settlement().await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let cursor_path = dir.path().join("cursor.json");
    let da_store = FsBlobStore::open(dir.path().join("da")).unwrap();
    let batch1 = pack_blocks(&BatcherConfig::default(), &[empty_block(1, 7)]).unwrap();
    let mut sender = LiveSender::new(
        s.provider.clone(),
        s.settlement_addr,
        da_store,
        0,
        2,
        cursor_path.clone(),
    );

    // ----- act -----
    let cursor1 = BatchCursor {
        next_index: 0,
        next_block: 2,
        last_batch_index: 0,
    };
    sender
        .post_confirmed(&batch1, cursor1)
        .await
        .expect("first post must confirm");
    let index_after_first_post = s.settlement.lastBatchIndex().call().await.unwrap();
    let stored_cursor = BatchCursor::load(&cursor_path)
        .unwrap()
        .expect("cursor written");

    // A foreign writer advances the CAS behind the sender's back...
    let foreign_receipt = s
        .settlement
        .postBatch(
            1,
            vec![B256::repeat_byte(0xEE)],
            2,
            9,
            B256::repeat_byte(0x4C),
        )
        .send()
        .await
        .expect("foreign post sends")
        .get_receipt()
        .await
        .unwrap();

    // So the sender's next post must fail-stop. Reconcile sees index 2
    // covering block 9, which is not this batch. It must not retry into a
    // fork.
    let batch2 = pack_blocks(&BatcherConfig::default(), &[empty_block(2, 8)]).unwrap();
    let cursor2 = BatchCursor {
        next_index: 0,
        next_block: 3,
        last_batch_index: 0,
    };
    let err = sender
        .post_confirmed(&batch2, cursor2)
        .await
        .expect_err("foreign CAS advance must fail-stop");

    // ----- assert -----
    assert_eq!(index_after_first_post, 1);
    assert_eq!(
        stored_cursor,
        BatchCursor {
            next_index: 0,
            next_block: 2,
            last_batch_index: 1,
        }
    );
    assert!(foreign_receipt.status());
    let msg = format!("{err:#}");
    assert!(
        msg.contains("second batcher"),
        "unexpected error chain: {msg}"
    );
}
