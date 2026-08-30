//! Anvil end-to-end: deploy KardamomL2Settlement through the kardamom
//! factory, call `postBatch` from the batcher EOA, and check the on-chain
//! `BatchPosted` event was emitted with the correct fields.
//!
//! Skips gracefully if anvil is not available (the same convention as the
//! deployer's `deploy_e2e.rs`).

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, B256, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::SolEvent;

use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_arg};

const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
const BATCHER: Address = address!("0000000000000000000000000000000000000BA7");
const L2_CHAIN_ID: u64 = 42;

async fn setup_anvil_with_erc7955() -> Option<(
    alloy_node_bindings::AnvilInstance,
    impl alloy_provider::Provider + Clone,
)> {
    let anvil = Anvil::new().try_spawn().ok()?;
    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(anvil.endpoint_url());

    let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
    let _: serde_json::Value = provider
        .raw_request("anvil_setCode".into(), (ERC7955_FACTORY, bytes_hex))
        .await
        .ok()?;
    let _: serde_json::Value = provider
        .raw_request(
            "anvil_setBalance".into(),
            (DEV_OWNER, U256::from(1_000_000_000_000_000_000_000u128)),
        )
        .await
        .ok()?;
    let _: serde_json::Value = provider
        .raw_request(
            "anvil_setBalance".into(),
            (BATCHER, U256::from(1_000_000_000_000_000_000_000u128)),
        )
        .await
        .ok()?;
    let _: serde_json::Value = provider
        .raw_request("anvil_impersonateAccount".into(), (DEV_OWNER,))
        .await
        .ok()?;
    let _: serde_json::Value = provider
        .raw_request("anvil_impersonateAccount".into(), (BATCHER,))
        .await
        .ok()?;

    Some((anvil, provider))
}

#[tokio::test]
async fn deploy_settlement_and_post_batch_emits_event() {
    let (anvil, provider) = match setup_anvil_with_erc7955().await {
        Some(p) => p,
        None => {
            eprintln!("SKIP: anvil unavailable");
            return;
        }
    };
    let _ = anvil; // keep alive

    let deployer = Deployer::new(provider.clone(), DEV_OWNER);
    deployer.ensure_factory(DEV_OWNER).await.unwrap();

    deployer
        .apply(
            &[Op::Deploy {
                l2_chain_id: L2_CHAIN_ID,
                id: ContractId::KardamomL2Settlement,
                init_args: encode_address_arg(BATCHER),
            }],
            DEV_OWNER,
        )
        .await
        .expect("deploy KardamomL2Settlement");

    let entries = deployer.addresses(Some(L2_CHAIN_ID)).await.unwrap();
    assert_eq!(entries.len(), 1);
    let settlement_addr = entries[0].proxy;
    assert_ne!(settlement_addr, Address::ZERO);

    // Sanity: initializer recorded the batcher.
    let settlement = IKardamomL2Settlement::new(settlement_addr, provider.clone());
    let on_chain_batcher = settlement.l1Batcher().call().await.unwrap();
    assert_eq!(on_chain_batcher, BATCHER);
    let last_idx_before = settlement.lastBatchIndex().call().await.unwrap();
    assert_eq!(last_idx_before, 0);

    // Post a batch. The blob bytes themselves are not sent to anvil in
    // this test; this only checks the calldata path. The contract records
    // the versioned hashes and emits the event.
    let blob_hashes = vec![B256::repeat_byte(0xA1), B256::repeat_byte(0xA2)];
    let receipt = settlement
        .postBatch(0, blob_hashes.clone(), 100, 105, B256::repeat_byte(0x4C))
        .from(BATCHER)
        .send()
        .await
        .expect("send postBatch tx")
        .get_receipt()
        .await
        .expect("await receipt");
    assert!(receipt.status(), "postBatch must succeed");

    // Index advanced.
    let last_idx_after = settlement.lastBatchIndex().call().await.unwrap();
    assert_eq!(last_idx_after, 1);

    // The `BatchPosted` event was emitted with the expected fields.
    let logs = receipt.logs();
    let topic0 = IKardamomL2Settlement::BatchPosted::SIGNATURE_HASH;
    let mut found = false;
    for log in logs {
        if log.address() == settlement_addr && log.topic0().copied().unwrap_or(B256::ZERO) == topic0
        {
            found = true;
            // batchIndex is indexed (topics[1]); other fields are in data.
            let topics = log.topics();
            assert_eq!(topics.len(), 2);
            // indexed uint64 left-padded
            let idx_bytes = topics[1].as_slice();
            assert_eq!(&idx_bytes[..24], &[0u8; 24]);
            let idx = u64::from_be_bytes(idx_bytes[24..32].try_into().unwrap());
            assert_eq!(idx, 1);
        }
    }
    assert!(found, "BatchPosted event must be emitted");

    // Replay protection: same prev index rejected.
    let res = settlement
        .postBatch(0, blob_hashes, 106, 110, B256::repeat_byte(0x4C))
        .from(BATCHER)
        .send()
        .await;
    let receipt = match res {
        Ok(p) => Some(p.get_receipt().await.unwrap()),
        Err(_) => None,
    };
    if let Some(r) = receipt {
        assert!(!r.status(), "stale prev_index must revert");
    }
}

/// The live-sender loop against real anvil: a confirmed post advances the
/// CAS and persists the cursor. A foreign advance of `lastBatchIndex` is a
/// fail-stop, not a silent retry.
#[tokio::test]
async fn live_sender_confirms_and_rejects_foreign_writer() {
    use alloy_network::EthereumWallet;
    use kardamom_batcher::batch::ClosedBlock;
    use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
    use kardamom_batcher::da_store::FsBlobStore;
    use kardamom_batcher::live::{BatchCursor, LiveSender};
    use kardamom_types::BPosition;

    let anvil = match Anvil::new().try_spawn() {
        Ok(a) => a,
        Err(_) => {
            eprintln!("SKIP: anvil unavailable");
            return;
        }
    };
    // Blob txs need a real signer. Use a funded anvil dev account as the
    // batcher EOA, and make it the settlement's `l1Batcher`.
    let batcher_signer: alloy_signer_local::PrivateKeySigner = anvil.keys()[2].clone().into();
    let batcher_addr = batcher_signer.address();
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(batcher_signer))
        .connect_http(anvil.endpoint_url());

    // Deploy the factory and settlement with a plain provider. The
    // wallet-filled provider would try to locally sign the impersonated
    // DEV_OWNER txs.
    let deploy_provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(anvil.endpoint_url());
    let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
    let _: serde_json::Value = deploy_provider
        .raw_request("anvil_setCode".into(), (ERC7955_FACTORY, bytes_hex))
        .await
        .unwrap();
    let _: serde_json::Value = deploy_provider
        .raw_request(
            "anvil_setBalance".into(),
            (DEV_OWNER, U256::from(1_000_000_000_000_000_000_000u128)),
        )
        .await
        .unwrap();
    let _: serde_json::Value = deploy_provider
        .raw_request("anvil_impersonateAccount".into(), (DEV_OWNER,))
        .await
        .unwrap();
    let deployer = Deployer::new(deploy_provider.clone(), DEV_OWNER);
    deployer.ensure_factory(DEV_OWNER).await.unwrap();
    deployer
        .apply(
            &[Op::Deploy {
                l2_chain_id: L2_CHAIN_ID,
                id: ContractId::KardamomL2Settlement,
                init_args: encode_address_arg(batcher_addr),
            }],
            DEV_OWNER,
        )
        .await
        .unwrap();
    let settlement_addr = deployer.addresses(Some(L2_CHAIN_ID)).await.unwrap()[0].proxy;

    let dir = tempfile::tempdir().unwrap();
    let cursor_path = dir.path().join("cursor.json");
    let da_store = FsBlobStore::open(dir.path().join("da")).unwrap();

    // One empty block: the smallest legal batch. Dense coverage posts
    // empty blocks too.
    let block1 = ClosedBlock {
        block_number: 1,
        l2_timestamp: 7,
        end_tx_idx: BPosition::from_index(0),
        txs: vec![],
    };
    let batch1 = pack_blocks(&BatcherConfig::default(), &[block1]).unwrap();

    let mut sender = LiveSender::new(
        provider.clone(),
        settlement_addr,
        da_store,
        0,
        2,
        cursor_path.clone(),
    );
    let cursor1 = BatchCursor {
        next_index: 0,
        next_block: 2,
        last_batch_index: 0,
    };
    sender
        .post_confirmed(&batch1, cursor1)
        .await
        .expect("first post must confirm");

    let settlement = IKardamomL2Settlement::new(settlement_addr, provider.clone());
    assert_eq!(settlement.lastBatchIndex().call().await.unwrap(), 1);
    let stored = BatchCursor::load(&cursor_path)
        .unwrap()
        .expect("cursor written");
    assert_eq!(
        stored,
        BatchCursor {
            next_index: 0,
            next_block: 2,
            last_batch_index: 1,
        }
    );

    // A foreign writer advances the CAS behind the sender's back...
    let receipt = settlement
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
    assert!(receipt.status());

    // So the sender's next post must fail-stop. Reconcile sees index 2
    // covering block 9, which is not this batch. It must not retry into a
    // fork.
    let block2 = ClosedBlock {
        block_number: 2,
        l2_timestamp: 8,
        end_tx_idx: BPosition::from_index(0),
        txs: vec![],
    };
    let batch2 = pack_blocks(&BatcherConfig::default(), &[block2]).unwrap();
    let cursor2 = BatchCursor {
        next_index: 0,
        next_block: 3,
        last_batch_index: 0,
    };
    let err = sender
        .post_confirmed(&batch2, cursor2)
        .await
        .expect_err("foreign CAS advance must fail-stop");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("second batcher"),
        "unexpected error chain: {msg}"
    );
}
