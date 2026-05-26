//! Anvil end-to-end: deploy KardamomL2Settlement via the kardamom factory,
//! then call `postBatch` from the batcher EOA and assert the on-chain
//! `BatchPosted` event was emitted with the correct fields.
//!
//! Skips gracefully if anvil isn't available (same convention as the
//! deployer's `deploy_e2e.rs`).

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, B256, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::SolEvent;

use batcher::settlement::IKardamomL2Settlement;
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

    // Post a batch. The blob bytes themselves don't get sent to anvil in this
    // test — we just verify the calldata path. The contract records the
    // versioned hashes and emits the event.
    let blob_hashes = vec![B256::repeat_byte(0xA1), B256::repeat_byte(0xA2)];
    let receipt = settlement
        .postBatch(0, blob_hashes.clone(), 100, 105)
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

    // The `BatchPosted` event was emitted with our fields.
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
        .postBatch(0, blob_hashes, 106, 110)
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
