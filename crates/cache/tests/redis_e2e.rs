//! Round trips against a real Redis container: the monotone write rule,
//! the account and receipt reads, and the mirror heads.
//!
//! Run with `cargo test -p kardamom-cache --features docker-e2e -- --ignored`.

#![cfg(feature = "docker-e2e")]

use std::num::NonZeroU64;

use alloy_primitives::{Address, B256, U256};
use kardamom_cache::{AccountCache, CacheConfig, RowsWritten};
use kardamom_types::{AccountRow, BPosition, Receipt};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

/// A Redis container and a client on it.
async fn redis() -> (ContainerAsync<GenericImage>, AccountCache) {
    let container = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379_u16.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .expect("redis container");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    let cfg = CacheConfig {
        url: Some(format!("redis://127.0.0.1:{port}")),
        receipt_ttl_secs: NonZeroU64::new(2).unwrap(),
        ..CacheConfig::default()
    };
    let cache = AccountCache::connect(&cfg).await.expect("connect");
    (container, cache)
}

fn row(byte: u8, nonce: u64, balance: u64) -> AccountRow {
    AccountRow {
        address: Address::repeat_byte(byte),
        nonce,
        balance: U256::from(balance),
    }
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn rows_apply_by_position_and_disagreements_are_counted() {
    let (_c, cache) = redis().await;
    let a = Address::repeat_byte(0xaa);

    let first = cache
        .write_rows(
            BPosition::from_index(10),
            &[row(0xaa, 1, 100), row(0xbb, 5, 1)],
        )
        .await
        .unwrap();
    assert_eq!(first.applied, 2);
    assert_eq!(cache.account(a).await.unwrap().unwrap().nonce, 1);

    // Older: discarded. Equal and same: discarded. Equal and different:
    // disagreed, and the stored value stays.
    let older = cache
        .write_rows(BPosition::from_index(5), &[row(0xaa, 9, 9)])
        .await
        .unwrap();
    assert_eq!(
        older,
        RowsWritten {
            discarded: 1,
            ..RowsWritten::default()
        }
    );
    let same = cache
        .write_rows(BPosition::from_index(10), &[row(0xaa, 1, 100)])
        .await
        .unwrap();
    assert_eq!(same.discarded, 1);
    let forged = cache
        .write_rows(BPosition::from_index(10), &[row(0xaa, 1, 999)])
        .await
        .unwrap();
    assert_eq!(forged.disagreed, 1);
    let view = cache.account(a).await.unwrap().unwrap();
    assert_eq!(
        (view.nonce, view.balance, view.tx_idx),
        (1, U256::from(100u64), 10)
    );

    // Newer: applied, and a position beyond 2^53 still orders correctly.
    let far = BPosition::from_index(1 << 60);
    assert_eq!(
        cache
            .write_rows(far, &[row(0xaa, 2, 50)])
            .await
            .unwrap()
            .applied,
        1
    );
    let back = cache
        .write_rows(BPosition::from_index(1 << 59), &[row(0xaa, 3, 1)])
        .await
        .unwrap();
    assert_eq!(back.discarded, 1);
    assert_eq!(cache.account(a).await.unwrap().unwrap().tx_idx, 1 << 60);

    assert!(
        cache
            .account(Address::repeat_byte(0x99))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn receipts_index_by_sender_and_nonce_and_expire() {
    let (_c, cache) = redis().await;
    let sender = Address::repeat_byte(0x11);
    let receipt = Receipt {
        tx_hash: B256::repeat_byte(0x42),
        from: sender,
        nonce: 3,
        status: true,
        gas_used: 21_000,
        ..Receipt::default()
    };
    let deposit = Receipt {
        tx_type: kardamom_types::TX_TYPE_DEPOSIT,
        from: sender,
        nonce: 0,
        ..Receipt::default()
    };
    cache
        .write_receipts(&[receipt.clone(), deposit])
        .await
        .unwrap();
    assert_eq!(cache.receipt(sender, 3).await.unwrap(), Some(receipt));
    assert_eq!(
        cache.receipt(sender, 0).await.unwrap(),
        None,
        "deposits are not indexed"
    );
    tokio::time::sleep(std::time::Duration::from_millis(2_500)).await;
    assert_eq!(
        cache.receipt(sender, 3).await.unwrap(),
        None,
        "the TTL expired it"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn heads_and_pending_round_trip() {
    let (_c, cache) = redis().await;
    cache.set_head(0, BPosition::from_index(100)).await.unwrap();
    cache.set_head(2, BPosition::from_index(90)).await.unwrap();
    assert_eq!(
        cache.heads(&[0, 1, 2]).await.unwrap(),
        vec![Some(100), None, Some(90)]
    );
    let a = Address::repeat_byte(0x22);
    assert_eq!(cache.pending(a).await.unwrap(), None);
    cache.set_pending(a, 8).await.unwrap();
    assert_eq!(cache.pending(a).await.unwrap(), Some(8));
    // No replica is attached: WAIT reports zero without an error.
    assert_eq!(cache.wait_replica().await.unwrap(), 0);
}
