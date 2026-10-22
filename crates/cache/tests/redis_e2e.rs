//! Round trips against a real Redis container: the monotone write rule,
//! the account and receipt reads, and the mirror heads.
//!
//! Run with `cargo test -p kardamom-cache --features docker-e2e -- --ignored`.

#![cfg(feature = "docker-e2e")]

use std::num::NonZeroU32;
use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use kardamom_cache::{
    AccountCache, BatchWrite, CacheReader, LiveAccounts, LiveAccountsConfig, RowsWritten,
};
use kardamom_types::{AccountRow, BPosition, Receipt};
use testcontainers::{ContainerAsync, GenericImage};

/// A Redis container and a client on it.
async fn redis() -> (ContainerAsync<GenericImage>, AccountCache) {
    let (container, cfg) = kardamom_cache::testing::redis().await;
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

/// The mirror writes a group of batches in one pipeline. Each row keeps
/// the end position of its own batch, so a group whose copies come out of
/// order still leaves the newest row.
#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn a_group_of_batches_applies_each_row_at_its_own_position() {
    let (_container, cache) = redis().await;
    let newer = [row(0xaa, 7, 70)];
    let older = [row(0xaa, 3, 30), row(0xbb, 1, 10)];
    let written = cache
        .write_batches(&[
            BatchWrite {
                end: BPosition::from_index(20),
                rows: &newer,
                receipts: &[],
            },
            BatchWrite {
                end: BPosition::from_index(12),
                rows: &older,
                receipts: &[],
            },
        ])
        .await
        .unwrap();
    assert_eq!((written.applied, written.discarded), (2, 1));
    let a = cache
        .account(Address::repeat_byte(0xaa))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((a.nonce, a.tx_idx), (7, 20));
    let b = cache
        .account(Address::repeat_byte(0xbb))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((b.nonce, b.tx_idx), (1, 12));
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

/// A skip that used no nonce, such as a copy of a transaction that the
/// sealer ordered again, never replaces the receipt that used the nonce,
/// in either order.
#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn a_skip_that_used_no_nonce_never_replaces_the_receipt_of_its_nonce() {
    let (_c, cache) = redis().await;
    let sender = Address::repeat_byte(0x12);
    let executed = Receipt {
        tx_hash: B256::repeat_byte(0x51),
        from: sender,
        nonce: 4,
        status: true,
        gas_used: 21_000,
        ..Receipt::default()
    };
    let skip = Receipt {
        tx_hash: B256::repeat_byte(0x51),
        from: sender,
        nonce: 4,
        skip_reason: Some(kardamom_types::SkipReason::NonceTooLow),
        write_set_hash: kardamom_types::EMPTY_WRITE_SET_HASH,
        ..Receipt::default()
    };
    cache
        .write_receipts(std::slice::from_ref(&executed))
        .await
        .unwrap();
    cache
        .write_receipts(std::slice::from_ref(&skip))
        .await
        .unwrap();
    assert_eq!(
        cache.receipt(sender, 4).await.unwrap(),
        Some(executed.clone())
    );

    let other = Address::repeat_byte(0x13);
    let skip_first = Receipt {
        from: other,
        ..skip
    };
    let executed_later = Receipt {
        from: other,
        ..executed
    };
    cache
        .write_receipts(std::slice::from_ref(&skip_first))
        .await
        .unwrap();
    assert_eq!(cache.receipt(other, 4).await.unwrap(), Some(skip_first));
    cache
        .write_receipts(std::slice::from_ref(&executed_later))
        .await
        .unwrap();
    assert_eq!(cache.receipt(other, 4).await.unwrap(), Some(executed_later));
}

/// A clear deletes every account and receipt key over several scan
/// pages, and keeps the heads and the pending floors.
#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn a_clear_deletes_the_projection_and_keeps_the_heads() {
    let (_c, cache) = redis().await;
    let rows: Vec<AccountRow> = (0..=255u8).map(|byte| row(byte, 1, 1)).collect();
    cache
        .write_rows(BPosition::from_index(50), &rows)
        .await
        .unwrap();
    let sender = Address::repeat_byte(0x11);
    let receipts: Vec<Receipt> = (0..1_500u64)
        .map(|nonce| Receipt {
            from: sender,
            nonce,
            ..Receipt::default()
        })
        .collect();
    cache.write_receipts(&receipts).await.unwrap();
    cache.set_head(0, BPosition::from_index(50)).await.unwrap();
    let pending = Address::repeat_byte(0x22);
    cache.set_pending(pending, 8).await.unwrap();

    assert_eq!(cache.clear_projection().await.unwrap(), 256 + 1_500);

    assert_eq!(cache.account(Address::repeat_byte(7)).await.unwrap(), None);
    assert_eq!(cache.receipt(sender, 1_499).await.unwrap(), None);
    assert_eq!(cache.heads(&[0]).await.unwrap(), vec![Some(50)]);
    assert_eq!(cache.pending(pending).await.unwrap(), Some(8));
    let lower = cache
        .write_rows(BPosition::from_index(10), &[row(7, 3, 3)])
        .await
        .unwrap();
    assert_eq!(lower.applied, 1, "a lower position applies after a clear");
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

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn reader_connects_lazily_and_gates_freshness_on_the_head() {
    let (_c, cfg) = kardamom_cache::testing::redis().await;
    let cache = AccountCache::connect(&cfg).await.unwrap();
    let (live, mut writer) = LiveAccounts::new(&LiveAccountsConfig::default());
    let reader = CacheReader::spawn(&cfg, NonZeroU32::new(3).unwrap(), live.clone());
    // The reader is usable at once and answers nothing until connected.
    let a = Address::repeat_byte(0xaa);
    let mut waited = 0;
    while !reader.connected() && waited < 50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        waited += 1;
    }
    assert!(reader.connected(), "the reader connects in the background");

    cache
        .write_rows(BPosition::from_index(10), &[row(0xaa, 4, 9)])
        .await
        .unwrap();
    let view = reader.account(a).await.expect("hit");
    assert_eq!((view.nonce, view.tx_idx), (4, 10));
    assert!(reader.account(Address::repeat_byte(0xbb)).await.is_none());

    // No live head: not fresh. A head within the bound, after the local
    // layer saw a frame: fresh. The local layer far beyond the head:
    // stale.
    assert!(!reader.fresh());
    writer.apply(BPosition::from_index(10), &[]);
    cache.set_head(1, BPosition::from_index(10)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(reader.head(), Some(10));
    assert!(reader.fresh());
    writer.apply(BPosition::from_index(10 + 20_000), &[]);
    assert!(!reader.fresh(), "the local head is beyond max_stale_txs");

    let receipt = Receipt {
        tx_idx: BPosition::from_index(10),
        tx_hash: B256::repeat_byte(0x11),
        from: a,
        nonce: 4,
        ..Receipt::default()
    };
    cache
        .write_receipts(std::slice::from_ref(&receipt))
        .await
        .unwrap();
    assert_eq!(reader.receipt(a, 4).await, Some(receipt));
    assert!(reader.receipt(a, 5).await.is_none());
}
