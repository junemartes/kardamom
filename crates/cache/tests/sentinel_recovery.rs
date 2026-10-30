//! Real Redis and Sentinel processes; no sequencer, executor or deployment stack.

#![cfg(feature = "docker-e2e")]

use kardamom_cache::testing::sentinel::{Fault, Fixture};

#[tokio::test]
#[ignore = "requires Linux Docker bridge routing and five disposable containers"]
async fn a_frozen_primary_promotes_a_replica_and_the_same_cache_client_recovers() {
    Fixture::new().await.recover_after(Fault::Freeze).await;
}

#[tokio::test]
#[ignore = "requires Linux Docker bridge routing and five disposable containers"]
async fn a_killed_primary_promotes_a_replica_and_the_same_cache_client_recovers() {
    Fixture::new().await.recover_after(Fault::Kill).await;
}

#[tokio::test]
#[ignore = "requires Linux Docker bridge routing and five disposable containers"]
async fn replica_loss_reports_zero_acknowledgements_without_stopping_primary_writes() {
    let fixture = Fixture::new().await;
    fixture.replica.stop().await.unwrap();
    fixture.write(2, 20).await;
    assert_eq!(fixture.cache.wait_replica().await.unwrap(), 0);
    fixture.assert_row(2, 20).await;
}

/// redis-total-loss-recover, the Sentinel part: every Redis process and
/// every Sentinel dies at once, and no process keeps data on disk. While
/// the group is down, the cache client fails. The group returns empty,
/// the Sentinels name a primary that answers as one, and the same cache
/// client writes and reads again.
#[tokio::test]
#[ignore = "requires Linux Docker bridge routing and five disposable containers"]
async fn a_total_loss_returns_an_empty_group_and_the_same_cache_client_recovers() {
    let fixture = Fixture::new().await;
    fixture.assert_row(1, 10).await;
    fixture.lose_everything().await;
    assert!(
        fixture
            .cache
            .account(Fixture::row(1).address)
            .await
            .is_err(),
        "the cache client must fail while the group is down"
    );
    fixture.start_everything().await;
    let lost = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut read = fixture.cache.account(Fixture::row(1).address).await;
        while read.is_err() {
            read = fixture.read_again(Fixture::row(1).address).await;
        }
        read.unwrap()
    })
    .await
    .expect("the same cache client reads the returned group");
    assert_eq!(
        lost, None,
        "the returned group holds no key from before the loss"
    );
    fixture.write(2, 20).await;
    fixture.assert_row(2, 20).await;
}
