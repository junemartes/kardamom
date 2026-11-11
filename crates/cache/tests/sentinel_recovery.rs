//! Real Redis and Sentinel processes; no sequencer, executor or deployment stack.

#![cfg(feature = "docker-e2e")]

#[path = "sentinel_recovery/starved.rs"]
mod starved;

use std::num::NonZeroU32;
use std::time::Duration;

use kardamom_cache::testing::sentinel::{Fault, Fixture};
use kardamom_cache::{CacheConfig, CacheReader, LiveAccounts, LiveAccountsConfig};
use starved::{Churn, SentinelName, Stalls};

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

/// The reader part of redis-total-loss-recover, read-path-loss-recover
/// and mirror-backlog-client-restart. The reader gets one Sentinel host
/// name, and that name resolves to two Sentinels. The first one hangs.
/// The primary stalls for 300 ms in every second, below the Sentinel
/// down-after bound, so no failover starts. A command that times out on
/// a live connection does not make a reconnect, and the reader keeps
/// reading in each running phase.
#[tokio::test]
#[ignore = "requires Linux Docker bridge routing and five disposable containers"]
async fn a_reader_keeps_its_live_connection_while_the_primary_stalls_and_a_sentinel_hangs() {
    let recorder = starved::recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let fixture = Fixture::new().await;
    let name = SentinelName::forward(&[fixture.sentinel_at(0), fixture.sentinel_at(1)]).await;
    let cfg = CacheConfig {
        sentinels: vec![name.url()],
        ..fixture.cfg.clone()
    };
    let (live, _writer) = LiveAccounts::new(&LiveAccountsConfig::default());
    let reader = CacheReader::spawn(&cfg, NonZeroU32::MIN, live);
    let address = Fixture::row(1).address;
    tokio::time::timeout(Duration::from_secs(30), async {
        while reader.read_account(address).await.is_err() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the reader must connect through the one Sentinel name");

    Fixture::send(fixture.sentinel_at(0).id(), "STOP").await;
    let before = Churn::of(&handle);
    let stalls = Stalls::start(
        fixture.primary.id().to_owned(),
        Duration::from_millis(300),
        Duration::from_millis(700),
    );
    let reads = Reads::during(&reader, address, Duration::from_secs(12)).await;
    drop_stalls(stalls);
    Fixture::send(fixture.primary.id(), "CONT").await;
    let churn = Churn::of(&handle).since(before);
    Fixture::send(fixture.sentinel_at(0).id(), "CONT").await;

    assert!(
        churn.reconnects <= 1.0,
        "a live connection must not reconnect on each stall: {churn:?}, {reads:?}"
    );
    assert!(
        churn.skipped <= 20.0,
        "the reader must not skip reads on a live connection: {churn:?}, {reads:?}"
    );
    assert!(
        reads.answered.saturating_mul(2) >= reads.total,
        "the reader must answer in each running phase: {reads:?}"
    );
}

/// End the stall cycle. The primary can stay in SIGSTOP; the caller
/// resumes it.
fn drop_stalls(_stalls: Stalls) {}

/// The reads of one window: all of them, and those Redis answered.
#[derive(Debug)]
struct Reads {
    total: u32,
    answered: u32,
}

impl Reads {
    async fn during(
        reader: &CacheReader,
        address: alloy_primitives::Address,
        window: Duration,
    ) -> Self {
        let end = tokio::time::Instant::now() + window;
        let mut reads = Self {
            total: 0,
            answered: 0,
        };
        while tokio::time::Instant::now() < end {
            reads.step(reader, address).await;
        }
        reads
    }

    async fn step(&mut self, reader: &CacheReader, address: alloy_primitives::Address) {
        self.total += 1;
        self.answered += u32::from(reader.read_account(address).await.is_ok());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
