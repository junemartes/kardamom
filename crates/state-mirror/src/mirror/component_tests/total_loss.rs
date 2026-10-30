//! redis-total-loss-recover on a real Sentinel group: a primary, a
//! replica and three Sentinels, all killed at once, with no data on disk.

use std::time::{Duration, Instant};

use super::support::{ACCOUNTS, Fixture};
use kardamom_cache::testing::sentinel::Fixture as Group;

/// The outage outlasts the mirror's bound for a lossless write retry, as
/// the deployed total loss does, so the recovery takes the rebuild path.
const OUTAGE: Duration = Duration::from_secs(11);
/// The recovery bound after the group returns: Sentinel discovery, the
/// landed retry, and a rebuild of every account.
const RECOVERY: Duration = Duration::from_secs(30);

/// The mirror keeps running through the loss. While the group is down,
/// a reader fails and the mirror's write retries mark a rebuild as due.
/// The group returns empty, the Sentinels name a primary, and the same
/// mirror lands its retried write, rebuilds every account from its
/// checkpoint, advances its head past the outage, and the same reader
/// reads exact values again.
#[tokio::test]
#[ignore = "requires Linux Docker bridge routing and five disposable containers"]
async fn a_total_loss_rebuilds_every_account_and_the_readers_recover() {
    let group = Group::new().await;
    let admin = Group::raw(&group.primary, 6379).await;
    let fixture = Fixture::on(group.cfg.clone(), admin).await;
    let (live, _writer) =
        kardamom_cache::LiveAccounts::new(&kardamom_cache::LiveAccountsConfig::default());
    let reader = kardamom_cache::CacheReader::spawn(&group.cfg, std::num::NonZeroU32::MIN, live);
    let actor = fixture.start(0, false).await;
    actor.send(&Fixture::batch(0, 1, 20));
    fixture.head(0, 20).await;
    fixture.assert_checkpoint_suffix(0).await;

    group.lose_everything().await;
    let dark = Instant::now();
    assert!(
        fixture
            .cache
            .account(Fixture::row(5, 1).address)
            .await
            .is_err(),
        "a reader must fail while the group is down"
    );
    let asked = Instant::now();
    assert_eq!(
        reader.read_account(Fixture::row(5, 1).address).await,
        Err(kardamom_cache::Degraded),
        "the production reader degrades instead of answering"
    );
    assert!(
        asked.elapsed() < Duration::from_secs(5),
        "a degraded read must not stall"
    );
    actor.send(&Fixture::batch(1, 2, 30));
    let head = fixture.head_file(0);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !head.rebuild_pending().unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a failed write must mark the rebuild before it retries");
    tokio::time::sleep(OUTAGE.saturating_sub(dark.elapsed())).await;

    group.start_everything().await;
    // The executors keep writing checkpoints; the rebuild needs one that
    // reaches the first live batch after the loss.
    fixture.checkpoint_at(2, 41, &[Fixture::row(1, 2), Fixture::row(2, 3)]);
    actor.send(&Fixture::batch(2, 3, 40));
    let back = Instant::now();
    fixture.head_within(0, 40, RECOVERY).await;
    eprintln!(
        "the mirror recovered {:?} after the group returned",
        back.elapsed()
    );
    fixture.assert_row(1, 2, 40).await;
    fixture.assert_row(2, 3, 40).await;
    fixture.assert_rows_at(3, 40).await;
    assert!(!head.rebuild_pending().unwrap(), "the rebuild finished");
    let address = Fixture::row(5, 1).address;
    let recovered = tokio::time::timeout(RECOVERY, async {
        let mut read = reader.read_account(address).await;
        while !matches!(read, Ok(Some(_))) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            read = reader.read_account(address).await;
        }
        read.unwrap().unwrap()
    })
    .await
    .expect("the production reader reads the rebuilt group again");
    assert_eq!(
        (recovered.nonce, recovered.tx_idx),
        (1, 40),
        "the reader sees the rebuilt row"
    );
    assert_eq!(ACCOUNTS, 1_025);
    actor.close().await.unwrap();
}

/// The counters of this test process, through one global Prometheus
/// recorder. The readers and mirrors run as tasks, so a thread-local
/// recorder would miss them.
fn counters() -> &'static metrics_exporter_prometheus::PrometheusHandle {
    static HANDLE: std::sync::OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
        std::sync::OnceLock::new();
    HANDLE.get_or_init(|| {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .install_recorder()
            .expect("the only global recorder of this test binary")
    })
}

/// The sum of every series of `name` whose labels hold `label`.
fn counter(name: &str, label: &str) -> f64 {
    counters()
        .render()
        .lines()
        .filter(|l| l.starts_with(name) && l.contains(label))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// Kill the mirrors: the drop of each actor aborts its task.
fn kill(_actors: [super::support::Actor; 3]) {}

/// One ingress reader: the production `CacheReader` over the group.
struct Reader(kardamom_cache::CacheReader);

impl Reader {
    fn spawn(group: &Group) -> Self {
        let (live, _writer) =
            kardamom_cache::LiveAccounts::new(&kardamom_cache::LiveAccountsConfig::default());
        let mirrors = std::num::NonZeroU32::new(3).unwrap();
        Self(kardamom_cache::CacheReader::spawn(
            &group.cfg, mirrors, live,
        ))
    }

    /// A read during the loss: a degraded answer, counted, with no stall.
    async fn assert_degraded(&self, address: alloy_primitives::Address) {
        let before = counter(kardamom_cache::metrics::DEGRADED_TOTAL, "");
        let asked = Instant::now();
        assert_eq!(
            self.0.read_account(address).await,
            Err(kardamom_cache::Degraded)
        );
        assert!(
            asked.elapsed() < Duration::from_secs(5),
            "a degraded read stalls"
        );
        assert!(
            counter(kardamom_cache::metrics::DEGRADED_TOTAL, "") > before,
            "not counted"
        );
    }

    /// A cold read after the return: a Redis hit with the exact value,
    /// counted as a lookup, and no degraded read.
    async fn assert_recovered(&self, address: alloy_primitives::Address, nonce: u64) {
        let degraded = counter(kardamom_cache::metrics::DEGRADED_TOTAL, "");
        let hits = counter(kardamom_cache::metrics::LOOKUPS_TOTAL, "outcome=\"hit\"");
        let view = self
            .0
            .read_account(address)
            .await
            .expect("a Redis answer")
            .expect("a row");
        assert_eq!(view.nonce, nonce);
        assert!(counter(kardamom_cache::metrics::LOOKUPS_TOTAL, "outcome=\"hit\"") > hits);
        assert_eq!(
            counter(kardamom_cache::metrics::DEGRADED_TOTAL, ""),
            degraded,
            "a degraded read"
        );
    }

    async fn wait_connected(&self) {
        tokio::time::timeout(RECOVERY, async {
            while !self.0.connected() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the reader connects to the returned group");
    }
}

/// read-path-loss-recover: the executors, Redis and all three mirrors
/// die. Both ingress readers degrade, counted, with no stall. Redis
/// returns empty first; the three mirrors return and wait for a
/// checkpoint; the executors return last with a newer checkpoint and a
/// live batch. Every mirror rebuilds every account and advances, and
/// each reader reads Redis again with no degraded read.
#[tokio::test]
#[ignore = "requires Linux Docker bridge routing and five disposable containers"]
async fn the_whole_read_path_returns_in_reverse_order_and_every_mirror_rebuilds() {
    counters();
    let group = Group::new().await;
    let admin = Group::raw(&group.primary, 6379).await;
    let fixture = Fixture::on(group.cfg.clone(), admin).await.with_mirrors(3);
    let readers = [Reader::spawn(&group), Reader::spawn(&group)];
    let actors = [
        fixture.start(0, false).await,
        fixture.start(1, false).await,
        fixture.start(2, false).await,
    ];
    for actor in &actors {
        actor.send(&Fixture::batch(0, 1, 20));
    }
    for id in 0..3 {
        fixture.head(id, 20).await;
    }
    kill(actors);
    group.lose_everything().await;
    let address = Fixture::row(5, 1).address;
    for reader in &readers {
        reader.assert_degraded(address).await;
    }

    group.start_everything().await;
    let rebuilds = counter("kardamom_state_mirror_rebuilds_total", "");
    let actors = [
        fixture.start(0, false).await,
        fixture.start(1, false).await,
        fixture.start(2, false).await,
    ];
    tokio::time::sleep(Duration::from_secs(2)).await;
    fixture.checkpoint_at(2, 41, &[Fixture::row(1, 2)]);
    for actor in &actors {
        actor.send(&Fixture::batch(1, 2, 40));
    }
    for id in 0..3 {
        fixture.head_within(id, 40, RECOVERY).await;
    }
    assert!(
        counter("kardamom_state_mirror_rebuilds_total", "") >= rebuilds + 3.0,
        "every mirror rebuilt"
    );
    fixture.assert_row(1, 2, 40).await;
    fixture.assert_rows_at(2, 40).await;
    for reader in &readers {
        reader.wait_connected().await;
        reader.assert_recovered(address, 1).await;
    }
    for actor in actors {
        actor.close().await.unwrap();
    }
}
