//! `control-plane-loss-recover`, at the discovery boundary. Running
//! workers lose their metadata service, and must keep moving data on the
//! connections they hold. The test runs the production discovery adapter
//! (the Consul client, the registration heartbeat, the catalog watch and
//! the stream plane) against a Consul-like agent, and a real Aeron media
//! driver for the data path:
//!
//! 1. An ingress-like publisher and an executor-like subscriber of the
//!    lane-0 `tx_data` stream find each other through the agent, and data
//!    flows.
//! 2. The agent partitions from both for longer than the check TTL and
//!    the deregistration window: the publisher's check expires and the
//!    agent deletes its record, as the chaos hold does on Consul.
//! 3. Through the partition, the subscriber keeps receiving every
//!    envelope in order, with no gap above the client bound.
//! 4. After the partition, the publisher registers again with a passing
//!    check, a new subscriber finds it through the agent, and the old
//!    subscriber still holds every envelope exactly once.
//!
//! Nomad placement of the workers after the partition is a cluster-only
//! check. Gated on the `docker-e2e` feature and on Docker.

#![cfg(feature = "docker-e2e")]

mod common;
#[path = "control_plane_loss/consul.rs"]
mod consul;

use std::net::Ipv4Addr;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use consul::ConsulAgent;
use kardamom_log::aeron_live::{AeronRuntime, TxDataPublisherHandle, TxDataSubscriberHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::{Instance, StreamPlane, catalog_from_config};
use kardamom_log::testing::AeronTestCluster;

/// The check TTL of the test's catalog records.
const TTL: Duration = Duration::from_secs(2);
/// The agent deletes a record whose check stays critical this long.
const DEREGISTER_AFTER: Duration = Duration::from_secs(5);
/// The partition: longer than the TTL and the deletion window, as the
/// chaos hold of 75 s is longer than the 10 s TTL and the 60 s window.
const HOLD: Duration = Duration::from_secs(9);
/// The longest pause between two delivered envelopes, during the
/// partition too.
const DELIVERY_BOUND: Duration = Duration::from_secs(1);
/// The bound for the return of the registration and the discovery.
const RECOVERY_BOUND: Duration = Duration::from_secs(15);
/// The pause between two published envelopes.
const PACE: Duration = Duration::from_millis(20);

fn config(base: &LogConfig, agent: &ConsulAgent) -> LogConfig {
    let mut cfg = base.clone();
    cfg.discovery.enabled = true;
    cfg.discovery.consul_http_addr = format!("http://{}", agent.addr);
    cfg.discovery.cluster_id = "control-plane".into();
    cfg.discovery.chain_id = 7;
    cfg.discovery.check_ttl_ms = millis(TTL);
    cfg.discovery.deregister_after_ms = millis(DEREGISTER_AFTER);
    cfg.discovery.blocking_wait_ms = NonZeroU64::new(1_000).unwrap();
    cfg.discovery.backoff_min_ms = NonZeroU64::new(100).unwrap();
    cfg.discovery.backoff_max_ms = NonZeroU64::new(1_000).unwrap();
    cfg
}

fn millis(d: Duration) -> NonZeroU64 {
    NonZeroU64::new(u64::try_from(d.as_millis()).unwrap()).unwrap()
}

/// One worker: a stream plane on the production Consul client.
fn plane(cfg: &LogConfig, label: &str) -> StreamPlane {
    StreamPlane::with_catalog(
        cfg,
        label,
        catalog_from_config(&cfg.discovery).unwrap(),
        Instance {
            id: format!("alloc-{label}"),
        },
        Ipv4Addr::LOCALHOST,
    )
}

/// The ids that one subscriber received, with the arrival time of each.
struct Received {
    ids: Vec<(u64, Instant)>,
}

impl Received {
    /// Drain `sub` for `window`.
    async fn drain(sub: &mut TxDataSubscriberHandle, window: Duration, into: &mut Self) {
        let end = Instant::now() + window;
        while let Some(left) = end.checked_duration_since(Instant::now()) {
            into.take(tokio::time::timeout(left, sub.recv()).await);
        }
    }

    fn take(
        &mut self,
        got: Result<
            Option<(kardamom_types::TxDataLoc, kardamom_types::TxEnvelope)>,
            tokio::time::error::Elapsed,
        >,
    ) {
        if let Ok(Some((_, env))) = got {
            self.ids.push((env.correlation_id, Instant::now()));
        }
    }

    /// The longest pause between two arrivals in `[from, to]`.
    fn longest_gap(&self, from: Instant, to: Instant) -> Duration {
        let times: Vec<Instant> = std::iter::once(from)
            .chain(
                self.ids
                    .iter()
                    .map(|(_, at)| *at)
                    .filter(|at| *at >= from && *at <= to),
            )
            .chain(std::iter::once(to))
            .collect();
        times
            .windows(2)
            .map(|w| w[1].duration_since(w[0]))
            .max()
            .unwrap_or(Duration::ZERO)
    }

    /// Every id from the first received one on, once and in order.
    fn assert_exact(&self, what: &str) {
        let ids: Vec<u64> = self.ids.iter().map(|(id, _)| *id).collect();
        let first = ids[0];
        let expected: Vec<u64> = (first..first + ids.len() as u64).collect();
        assert_eq!(
            ids, expected,
            "{what}: lost, doubled or reordered envelopes"
        );
    }
}

/// Publish envelope ids from 0 every [`PACE`], on a thread of its own,
/// until the stop flag. The publication is a dynamic MDC one: an offer
/// before a subscriber joins is not connected and tries again.
fn spawn_publisher(
    publisher: TxDataPublisherHandle,
    next: Arc<AtomicU64>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while next.load(Ordering::Acquire) != u64::MAX {
            publish_next(&publisher, &next);
        }
    })
}

/// Offer the next id once; it advances only when the offer is taken.
fn publish_next(publisher: &TxDataPublisherHandle, next: &AtomicU64) {
    let id = next.load(Ordering::Acquire);
    let taken = publisher.publish(&common::tx_envelope(id, 1, 64)).is_ok();
    // The stop value is never overwritten by a late store.
    let _ = next.compare_exchange(
        id,
        id + u64::from(taken),
        Ordering::AcqRel,
        Ordering::Acquire,
    );
    std::thread::sleep(PACE);
}

async fn wait_for(what: &str, bound: Duration, mut check: impl FnMut() -> bool) -> Duration {
    let started = Instant::now();
    tokio::time::timeout(bound, async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within {bound:?}"));
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run the control-plane-loss component target"]
async fn workers_keep_their_data_path_through_a_metadata_partition_and_reregister() {
    kardamom_log::testing::require_docker().await;
    let started = Instant::now();
    let cluster = AeronTestCluster::single_node().await.unwrap();
    let rt = AeronRuntime::spawn_with_dir(cluster.aeron_dir_host(0)).unwrap();
    let agent = ConsulAgent::start().await;
    let cfg = config(&LogConfig::default(), &agent);

    let mut ingress = plane(&cfg, "ingress-0");
    let publisher = ingress.tx_data_publisher(&rt, 0).await.unwrap();
    let mut executor = plane(&cfg, "executor-0");
    let mut sub = executor.tx_data_subscriber(&rt, 0).unwrap();
    let next = Arc::new(AtomicU64::new(0));
    let pump = spawn_publisher(publisher, next.clone());

    let mut old = Received { ids: Vec::new() };
    Received::drain(&mut sub, Duration::from_secs(5), &mut old).await;
    assert!(
        old.ids.len() > 20,
        "data flows before the partition: {}",
        old.ids.len()
    );

    agent.set_partitioned(true);
    let cut = Instant::now();
    Received::drain(&mut sub, HOLD, &mut old).await;
    let healed = Instant::now();
    let reaped = agent.reaped();
    assert!(
        reaped.iter().any(|id| id.contains("ingress-0")),
        "the partition outlasts the deletion window: the agent deleted the publisher's record ({reaped:?})"
    );
    let gap = old.longest_gap(cut, healed);
    assert!(
        gap <= DELIVERY_BOUND,
        "the held connection keeps delivering through the partition: longest gap {gap:?}"
    );
    agent.set_partitioned(false);

    let returned = wait_for(
        "the publisher registers again with a passing check",
        RECOVERY_BOUND,
        || agent.passing("alloc-ingress-0"),
    )
    .await;
    let mut fresh_plane = plane(&cfg, "executor-1");
    let mut fresh = fresh_plane.tx_data_subscriber(&rt, 0).unwrap();
    let mut late = Received { ids: Vec::new() };
    let found = Instant::now();
    tokio::time::timeout(RECOVERY_BOUND, async {
        while late.ids.len() < 20 {
            Received::drain(&mut fresh, Duration::from_millis(200), &mut late).await;
        }
    })
    .await
    .expect("a new subscriber finds the publisher after the partition");
    let discovered = found.elapsed();
    Received::drain(&mut sub, Duration::from_secs(1), &mut old).await;

    next.store(u64::MAX, Ordering::Release);
    pump.join().unwrap();
    old.assert_exact("the subscriber that held its connection");
    late.assert_exact("the subscriber that joined after the partition");
    println!(
        "CONTROL_PLANE_LOSS delivered={} longest_gap_ms={} reregistered_s={:.2} discovered_s={:.2} seconds={:.1}",
        old.ids.len(),
        gap.as_millis(),
        returned.as_secs_f64(),
        discovered.as_secs_f64(),
        started.elapsed().as_secs_f64()
    );
    fresh_plane.shutdown().await;
    executor.shutdown().await;
    ingress.shutdown().await;
}
