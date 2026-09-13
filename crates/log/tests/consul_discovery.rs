//! Real-Consul checks for the discovery core: a registration becomes a
//! member once its check passes, a watch sees members join and leave, a
//! stopped heartbeat drops a member after its TTL, and a rebuilt index
//! never wedges the watch.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU64;
use std::time::Duration;

use kardamom_log::config::DiscoveryConfig;
use kardamom_log::discovery::{
    Catalog, Membership, MembershipWatch, PUBLISHER_SERVICE, PublisherRecord, Registration,
    RegistrationSpec, Scope, ServiceId, Topic, WatchTiming, catalog_from_config,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const CONSUL_IMAGE: &str = "hashicorp/consul";
const CONSUL_TAG: &str = "1.21";

async fn consul() -> (ContainerAsync<GenericImage>, DiscoveryConfig) {
    let container = GenericImage::new(CONSUL_IMAGE, CONSUL_TAG)
        .with_exposed_port(8500_u16.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Synced node info"))
        .with_cmd(["agent", "-dev", "-client=0.0.0.0"])
        .start()
        .await
        .expect("consul container");
    let port = container
        .get_host_port_ipv4(8500_u16.tcp())
        .await
        .expect("mapped consul port");
    let cfg = DiscoveryConfig {
        enabled: true,
        consul_http_addr: format!("http://127.0.0.1:{port}"),
        cluster_id: "test".into(),
        chain_id: 7,
        advertise_interface: Some("127.0.0.0/8".to_string().try_into().unwrap()),
        blocking_wait_ms: NonZeroU64::new(2_000).unwrap(),
        backoff_min_ms: NonZeroU64::new(100).unwrap(),
        backoff_max_ms: NonZeroU64::new(500).unwrap(),
        check_ttl_ms: NonZeroU64::new(3_000).unwrap(),
        deregister_after_ms: NonZeroU64::new(60_000).unwrap(),
        ..DiscoveryConfig::default()
    };
    (container, cfg)
}

fn scope(cfg: &DiscoveryConfig) -> Scope {
    Scope {
        cluster_id: cfg.cluster_id.clone(),
        chain_id: cfg.chain_id,
    }
}

fn publisher(id: &str, port: u16) -> PublisherRecord {
    PublisherRecord {
        id: ServiceId::new(id.into()),
        control: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), port),
        topic: Topic::TxErrors,
        stream_id: 1015,
        lane: None,
        publisher_id: id.into(),
        session_id: None,
    }
}

fn spec(cfg: &DiscoveryConfig, record: &PublisherRecord) -> RegistrationSpec {
    RegistrationSpec {
        entry: record.entry(&scope(cfg)),
        ttl: cfg.check_ttl(),
        deregister_after: cfg.deregister_after(),
    }
}

fn start_watch(
    cfg: &DiscoveryConfig,
    catalog: &Catalog,
) -> (watch::Receiver<Membership>, CancellationToken) {
    let cancel = CancellationToken::new();
    let mut filter = scope(cfg).meta();
    filter.insert("topic".into(), Topic::TxErrors.as_str().into());
    let (w, rx) = MembershipWatch::new(
        catalog.clone(),
        PUBLISHER_SERVICE.into(),
        filter,
        WatchTiming::from_config(cfg),
        cancel.clone(),
    );
    tokio::spawn(w.run());
    (rx, cancel)
}

async fn wait_for(
    rx: &mut watch::Receiver<Membership>,
    budget: Duration,
    want: impl Fn(&Membership) -> bool,
) -> Membership {
    tokio::time::timeout(budget, async {
        while !want(&rx.borrow()) {
            rx.changed().await.expect("watch alive");
        }
        rx.borrow().clone()
    })
    .await
    .expect("membership within the budget")
}

fn ids(m: &Membership) -> BTreeMap<String, u16> {
    m.publishers()
        .values()
        .map(|p| (p.publisher_id.clone(), p.control.port()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test consul_discovery -- --ignored`"]
async fn members_join_leave_and_expire_through_consul() {
    kardamom_log::testing::require_docker().await;
    let (_container, cfg) = consul().await;
    let catalog = catalog_from_config(&cfg).expect("consul client");
    let (mut rx, cancel) = start_watch(&cfg, &catalog);

    let first = wait_for(&mut rx, Duration::from_secs(10), Membership::is_known).await;
    assert!(
        first.entries.is_empty(),
        "an empty catalog is a known empty set"
    );

    let a = publisher("a", 41000);
    let reg_a = Registration::register(catalog.clone(), spec(&cfg, &a))
        .await
        .expect("register a");
    let m = wait_for(&mut rx, Duration::from_secs(10), |m| m.entries.len() == 1).await;
    assert_eq!(ids(&m), BTreeMap::from([("a".to_string(), 41000)]));

    // A record of another topic never appears in this watch.
    let mut other = publisher("other", 41005);
    other.topic = Topic::TxBal;
    let reg_other = Registration::register(catalog.clone(), spec(&cfg, &other))
        .await
        .expect("register other");

    let b = publisher("b", 41001);
    let reg_b = Registration::register(catalog.clone(), spec(&cfg, &b))
        .await
        .expect("register b");
    let m = wait_for(&mut rx, Duration::from_secs(10), |m| m.entries.len() == 2).await;
    assert_eq!(
        ids(&m),
        BTreeMap::from([("a".to_string(), 41000), ("b".to_string(), 41001)])
    );

    // Graceful exit: deregistered at once.
    reg_b.deregister().await.expect("deregister b");
    let m = wait_for(&mut rx, Duration::from_secs(10), |m| m.entries.len() == 1).await;
    assert_eq!(ids(&m), BTreeMap::from([("a".to_string(), 41000)]));

    // Crash: the heartbeat stops, the TTL expires, the member is gone.
    drop(reg_a);
    let m = wait_for(&mut rx, Duration::from_secs(15), |m| m.entries.is_empty()).await;
    assert!(m.entries.is_empty());

    // The watch survived every transition without an error.
    assert_eq!(m.health, kardamom_log::discovery::CatalogHealth::Fresh);
    reg_other.deregister().await.expect("deregister other");
    cancel.cancel();
}
