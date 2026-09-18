use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use super::*;
use crate::discovery::memory::MemoryCatalog;
use crate::discovery::record::{ARCHIVE_SERVICE, CLUSTER_MEMBER_SERVICE, ServiceEntry, ServiceId};
use crate::refetch::EndpointSource;

fn test_config() -> LogConfig {
    let mut cfg = LogConfig::default();
    cfg.discovery.enabled = true;
    cfg.discovery.cluster_id = "test".into();
    cfg.discovery.chain_id = 7;
    cfg.discovery.blocking_wait_ms = std::num::NonZeroU64::new(50).unwrap();
    cfg
}

fn test_plane(catalog: &MemoryCatalog) -> StreamPlane {
    StreamPlane::with_catalog(
        &test_config(),
        "test",
        Catalog::Memory(catalog.clone()),
        Instance {
            id: "alloc-1".into(),
            ports: None,
        },
        Ipv4Addr::LOCALHOST,
    )
}

fn register(catalog: &MemoryCatalog, name: &str, id: &str, port: u16, extra: &[(&str, &str)]) {
    let scope = scope_from_config(&test_config().discovery);
    let mut meta = scope.meta();
    meta.extend(
        extra
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
    );
    let spec = RegistrationSpec {
        entry: ServiceEntry {
            id: ServiceId::new(id.into()),
            name: name.into(),
            address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
            port,
            meta,
        },
        ttl: Duration::from_secs(10),
        deregister_after: Duration::from_secs(60),
    };
    catalog.register(&spec).unwrap();
    catalog.pass(&spec.entry.id).unwrap();
}

#[tokio::test]
async fn cluster_members_resolve_sorted_by_member_id() {
    let catalog = MemoryCatalog::new();
    register(
        &catalog,
        CLUSTER_MEMBER_SERVICE,
        "m2",
        40200,
        &[("member_id", "2")],
    );
    register(
        &catalog,
        CLUSTER_MEMBER_SERVICE,
        "m0",
        40200,
        &[("member_id", "0")],
    );
    let plane = test_plane(&catalog);
    let endpoints = plane.cluster_ingress_endpoints().await.unwrap();
    assert_eq!(
        endpoints.as_deref(),
        Some("0=10.0.0.5:40200,2=10.0.0.5:40200")
    );
}

#[tokio::test]
async fn no_cluster_members_leaves_the_static_config_in_place() {
    let catalog = MemoryCatalog::new();
    let plane = test_plane(&catalog);
    assert_eq!(plane.cluster_ingress_endpoints().await.unwrap(), None);
    let static_plane = StreamPlane::static_only(LogConfig::default().channels);
    assert_eq!(
        static_plane.cluster_ingress_endpoints().await.unwrap(),
        None
    );
}

#[tokio::test]
async fn a_malformed_member_record_is_an_error_not_a_silent_skip() {
    let catalog = MemoryCatalog::new();
    register(
        &catalog,
        CLUSTER_MEMBER_SERVICE,
        "m0",
        40200,
        &[("member_id", "zero")],
    );
    let plane = test_plane(&catalog);
    assert!(plane.cluster_ingress_endpoints().await.is_err());
}

#[tokio::test]
async fn archive_endpoints_follow_the_catalog_per_topic() {
    let catalog = MemoryCatalog::new();
    register(
        &catalog,
        ARCHIVE_SERVICE,
        "arch-ingress",
        8010,
        &[("archive_id", "ingress-0"), ("topics", "tx_data")],
    );
    register(
        &catalog,
        ARCHIVE_SERVICE,
        "arch-aux",
        8011,
        &[("archive_id", "aux-0"), ("topics", "tx_deposits,tx_data")],
    );
    let mut plane = test_plane(&catalog);
    let archives = plane.watch_archives().expect("discovered plane");
    let mut rx = archives.clone();
    tokio::time::timeout(Duration::from_secs(5), async {
        while rx.borrow().entries.len() < 2 {
            rx.changed().await.unwrap();
        }
    })
    .await
    .expect("both archives listed");

    let tx_data = EndpointSource::Discovered {
        topic: Topic::TxData,
        archives: archives.clone(),
    };
    let deposits = EndpointSource::Discovered {
        topic: Topic::TxDeposits,
        archives,
    };
    let mut data_endpoints = tx_data.current();
    data_endpoints.sort();
    assert_eq!(
        data_endpoints,
        vec!["10.0.0.5:8010".to_string(), "10.0.0.5:8011".to_string()]
    );
    assert_eq!(deposits.current(), vec!["10.0.0.5:8011".to_string()]);
    assert!(tx_data.is_configured());
    assert!(!EndpointSource::Static(Vec::new()).is_configured());
    let _ = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    plane.shutdown().await;
}

#[test]
fn stream_key_filter_carries_topic_stream_and_lane() {
    let scope = scope_from_config(&test_config().discovery);
    let key = StreamKey {
        topic: Topic::TxData,
        stream_id: 2003,
        lane: Some(3),
    };
    let filter = key.filter(&scope);
    let expect: BTreeMap<String, String> = [
        ("discovery_version", "1"),
        ("cluster_id", "test"),
        ("chain_id", "7"),
        ("topic", "tx_data"),
        ("stream_id", "2003"),
        ("lane_id", "3"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    assert_eq!(filter, expect);
}
