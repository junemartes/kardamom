//! Tests of the registration heartbeat: a moved record stays moved
//! through a re-registration after a lost check.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::time::{Duration, Instant};

use super::Registration;
use crate::discovery::catalog::{Catalog, Query, RegistrationSpec};
use crate::discovery::memory::MemoryCatalog;
use crate::discovery::record::{PUBLISHER_SERVICE, ServiceEntry, ServiceId};

fn spec(port: u16) -> RegistrationSpec {
    RegistrationSpec {
        entry: ServiceEntry {
            id: ServiceId::new("alloc-1:tx_receipts:1002".into()),
            name: PUBLISHER_SERVICE.into(),
            address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
            port,
            meta: BTreeMap::new(),
        },
        ttl: Duration::from_millis(300),
        deregister_after: Duration::from_mins(1),
    }
}

/// The port the catalog holds for the record, or `None` while the record
/// is absent or not passing.
async fn port_of(catalog: &MemoryCatalog, id: &ServiceId) -> Option<u16> {
    let result = catalog
        .query(&Query {
            service: PUBLISHER_SERVICE.into(),
            meta_equals: BTreeMap::new(),
            index: 0,
            wait: Duration::ZERO,
        })
        .await
        .unwrap();
    result.entries.iter().find(|e| &e.id == id).map(|e| e.port)
}

async fn wait_for_port(catalog: &MemoryCatalog, id: &ServiceId, port: u16) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while port_of(catalog, id).await != Some(port) {
        assert!(
            Instant::now() < deadline,
            "the record never showed port {port}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_moved_record_survives_a_lost_check() {
    let catalog = MemoryCatalog::new();
    let first = spec(1);
    let id = first.entry.id.clone();
    let registration = Registration::register(Catalog::Memory(catalog.clone()), first.clone())
        .await
        .unwrap();
    assert_eq!(port_of(&catalog, &id).await, Some(1));

    let moved = ServiceEntry {
        port: 2,
        ..first.entry.clone()
    };
    registration.mover().relocate(moved).await.unwrap();
    assert_eq!(port_of(&catalog, &id).await, Some(2));

    // The agent forgets the service. The next pass fails, and the
    // heartbeat registers again: the moved entry, not the first one.
    catalog.deregister(&id).unwrap();
    assert_eq!(port_of(&catalog, &id).await, None);
    wait_for_port(&catalog, &id, 2).await;

    registration.deregister().await.unwrap();
    assert_eq!(port_of(&catalog, &id).await, None);
}

#[tokio::test]
async fn a_move_after_the_registration_ended_is_an_error() {
    let catalog = MemoryCatalog::new();
    let first = spec(1);
    let registration = Registration::register(Catalog::Memory(catalog.clone()), first.clone())
        .await
        .unwrap();
    let mover = registration.mover();
    registration.deregister().await.unwrap();
    assert!(mover.relocate(first.entry).await.is_err());
}
