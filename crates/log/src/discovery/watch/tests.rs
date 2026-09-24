use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use super::*;
use crate::discovery::catalog::RegistrationSpec;
use crate::discovery::memory::MemoryCatalog;
use crate::discovery::record::{PUBLISHER_SERVICE, ServiceEntry};

fn spec(id: &str, topic: &str) -> RegistrationSpec {
    RegistrationSpec {
        entry: ServiceEntry {
            id: ServiceId::new(id.into()),
            name: PUBLISHER_SERVICE.into(),
            address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
            port: 41000,
            meta: BTreeMap::from([
                ("topic".to_string(), topic.to_string()),
                ("stream_id".to_string(), "1015".to_string()),
                ("publisher_id".to_string(), id.to_string()),
            ]),
        },
        ttl: Duration::from_secs(10),
        deregister_after: Duration::from_secs(60),
    }
}

fn timing() -> WatchTiming {
    WatchTiming {
        wait: Duration::from_millis(50),
        backoff_min: Duration::from_millis(10),
        backoff_max: Duration::from_millis(40),
    }
}

fn start(catalog: &MemoryCatalog, topic: &str) -> (watch::Receiver<Membership>, CancellationToken) {
    let cancel = CancellationToken::new();
    let (w, rx) = MembershipWatch::new(
        Catalog::Memory(catalog.clone()),
        PUBLISHER_SERVICE.into(),
        BTreeMap::from([("topic".to_string(), topic.to_string())]),
        timing(),
        cancel.clone(),
    );
    tokio::spawn(w.run());
    (rx, cancel)
}

async fn wait_for(
    rx: &mut watch::Receiver<Membership>,
    want: impl Fn(&Membership) -> bool,
) -> Membership {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !want(&rx.borrow()) {
            rx.changed().await.expect("watch alive");
        }
        rx.borrow().clone()
    })
    .await
    .expect("membership within the budget")
}

#[tokio::test]
async fn passing_members_of_the_filtered_topic_appear_and_leave() {
    let catalog = MemoryCatalog::new();
    let (mut rx, cancel) = start(&catalog, "tx_errors");
    let a = spec("a", "tx_errors");
    let other = spec("other", "tx_bal");
    catalog.register(&a).unwrap();
    catalog.register(&other).unwrap();
    catalog.pass(&other.entry.id).unwrap();

    let m = wait_for(&mut rx, Membership::is_known).await;
    assert!(m.entries.is_empty(), "a critical check is not a member");

    catalog.pass(&a.entry.id).unwrap();
    let m = wait_for(&mut rx, |m| !m.entries.is_empty()).await;
    assert_eq!(m.entries.len(), 1);
    assert!(m.entries.contains_key(&a.entry.id));
    assert_eq!(m.health, CatalogHealth::Fresh);

    catalog.expire(&a.entry.id);
    let m = wait_for(&mut rx, |m| m.entries.is_empty() && m.index > 3).await;
    assert!(m.entries.is_empty());
    cancel.cancel();
}

#[tokio::test]
async fn a_read_error_keeps_the_last_membership_and_marks_degraded() {
    let catalog = MemoryCatalog::new();
    let (mut rx, cancel) = start(&catalog, "tx_errors");
    let a = spec("a", "tx_errors");
    catalog.register(&a).unwrap();
    catalog.pass(&a.entry.id).unwrap();
    let m = wait_for(&mut rx, |m| !m.entries.is_empty()).await;
    let index = m.index;

    catalog.fail_next_reads(2);
    let m = wait_for(&mut rx, |m| m.health != CatalogHealth::Fresh).await;
    assert_eq!(m.entries.len(), 1, "an error is not an empty membership");
    assert_eq!(m.index, index);

    let m = wait_for(&mut rx, |m| m.health == CatalogHealth::Fresh).await;
    assert_eq!(m.entries.len(), 1);
    cancel.cancel();
}

#[tokio::test]
async fn an_index_reset_restarts_the_blocking_query() {
    let catalog = MemoryCatalog::new();
    let (mut rx, cancel) = start(&catalog, "tx_errors");
    let a = spec("a", "tx_errors");
    catalog.register(&a).unwrap();
    catalog.pass(&a.entry.id).unwrap();
    let _ = wait_for(&mut rx, |m| !m.entries.is_empty()).await;
    // Churn a throwaway record so the watch sits at a high index before
    // the reset.
    let c = spec("c", "tx_errors");
    for _ in 0..3 {
        catalog.register(&c).unwrap();
        catalog.deregister(&c.entry.id).unwrap();
    }
    let high = catalog.index();
    let before = wait_for(&mut rx, |m| m.index >= high).await;

    catalog.reset_index();
    let b = spec("b", "tx_errors");
    catalog.register(&b).unwrap();
    catalog.pass(&b.entry.id).unwrap();
    let after = wait_for(&mut rx, |m| m.entries.len() == 2).await;
    assert!(
        after.index < before.index,
        "the watch followed the catalog back to a low index"
    );
    cancel.cancel();
}

#[tokio::test]
async fn cancel_ends_the_watch() {
    let catalog = MemoryCatalog::new();
    let cancel = CancellationToken::new();
    let (w, rx) = MembershipWatch::new(
        Catalog::Memory(catalog),
        PUBLISHER_SERVICE.into(),
        BTreeMap::new(),
        timing(),
        cancel.clone(),
    );
    let task = tokio::spawn(w.run());
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("the watch stops on cancel")
        .expect("no panic");
    drop(rx);
}
