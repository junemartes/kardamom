use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr};

use super::*;
use crate::discovery::record::{PUBLISHER_SERVICE, ServiceEntry, ServiceId};

const GRACE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct StubArchive {
    starts: RefCell<Vec<String>>,
    stops: RefCell<Vec<i64>>,
}

impl RecorderArchive for StubArchive {
    fn start(&self, uri: &str, _: i32) -> Result<i64, LogError> {
        self.starts.borrow_mut().push(uri.into());
        Ok(7)
    }

    fn stop(&self, sub: i64) -> Result<(), LogError> {
        self.stops.borrow_mut().push(sub);
        Ok(())
    }

    fn latest(&self, _: &Started) -> Option<i64> {
        Some(11)
    }
}

fn membership(present: bool) -> Membership {
    let entry = ServiceEntry {
        id: ServiceId::new("alloc-test:tx_data:1001".into()),
        name: PUBLISHER_SERVICE.into(),
        address: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 41000,
        meta: BTreeMap::from([
            ("topic".into(), "tx_data".into()),
            ("stream_id".into(), "1001".into()),
            ("publisher_id".into(), "test".into()),
            ("session_id".into(), "42".into()),
        ]),
    };
    Membership {
        index: 1,
        entries: present
            .then_some((entry.id.clone(), entry))
            .into_iter()
            .collect(),
        health: CatalogHealth::Fresh,
    }
}

fn recording() -> Recording<StubArchive> {
    let (_, membership) = watch::channel(Membership::unknown());
    Recording {
        spec: DiscoveredRecorder {
            aeron_dir: None,
            aeron_cfg: AeronConfig::default(),
            local_ip: Ipv4Addr::LOCALHOST,
            own_instance: "alloc-test".into(),
            expected_own: 1,
            removal_grace: GRACE,
            membership,
            stop: CancellationToken::new(),
            runtime: tokio::runtime::Handle::current(),
        },
        archive: StubArchive::default(),
        started: BTreeMap::new(),
    }
}

#[tokio::test]
async fn a_publisher_returning_within_grace_keeps_the_same_recording() {
    let mut state = recording();
    let now = Instant::now();
    state.reconcile(&membership(true), now);
    state.reconcile(&membership(false), now + Duration::from_secs(1));
    state.reconcile(&membership(true), now + Duration::from_secs(4));
    assert_eq!(state.archive.starts.borrow().len(), 1);
    assert!(state.archive.stops.borrow().is_empty());
    assert_eq!(state.started.len(), 1);
}

#[tokio::test]
async fn a_confirmed_departure_stops_after_the_full_grace() {
    let mut state = recording();
    let now = Instant::now();
    state.reconcile(&membership(true), now);
    state.reconcile(&membership(false), now + Duration::from_secs(1));
    assert!(state.archive.stops.borrow().is_empty());
    state.reconcile(&membership(false), now + Duration::from_secs(1) + GRACE);
    assert_eq!(*state.archive.stops.borrow(), vec![7]);
    assert!(state.started.is_empty());
}

#[tokio::test]
async fn a_catalog_outage_preserves_recordings_and_resets_removal_proof() {
    let mut state = recording();
    let now = Instant::now();
    state.reconcile(&membership(true), now);
    state.reconcile(&membership(false), now + Duration::from_secs(1));
    let mut degraded = membership(false);
    degraded.health = CatalogHealth::Degraded {
        consecutive_errors: 1,
    };
    state.reconcile(&degraded, now + GRACE * 2);
    state.reconcile(&membership(false), now + GRACE * 3);
    assert!(state.archive.stops.borrow().is_empty());
    state.reconcile(&membership(false), now + GRACE * 4);
    assert_eq!(*state.archive.stops.borrow(), vec![7]);
}
