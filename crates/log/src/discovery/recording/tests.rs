use std::cell::{Cell, RefCell};
use std::net::{IpAddr, Ipv4Addr};

use super::*;
use crate::discovery::record::{PUBLISHER_SERVICE, ServiceEntry, ServiceId};

const GRACE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct StubArchive {
    starts: RefCell<Vec<String>>,
    stops: RefCell<Vec<i64>>,
    failures: Cell<u32>,
    records: RefCell<Vec<(i64, i32, i64)>>,
}

impl RecorderArchive for StubArchive {
    fn start(&self, uri: &str, _: i32) -> Result<i64, LogError> {
        self.starts.borrow_mut().push(uri.into());
        if self.failures.get() > 0 {
            self.failures.set(self.failures.get() - 1);
            return Err(LogError::Aeron("start rejected".into()));
        }
        self.records.borrow_mut().push((11, 42, -1));
        Ok(7)
    }

    fn stop(&self, sub: i64) -> Result<(), LogError> {
        self.stops.borrow_mut().push(sub);
        Ok(())
    }

    fn latest(&self, started: &Started) -> Option<i64> {
        self.records
            .borrow()
            .iter()
            .filter(|(_, session, stop)| started.matches_recording(*session, *stop))
            .map(|(id, _, _)| *id)
            .max()
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

#[tokio::test]
async fn a_failed_start_retries_and_only_then_reports_ready() {
    let mut state = recording();
    state.archive.failures.set(1);
    let (tx, rx) = std::sync::mpsc::channel();
    let mut ready = Some(|progress| tx.send(progress).unwrap());
    let now = Instant::now();
    state.reconcile(&membership(true), now);
    state.resolve_pending();
    state.report(&mut ready);
    assert!(rx.try_recv().is_err());
    assert!(state.started.is_empty());
    state.reconcile(&membership(true), now + POLL);
    state.resolve_pending();
    state.report(&mut ready);
    assert_eq!(
        rx.try_recv().unwrap(),
        RecorderProgress::Ready { own_recordings: 1 }
    );
    assert_eq!(state.archive.starts.borrow().len(), 2);
}

#[tokio::test]
async fn a_rejected_start_adopts_a_matching_live_recording() {
    let mut state = recording();
    state.archive.failures.set(1);
    state.archive.records.borrow_mut().push((9, 42, -1));
    let now = Instant::now();
    state.reconcile(&membership(true), now);
    let adopted = state.started.values().next().unwrap();
    assert_eq!(adopted.recording_id, Some(9));
    assert_eq!(adopted.subscription_id, None);
    state.reconcile(&membership(true), now + POLL);
    assert_eq!(state.archive.starts.borrow().len(), 1);
}

#[tokio::test]
async fn stopped_or_foreign_recordings_do_not_suppress_start_retries() {
    let mut state = recording();
    state.archive.failures.set(1);
    state
        .archive
        .records
        .borrow_mut()
        .extend([(8, 42, 1024), (9, 99, -1)]);
    let now = Instant::now();
    state.reconcile(&membership(true), now);
    assert!(state.started.is_empty());
    state.reconcile(&membership(true), now + POLL);
    state.resolve_pending();
    assert_eq!(
        state.started.values().next().unwrap().recording_id,
        Some(11)
    );
    assert_eq!(state.archive.starts.borrow().len(), 2);
}
