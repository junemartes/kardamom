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

    fn stop_position(&self, recording_id: i64) -> Result<i64, LogError> {
        self.records
            .borrow()
            .iter()
            .find(|(id, _, _)| *id == recording_id)
            .map(|(_, _, stop)| *stop)
            .ok_or_else(|| LogError::Aeron("unknown recording".into()))
    }

    fn live(&self, started: &Started) -> Result<Option<i64>, LogError> {
        Ok(self
            .records
            .borrow()
            .iter()
            .filter(|(_, session, stop)| started.matches_recording(*session, *stop))
            .map(|(id, _, _)| *id)
            .max())
    }
}

/// The membership with the own publication of the recorder, or without it.
fn membership(present: bool) -> Membership {
    membership_of("alloc-test", present)
}

/// The membership with the publication of another process, or without it.
fn peer(present: bool) -> Membership {
    membership_of("alloc-peer", present)
}

fn membership_of(instance: &str, present: bool) -> Membership {
    let entry = ServiceEntry {
        id: ServiceId::new(format!("{instance}:tx_data:1001")),
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

/// The image of the publisher went, so the archive ended its recording.
fn end_recordings(state: &Recording<StubArchive>) {
    state
        .archive
        .records
        .borrow_mut()
        .iter_mut()
        .for_each(|r| r.2 = 4096);
}

/// The membership of the peer at `now`, then missing for the full grace.
/// Returns the time at which the peer departs.
fn depart_peer(state: &mut Recording<StubArchive>, now: Instant) -> Instant {
    state.reconcile(&peer(true), now);
    state.resolve_pending();
    state.reconcile(&peer(false), now + Duration::from_secs(1));
    let departed = now + Duration::from_secs(1) + GRACE;
    state.reconcile(&peer(false), departed);
    state.follow_archive();
    departed
}

#[tokio::test]
async fn a_departed_publisher_stops_once_the_archive_ends_its_recording() {
    let mut state = recording();
    depart_peer(&mut state, Instant::now());
    assert!(
        state.archive.stops.borrow().is_empty(),
        "a live recording outlasts the grace"
    );
    end_recordings(&state);
    state.follow_archive();
    assert_eq!(*state.archive.stops.borrow(), vec![7]);
    assert!(state.started.is_empty());
}

#[tokio::test]
async fn a_departed_publisher_keeps_a_live_recording_for_any_time() {
    let mut state = recording();
    let departed = depart_peer(&mut state, Instant::now());
    state.reconcile(&peer(false), departed + GRACE * 100);
    state.follow_archive();
    assert!(state.archive.stops.borrow().is_empty());
    assert_eq!(state.started.len(), 1);
}

#[tokio::test]
async fn a_departed_publisher_listed_again_keeps_its_recording() {
    let mut state = recording();
    let departed = depart_peer(&mut state, Instant::now());
    state.reconcile(&peer(true), departed + POLL);
    end_recordings(&state);
    state.follow_archive();
    assert!(state.archive.stops.borrow().is_empty());
    assert_eq!(
        state.archive.starts.borrow().len(),
        1,
        "no second recording"
    );
    assert_eq!(state.started.len(), 1);
}

#[tokio::test]
async fn a_catalog_lapse_of_an_own_publication_keeps_its_recording() {
    let mut state = recording();
    let now = Instant::now();
    state.reconcile(&membership(true), now);
    state.resolve_pending();
    state.reconcile(&membership(false), now + Duration::from_secs(1));
    state.reconcile(&membership(false), now + GRACE * 100);
    state.follow_archive();
    assert!(state.archive.stops.borrow().is_empty());
    let own = state.started.values().next().unwrap();
    assert!(!own.departed, "an own publication never departs");
    assert_eq!(own.recording_id, Some(11));
}

#[tokio::test]
async fn an_ended_recording_of_a_listed_publisher_keeps_its_subscription() {
    let mut state = recording();
    state.reconcile(&peer(true), Instant::now());
    state.resolve_pending();
    end_recordings(&state);
    state.follow_archive();
    assert!(state.archive.stops.borrow().is_empty());
    assert_eq!(state.started.values().next().unwrap().recording_id, None);
    state.archive.records.borrow_mut().push((12, 42, -1));
    state.resolve_pending();
    assert_eq!(
        state.started.values().next().unwrap().recording_id,
        Some(12),
        "the next recording of the publisher resolves"
    );
}

#[tokio::test]
async fn a_catalog_outage_preserves_recordings_and_resets_removal_proof() {
    let mut state = recording();
    let now = Instant::now();
    state.reconcile(&peer(true), now);
    state.reconcile(&peer(false), now + Duration::from_secs(1));
    let mut degraded = peer(false);
    degraded.health = CatalogHealth::Degraded {
        consecutive_errors: 1,
    };
    state.reconcile(&degraded, now + GRACE * 2);
    state.reconcile(&peer(false), now + GRACE * 3);
    assert!(!state.started.values().next().unwrap().departed);
    state.reconcile(&peer(false), now + GRACE * 4);
    end_recordings(&state);
    state.follow_archive();
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
