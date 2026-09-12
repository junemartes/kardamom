use std::cell::RefCell;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};

use super::*;
use crate::discovery::record::{PUBLISHER_SERVICE, ServiceEntry};
use crate::discovery::watch::CatalogHealth;

const LOCAL: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9));
const GRACE: Duration = Duration::from_secs(5);

/// A port that records every call and fails the URIs in `failing`.
#[derive(Default)]
struct RecordingPort {
    calls: RefCell<Vec<String>>,
    failing: Vec<String>,
}

impl DestinationPort for RecordingPort {
    fn attach(&self, uri: &str) -> Result<(), LogError> {
        self.calls.borrow_mut().push(format!("attach {uri}"));
        if self.failing.iter().any(|f| f == uri) {
            return Err(LogError::Aeron("driver said no".into()));
        }
        Ok(())
    }

    fn detach(&self, uri: &str) -> Result<(), LogError> {
        self.calls.borrow_mut().push(format!("detach {uri}"));
        Ok(())
    }
}

fn publisher(id: &str, port: u16) -> ServiceEntry {
    ServiceEntry {
        id: ServiceId::new(id.into()),
        name: PUBLISHER_SERVICE.into(),
        address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
        port,
        meta: BTreeMap::from([
            ("topic".to_string(), "tx_errors".to_string()),
            ("stream_id".to_string(), "1015".to_string()),
            ("publisher_id".to_string(), id.to_string()),
        ]),
    }
}

fn membership(index: u64, entries: Vec<ServiceEntry>) -> Membership {
    Membership {
        index,
        entries: entries.into_iter().map(|e| (e.id.clone(), e)).collect(),
        health: CatalogHealth::Fresh,
    }
}

fn uri(port: u16) -> String {
    format!("aeron:udp?endpoint=10.0.0.9:0|control=10.0.0.5:{port}|control-mode=dynamic")
}

#[test]
fn unknown_membership_plans_nothing() {
    let mut r = Reconciler::new(LOCAL, GRACE);
    let plan = r.plan(&Membership::unknown(), Instant::now());
    assert_eq!(plan, Plan::default());
}

#[test]
fn new_members_attach_and_a_second_snapshot_is_idempotent() {
    let mut r = Reconciler::new(LOCAL, GRACE);
    let port = RecordingPort::default();
    let m = membership(5, vec![publisher("a", 41000), publisher("b", 41001)]);
    let now = Instant::now();
    let plan = r.plan(&m, now);
    assert_eq!(plan.attach, vec![uri(41000), uri(41001)]);
    assert!(plan.detach.is_empty());
    r.apply(&plan, &port, &m);
    assert_eq!(r.attached(), vec![uri(41000), uri(41001)]);

    let again = r.plan(&m, now);
    assert_eq!(
        again,
        Plan::default(),
        "an unchanged snapshot asks for nothing"
    );
}

#[test]
fn a_missing_member_detaches_only_after_the_grace() {
    let mut r = Reconciler::new(LOCAL, GRACE);
    let port = RecordingPort::default();
    let both = membership(5, vec![publisher("a", 41000), publisher("b", 41001)]);
    let t0 = Instant::now();
    let plan = r.plan(&both, t0);
    r.apply(&plan, &port, &both);

    let only_a = membership(6, vec![publisher("a", 41000)]);
    let early = r.plan(&only_a, t0 + Duration::from_secs(1));
    assert!(early.detach.is_empty(), "inside the grace nothing detaches");

    let late = r.plan(&only_a, t0 + GRACE + Duration::from_secs(1));
    assert_eq!(late.detach, vec![uri(41001)]);
    r.apply(&late, &port, &only_a);
    assert_eq!(r.attached(), vec![uri(41000)]);
    assert_eq!(
        port.calls.borrow().last().map(String::as_str),
        Some(format!("detach {}", uri(41001)).as_str())
    );
}

#[test]
fn a_member_that_returns_inside_the_grace_stays_attached() {
    let mut r = Reconciler::new(LOCAL, GRACE);
    let port = RecordingPort::default();
    let both = membership(5, vec![publisher("a", 41000), publisher("b", 41001)]);
    let t0 = Instant::now();
    let plan = r.plan(&both, t0);
    r.apply(&plan, &port, &both);

    let only_a = membership(6, vec![publisher("a", 41000)]);
    let _ = r.plan(&only_a, t0 + Duration::from_secs(1));
    let back = membership(7, vec![publisher("a", 41000), publisher("b", 41001)]);
    let _ = r.plan(&back, t0 + Duration::from_secs(2));
    let gone_again = membership(8, vec![publisher("a", 41000)]);
    let plan = r.plan(&gone_again, t0 + GRACE);
    assert!(
        plan.detach.is_empty(),
        "the grace restarts when a member returns"
    );
}

#[test]
fn a_successful_empty_read_detaches_everything_after_the_grace() {
    let mut r = Reconciler::new(LOCAL, GRACE);
    let port = RecordingPort::default();
    let m = membership(5, vec![publisher("a", 41000)]);
    let t0 = Instant::now();
    let plan = r.plan(&m, t0);
    r.apply(&plan, &port, &m);

    let empty = membership(6, vec![]);
    let first_miss = r.plan(&empty, t0 + Duration::from_secs(1));
    assert!(
        first_miss.detach.is_empty(),
        "the grace starts at the first miss"
    );
    let plan = r.plan(&empty, t0 + Duration::from_secs(1) + GRACE);
    assert_eq!(plan.detach, vec![uri(41000)]);
}

#[test]
fn a_failed_attach_is_retried_on_the_next_snapshot() {
    let mut r = Reconciler::new(LOCAL, GRACE);
    let port = RecordingPort {
        calls: RefCell::new(Vec::new()),
        failing: vec![uri(41000)],
    };
    let m = membership(5, vec![publisher("a", 41000)]);
    let now = Instant::now();
    let plan = r.plan(&m, now);
    r.apply(&plan, &port, &m);
    assert!(r.attached().is_empty(), "a failed attach is not recorded");
    let retry = r.plan(&m, now);
    assert_eq!(retry.attach, vec![uri(41000)]);
}

#[test]
fn a_replacement_incarnation_on_the_same_endpoint_keeps_the_destination() {
    let mut r = Reconciler::new(LOCAL, GRACE);
    let port = RecordingPort::default();
    let old = membership(5, vec![publisher("alloc-1", 41000)]);
    let t0 = Instant::now();
    let plan = r.plan(&old, t0);
    r.apply(&plan, &port, &old);

    let replaced = membership(6, vec![publisher("alloc-2", 41000)]);
    let plan = r.plan(&replaced, t0 + GRACE * 2);
    assert_eq!(plan, Plan::default(), "same endpoint: no detach, no attach");
    assert_eq!(r.attached(), vec![uri(41000)]);
}

#[test]
fn malformed_records_are_skipped() {
    let mut r = Reconciler::new(LOCAL, GRACE);
    let mut bad = publisher("bad", 41002);
    bad.meta.remove("stream_id");
    let m = membership(5, vec![publisher("a", 41000), bad]);
    let plan = r.plan(&m, Instant::now());
    assert_eq!(plan.attach, vec![uri(41000)]);
}
