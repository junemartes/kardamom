use kardamom_leases::{Lease, LeaseConfig};
use kardamom_types::{BPosition, FsyncWatermark, QuorumWatermark};

fn pos(t: i32, o: i32) -> BPosition {
    BPosition {
        term_id: t,
        term_offset: o,
    }
}

#[test]
fn no_quorum_no_lease() {
    let lease = Lease::new(LeaseConfig {
        self_id: 0,
        all_ids: vec![0, 1, 2],
        caught_up_window: 1024,
    });
    assert!(!lease.held_by_us());
}

#[test]
fn lowest_caught_up_id_holds_lease() {
    let mut lease = Lease::new(LeaseConfig {
        self_id: 0,
        all_ids: vec![0, 1, 2],
        caught_up_window: 1024,
    });
    lease.observe_quorum(QuorumWatermark {
        position: pos(1, 1000),
    });
    lease.observe_fsync(FsyncWatermark {
        recorder_id: 0,
        position: pos(1, 900),
    });
    lease.observe_fsync(FsyncWatermark {
        recorder_id: 1,
        position: pos(1, 1000),
    });
    lease.observe_fsync(FsyncWatermark {
        recorder_id: 2,
        position: pos(1, 1000),
    });
    assert!(lease.held_by_us(), "id=0 is lowest caught-up");
}

#[test]
fn lease_transfers_when_lowest_falls_behind() {
    let mut lease = Lease::new(LeaseConfig {
        self_id: 1,
        all_ids: vec![0, 1, 2],
        caught_up_window: 1024,
    });
    lease.observe_quorum(QuorumWatermark {
        position: pos(1, 10_000),
    });
    lease.observe_fsync(FsyncWatermark {
        recorder_id: 0,
        position: pos(1, 0),
    }); // far behind
    lease.observe_fsync(FsyncWatermark {
        recorder_id: 1,
        position: pos(1, 10_000),
    });
    lease.observe_fsync(FsyncWatermark {
        recorder_id: 2,
        position: pos(1, 10_000),
    });
    assert!(
        lease.held_by_us(),
        "id=1 holds lease because id=0 lags > window"
    );
}

#[test]
fn no_one_caught_up_no_lease() {
    let mut lease = Lease::new(LeaseConfig {
        self_id: 0,
        all_ids: vec![0, 1, 2],
        caught_up_window: 10,
    });
    lease.observe_quorum(QuorumWatermark {
        position: pos(1, 10_000),
    });
    lease.observe_fsync(FsyncWatermark {
        recorder_id: 0,
        position: pos(1, 0),
    });
    lease.observe_fsync(FsyncWatermark {
        recorder_id: 1,
        position: pos(1, 5_000),
    });
    assert!(!lease.held_by_us());
}
