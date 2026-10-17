use std::time::{Duration, Instant};

use super::{ClusterStatus, SEALER_SILENCE, SNAPSHOT_SILENCE, SilenceDeadline};

fn notice() -> ClusterStatus {
    ClusterStatus {
        snapshot_follows: true,
        ..ClusterStatus::default()
    }
}

#[test]
fn the_normal_budget_is_the_sealer_silence() {
    let start = Instant::now();
    let deadline = SilenceDeadline::new(start);
    assert_eq!(deadline.expired(start + Duration::from_millis(9_999)), None);
    assert_eq!(
        deadline.expired(start + SEALER_SILENCE),
        Some(SEALER_SILENCE)
    );
    assert!(!deadline.in_snapshot(start));
}

/// A notice gives the snapshot budget. A relayed record after it does not
/// end the snapshot; the next boundary or plain status does.
#[test]
fn a_notice_holds_until_a_boundary_or_a_plain_status() {
    let start = Instant::now();
    let mut deadline = SilenceDeadline::new(start);
    deadline.on_status(&notice(), start);
    assert!(deadline.in_snapshot(start + Duration::from_secs(14)));
    assert_eq!(deadline.expired(start + Duration::from_secs(14)), None);
    assert_eq!(
        deadline.expired(start + SNAPSHOT_SILENCE),
        Some(SNAPSHOT_SILENCE)
    );
    assert!(
        !deadline.in_snapshot(start + SNAPSHOT_SILENCE),
        "a stall past the budget is no snapshot"
    );

    deadline.on_boundary(start);
    assert_eq!(
        deadline.budget(),
        SEALER_SILENCE,
        "a boundary ends the snapshot"
    );

    deadline.on_status(&notice(), start);
    deadline.on_status(&ClusterStatus::default(), start);
    assert_eq!(
        deadline.budget(),
        SEALER_SILENCE,
        "a plain status ends the snapshot"
    );
}
