use super::{SealerRules, SealerVersion};

fn rules() -> (SealerRules, SealerVersion) {
    let shared = SealerVersion::default();
    (SealerRules::new(0, shared.clone()), shared)
}

#[test]
fn a_report_after_its_session_start_counts() {
    let (mut r, v) = rules();
    r.on_session_start(7);
    r.on_sealer_info(7, 3);
    assert_eq!(v.get(), 3);
}

#[test]
fn a_report_before_its_session_start_counts() {
    let (mut r, v) = rules();
    r.on_sealer_info(7, 3);
    assert_eq!(v.get(), 0, "no session yet");
    r.on_session_start(7);
    assert_eq!(v.get(), 3);
}

#[test]
fn a_new_session_forgets_the_version_and_an_old_report_never_counts() {
    let (mut r, v) = rules();
    r.on_session_start(7);
    r.on_sealer_info(7, 3);
    r.on_session_start(8);
    assert_eq!(v.get(), 0);
    r.on_sealer_info(7, 3);
    assert_eq!(v.get(), 0, "a report of session 7");
    r.on_sealer_info(8, 3);
    assert_eq!(v.get(), 3);
}

#[test]
fn an_early_report_of_another_session_does_not_count() {
    let (mut r, v) = rules();
    r.on_sealer_info(6, 3);
    r.on_session_start(7);
    assert_eq!(v.get(), 0);
}

#[test]
fn a_sealer_on_version_four_orders_by_position() {
    let (mut r, v) = rules();
    r.on_session_start(7);
    r.on_sealer_info(7, 3);
    assert!(!v.orders_by_position());
    r.on_sealer_info(7, 4);
    assert!(v.orders_by_position());
    r.on_session_start(8);
    assert!(!v.orders_by_position(), "a new session has no report yet");
}
