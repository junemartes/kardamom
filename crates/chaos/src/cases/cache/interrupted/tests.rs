use super::Target;

#[test]
fn a_blocked_scan_needs_serving_zero_and_a_positive_retry_in_one_body() {
    let body = "kardamom_state_mirror_serving 0\nkardamom_state_mirror_write_retries_total 7\n";
    assert_eq!(Target::blocked_scan(body).unwrap().get(), 7);
}

#[test]
fn completed_or_missing_scan_evidence_cannot_certify_an_interruption() {
    for body in [
        "kardamom_state_mirror_serving 1\nkardamom_state_mirror_write_retries_total 7\n",
        "kardamom_state_mirror_serving 0\nkardamom_state_mirror_write_retries_total 0\n",
        "kardamom_state_mirror_write_retries_total 7\n",
        "kardamom_state_mirror_serving 0\n",
        "",
    ] {
        assert!(Target::blocked_scan(body).is_err(), "{body:?}");
    }
}

#[test]
fn the_failure_distinguishes_a_missing_exporter_field_from_a_completed_scan() {
    assert!(
        Target::blocked_scan("")
            .unwrap_err()
            .to_string()
            .contains("missing serving")
    );
    assert!(
        Target::blocked_scan("kardamom_state_mirror_serving 1\n")
            .unwrap_err()
            .to_string()
            .contains("serves before interruption: 1")
    );
}
