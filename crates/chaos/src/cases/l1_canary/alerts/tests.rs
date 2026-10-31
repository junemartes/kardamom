use serde_json::json;

use super::Alerts;

fn snapshot(source: &str, inhibitor: &str) -> Alerts {
    serde_json::from_value(json!([
        {"labels":{"alertname":source,"severity":"critical"},"fingerprint":"root-id","status":{"state":"active","inhibitedBy":[]}},
        {"labels":{"alertname":"KardamomCanaryFailing","severity":"critical"},"fingerprint":"canary-id","status":{"state":"suppressed","inhibitedBy":[inhibitor]}}
    ])).unwrap()
}

#[test]
fn a_canary_page_names_the_actual_da_root() {
    assert_eq!(
        snapshot("KardamomHaltDaLag", "root-id").inhibited_by_da_guard(),
        Some("KardamomCanaryFailing")
    );
}

#[test]
fn an_unrelated_root_or_unknown_inhibitor_does_not_prove_da_inhibition() {
    assert!(
        snapshot("KardamomHaltDisk", "root-id")
            .inhibited_by_da_guard()
            .is_none()
    );
    assert!(
        snapshot("KardamomHaltDaLag", "old-root-id")
            .inhibited_by_da_guard()
            .is_none()
    );
    assert!(Alerts(Vec::new()).inhibited_by_da_guard().is_none());
}

#[test]
fn a_suppressed_root_or_noncritical_notice_does_not_prove_a_root_page() {
    let mut suppressed = snapshot("KardamomHaltDaLag", "root-id");
    suppressed.0[0].status.state = super::State::Suppressed;
    assert!(suppressed.inhibited_by_da_guard().is_none());
    let mut notice = snapshot("KardamomHaltDaLag", "root-id");
    notice.0[0].labels.severity = Some("warning".into());
    assert!(notice.inhibited_by_da_guard().is_none());
}
