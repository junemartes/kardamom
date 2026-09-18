use super::*;

#[test]
fn verdict_needs_positive_evidence_and_checks_errors_in_the_same_scrape() {
    let healthy = format!("{COMMITTED} 10\n{VERIFIED} 5\n{SHADOW_CHECKS} 3\n");
    assert!(Sample::read(&healthy).is_ok());
    assert!(Sample::read("").is_err());
    assert!(Sample::read(&format!("{COMMITTED} 10\n")).is_err());
    assert!(Sample::read(&format!("{healthy}{DIVERGENCE} 1\n")).is_err());
    assert!(Sample::read(&format!("{healthy}{SHADOW_MISMATCH} 1\n")).is_err());
}
