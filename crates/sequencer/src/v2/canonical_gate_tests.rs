use super::*;
use crate::v2::AdmissionSettings;

#[test]
fn early_confirmations_are_bounded_and_rollback_forgets_them() {
    let cap = NonZeroUsize::new(4).unwrap();
    let mut gate = CanonicalGate::new(cap);
    let cfg = AdmissionSettings::default().loop_config(std::time::Duration::from_secs(4), 0);
    let mut admission = Admission::new(cfg.admission, std::time::Instant::now());
    (0..=100u8).for_each(|n| {
        gate.observe(B256::repeat_byte(n), &mut admission);
        assert!(gate.history.len() <= cap.get());
        assert_eq!(gate.history.len(), gate.seen.len());
        assert!(gate.flights.is_empty());
        assert!(gate.held.is_empty());
    });
    let last = B256::repeat_byte(100);
    gate.observe(last, &mut admission);
    assert_eq!(gate.history.len(), cap.get());
    assert!(gate.seen.contains(&last));
    assert!(!gate.seen.contains(&B256::ZERO));
    gate.rollback();
    assert!(gate.history.is_empty());
    assert!(gate.seen.is_empty());
}
