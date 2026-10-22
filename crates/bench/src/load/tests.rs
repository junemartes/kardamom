use std::num::NonZeroU32;

use super::LoadConfig;

/// A run with the default in-flight bound fits the open-file limit of a
/// test process, so the raise passes.
#[test]
fn a_default_run_raises_its_open_file_limit() {
    LoadConfig::default().raise_open_files().unwrap();
}

/// A run that needs more open files than the system gives fails before
/// it submits, with an error that names the limit.
#[test]
fn a_run_past_the_hard_limit_fails_before_it_starts() {
    let cfg = LoadConfig {
        max_in_flight: NonZeroU32::MAX,
        ..LoadConfig::default()
    };
    assert!(cfg.raise_open_files().is_err());
}
