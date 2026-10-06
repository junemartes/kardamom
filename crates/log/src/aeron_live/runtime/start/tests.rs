use std::time::Instant;

use super::*;
use crate::aeron_live::AeronRuntime;

fn budget_for(ms: u64) -> Duration {
    StartBudget::from_driver_timeout_ms(ms).unwrap().0
}

#[test]
fn the_budget_is_the_driver_timeout_plus_the_margin() {
    assert_eq!(budget_for(30_000), Duration::from_secs(35));
    assert_eq!(budget_for(10_000), Duration::from_secs(15));
    assert_eq!(budget_for(10_001), Duration::from_millis(15_001));
}

#[test]
fn a_short_driver_timeout_keeps_the_floor() {
    assert_eq!(budget_for(0), Duration::from_secs(10));
    assert_eq!(budget_for(1_000), Duration::from_secs(10));
    assert_eq!(budget_for(5_000), Duration::from_secs(10));
}

#[test]
fn the_largest_driver_timeout_has_a_budget() {
    assert!(StartBudget::from_driver_timeout_ms(u64::MAX).is_ok());
}

/// The budget reads the driver timeout the context uses, so a value that
/// `make_ctx` sets wins over the environment. This needs no media driver.
#[test]
fn the_budget_follows_the_context() {
    let ctx = AeronContext::new().unwrap();
    ctx.set_driver_timeout_ms(30_000).unwrap();
    assert_eq!(StartBudget::of(&ctx).unwrap().0, Duration::from_secs(35));
}

#[test]
fn a_failed_context_build_fails_the_spawn() {
    let Err(e) = AeronRuntime::spawn_with(|| Err(LogError::Aeron("no context".into()))) else {
        panic!("the spawn must fail");
    };
    assert_eq!(e.to_string(), "aeron: no context");
}

/// With no media driver, the C client waits its whole driver timeout for
/// the `CnC` file, and then fails. A driver timeout above 10 s must give
/// that error, not a start timeout of the runtime.
#[test]
fn the_spawn_waits_out_a_driver_timeout_above_ten_seconds() {
    let dir = tempfile::tempdir().unwrap();
    let dir_c = crate::ffi::dir_cstring(dir.path()).unwrap();
    let begin = Instant::now();
    let Err(e) = AeronRuntime::spawn_with(move || {
        let ctx = AeronContext::new().map_err(|e| LogError::Aeron(e.to_string()))?;
        ctx.set_dir(dir_c.as_c_str())
            .map_err(|e| LogError::Aeron(e.to_string()))?;
        ctx.set_driver_timeout_ms(11_000)
            .map_err(|e| LogError::Aeron(e.to_string()))?;
        Ok(ctx)
    }) else {
        panic!("the spawn must fail with no media driver");
    };
    let msg = e.to_string();
    assert!(msg.starts_with("aeron: Aeron::new"), "{msg}");
    assert!(begin.elapsed() >= Duration::from_secs(11), "{msg}");
}
