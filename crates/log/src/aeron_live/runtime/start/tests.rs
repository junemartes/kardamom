use std::time::Instant;

use super::*;
use crate::aeron_live::AeronRuntime;
use crate::driver_budget::DriverBudget;

/// The budget reads the driver timeout the context uses, so a value that
/// `make_ctx` sets wins over the environment. This needs no media driver.
#[test]
fn the_budget_follows_the_context() {
    let ctx = AeronContext::new().unwrap();
    ctx.set_driver_timeout_ms(30_000).unwrap();
    assert_eq!(
        DriverBudget::of_client(&ctx).unwrap().duration(),
        Duration::from_secs(35)
    );
}

#[test]
fn a_failed_context_build_fails_the_spawn() {
    let Err(e) = AeronRuntime::spawn_with(|| Err(LogError::Aeron("no context".into()))) else {
        panic!("the spawn must fail");
    };
    assert_eq!(e.to_string(), "aeron: no context");
}

/// With no media driver, the C client waits its driver timeout for the
/// `CnC` file, and then fails. A driver timeout above the context budget
/// must give that error, not a start timeout of the runtime. The C client
/// measures its timeout on its own coarse clock, so the test asserts only
/// that the wait passed the context budget, not the full 11 s.
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
    assert!(begin.elapsed() > CONTEXT_BUDGET, "{msg}");
}
