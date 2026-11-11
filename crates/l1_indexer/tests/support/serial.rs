//! One test at a time over the process lifecycle.

use kardamom_obs::lifecycle::process;

/// The process lifecycle holds one halt for the whole binary, so two
/// tests that use it must not run at the same time. A plain `cargo test`
/// runs the tests on parallel threads.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The turn of one test: the lifecycle is clear and running, and no other
/// test of the binary holds it. The turn ends when the value drops, so a
/// fixture keeps it as its last field.
pub(crate) struct Turn(tokio::sync::MutexGuard<'static, ()>);

impl Turn {
    /// Wait for the turn, then clear the halt and resume the process.
    pub(crate) async fn take() -> Self {
        let guard = SERIAL.lock().await;
        process().clear();
        process().resume();
        Self(guard)
    }
}
