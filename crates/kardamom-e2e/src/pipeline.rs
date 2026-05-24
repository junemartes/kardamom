//! Convenience helpers for the cross-component pipeline test.
//!
//! The actual pipeline composition lives in `tests/full_pipeline_e2e.rs`
//! (behind the `full-pipeline-e2e` feature). This module exposes a couple of
//! small helpers that test code reuses without needing to thread feature
//! gates through every call site:
//!
//! - [`channel_uri_for`] — canonical URI builder for the e2e test's channels,
//!   parameterised by a unique session id so concurrent test invocations don't
//!   collide on the same Aeron media driver.
//!
//! Anything heavier (the live `AeronRuntime`, the `AeronTestCluster` harness,
//! the actual subsystem wiring) lives in the test file behind the gate.

/// Build an Aeron IPC URI for one of the pipeline's channels.
///
/// We use `aeron:ipc?alias=<session>-<chan>` because the e2e test runs
/// in-process against a single Aeron media driver in a single container; IPC
/// is the fastest and most reliable transport for that topology. The
/// `session_id` parameter scopes URIs per-test-run so reruns and parallel
/// runs don't share residual stream state.
pub fn channel_uri_for(session_id: &str, channel_name: &str) -> String {
    format!("aeron:ipc?alias={session_id}-{channel_name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_is_deterministic() {
        assert_eq!(channel_uri_for("s1", "b"), "aeron:ipc?alias=s1-b");
        assert_eq!(channel_uri_for("s1", "c"), "aeron:ipc?alias=s1-c");
    }
}
