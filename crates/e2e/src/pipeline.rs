//! Helper functions for the cross-component pipeline test.
//!
//! The pipeline composition lives in `tests/full_pipeline_e2e.rs`
//! (behind the `full-pipeline-e2e` feature). This module has small helpers
//! that test code reuses. This avoids feature gates at every call site.
//!
//! - [`channel_uri_for`] — builds a canonical URI for the e2e test's
//!   channels. A unique session id stops concurrent test runs from using
//!   the same Aeron media driver.

/// Build an Aeron IPC URI for one of the pipeline's channels.
///
/// The format is `aeron:ipc?alias=<session>-<chan>`. The e2e test runs
/// in-process against one Aeron media driver in one container, so IPC is
/// the fastest and most reliable transport. The `session_id` parameter
/// scopes URIs per test run, so reruns and parallel runs do not share
/// leftover stream state.
pub fn channel_uri_for(session_id: &str, channel_name: &str) -> String {
    format!("aeron:ipc?alias={session_id}-{channel_name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_is_deterministic() {
        assert_eq!(
            channel_uri_for("s1", "tx_data"),
            "aeron:ipc?alias=s1-tx_data"
        );
        assert_eq!(
            channel_uri_for("s1", "tx_receipts"),
            "aeron:ipc?alias=s1-tx_receipts"
        );
    }
}
