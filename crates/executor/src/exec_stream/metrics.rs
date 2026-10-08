//! The metrics of the executor stream.

use std::time::Duration;

/// The recorded cursor: every slot at or below it is dispatched, and the
/// local archive has written every record of those slots.
pub(crate) const RECORDED_INDEX: &str = "kardamom_executor_exec_stream_recorded_index";

/// The Aeron session id of the recorded publication. A restarted executor
/// shows a new value.
pub(crate) const SESSION_ID: &str = "kardamom_executor_exec_stream_session_id";

/// The time the publisher waited for the recorded publication to take a
/// record, in milliseconds.
pub(crate) const PUBLISH_BLOCKED_MS_TOTAL: &str =
    "kardamom_executor_exec_stream_publish_blocked_ms_total";

/// The executor-stream metrics.
pub struct ExecStreamMetrics;

impl ExecStreamMetrics {
    /// Register the descriptions of the executor-stream metrics.
    pub fn describe() {
        metrics::describe_gauge!(
            RECORDED_INDEX,
            "the highest canonical index whose records the local archive has written"
        );
        metrics::describe_gauge!(
            SESSION_ID,
            "the Aeron session id of the recorded exec_txs publication"
        );
        metrics::describe_counter!(
            PUBLISH_BLOCKED_MS_TOTAL,
            "milliseconds the exec stream publisher waited for the archive to take a record"
        );
    }

    pub(crate) fn session(session_id: i32) {
        metrics::gauge!(SESSION_ID).set(f64::from(session_id));
    }

    /// Show the cursor, when it moved.
    #[allow(
        clippy::cast_precision_loss,
        reason = "canonical indices stay far below 2^52"
    )]
    pub(crate) fn recorded(moved: Option<u64>) {
        if let Some(through) = moved {
            metrics::gauge!(RECORDED_INDEX).set(through as f64);
        }
    }

    pub(crate) fn blocked(waited: Duration) {
        // A wait of 2^64 ms does not occur. The cap only keeps the counter
        // monotone.
        let ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX);
        metrics::counter!(PUBLISH_BLOCKED_MS_TOTAL).increment(ms);
    }
}
