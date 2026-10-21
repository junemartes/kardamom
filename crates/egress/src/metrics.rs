//! The metrics of the egress.

/// The copies of a canonical frame whose bytes differ from the first copy.
pub const COPY_MISMATCH_TOTAL: &str = "kardamom_egress_copy_mismatch_total";
/// The times that every copy missed a frame and the merge dropped the
/// frames that waited behind it.
pub const WAITING_DROPPED_TOTAL: &str = "kardamom_egress_waiting_dropped_total";

pub(crate) fn count_copy_mismatch() {
    metrics::counter!(COPY_MISMATCH_TOTAL).increment(1);
}

pub(crate) fn count_waiting_dropped() {
    metrics::counter!(WAITING_DROPPED_TOTAL).increment(1);
}

/// Describe every metric of the egress for the exporter.
pub fn describe() {
    metrics::describe_counter!(
        COPY_MISMATCH_TOTAL,
        "Copies of a canonical frame whose bytes differ from the first copy."
    );
    metrics::describe_counter!(
        WAITING_DROPPED_TOTAL,
        "Times that every copy missed a frame and the merge dropped the frames behind it."
    );
}
