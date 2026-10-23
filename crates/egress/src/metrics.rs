//! The metrics of the egress.

/// The copies of a canonical frame whose bytes differ from the first copy.
pub const COPY_MISMATCH_TOTAL: &str = "kardamom_egress_copy_mismatch_total";
/// The times that every copy missed a frame and the merge dropped the
/// frames that waited behind it.
pub const WAITING_DROPPED_TOTAL: &str = "kardamom_egress_waiting_dropped_total";

/// The frames of a peer egress that filled the place between the head and
/// the copies.
pub const CAUGHT_FRAMES_TOTAL: &str = "kardamom_egress_caught_frames_total";
/// The times that the egress started its stream again: after a boundary of
/// a copy, with no peer (`reason="boundary"`), or at the floor of a peer
/// above its head (`reason="peer-floor"`).
pub const REBASED_TOTAL: &str = "kardamom_egress_rebased_total";

pub(crate) fn count_copy_mismatch() {
    metrics::counter!(COPY_MISMATCH_TOTAL).increment(1);
}

pub(crate) fn count_waiting_dropped() {
    metrics::counter!(WAITING_DROPPED_TOTAL).increment(1);
}

pub(crate) fn count_caught(frames: usize) {
    metrics::counter!(CAUGHT_FRAMES_TOTAL).increment(u64::try_from(frames).unwrap_or(u64::MAX));
}

pub(crate) fn count_rebased(reason: &'static str) {
    metrics::counter!(REBASED_TOTAL, "reason" => reason).increment(1);
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
    metrics::describe_counter!(
        CAUGHT_FRAMES_TOTAL,
        "Frames of a peer egress that filled the place between the head and the copies."
    );
    metrics::describe_counter!(
        REBASED_TOTAL,
        "Times that the egress started its stream again, by reason."
    );
}
