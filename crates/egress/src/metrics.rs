//! The metrics of the egress.

/// The copies of a canonical frame whose bytes differ from the first copy.
pub const COPY_MISMATCH_TOTAL: &str = "kardamom_egress_copy_mismatch_total";
/// The times that every copy missed a frame and the merge dropped the
/// frames that waited behind it.
pub const WAITING_DROPPED_TOTAL: &str = "kardamom_egress_waiting_dropped_total";

/// The canonical indexes between the stored head and the newest record
/// that any executor copy sent.
pub const HEAD_DISTANCE_FRAMES: &str = "kardamom_egress_head_distance_frames";

pub(crate) fn set_head_distance(frames: u64) {
    // A distance past `u32::MAX` shows as `u32::MAX`: far past any replay
    // window of the sealer.
    metrics::gauge!(HEAD_DISTANCE_FRAMES).set(u32::try_from(frames).unwrap_or(u32::MAX));
}

/// The frames of a peer egress that filled the place between the head and
/// the copies.
pub const CAUGHT_FRAMES_TOTAL: &str = "kardamom_egress_caught_frames_total";
/// The times that the egress started its stream again: after a boundary of
/// a copy, with no peer (`reason="boundary"`), or at the floor of a peer
/// above its head (`reason="peer-floor"`).
pub const REBASED_TOTAL: &str = "kardamom_egress_rebased_total";

/// The highest canonical index of a stored transaction reference
/// (`RT_TXREF`), and its block. Both are 0 when the store holds none.
pub const REF_FRAME_MAX_INDEX: &str = "kardamom_egress_ref_frame_max_index";
pub const REF_FRAME_MAX_BLOCK: &str = "kardamom_egress_ref_frame_max_block";

pub(crate) fn set_ref_mark(mark: crate::ref_mark::RefMark) {
    #[allow(
        clippy::cast_precision_loss,
        reason = "indexes and blocks stay far below 2^52; f64 represents them exactly"
    )]
    {
        metrics::gauge!(REF_FRAME_MAX_INDEX).set(mark.index as f64);
        metrics::gauge!(REF_FRAME_MAX_BLOCK).set(mark.block as f64);
    }
}

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
    metrics::describe_gauge!(
        HEAD_DISTANCE_FRAMES,
        "Canonical indexes between the stored head and the newest record that any executor copy sent."
    );
    metrics::describe_counter!(
        CAUGHT_FRAMES_TOTAL,
        "Frames of a peer egress that filled the place between the head and the copies."
    );
    metrics::describe_gauge!(
        REF_FRAME_MAX_INDEX,
        "Highest canonical index of a stored transaction reference (RT_TXREF); 0 with none."
    );
    metrics::describe_gauge!(
        REF_FRAME_MAX_BLOCK,
        "Block of the highest stored transaction reference (RT_TXREF); 0 with none."
    );
    metrics::describe_counter!(
        REBASED_TOTAL,
        "Times that the egress started its stream again, by reason."
    );
}
