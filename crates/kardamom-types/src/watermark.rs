//! Fsync watermark types.

use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;

/// Single-recorder fsync progress. Published on a per-recorder watermark stream.
///
/// The ingress proxy subscribes to one of these streams (typically its
/// co-located recorder's) and releases acks once the watermark advances past
/// the tx's B-position (D-Sh13). There is no quorum aggregation on the ack
/// path — see `docs/plans/2026-05-23-S0-shared-decisions.md`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct FsyncWatermark {
    pub recorder_id: u8,
    pub position: BPosition,
}
