//! Fsync watermark types.

use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;

/// Single-recorder fsync progress. Published on a per-recorder watermark stream.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct FsyncWatermark {
    pub recorder_id: u8,
    pub position: BPosition,
}

/// Q-of-N aggregated fsync progress. Published on the shared watermark stream
/// that proxies subscribe to for the I2 ack guarantee.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct QuorumWatermark {
    pub position: BPosition,
}
