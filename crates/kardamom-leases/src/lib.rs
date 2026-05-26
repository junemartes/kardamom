//! Lease primitive used by sequencer hot-standby (S2), sealer leader election
//! (S5), and L1 batcher leader election (S7).
//!
//! V0 implementation: deterministic *lowest-host-id among caught-up recorders*,
//! computed from per-recorder `FsyncWatermark` streams. No external KV, no
//! consensus library. A host "holds the lease" iff it has the lowest id among
//! recorders whose `FsyncWatermark.position` is within `caught_up_window` of
//! the quorum watermark.

pub mod lease;
pub use lease::{Lease, LeaseConfig};
