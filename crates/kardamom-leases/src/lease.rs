//! Deterministic lowest-host-id-among-caught-up-recorders lease.
//!
//! V0 implementation: a host holds the lease iff it has the lowest host id
//! among recorders whose latest `FsyncWatermark.position` is within
//! `caught_up_window` bytes of the most-advanced observed recorder
//! position. Fully deterministic — no external KV, no consensus library.
//!
//! Per D-Sh13 (S0 shared decisions) this lease is derived from per-recorder
//! `FsyncWatermark` streams only; there is no quorum-aggregated watermark.

use std::collections::HashMap;

use kardamom_types::{BPosition, FsyncWatermark};

#[derive(Clone, Debug)]
pub struct LeaseConfig {
    /// This host's recorder id.
    pub self_id: u8,
    /// All recorder ids in the cluster.
    pub all_ids: Vec<u8>,
    /// Bytes of stream lag that still count as "caught up".
    pub caught_up_window: i64,
}

/// Lease state machine. Feed it `FsyncWatermark` updates from every
/// recorder the host observes; call [`Lease::held_by_us`] to learn whether
/// this host currently holds the lease.
///
/// The "reference" position used to decide who is caught up is the
/// highest position observed across all per-recorder fsync streams.
#[derive(Clone, Debug)]
pub struct Lease {
    cfg: LeaseConfig,
    last_per_recorder: HashMap<u8, BPosition>,
}

impl Lease {
    pub fn new(cfg: LeaseConfig) -> Self {
        Self {
            cfg,
            last_per_recorder: HashMap::new(),
        }
    }

    pub fn observe_fsync(&mut self, w: FsyncWatermark) {
        let prev = self.last_per_recorder.get(&w.recorder_id).copied();
        if prev.is_none_or(|p| p < w.position) {
            self.last_per_recorder.insert(w.recorder_id, w.position);
        }
    }

    /// The reference position used to decide "caught up". Equals the
    /// highest position observed across all recorder streams, or `None`
    /// if no recorder has reported yet.
    fn reference_position(&self) -> Option<BPosition> {
        self.last_per_recorder.values().copied().max()
    }

    /// Returns `true` if this host currently holds the lease.
    pub fn held_by_us(&self) -> bool {
        let reference = match self.reference_position() {
            Some(p) => p,
            None => return false,
        };
        let caught_up_ids: Vec<u8> = self
            .cfg
            .all_ids
            .iter()
            .copied()
            .filter(|id| {
                self.last_per_recorder
                    .get(id)
                    .map(|p| within_window(*p, reference, self.cfg.caught_up_window))
                    .unwrap_or(false)
            })
            .collect();
        caught_up_ids.iter().min() == Some(&self.cfg.self_id)
    }
}

fn within_window(pos: BPosition, reference: BPosition, window: i64) -> bool {
    // Convert positions to absolute byte offsets using the same TERM_LEN as
    // the rest of the system (16 MiB). The exact constant must match the
    // recorder's `aeron.term.buffer.length`.
    const TERM_LEN: i64 = 16 * 1024 * 1024;
    let pos_abs = (pos.term_id as i64) * TERM_LEN + pos.term_offset as i64;
    let r_abs = (reference.term_id as i64) * TERM_LEN + reference.term_offset as i64;
    (r_abs - pos_abs).abs() <= window
}
