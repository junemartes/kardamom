//! Deterministic lowest-host-id-among-caught-up-recorders lease.
//!
//! V0 implementation: a host holds the lease iff it has the lowest host id
//! among recorders whose latest `FsyncWatermark.position` is within
//! `caught_up_window` bytes of the current `QuorumWatermark`. Fully
//! deterministic — no external KV, no consensus library.

use std::collections::HashMap;

use kardamom_types::{BPosition, FsyncWatermark, QuorumWatermark};

#[derive(Clone, Debug)]
pub struct LeaseConfig {
    /// This host's recorder id.
    pub self_id: u8,
    /// All recorder ids in the cluster.
    pub all_ids: Vec<u8>,
    /// Bytes of stream lag that still count as "caught up".
    pub caught_up_window: i64,
}

/// Lease state machine. Feed it `FsyncWatermark` updates from each recorder
/// and the current `QuorumWatermark`; call [`Lease::held_by_us`] to learn
/// whether this host currently holds the lease.
#[derive(Clone, Debug)]
pub struct Lease {
    cfg: LeaseConfig,
    last_per_recorder: HashMap<u8, BPosition>,
    last_quorum: Option<BPosition>,
}

impl Lease {
    pub fn new(cfg: LeaseConfig) -> Self {
        Self {
            cfg,
            last_per_recorder: HashMap::new(),
            last_quorum: None,
        }
    }

    pub fn observe_fsync(&mut self, w: FsyncWatermark) {
        let prev = self.last_per_recorder.get(&w.recorder_id).copied();
        if prev.is_none_or(|p| p < w.position) {
            self.last_per_recorder.insert(w.recorder_id, w.position);
        }
    }

    pub fn observe_quorum(&mut self, q: QuorumWatermark) {
        self.last_quorum = Some(q.position);
    }

    /// Returns `true` if this host currently holds the lease.
    pub fn held_by_us(&self) -> bool {
        let quorum = match self.last_quorum {
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
                    .map(|p| within_window(*p, quorum, self.cfg.caught_up_window))
                    .unwrap_or(false)
            })
            .collect();
        caught_up_ids.iter().min() == Some(&self.cfg.self_id)
    }
}

fn within_window(pos: BPosition, quorum: BPosition, window: i64) -> bool {
    // Convert positions to absolute byte offsets using the same TERM_LEN as
    // the rest of the system (16 MiB). The exact constant must match the
    // recorder's `aeron.term.buffer.length`.
    const TERM_LEN: i64 = 16 * 1024 * 1024;
    let pos_abs = (pos.term_id as i64) * TERM_LEN + pos.term_offset as i64;
    let q_abs = (quorum.term_id as i64) * TERM_LEN + quorum.term_offset as i64;
    (q_abs - pos_abs).abs() <= window
}
