//! Quorum fsync-watermark aggregator.
//!
//! Per-recorder fsync positions arrive on N independent Aeron streams. The
//! aggregator keeps the latest position per recorder, and on every update
//! computes the Q-th largest known position — that is the watermark proxies
//! consume for the I2 ack guarantee.
//!
//! Liveness is *not* tracked here: a dead recorder's slot simply stops
//! advancing, and the quorum stalls past it once Q-1 survivors have moved
//! beyond it. The supervisor is responsible for restarting dead recorders.

use kardamom_types::{BPosition, FsyncWatermark};

#[derive(Clone, Debug)]
pub struct QuorumState {
    n: usize,
    q: usize,
    /// `positions[i] = Some(p)` once recorder `i` has reported at least once.
    positions: Vec<Option<BPosition>>,
}

impl QuorumState {
    pub fn new(n: usize, q: usize) -> Self {
        assert!(q >= 1 && q <= n, "0 < q <= n required (got q={q}, n={n})");
        Self {
            n,
            q,
            positions: vec![None; n],
        }
    }

    pub fn n(&self) -> usize {
        self.n
    }

    pub fn q(&self) -> usize {
        self.q
    }

    pub fn observe(&mut self, w: FsyncWatermark) {
        let i = w.recorder_id as usize;
        assert!(i < self.n, "recorder_id {i} out of range for N={}", self.n);
        // Monotonic per recorder: never accept a regression.
        match self.positions[i] {
            Some(prev) if prev >= w.position => {}
            _ => self.positions[i] = Some(w.position),
        }
    }

    /// Returns the highest position that at least Q recorders have fsynced
    /// past, or `None` if fewer than Q recorders have reported.
    ///
    /// Equivalent to the Q-th *largest* among reported positions: position P
    /// is "Q-acked" iff `|{i : pos[i] >= P}| >= Q`, and the largest such P is
    /// the Q-th largest reported position.
    pub fn quorum(&self) -> Option<BPosition> {
        let mut known: Vec<BPosition> = self.positions.iter().copied().flatten().collect();
        if known.len() < self.q {
            return None;
        }
        known.sort();
        // Q-th largest: index `known.len() - q` of the ascending-sorted slice.
        Some(known[known.len() - self.q])
    }
}

// ---------------------------------------------------------------------------
// QuorumAggregator runner task (gated behind `aeron-live`)
// ---------------------------------------------------------------------------

#[cfg(feature = "aeron-live")]
mod aggregator {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::task::JoinHandle;

    use crate::config::QuorumConfig;
    use crate::error::LogError;
    use crate::publisher::QuorumPublisher;
    use crate::subscriber::Subscribers;
    use kardamom_types::QuorumWatermark;

    use super::QuorumState;

    /// Tokio task that drains all N per-recorder watermark subscriptions and
    /// republishes the quorum position whenever it advances.
    pub struct QuorumAggregator {
        pub handle: JoinHandle<()>,
    }

    impl QuorumAggregator {
        pub fn start(
            subscribers: Subscribers,
            publisher: Arc<QuorumPublisher>,
            cfg: QuorumConfig,
        ) -> Result<Self, LogError> {
            let mut state = QuorumState::new(cfg.n, cfg.q);
            let mut subs: Vec<_> = (0..cfg.n)
                .map(|rid| subscribers.watermark(rid as u8))
                .collect::<Result<_, _>>()?;

            let handle = tokio::task::spawn_blocking(move || {
                let mut last_published = None;
                loop {
                    let mut any = false;
                    for sub in subs.iter_mut() {
                        any |= sub.poll(|w, _| state.observe(w), 64) > 0;
                    }
                    if any {
                        if let Some(p) = state.quorum() {
                            if last_published != Some(p) {
                                if let Err(e) = publisher.publish(&QuorumWatermark { position: p }) {
                                    tracing::error!(error = %e, "quorum publish failed");
                                } else {
                                    last_published = Some(p);
                                }
                            }
                        }
                    }
                    if !any {
                        std::thread::sleep(Duration::from_micros(50));
                    }
                }
            });
            Ok(Self { handle })
        }
    }
}

#[cfg(feature = "aeron-live")]
pub use aggregator::QuorumAggregator;
