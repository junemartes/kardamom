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

use kardamom_types::{BPosition, FsyncWatermark, QuorumWatermark};

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
// Shared aggregation loop + runner task.
// ---------------------------------------------------------------------------

/// How long the loop idles when no subscription yielded a fragment, to avoid
/// a busy-spin. Small enough that quorum-advance tail latency stays low.
const IDLE_BACKOFF: std::time::Duration = std::time::Duration::from_micros(50);

/// Drain all N per-recorder fsync-watermark subscriptions and republish the
/// aggregated quorum position whenever it advances, until `should_stop`.
///
/// Synchronous and thread-confined: `subs`/`publisher` wrap thread-confined
/// Aeron handles, so this must run on the thread that owns them. Both the
/// tokio [`QuorumAggregator`] (via `spawn_blocking`) and the standalone
/// `kardamom-recorder --aggregate` process call this — one loop, one place to
/// reason about the quorum-advance/publish semantics.
pub fn run_quorum_loop(
    subs: &mut [crate::subscriber::WatermarkSubscriber],
    state: &mut QuorumState,
    publisher: &crate::publisher::QuorumPublisher,
    mut should_stop: impl FnMut() -> bool,
) {
    let mut last_published = None;
    while !should_stop() {
        let mut any = false;
        for sub in subs.iter_mut() {
            any |= sub.poll(|w, _| state.observe(w), 64) > 0;
        }
        if any
            && let Some(p) = state.quorum()
            && last_published != Some(p)
        {
            if let Err(e) = publisher.publish(&QuorumWatermark { position: p }) {
                tracing::error!(error = %e, "quorum publish failed");
            } else {
                last_published = Some(p);
            }
        }
        if !any {
            std::thread::sleep(IDLE_BACKOFF);
        }
    }
}

mod aggregator {
    use std::sync::Arc;

    use tokio::task::JoinHandle;

    use crate::config::QuorumConfig;
    use crate::error::LogError;
    use crate::publisher::QuorumPublisher;
    use crate::subscriber::Subscribers;

    use super::{QuorumState, run_quorum_loop};

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
                // Never stops on its own; the task is aborted on shutdown.
                run_quorum_loop(&mut subs, &mut state, &publisher, || false);
            });
            Ok(Self { handle })
        }
    }
}
pub use aggregator::QuorumAggregator;
