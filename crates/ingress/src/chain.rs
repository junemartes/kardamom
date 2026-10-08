//! The ingress's view of the chain, and its reaction to it.
//!
//! The sealer has no Rust runtime on the `events` stream, so the ingress
//! observes it on its cluster session and keeps its state in a lifecycle
//! of its own (`service = "sealer"`, `instance = "cluster"`): halted on
//! `record_lag` while the status frame says the record-lag guard refuses,
//! on `da_lag` while it says the DA-lag guard refuses, and on
//! `sealer_no_quorum` while no status frame arrives for
//! [`SEALER_SILENCE`]. The binary publishes that lifecycle on the stream.
//!
//! The ingress pauses submits on a root: the sealer's halt first, then
//! every live executor halted (no receipt can come). One executor halted
//! changes nothing. The pause ends by itself when the root clears.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use kardamom_obs::events::BoardView;
use kardamom_obs::halt::{Halt, HaltCause, HaltRef, Record};
use kardamom_obs::lifecycle::{Lifecycle, Slots, process};
use kardamom_types::ClusterStatus;
use kardamom_types::cluster_status::RecordLagStatus;
use tokio::sync::watch;
use tokio::time::Instant;

/// The service name the ingress publishes the sealer's state under.
pub const SEALER: &str = HaltRef::SEALER;
/// The instance name of the sealer's state: the cluster as a whole.
pub const SEALER_INSTANCE: &str = HaltRef::SEALER_INSTANCE;
/// The service name of the executors on the stream.
pub const EXECUTOR: &str = "executor";
/// The service name of the da-watcher on the stream.
pub const DA_WATCHER: &str = "da-watcher";
/// The service name of the batcher on the stream.
pub const BATCHER: &str = "batcher";

/// How long the cluster may send no status frame before the ingress
/// calls the sealer halted on a lost quorum. The sealer sends one on
/// every boundary tick.
pub use kardamom_obs::events::SEALER_SILENCE;

/// How often the watch checks the silence and the board when nothing
/// arrives.
const TICK: Duration = Duration::from_secs(1);

/// The sealer's lifecycle as the ingress observes it, with the gauges
/// labelled `service="sealer"`.
pub type SealerLifecycle = Arc<Lifecycle>;

/// The task that mirrors the cluster status into the sealer's lifecycle
/// and follows the root of the ingress's pause.
pub(crate) struct ChainWatch {
    status: watch::Receiver<ClusterStatus>,
    board: watch::Receiver<BoardView>,
    sealer: SealerLifecycle,
    /// When the last status frame arrived. `None` until the first one:
    /// an ingress without a cluster session never calls the sealer
    /// silent.
    last_status: Option<Instant>,
}

impl ChainWatch {
    pub(crate) fn new(
        status: watch::Receiver<ClusterStatus>,
        board: watch::Receiver<BoardView>,
        sealer: SealerLifecycle,
    ) -> Self {
        Self {
            status,
            board,
            sealer,
            last_status: None,
        }
    }

    pub(crate) fn spawn(mut self) {
        tokio::spawn(async move { while self.step().await.is_continue() {} });
    }

    /// Wait for a status, a board change, or a tick; then follow the
    /// root. `Break` when the status channel closes.
    async fn step(&mut self) -> ControlFlow<()> {
        tokio::select! {
            changed = self.status.changed() => match changed {
                Ok(()) => {
                    let status = *self.status.borrow_and_update();
                    self.on_status(&status, Instant::now());
                }
                Err(_) => return ControlFlow::Break(()),
            },
            _ = self.board.changed() => {}
            () = tokio::time::sleep(TICK) => {}
        }
        self.check_silence(Instant::now());
        process().follow(Self::submit_root(
            &self.sealer.slots(),
            &self.board.borrow(),
        ));
        ControlFlow::Continue(())
    }

    /// Mirror one status frame: the gauges, and the sealer's
    /// `record_lag` or `da_lag` halt. Any status frame proves the quorum,
    /// so it also ends a `sealer_no_quorum` halt.
    fn on_status(&mut self, status: &ClusterStatus, now: Instant) {
        self.last_status = Some(now);
        crate::metrics::record_cluster_status(status);
        match Self::status_halt(status) {
            Some(halt) => self.sealer.raise(halt),
            None => {
                self.sealer.clear();
            }
        }
    }

    /// The sealer's halt that one status frame names. When both guards
    /// refuse, the record lag is the root: the executors record before
    /// the batcher can post.
    fn status_halt(status: &ClusterStatus) -> Option<Halt> {
        Self::record_lag_halt(&status.record_lag).or_else(|| Self::da_lag_halt(status))
    }

    /// The `record_lag` halt while the record-lag guard refuses.
    fn record_lag_halt(lag: &RecordLagStatus) -> Option<Halt> {
        lag.halted.then(|| {
            let recorded = lag
                .best_recorded
                .map_or_else(|| "none".to_string(), |index| index.to_string());
            Halt::new(
                HaltCause::RecordLag,
                format!(
                    "the best recorded index is {recorded}; the budget is {} records",
                    lag.budget
                ),
            )
        })
    }

    /// The `da_lag` halt while the DA-lag guard refuses.
    fn da_lag_halt(status: &ClusterStatus) -> Option<Halt> {
        status.halted.then(|| {
            Halt::new(
                HaltCause::DaLag,
                format!(
                    "sealed head {} is {} blocks past the posted head {}; the budget is {}",
                    status.sealed_head,
                    status.lag(),
                    status.posted_head,
                    status.budget_blocks
                ),
            )
        })
    }

    /// Call the sealer halted on a lost quorum once no status frame
    /// arrived for [`SEALER_SILENCE`].
    fn check_silence(&self, now: Instant) {
        let Some(silent) = self
            .last_status
            .map(|at| now.saturating_duration_since(at))
            .filter(|silent| *silent >= SEALER_SILENCE)
        else {
            return;
        };
        self.sealer.raise(Halt::new(
            HaltCause::SealerNoQuorum,
            format!(
                "no status frame on the cluster session for {} s",
                silent.as_secs()
            ),
        ));
    }

    /// The root the ingress pauses submits on: the sealer's halt, then
    /// every live executor halted. `None` while the chain can take a
    /// transaction.
    pub(crate) fn submit_root(sealer: &Slots, board: &BoardView) -> Option<HaltRef> {
        Self::sealer_root(sealer).or_else(|| board.all_halted(EXECUTOR))
    }

    /// The sealer's halt as a root.
    pub(crate) fn sealer_root(sealer: &Slots) -> Option<HaltRef> {
        sealer.halt.as_ref().map(|halt| HaltRef::sealer(halt.cause))
    }
}

/// The chain status `kardamom_chainStatus` returns.
pub(crate) struct ChainStatus<'a> {
    pub(crate) cluster: ClusterStatus,
    pub(crate) sealer: &'a Slots,
    pub(crate) ingress: &'a Slots,
    pub(crate) board: &'a BoardView,
}

impl ChainStatus<'_> {
    /// The record: the heads, the roots, the summaries of the table's
    /// rows, and every service's latest state.
    pub(crate) fn to_json(&self) -> serde_json::Value {
        let roots: Vec<serde_json::Value> = ChainWatch::sealer_root(self.sealer)
            .into_iter()
            .chain(
                self.board
                    .roots()
                    .into_iter()
                    .filter(|r| r.service != SEALER),
            )
            .map(|root| root.to_json())
            .collect();
        serde_json::json!({
            "posted_head": self.cluster.posted_head,
            "sealed_head": self.cluster.sealed_head,
            "da_lag_budget_blocks": self.cluster.budget_blocks,
            "roots": roots,
            "sealer": self.sealer.to_json(),
            "ingress": self.ingress.to_json(),
            "batcher_halted": self.board.any_halted(BATCHER).map(|r| r.to_json()),
            "deposits_delayed": self.board.any_halted(DA_WATCHER).is_some(),
            "services": self.board.to_json(),
        })
    }
}

#[cfg(test)]
#[path = "chain_tests.rs"]
mod tests;
