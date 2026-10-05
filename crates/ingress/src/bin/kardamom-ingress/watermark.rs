//! The cluster egress watermark thread: folds the cluster's egress
//! progress into the proxy's on-quorum watermark bus, and publishes a
//! `Sealed` status for every relayed transaction.

use std::ops::ControlFlow;

use kardamom_cluster_adapter::{LiveCluster, LiveEgress};
use kardamom_ingress::cluster::ClusterWatermarkObserver;
use kardamom_log::aeron_live::TxStatusPublisherHandle;
use kardamom_types::{QuorumWatermark, TxStatus};
use tokio::sync::broadcast;
use tokio_util::sync::{CancellationToken, DropGuard};

/// The watermark thread's state: the egress observer, the watermark bus,
/// the status publisher, and the stop token. The observer holds the
/// `!Send` cluster client, so the loop runs a blocking egress poll on a
/// dedicated std thread. The bus is a tokio `broadcast` channel, so the
/// send never blocks, and a send with no live receiver is not an error
/// here. The status publish is fire-and-forget on the Aeron thread.
pub(crate) struct ClusterWatermarkPump {
    observer: ClusterWatermarkObserver<LiveEgress>,
    tx: broadcast::Sender<QuorumWatermark>,
    status: TxStatusPublisherHandle,
    stop: CancellationToken,
}

/// A running watermark thread: the guard that cancels its stop token,
/// and the cluster session it polls. Field order is drop order: the
/// token cancels first, then the session ends. The thread is not joined.
/// Its egress poll returns only when the session ends, so the thread
/// exits on its own right after this value drops.
pub(crate) struct ClusterWatermark {
    #[allow(
        dead_code,
        reason = "held only for its Drop impl, which cancels the thread's stop token; never read"
    )]
    stop: DropGuard,
    #[allow(
        dead_code,
        reason = "held only for its Drop impl, which ends the cluster session; never read"
    )]
    cluster_guard: LiveCluster,
}

impl ClusterWatermarkPump {
    pub(crate) fn new(
        observer: ClusterWatermarkObserver<LiveEgress>,
        tx: broadcast::Sender<QuorumWatermark>,
        status: TxStatusPublisherHandle,
    ) -> Self {
        Self {
            observer,
            tx,
            status,
            stop: CancellationToken::new(),
        }
    }

    /// Spawn the thread. It stops when the returned [`ClusterWatermark`]
    /// drops, or when the observer ends. `cluster_guard` is the session
    /// the observer polls; the returned value keeps it alive for as long
    /// as the thread runs.
    ///
    /// # Errors
    ///
    /// Returns the OS error if the thread cannot be spawned.
    pub(crate) fn spawn(self, cluster_guard: LiveCluster) -> std::io::Result<ClusterWatermark> {
        let stop = self.stop.clone().drop_guard();
        std::thread::Builder::new()
            .name("cluster-watermark".into())
            .spawn(move || self.run())?;
        Ok(ClusterWatermark {
            stop,
            cluster_guard,
        })
    }

    fn run(mut self) {
        while let ControlFlow::Continue(()) = self.step() {}
    }

    /// Poll one egress frame: send its durable count, and publish the
    /// `Sealed` status of the transaction it relayed. `Break` ends the
    /// thread: the stop token fired, or the observer ended.
    fn step(&mut self) -> ControlFlow<()> {
        if self.stop.is_cancelled() {
            return ControlFlow::Break(());
        }
        let Some(progress) = self.observer.next_progress() else {
            return ControlFlow::Break(());
        };
        if let Some(position) = progress.durable {
            let _ = self.tx.send(QuorumWatermark { position });
        }
        if let Some(tx_hash) = progress.sealed {
            self.publish_sealed(tx_hash);
        }
        ControlFlow::Continue(())
    }

    /// Publish one `Sealed` status. An encode failure is logged and the
    /// status dropped: the receipt stream stays the truth.
    fn publish_sealed(&self, tx_hash: alloy_primitives::B256) {
        if let Err(e) = self.status.publish_best_effort(&TxStatus::sealed(tx_hash)) {
            tracing::warn!(error = %e, "tx_status publish failed (dropped)");
        }
    }
}
