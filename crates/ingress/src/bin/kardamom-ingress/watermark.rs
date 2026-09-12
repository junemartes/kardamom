//! The cluster egress watermark thread: folds the cluster's egress
//! progress into the proxy's on-quorum watermark bus.

use std::ops::ControlFlow;

use kardamom_cluster_adapter::LiveEgress;
use kardamom_ingress::cluster::ClusterWatermarkObserver;
use kardamom_types::QuorumWatermark;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// The watermark thread's state: the egress observer, the watermark bus,
/// and the stop token. The observer holds the `!Send` cluster client, so
/// the loop runs a blocking egress poll on a dedicated std thread. The
/// bus is a tokio `broadcast` channel, so the send never blocks, and a
/// send with no live receiver is not an error here.
pub(crate) struct ClusterWatermarkPump {
    observer: ClusterWatermarkObserver<LiveEgress>,
    tx: broadcast::Sender<QuorumWatermark>,
    stop: CancellationToken,
}

impl ClusterWatermarkPump {
    pub(crate) fn new(
        observer: ClusterWatermarkObserver<LiveEgress>,
        tx: broadcast::Sender<QuorumWatermark>,
        stop: CancellationToken,
    ) -> Self {
        Self { observer, tx, stop }
    }

    /// Spawn the thread. It stops on the stop token, or when the
    /// observer ends.
    ///
    /// # Errors
    ///
    /// Returns the OS error if the thread cannot be spawned.
    pub(crate) fn spawn(self) -> std::io::Result<std::thread::JoinHandle<()>> {
        std::thread::Builder::new()
            .name("cluster-watermark".into())
            .spawn(move || self.run())
    }

    fn run(mut self) {
        while let ControlFlow::Continue(()) = self.step() {}
    }

    /// Poll one egress position and send it as the durable count.
    /// `Break` ends the thread: the stop token fired, or the observer
    /// ended.
    fn step(&mut self) -> ControlFlow<()> {
        if self.stop.is_cancelled() {
            return ControlFlow::Break(());
        }
        let Some(position) = self.observer.next_position() else {
            return ControlFlow::Break(());
        };
        let _ = self.tx.send(QuorumWatermark { position });
        ControlFlow::Continue(())
    }
}
