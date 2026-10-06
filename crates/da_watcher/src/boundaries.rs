//! The sealer's L1 origin, read off the boundaries of a boundary-only
//! cluster session.
//!
//! Every boundary carries the sealer's L1 origin: the last epoch it
//! committed. The sealer sends the boundaries to every session, so the
//! da-watcher's session needs no subscribe announcement. Its filter drops
//! every other egress kind on the session thread.
//!
//! The session thread and the watcher task are on different threads, so a
//! `watch` channel carries the origin between them. The watcher needs only
//! the newest origin, and the channel keeps only that.

use std::ops::ControlFlow;

use kardamom_cluster_adapter::gateway::ClusterEgress;
use kardamom_cluster_adapter::live::{self, ConnectOptions};
use kardamom_cluster_adapter::wire::{self, EgressItem};
use kardamom_cluster_adapter::{LiveCluster, LiveClusterConfig, LiveEgress, LiveError};
use kardamom_log::aeron_live::AeronRuntime;
use tokio::sync::watch;

/// Why the boundary session did not start.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The cluster session did not open.
    #[error("connect the boundary session: {0}")]
    Session(#[from] LiveError),
    /// The boundary thread did not spawn.
    #[error("spawn the boundary thread: {0}")]
    Thread(std::io::Error),
}

/// The egress kinds the da-watcher's session reads: the boundaries only.
pub const BOUNDARY_EGRESS_KINDS: [u8; 1] = [wire::EGRESS_KIND_BOUNDARY];

/// Reads boundaries off a cluster egress, and sends each new L1 origin to
/// the watcher. The receiver holds `None` before the first boundary.
pub struct BoundaryFeed<E> {
    egress: E,
    origin: watch::Sender<Option<u64>>,
}

impl<E: ClusterEgress> BoundaryFeed<E> {
    /// A feed over `egress`, and the receiver the watcher follows.
    pub fn new(egress: E) -> (Self, watch::Receiver<Option<u64>>) {
        let (origin, rx) = watch::channel(None);
        (Self { egress, origin }, rx)
    }

    /// Read boundaries until the egress closes or the watcher is gone.
    /// The egress read blocks, so this runs on its own thread.
    pub fn run(mut self) {
        while self.next().is_continue() {}
    }

    /// Read one frame, and send the origin when it is a boundary with a
    /// new origin. `Break` when the egress closed or no receiver is left.
    fn next(&mut self) -> ControlFlow<()> {
        let Some(frame) = self.egress.recv() else {
            return ControlFlow::Break(());
        };
        if let Ok(EgressItem::Boundary(boundary)) = EgressItem::decode(&frame) {
            self.origin.send_if_modified(|origin| {
                let new = Some(boundary.l1_origin);
                let changed = *origin != new;
                *origin = new;
                changed
            });
        }
        if self.origin.is_closed() {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

impl BoundaryFeed<LiveEgress> {
    /// Connect a boundary-only session to the sealer cluster, and read
    /// its boundaries on a thread of their own. The session never offers.
    /// Keep the returned [`LiveCluster`] alive while the watcher follows:
    /// dropping it stops the session, which closes the egress and ends
    /// the thread.
    ///
    /// # Errors
    ///
    /// Returns an error when the config is invalid, the session does not
    /// open, or the thread does not spawn.
    pub fn connect(
        rt: AeronRuntime,
        cfg: LiveClusterConfig,
    ) -> Result<(LiveCluster, watch::Receiver<Option<u64>>), ConnectError> {
        let (cluster, _ingress, egress) = live::connect_with(
            rt,
            cfg,
            ConnectOptions {
                egress_kind_filter: Some(BOUNDARY_EGRESS_KINDS.to_vec()),
                ..Default::default()
            },
        )?;
        let (feed, origins) = Self::new(egress);
        std::thread::Builder::new()
            .name("sealer-boundaries".into())
            .spawn(move || feed.run())
            .map_err(ConnectError::Thread)?;
        Ok((cluster, origins))
    }
}
