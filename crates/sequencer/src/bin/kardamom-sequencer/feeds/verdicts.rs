//! The verdict task: the verdicts of the v2 loop onto the `tx_verdicts`
//! stream of each target ingress.
//!
//! The task owns its Aeron runtime and its stream plane, so its offers do
//! not share a polling thread with the `tx_data` or the receipts
//! subscription. It opens the publication of an ingress on the first frame
//! for that ingress. Each offer is best effort: a slow or absent ingress
//! holds only the frames of its own publication, until the offer deadline.

use anyhow::Context;
use kardamom_log::aeron_live::{AeronRuntime, TxVerdictsPublisherHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;
use kardamom_sequencer::sequencer::Shutdown;
use kardamom_sequencer::v2::VerdictOut;
use kardamom_types::TxStatus;
use tokio::sync::mpsc;

/// The verdicts that may wait for the verdict task. A verdict answers one
/// transaction, so this covers a fraction of a second of a full lane. A
/// full queue drops verdicts, never the loop's progress.
const QUEUE: usize = 65_536;

/// One publication for each ingress id that a verdict went to.
type Publications = Box<[Option<TxVerdictsPublisherHandle>; 256]>;

pub(crate) struct VerdictFeed {
    rt: AeronRuntime,
    plane: StreamPlane,
    publications: Publications,
}

impl VerdictFeed {
    fn new(rt: AeronRuntime, plane: StreamPlane) -> Self {
        Self {
            rt,
            plane,
            publications: Box::new(std::array::from_fn(|_| None)),
        }
    }

    /// Start the task on its own runtime and a stream plane labelled
    /// `label`. Returns the queue of the loop and the task handle.
    pub(crate) fn start(
        log_cfg: &LogConfig,
        label: &str,
        aeron_dir: Option<&std::path::Path>,
        shutdown: &Shutdown,
    ) -> anyhow::Result<(mpsc::Sender<VerdictOut>, tokio::task::JoinHandle<()>)> {
        let rt = AeronRuntime::spawn(aeron_dir).context("spawn verdicts AeronRuntime")?;
        let plane =
            StreamPlane::from_config(log_cfg, label).context("build the verdict stream plane")?;
        let (frames, rx) = mpsc::channel(QUEUE);
        let task = Self::new(rt, plane).spawn(rx, shutdown.clone());
        tracing::info!("sequencer v2: the verdict of each transaction goes to its ingress");
        Ok((frames, task))
    }

    /// Spawn the task. It ends at shutdown, or when the loop drops its end
    /// of `frames`. It then ends its plane, and its runtime drops.
    fn spawn(
        self,
        frames: mpsc::Receiver<VerdictOut>,
        shutdown: Shutdown,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run(frames, shutdown))
    }

    async fn run(mut self, mut frames: mpsc::Receiver<VerdictOut>, shutdown: Shutdown) {
        while let Some(frame) = Self::next(&mut frames, &shutdown).await {
            self.on_frame(frame).await;
        }
        self.plane.shutdown().await;
    }

    async fn next(
        frames: &mut mpsc::Receiver<VerdictOut>,
        shutdown: &Shutdown,
    ) -> Option<VerdictOut> {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => None,
            frame = frames.recv() => frame,
        }
    }

    async fn on_frame(&mut self, frame: VerdictOut) {
        self.publish(frame.ingress, &frame.status).await;
    }

    async fn publish(&mut self, ingress: u8, status: &TxStatus) {
        let Some(publication) = self.publication(ingress).await else {
            return;
        };
        if let Err(e) = publication.publish_status(status) {
            tracing::warn!(ingress, error = %e, "a verdict did not encode");
        }
    }

    /// The publication of `ingress`, opened on its first frame. A failed
    /// open drops the frame, and the next frame tries again.
    async fn publication(&mut self, ingress: u8) -> Option<&TxVerdictsPublisherHandle> {
        let slot = &mut self.publications[usize::from(ingress)];
        if slot.is_none() {
            match self.plane.tx_verdicts_publisher(&self.rt, ingress).await {
                Ok(publication) => *slot = Some(publication),
                Err(e) => {
                    tracing::warn!(ingress, error = %e, "the verdict publication did not open");
                }
            }
        }
        slot.as_ref()
    }
}
