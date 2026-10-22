//! The ingress service inside the sequencer process.
//!
//! The `ingress` subcommand takes the flags of `kardamom-ingress`. The
//! service then runs on its own Tokio runtime on its own thread, so the
//! JSON-RPC work never takes a worker of the sequencer runtime. It shares
//! the media driver, the log config, and the `[cluster]` and `[cache]`
//! sections of the sequencer. Its metrics go to the exporter of the
//! sequencer.
//!
//! The egress tap of the ingress is a second cluster session. It needs an
//! egress endpoint of its own, `--cluster-egress-endpoint` after the
//! subcommand: the session of the sequencer holds the endpoint of the
//! sequencer. Without it, the ingress starts no egress tap. The variable
//! `KARDAMOM_CLUSTER_EGRESS_ENDPOINT` sets the flag of both, so a process
//! with the ingress takes the endpoints from the flags.

use anyhow::{Context, Result};
use kardamom_ingress::config::IngressFileConfig;
use kardamom_ingress::service::{IngressService, IngressSettings};
use kardamom_log::config::LogConfig;
use kardamom_sequencer::config::{ClusterConfig, SequencerConfig};
use tokio::sync::oneshot;

use crate::args::Args;

/// The worker threads of the ingress runtime, as in `kardamom-ingress`.
const INGRESS_WORKERS: usize = 4;

/// A service that runs inside the sequencer process.
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Embedded {
    /// Runs the ingress service in this process. It takes the flags of
    /// `kardamom-ingress`, except the process flags: the config files, the
    /// Aeron directory, the metrics address and the host id come from the
    /// sequencer.
    Ingress(IngressSettings),
}

/// A running ingress inside this process: the stop signal and the thread
/// that runs its runtime.
pub(crate) struct EmbeddedIngress {
    stop: oneshot::Sender<()>,
    thread: std::thread::JoinHandle<()>,
}

impl EmbeddedIngress {
    /// Starts the ingress when the flags ask for it, and returns when it
    /// serves. Returns `None` when the flags name no ingress.
    ///
    /// # Errors
    ///
    /// Returns a failed runtime or thread start, or the start error of
    /// the service.
    pub(crate) async fn start(
        args: &Args,
        cfg: &SequencerConfig,
        log_cfg: &LogConfig,
    ) -> Result<Option<Self>> {
        let Some(Embedded::Ingress(settings)) = &args.embedded else {
            return Ok(None);
        };
        kardamom_ingress::metrics::describe();
        kardamom_cache::metrics::describe();
        let service = IngressService {
            settings: settings.clone(),
            aeron_dir: args.aeron_dir.clone(),
            log_cfg: log_cfg.clone(),
            file_cfg: IngressFileConfig {
                cluster: ClusterConfig {
                    egress_channel: String::new(),
                    ..cfg.cluster.clone()
                },
                cache: cfg.cache.clone(),
            },
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(INGRESS_WORKERS)
            .thread_name("ingress")
            .enable_all()
            .build()
            .context("build the ingress runtime")?;
        let (started_tx, started) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("ingress-main".into())
            .spawn(move || {
                runtime.block_on(
                    IngressThread {
                        service,
                        started: started_tx,
                        stop: stop_rx,
                    }
                    .run(),
                );
            })
            .context("spawn the ingress thread")?;
        started
            .await
            .context("the ingress thread ended before the start")??;
        tracing::info!(
            ingress_id = settings.ingress_id(),
            "kardamom-sequencer: the ingress runs in this process"
        );
        Ok(Some(Self { stop, thread }))
    }

    /// Drains the ingress and waits until its runtime ends. The sequencer
    /// loops must still run: a parked submit waits for their verdicts.
    pub(crate) async fn stop(self) {
        let _ = self.stop.send(());
        let thread = self.thread;
        let joined = tokio::task::spawn_blocking(move || thread.join()).await;
        if !matches!(joined, Ok(Ok(()))) {
            tracing::warn!("kardamom-sequencer: the ingress thread panicked");
        }
    }
}

/// The body of the ingress thread: the service, the channel that reports
/// its start, and the stop signal.
struct IngressThread {
    service: IngressService,
    started: oneshot::Sender<Result<()>>,
    stop: oneshot::Receiver<()>,
}

impl IngressThread {
    /// Start the service, report the start, wait for the stop signal, then
    /// drain and stop. A dropped stop sender also stops the service. The
    /// runtime ends on this thread when the body returns.
    async fn run(self) {
        match self.service.run().await {
            Ok(running) => {
                let _ = self.started.send(Ok(()));
                let _ = self.stop.await;
                running.stop().await;
            }
            Err(e) => {
                let _ = self.started.send(Err(e));
            }
        }
    }
}
