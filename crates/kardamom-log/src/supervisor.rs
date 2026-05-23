//! Spawns the Aeron Media Driver and the Aeron Archive as child processes.
//!
//! The Aeron client (rusteron) talks to the Media Driver over shared-memory
//! ring buffers in `aeron_dir`. The Archive talks to the Media Driver the same
//! way. We do not embed either — they are Java/C++ processes we drive.
//!
//! Restart policy: V0 is "if any child dies, log loudly and exit". Production-
//! grade restart is a follow-up.

use std::process::Stdio;
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tracing::{error, info, warn};

use crate::config::AeronConfig;
use crate::error::LogError;

pub struct Supervisor {
    cfg: AeronConfig,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Supervisor {
    pub fn new(cfg: AeronConfig) -> Self {
        Self {
            cfg,
            shutdown_tx: None,
        }
    }

    /// Spawn Media Driver, wait for its readiness file, then spawn Archive.
    /// Returns once both are up. Background task supervises restarts.
    pub async fn start(&mut self) -> Result<(), LogError> {
        std::fs::create_dir_all(&self.cfg.aeron_dir)?;
        std::fs::create_dir_all(&self.cfg.archive_dir)?;

        let md = spawn(&self.cfg.media_driver_cmd, &self.cfg).await?;
        info!(pid = md.id(), "media driver started");

        // Wait for the Media Driver to create its CnC file before launching the Archive.
        wait_for_path(&self.cfg.aeron_dir.join("cnc.dat"), Duration::from_secs(5)).await?;

        let arch = spawn(&self.cfg.archive_cmd, &self.cfg).await?;
        info!(pid = arch.id(), "archive started");

        let (tx, rx) = oneshot::channel();
        self.shutdown_tx = Some(tx);
        tokio::spawn(supervise(vec![md, arch], rx));
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

async fn spawn(argv: &[String], cfg: &AeronConfig) -> Result<Child, LogError> {
    let (exe, args) = argv
        .split_first()
        .ok_or_else(|| LogError::Supervisor("empty argv".into()))?;
    Command::new(exe)
        .args(args)
        .env("AERON_DIR", &cfg.aeron_dir)
        .env("AERON_ARCHIVE_DIR", &cfg.archive_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| LogError::Supervisor(format!("spawn {exe}: {e}")))
}

async fn wait_for_path(path: &std::path::Path, timeout: Duration) -> Result<(), LogError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if path.exists() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(LogError::Supervisor(format!(
                "timeout waiting for {path:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn supervise(mut children: Vec<Child>, mut shutdown: oneshot::Receiver<()>) {
    // V0: if any child dies, log loudly and exit. Production-grade restart
    // policy is a follow-up; for now the operator restarts the process.
    tokio::select! {
        _ = &mut shutdown => {
            for c in children.iter_mut() {
                let _ = c.start_kill();
            }
        }
        res = wait_any(&mut children) => {
            match res {
                Ok((i, status)) => warn!(child = i, ?status, "aeron child exited"),
                Err(e) => error!(error = %e, "aeron child wait failed"),
            }
        }
    }
}

async fn wait_any(children: &mut [Child]) -> std::io::Result<(usize, std::process::ExitStatus)> {
    let futs: Vec<_> = children
        .iter_mut()
        .enumerate()
        .map(|(i, c)| {
            Box::pin(async move {
                let s = c.wait().await?;
                Ok::<_, std::io::Error>((i, s))
            })
        })
        .collect();
    let (res, _idx, _rest) = futures::future::select_all(futs).await;
    res
}
