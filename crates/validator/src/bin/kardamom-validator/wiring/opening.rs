//! A source reset keeps the process alive while its unchanged pipeline reopens.

use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::Result;
use kardamom_log::LogError;
use tokio_util::sync::CancellationToken;

use super::run::Ready;
use super::startup::{Boot, Startup};

const RETRY: Duration = Duration::from_secs(1);

pub(super) struct ActivePipeline;

impl ActivePipeline {
    #[must_use]
    pub(super) fn new() -> Self {
        kardamom_validator::metrics::set_pipeline_active(true);
        Self
    }
}

impl Drop for ActivePipeline {
    fn drop(&mut self) {
        kardamom_validator::metrics::set_pipeline_active(false);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpenMode {
    #[default]
    Normal,
    Resume,
}

pub(super) struct Opening {
    mode: OpenMode,
    stop: CancellationToken,
}

impl Opening {
    pub(super) fn new(mode: OpenMode, stop: CancellationToken) -> Self {
        Self { mode, stop }
    }

    pub(super) async fn ready(&self, boot: &Boot) -> Result<Option<Ready>> {
        loop {
            match Box::pin(self.attempt(boot)).await? {
                ControlFlow::Break(ready) => return Ok(ready),
                ControlFlow::Continue(()) => (),
            }
        }
    }

    async fn attempt(&self, boot: &Boot) -> Result<ControlFlow<Option<Ready>>> {
        if self.stop.is_cancelled() {
            return Ok(ControlFlow::Break(None));
        }
        match Box::pin(boot.open_pipeline()).await {
            Ok(ready) => Ok(ControlFlow::Break(Some(ready))),
            Err(error) => self
                .after_failure(error)
                .await
                .map(|flow| flow.map_break(|()| None)),
        }
    }

    /// Only a typed source reset enables retry of a catalog availability
    /// failure. State, configuration and permission failures retain their
    /// errors, also when reopening a previously running pipeline.
    async fn after_failure(&self, error: anyhow::Error) -> Result<ControlFlow<()>> {
        if self.mode == OpenMode::Normal {
            return Err(error);
        }
        if self.stop.is_cancelled() {
            return Ok(ControlFlow::Break(()));
        }
        if !error
            .downcast_ref::<LogError>()
            .is_some_and(LogError::is_transient)
        {
            return Err(error);
        }
        tracing::warn!(error = %format!("{error:#}"), "validator source-reset reopen failed; retrying");
        Ok(self.wait().await)
    }

    pub(super) async fn wait(&self) -> ControlFlow<()> {
        tokio::select! {
            biased;
            () = self.stop.cancelled() => ControlFlow::Break(()),
            () = tokio::time::sleep(RETRY) => ControlFlow::Continue(()),
        }
    }
}

impl Boot {
    async fn open_pipeline(&self) -> Result<Ready> {
        Ok(Startup::from_boot(self)?
            .open_state()?
            .open_streams()
            .await?
            .spawn_pumps()?
            .spawn_writer()?
            .spawn_attester()?
            .build_sink())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_activity_ends_with_the_pipeline_guard() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let active = |value: u8| {
            recorder.handle().render().lines().any(|line| {
                line.starts_with(kardamom_validator::metrics::PIPELINE_ACTIVE)
                    && line.ends_with(&format!(" {value}"))
            })
        };
        metrics::with_local_recorder(&recorder, || {
            {
                let _pipeline = ActivePipeline::new();
                assert!(active(1));
            }
            assert!(active(0));
        });
    }

    #[tokio::test(start_paused = true)]
    async fn first_start_errors_propagate_but_source_reset_retries_wait() {
        let first = Opening::new(OpenMode::Normal, CancellationToken::new());
        assert!(
            first
                .after_failure(LogError::CatalogUnavailable("DNS unavailable".into()).into())
                .await
                .is_err()
        );
        let restart = Opening::new(OpenMode::Resume, CancellationToken::new());
        let started = tokio::time::Instant::now();
        assert_eq!(
            restart
                .after_failure(
                    anyhow::Error::new(LogError::CatalogUnavailable("DNS unavailable".into()))
                        .context("open canonical streams"),
                )
                .await
                .unwrap(),
            ControlFlow::Continue(())
        );
        assert_eq!(started.elapsed(), RETRY);
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_reset_does_not_retry_state_or_permission_failures() {
        let restart = Opening::new(OpenMode::Resume, CancellationToken::new());
        assert!(
            restart
                .after_failure(anyhow::anyhow!("invalid state recovery point"))
                .await
                .is_err()
        );
        assert!(
            restart
                .after_failure(LogError::Discovery("catalog ACL denied".into()).into())
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_interrupts_the_source_reset_retry() {
        let stop = CancellationToken::new();
        let restart = Opening::new(OpenMode::Resume, stop.clone());
        stop.cancel();
        let started = tokio::time::Instant::now();
        assert_eq!(
            restart
                .after_failure(LogError::CatalogUnavailable("DNS unavailable".into()).into())
                .await
                .unwrap(),
            ControlFlow::Break(())
        );
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_a_catalog_open_ends_the_resume_cleanly() {
        let stop = CancellationToken::new();
        let restart = Opening::new(OpenMode::Resume, stop.clone());
        stop.cancel();
        assert_eq!(
            restart
                .after_failure(LogError::Discovery("stopped during start-up open".into()).into())
                .await
                .unwrap(),
            ControlFlow::Break(())
        );
    }
}
