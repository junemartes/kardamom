//! Measured case traffic excludes the local canary except in its own case.

use crate::harness::Harness;
use crate::nomad::SavedJob;

impl Harness {
    /// Stop the funded local canary before the shard measures case traffic.
    ///
    /// # Errors
    ///
    /// Returns an error if its registered job cannot be captured or stopped.
    pub async fn prepare_case_services(&mut self) -> anyhow::Result<()> {
        if self.lifecycle.vars().canary_local == Some(true) {
            let saved = SavedJob::capture(&self.nomad, "canary").await?;
            saved.stop().await?;
            self.case_canary = Some(saved);
        }
        Ok(())
    }

    pub(crate) async fn start_case_canary(&self) -> anyhow::Result<()> {
        match &self.case_canary {
            Some(saved) => saved.restore().await,
            None => Ok(()),
        }
    }

    pub(crate) async fn stop_case_canary(&self) -> anyhow::Result<()> {
        match &self.case_canary {
            Some(saved) => saved.stop().await,
            None => Ok(()),
        }
    }
}
