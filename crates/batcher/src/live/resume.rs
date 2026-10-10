//! The batcher's start against L1: the truth from the settlement
//! contract, and the cursor rebuilt from the last batch's payload when
//! no cursor file exists. The start retries until L1 answers.

use std::time::Duration;

use alloy_provider::Provider;
use anyhow::Result;
use kardamom_obs::halt::{self, Halt, HaltCause};
use metrics::counter;
use tracing::warn;

use crate::indexer::IndexerClient;

use super::cursor::{BatchCursor, L1Truth, PayloadSources};
use super::live_metric_names;
use super::run::LiveArgs;

/// What a resume read from L1: its truth, and the cursor rebuilt from
/// the last batch's payload when no cursor file exists.
pub(super) struct Resumed {
    pub(super) l1_truth: L1Truth,
    pub(super) rebuilt: Option<(BatchCursor, u64)>,
}

impl LiveArgs {
    /// One resume: L1's truth from the contract, and the cursor rebuilt
    /// from the last batch's payload when there is no cursor file.
    async fn resume_once<P: Provider>(
        &self,
        provider: &P,
        loaded: Option<BatchCursor>,
    ) -> Result<Resumed> {
        let l1_truth = L1Truth::read(provider, self.settlement).await?;
        let indexer = self.indexer_url.as_deref().map(IndexerClient::new);
        let rebuilt = match loaded {
            None if l1_truth.last_batch_index > 0 => Some(
                PayloadSources {
                    indexer: indexer.as_ref(),
                }
                .resume(l1_truth)
                .await?,
            ),
            _ => None,
        };
        Ok(Resumed { l1_truth, rebuilt })
    }

    /// Resume, again after every failure, until L1 answers. A batcher
    /// without L1 has nothing to do, and an exit would hide the failure
    /// behind the orchestrator's restart loop: the exporter stays up, each
    /// failure counts on the resume-failures metric, raises the
    /// `l1_unreachable` halt, and the log names it.
    pub(super) async fn resume_until_l1_answers<P: Provider>(
        &self,
        provider: &P,
        loaded: Option<BatchCursor>,
    ) -> Resumed {
        let mut attempt: u32 = 0;
        loop {
            if let Some(resumed) = self.resume_step(provider, loaded, &mut attempt).await {
                return resumed;
            }
        }
    }

    /// One attempt; `None` after a counted failure and its backoff.
    async fn resume_step<P: Provider>(
        &self,
        provider: &P,
        loaded: Option<BatchCursor>,
        attempt: &mut u32,
    ) -> Option<Resumed> {
        let error = match self.resume_once(provider, loaded).await {
            Ok(resumed) => return Some(resumed),
            Err(error) => error,
        };
        counter!(live_metric_names::RESUME_FAILURES).increment(1);
        // Bounded by the shift below; saturate so a long outage keeps
        // counting instead of wrapping to a short backoff.
        *attempt = attempt.saturating_add(1);
        let backoff = Duration::from_secs(1 << (*attempt).min(5));
        warn!(
            attempt = *attempt,
            backoff_s = backoff.as_secs(),
            error = %format!("{error:#}"),
            "resume failed; L1 did not answer; retrying"
        );
        // The batcher is halted while L1 does not answer. The halt clears
        // when the reader stack starts.
        halt::raise(Halt::new(HaltCause::L1Unreachable, format!("{error:#}")));
        tokio::time::sleep(backoff).await;
        None
    }
}
