use anyhow::Context;
use serde::Serialize;

use crate::{metrics, probes};

#[derive(Debug, Serialize)]
pub(super) struct Progress {
    pub block: i64,
    applied: i64,
    failed: i64,
    log_enospc: usize,
    checkpoint_enospc: usize,
}

impl Progress {
    pub(super) fn read(body: &str, logs: &str) -> anyhow::Result<Self> {
        let enospc = |message| {
            logs.lines()
                .filter(|line| {
                    line.contains(message)
                        && (line.contains("No space left on device")
                            || line.contains("os error 28"))
                })
                .count()
        };
        Ok(Self {
            block: metrics::first(body, probes::EXECUTOR_BLOCK_METRIC)
                .context("missing victim block")?,
            applied: metrics::sum(body, probes::EXECUTOR_TX_APPLIED_METRIC)
                .context("missing victim applied count")?,
            // This counter is registered only by a failure; the required gauges above prove a scrape.
            failed: metrics::sum(body, "kardamom_checkpoint_log_failed_total").unwrap_or(0),
            log_enospc: enospc("the block log failed; the next batch starts a new segment"),
            checkpoint_enospc: enospc("checkpoint base round failed"),
        })
    }

    pub(super) fn failed_after(&self, before: &Self) -> bool {
        self.failed > before.failed
            && self.log_enospc > before.log_enospc
            && self.checkpoint_enospc > before.checkpoint_enospc
    }

    pub(super) fn advanced_after(&self, before: &Self) -> bool {
        self.block > before.block && self.applied > before.applied
    }

    pub(super) fn continued_during(&self, before: &Self) -> bool {
        self.advanced_after(before)
            && self.failed > before.failed
            && self.log_enospc > before.log_enospc
    }
}
