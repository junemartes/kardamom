//! `safe`: a sampled `transfer` must reach the safe head, the block the
//! batcher posted to L1, within twice the batcher's idle flush. The check
//! runs in its own task, so the `transfer` probe keeps its cadence.

use std::sync::Arc;

use tokio::time::Instant;

use super::Context;
use crate::metrics;
use crate::outcome::{Outcome, Stage};
use crate::rpc::Rpc;
use crate::wait::Poll;

const NAME: &str = "safe";

/// One sampled transaction: its block, the endpoint that served it, and
/// the time its receipt arrived.
#[derive(Debug)]
pub struct Safe {
    pub ctx: Arc<Context>,
    pub rpc: Rpc,
    pub block: u64,
    pub landed: Instant,
}

impl Safe {
    /// Wait for the safe head to reach the block, and record the outcome.
    pub async fn check(self) {
        let reached = Poll::within(self.ctx.timing.safe_timeout, self.ctx.timing.poll)
            .until(|| async {
                self.rpc
                    .safe_head()
                    .await
                    .ok()
                    .filter(|head| *head >= self.block)
            })
            .await;
        let outcome = match reached {
            Some(_) => {
                metrics::stage(NAME, &self.rpc.endpoint.name, "safe", self.landed.elapsed());
                Outcome::Success
            }
            None => Outcome::Timeout(Stage::Safe),
        };
        outcome.record(NAME, &self.rpc.endpoint.name);
    }
}
