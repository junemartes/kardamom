//! `read`: on each ingress endpoint, the head must move between two
//! runs, and the receipt of an old canary transaction must still answer
//! (the persisted receipt path).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use super::{Context, Probe};
use crate::outcome::Outcome;
use crate::rpc::Rpc;

const NAME: &str = "read";

#[derive(Debug)]
pub struct Read {
    ctx: Arc<Context>,
    /// The head each endpoint answered on the last run.
    heads: HashMap<String, u64>,
}

/// One endpoint's answer: its head, if it gave one, and the outcome.
struct Answer {
    endpoint: String,
    head: Option<u64>,
    outcome: Outcome,
}

impl Read {
    #[must_use]
    pub fn new(ctx: Arc<Context>) -> Self {
        Self {
            ctx,
            heads: HashMap::new(),
        }
    }

    async fn check(&self, rpc: &Rpc) -> Answer {
        let endpoint = rpc.endpoint.name.clone();
        let head = match rpc.block_number().await {
            Ok(head) => head,
            Err(e) => {
                return Answer {
                    endpoint,
                    head: None,
                    outcome: e.into(),
                };
            }
        };
        let stalled = self.heads.get(&endpoint).is_some_and(|last| head <= *last);
        let outcome = match self.old_receipt(rpc).await {
            Err(outcome) => outcome,
            Ok(()) if stalled => Outcome::HeadStalled,
            Ok(()) => Outcome::Success,
        };
        Answer {
            endpoint,
            head: Some(head),
            outcome,
        }
    }

    async fn old_receipt(&self, rpc: &Rpc) -> Result<(), Outcome> {
        let anchor = *self.ctx.anchor.borrow();
        let Some(hash) = anchor else {
            return Ok(());
        };
        match rpc.receipt(hash).await? {
            Some(_) => Ok(()),
            None => Err(Outcome::ReceiptLost),
        }
    }
}

impl Probe for Read {
    fn interval(&self) -> Duration {
        self.ctx.timing.read
    }

    async fn run(&mut self) {
        let ctx = Arc::clone(&self.ctx);
        let answers: Vec<Answer> = futures::stream::iter(ctx.endpoints.iter())
            .then(|rpc| self.check(rpc))
            .collect()
            .await;
        for answer in &answers {
            answer.outcome.record(NAME, &answer.endpoint);
        }
        self.heads.extend(
            answers
                .into_iter()
                .filter_map(|a| a.head.map(|head| (a.endpoint, head))),
        );
    }
}
