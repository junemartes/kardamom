//! The probes. Each runs in its own task on its own timer, with its own
//! deadlines, so a failing probe stops no other work. A run records its
//! outcomes itself.

pub mod contract;
pub mod read;
pub mod transfer;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};

use crate::config::Timing;
use crate::feed::{BoardHandle, Stages};
use crate::metrics;
use crate::outcome::{Outcome, Stage};
use crate::ring::{Lease, Ring};
use crate::rpc::{Receipt, Rpc};
use crate::store::Store;
use crate::wait::Poll;

/// What every probe shares.
#[derive(Debug)]
pub struct Context {
    pub ring: Arc<Ring>,
    pub endpoints: Vec<Rpc>,
    pub board: BoardHandle,
    pub timing: Timing,
    pub store: Store,
    /// The hash of an old canary transaction: the `transfer` probe sets
    /// it once, the `read` probe asks for its receipt.
    pub anchor: watch::Sender<Option<B256>>,
}

/// A transaction's receipt and the time it arrived.
#[derive(Debug, Clone)]
pub struct Landed {
    pub receipt: Receipt,
    pub at: Instant,
}

impl Context {
    /// The endpoint of turn `turn`: each one in turn.
    #[must_use]
    pub fn endpoint(&self, turn: usize) -> &Rpc {
        &self.endpoints[turn % self.endpoints.len()]
    }

    /// Lease a ring account. A probe waits for an account in use by
    /// another probe up to the receipt timeout: the other probe ends by
    /// then.
    ///
    /// # Errors
    ///
    /// The outcome a user would see when no account can send.
    pub async fn lease(&self, rpc: &Rpc) -> Result<Lease, Outcome> {
        let wait = Poll::within(self.timing.receipt_timeout, self.timing.poll);
        self.ring.lease(rpc, wait).await
    }

    /// Another ring account than `from`.
    #[must_use]
    pub fn neighbor(&self, from: Address) -> Address {
        let ring = self.ring.addresses();
        let at = ring.iter().position(|a| *a == from).unwrap_or(0);
        ring[at.wrapping_add(1) % ring.len()]
    }

    /// Wait for the receipt of `hash` on `rpc`. A rejection on the status
    /// feed ends the wait.
    ///
    /// # Errors
    ///
    /// `rejected` with the feed's reason, or a receipt timeout.
    pub async fn landed(&self, rpc: &Rpc, hash: B256) -> Result<Landed, Outcome> {
        Poll::within(self.timing.receipt_timeout, self.timing.poll)
            .until(|| self.landed_once(rpc, hash))
            .await
            .unwrap_or(Err(Outcome::Timeout(Stage::Receipt)))
    }

    async fn landed_once(&self, rpc: &Rpc, hash: B256) -> Option<Result<Landed, Outcome>> {
        if let Ok(Some(receipt)) = rpc.receipt(hash).await {
            return Some(Ok(Landed {
                receipt,
                at: Instant::now(),
            }));
        }
        let rejected = self.board.stages(hash).await.rejected;
        rejected.map(|reason| Err(Outcome::Rejected(reason)))
    }

    /// The stages of `hash` once `executed` arrives, or what the board
    /// holds when the grace after the receipt ends.
    pub async fn stages(&self, hash: B256) -> Stages {
        let done = Poll::within(self.timing.feed_grace, self.timing.poll)
            .until(|| async {
                let stages = self.board.stages(hash).await;
                stages.executed.is_some().then_some(stages)
            })
            .await;
        match done {
            Some(stages) => stages,
            None => self.board.stages(hash).await,
        }
    }
}

/// Record the feed stages of a landed transaction, timed from its hash.
/// A missing stage that a later one implies is no gap; a missing stage
/// with no later one is a feed gap, never a transaction failure.
pub fn record_stages(probe: &'static str, endpoint: &str, hashed: Instant, stages: &Stages) {
    let marks = [
        ("offered", stages.offered),
        ("sealed", stages.sealed),
        ("executed", stages.executed),
    ];
    marks.iter().enumerate().for_each(|(i, (name, at))| {
        let later = marks[i + 1..].iter().any(|(_, a)| a.is_some());
        match at {
            Some(at) => metrics::stage(probe, endpoint, name, at.saturating_duration_since(hashed)),
            None if !later => metrics::feed_gap(gap_kind(name)),
            None => {}
        }
    });
}

fn gap_kind(stage: &str) -> &'static str {
    match stage {
        "offered" => "missing_offered",
        "sealed" => "missing_sealed",
        _ => "missing_executed",
    }
}

/// One probe: a name, a cadence, and one run.
pub trait Probe: Send + 'static {
    /// The run's interval.
    fn interval(&self) -> Duration;
    /// One run. It records its outcomes.
    fn run(&mut self) -> impl Future<Output = ()> + Send;
}

/// Run `probe` on its timer for ever. A run that outlasts the interval
/// delays the next one; missed ticks are skipped.
pub async fn drive<P: Probe>(mut probe: P) {
    let mut tick = tokio::time::interval(probe.interval());
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        probe.run().await;
    }
}
