//! The fill of the local account layer after a miss of the verify agent
//! (`docs/specs/2026-10-23-hot-path-agents.md`, section 5.5).
//!
//! The agent reads the local layer only, and never waits for Redis. On a
//! miss it sends the sender into a bounded channel and admits. The filler
//! task reads Redis and gives each row to the one writer of the local
//! layer (the receipt pump), which applies it through the monotone rule.
//! The next submit of that sender then finds the row.
//!
//! - A full channel drops the miss. The next miss of that sender asks
//!   again.
//! - The filler takes a burst of misses at once and drops the duplicates,
//!   so one cold sender costs one read, whatever its submits.
//! - The filler ends when the agent drops its sender, at the end of the
//!   agent.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use alloy_primitives::Address;
use kardamom_cache::{AccountView, CacheReader};
use tokio::sync::mpsc;

use crate::channels::IngressSubscription;

/// The misses that wait for the filler.
pub(crate) const MISS_CAPACITY: usize = 1024;

/// Where the filler reads an account: Redis, or a fake in a test.
pub(crate) trait AccountSource: Send + Sync + 'static {
    /// The state of `address`, or `None` on a miss and on each degraded
    /// read.
    fn account(&self, address: Address) -> impl Future<Output = Option<AccountView>> + Send;
}

impl AccountSource for CacheReader {
    fn account(&self, address: Address) -> impl Future<Output = Option<AccountView>> + Send {
        CacheReader::account(self, address)
    }
}

/// The task that reads the account of each missed sender, and gives the
/// row to the writer of the local layer of `sink`.
pub(crate) struct AccountFiller<A, S> {
    misses: mpsc::Receiver<Address>,
    source: Arc<A>,
    sink: S,
}

impl<A: AccountSource, S: IngressSubscription> AccountFiller<A, S> {
    pub(crate) fn new(misses: mpsc::Receiver<Address>, source: Arc<A>, sink: S) -> Self {
        Self {
            misses,
            source,
            sink,
        }
    }

    /// Start the filler on the runtime of the caller.
    pub(crate) fn spawn(self) {
        tokio::spawn(self.run());
    }

    /// Wait for a miss, take the other misses that wait, and fill each
    /// sender once.
    pub(crate) async fn run(mut self) {
        while let Some(first) = self.misses.recv().await {
            let mut burst = BTreeSet::from([first]);
            burst.extend(std::iter::from_fn(|| self.misses.try_recv().ok()));
            self.fill_each(burst).await;
        }
    }

    async fn fill_each(&self, senders: BTreeSet<Address>) {
        for address in senders {
            self.fill(address).await;
        }
    }

    async fn fill(&self, address: Address) {
        if let Some(view) = self.source.account(address).await {
            self.sink.fill_account(address, view);
        }
    }
}
