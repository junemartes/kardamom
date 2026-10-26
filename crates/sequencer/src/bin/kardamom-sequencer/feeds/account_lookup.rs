//! The account lookup of the v2 loop: the chain state of a sender that the
//! admission filter holds no state of.
//!
//! The answer comes from the first source that has one: the local account
//! layer (the rows of this replica's vslots), Redis, then the executors.
//! The local layer and Redis give the nonce and the balance of one
//! position. An executor answers them in two queries, which can read two
//! snapshots. The task asks again once when the positions differ, and then
//! takes the pair at the older position: a balance older than its nonce
//! can let one transaction through that the executor then skips. An
//! answer of position 0 (an executor before its first block) is older
//! than any row, so the first row replaces it. A sender with no answer
//! keeps its transactions parked until they expire. Failed lookups retry
//! while the feed runs, so a source that returns can release parked work.
//!
//! Each attempt counts once in `nonce_lookups_total`, by the source that
//! answered (`live`, `redis`, `ok` for an executor) or `error`.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use kardamom_cache::{AccountView, CacheReader, ExecutorQuery, LiveAccounts};
use kardamom_obs::pipeline::Step;
use kardamom_sequencer::admission::AccountState;
use kardamom_sequencer::metrics;
use kardamom_sequencer::sequencer::Shutdown;
use kardamom_sequencer::v2::AccountEvent;
use tokio::sync::Semaphore;

/// The lookups that run at once.
const IN_FLIGHT: usize = 64;
const RETRY_AFTER: Duration = Duration::from_secs(2);

pub(crate) struct AccountLookupFeed {
    rx: tokio::sync::mpsc::Receiver<Address>,
    answers: crossbeam_channel::Sender<AccountEvent>,
    /// Dropping the feed cancels retries before its sources are released.
    _retry_guard: tokio_util::sync::DropGuard,
    sources: Arc<Sources>,
    shutdown: Shutdown,
    limit: Arc<Semaphore>,
}

/// The three sources, in the order of their cost.
struct Sources {
    /// The partition label of the metrics.
    partition: u32,
    live: Arc<LiveAccounts>,
    redis: Option<Arc<CacheReader>>,
    query: Option<ExecutorQuery>,
    /// One lookup, from the first source to the answer.
    latency: Step,
    retry_stop: Shutdown,
}

impl AccountLookupFeed {
    pub(crate) fn new(
        rx: tokio::sync::mpsc::Receiver<Address>,
        answers: crossbeam_channel::Sender<AccountEvent>,
        live: Arc<LiveAccounts>,
        redis: Option<Arc<CacheReader>>,
        query: Option<ExecutorQuery>,
        shutdown: Shutdown,
        partition: u32,
    ) -> Self {
        let retry_stop = Shutdown::new();
        Self {
            rx,
            answers,
            _retry_guard: retry_stop.guard(),
            sources: Arc::new(Sources {
                partition,
                live,
                redis,
                query,
                latency: Step::new("sequencer.account_lookup"),
                retry_stop,
            }),
            shutdown,
            limit: Arc::new(Semaphore::new(IN_FLIGHT)),
        }
    }

    pub(crate) fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run())
    }

    async fn run(mut self) {
        while self.tick().await.is_continue() {}
    }

    /// Wait for a request or for shutdown, and start its lookup.
    async fn tick(&mut self) -> ControlFlow<()> {
        let address = tokio::select! {
            biased;
            () = self.shutdown.cancelled() => return ControlFlow::Break(()),
            request = self.rx.recv() => match request {
                Some(address) => address,
                None => return ControlFlow::Break(()),
            },
        };
        let permit = tokio::select! {
            biased;
            () = self.shutdown.cancelled() => return ControlFlow::Break(()),
            permit = self.limit.clone().acquire_owned() => permit,
        };
        let Ok(permit) = permit else {
            return ControlFlow::Break(());
        };
        let sources = self.sources.clone();
        let answers = self.answers.clone();
        tokio::spawn(async move {
            // The permit ends with the task.
            let _permit = permit;
            if let Some(state) = sources.observe(address).await {
                let _ = answers.send(AccountEvent::Answer { address, state });
            } else {
                sources.retry(address, &answers).await;
            }
        });
        ControlFlow::Continue(())
    }
}

impl Sources {
    async fn observe(&self, address: Address) -> Option<AccountState> {
        let started = Instant::now();
        let (outcome, state) = self.state_of(address).await;
        self.latency.since(started);
        metrics::record_nonce_lookup(self.partition, outcome);
        state
    }

    /// The original task and permit bound retries to the lookup limit.
    async fn retry(&self, address: Address, answers: &crossbeam_channel::Sender<AccountEvent>) {
        while self.retry_once(address, answers).await.is_continue() {}
    }

    async fn retry_once(
        &self,
        address: Address,
        answers: &crossbeam_channel::Sender<AccountEvent>,
    ) -> ControlFlow<()> {
        tokio::select! {
            biased;
            () = self.retry_stop.cancelled() => return ControlFlow::Break(()),
            () = tokio::time::sleep(RETRY_AFTER) => {},
        }
        let state = tokio::select! {
            biased;
            () = self.retry_stop.cancelled() => return ControlFlow::Break(()),
            state = self.observe(address) => state,
        };
        let Some(state) = state else {
            return ControlFlow::Continue(());
        };
        let _ = answers.send(AccountEvent::Answer { address, state });
        ControlFlow::Break(())
    }

    /// The state of `address`, and the outcome label of the lookup.
    async fn state_of(&self, address: Address) -> (&'static str, Option<AccountState>) {
        if let Some(view) = self.live.get(address) {
            return ("live", Some(state_of_view(&view)));
        }
        let cached = match &self.redis {
            Some(redis) => redis.account(address).await,
            None => None,
        };
        if let Some(view) = cached {
            return ("redis", Some(state_of_view(&view)));
        }
        match self.executor_state(address).await {
            Some(state) => ("ok", Some(state)),
            None => ("error", None),
        }
    }

    /// The nonce and the balance from the executors: a pair of one
    /// position, or after one more try the pair at the older position.
    async fn executor_state(&self, address: Address) -> Option<AccountState> {
        let first = self.executor_pair(address).await?;
        if first.0 {
            return Some(first.1);
        }
        Some(
            self.executor_pair(address)
                .await
                .map_or(first.1, |(_, state)| state),
        )
    }

    /// One pair of queries: whether both named one position, and the state
    /// at the older of the two.
    async fn executor_pair(&self, address: Address) -> Option<(bool, AccountState)> {
        let query = self.query.as_ref()?;
        let (nonce, balance) = tokio::join!(query.nonce(address), query.balance(address));
        let (nonce, balance) = (nonce.ok()?, balance.ok()?);
        let state = AccountState {
            nonce: u64::try_from(nonce.value).ok()?,
            balance: balance.value,
            delegated: false,
            tx_idx: nonce.tx_idx.min(balance.tx_idx),
        };
        Some((nonce.tx_idx == balance.tx_idx, state))
    }
}

fn state_of_view(view: &AccountView) -> AccountState {
    AccountState {
        nonce: view.nonce,
        balance: view.balance,
        delegated: false,
        tx_idx: view.tx_idx,
    }
}

#[cfg(test)]
#[path = "account_lookup_tests.rs"]
mod tests;
