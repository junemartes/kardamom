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
//! keeps its transactions parked until a row of the sender or the expiry
//! releases them.
//!
//! A lookup asks again only when a source failed (an error or a timeout),
//! so a source that returns can release parked work. A miss on every
//! source is an answer: no source holds the account, and asking again
//! changes nothing. The retries end when a parked transaction of the
//! sender expires. Each attempt holds a permit only while it runs: a retry
//! holds no permit while it sleeps, so a sleeping retry never blocks a
//! first attempt.
//!
//! Each attempt counts once in `nonce_lookups_total`, by the source that
//! answered (`live`, `redis`, `ok` for an executor), `miss`, or `error`.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::admission::AccountState;
use crate::metrics;
use crate::sequencer::Shutdown;
use crate::v2::AccountEvent;
use alloy_primitives::Address;
use kardamom_cache::{AccountView, CacheReader, ExecutorQuery, LiveAccounts};
use kardamom_obs::pipeline::Step;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// The lookups that run at once.
const IN_FLIGHT: usize = 64;
const RETRY_AFTER: Duration = Duration::from_secs(2);

/// The retries of one lookup after a failed first attempt. They cover the
/// lifetime of a parked transaction: after it, no parked work waits for
/// the answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retries(usize);

impl Retries {
    /// The retries, `RETRY_AFTER` apart, that cover `park_ttl`, rounded up.
    #[must_use]
    pub fn covering(park_ttl: Duration) -> Self {
        let count = park_ttl.as_millis().div_ceil(RETRY_AFTER.as_millis());
        // A count above `usize::MAX` retries until the feed stops.
        Self(usize::try_from(count).unwrap_or(usize::MAX))
    }
}

/// The sources of a lookup, in the order of their cost.
pub struct LookupSources {
    pub live: Arc<LiveAccounts>,
    pub redis: Option<Arc<CacheReader>>,
    pub query: Option<ExecutorQuery>,
}

pub struct AccountLookupFeed {
    rx: tokio::sync::mpsc::Receiver<Address>,
    answers: crossbeam_channel::Sender<AccountEvent>,
    /// Dropping the feed cancels retries before its sources are released.
    _retry_guard: tokio_util::sync::DropGuard,
    sources: Arc<Sources>,
    shutdown: Shutdown,
    limit: Arc<Semaphore>,
}

/// The sources of the feed and what each lookup shares.
struct Sources {
    /// The partition label of the metrics.
    partition: u32,
    from: LookupSources,
    retries: Retries,
    /// One lookup, from the first source to the answer.
    latency: Step,
    retry_stop: Shutdown,
}

impl AccountLookupFeed {
    #[must_use]
    pub fn new(
        rx: tokio::sync::mpsc::Receiver<Address>,
        answers: crossbeam_channel::Sender<AccountEvent>,
        sources: LookupSources,
        shutdown: Shutdown,
        partition: u32,
        retries: Retries,
    ) -> Self {
        let retry_stop = Shutdown::new();
        Self {
            rx,
            answers,
            _retry_guard: retry_stop.guard(),
            sources: Arc::new(Sources {
                partition,
                from: sources,
                retries,
                latency: Step::new("sequencer.account_lookup"),
                retry_stop,
            }),
            shutdown,
            limit: Arc::new(Semaphore::new(IN_FLIGHT)),
        }
    }

    #[must_use]
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
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
        let Some(permit) = self.permit().await else {
            return ControlFlow::Break(());
        };
        let lookup = Lookup {
            address,
            sources: self.sources.clone(),
            answers: self.answers.clone(),
            limit: self.limit.clone(),
        };
        tokio::spawn(lookup.run(permit));
        ControlFlow::Continue(())
    }

    /// Only a saturated feed waits on a permit and shutdown together.
    async fn permit(&self) -> Option<OwnedSemaphorePermit> {
        match self.limit.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(TryAcquireError::Closed) => None,
            Err(TryAcquireError::NoPermits) => tokio::select! {
                biased;
                () = self.shutdown.cancelled() => None,
                permit = self.limit.clone().acquire_owned() => permit.ok(),
            },
        }
    }
}

/// What one attempt of a lookup found.
enum Found {
    /// A source holds the account. The label names the source.
    State(&'static str, AccountState),
    /// Every source answered, and none holds the account.
    Missing,
    /// A source failed, so another attempt can find the account.
    Failed,
}

impl Found {
    fn label(&self) -> &'static str {
        match self {
            Self::State(label, _) => label,
            Self::Missing => "miss",
            Self::Failed => "error",
        }
    }
}

/// One lookup of one address, from its first attempt to its last retry.
struct Lookup {
    address: Address,
    sources: Arc<Sources>,
    answers: crossbeam_channel::Sender<AccountEvent>,
    limit: Arc<Semaphore>,
}

impl Lookup {
    async fn run(self, permit: OwnedSemaphorePermit) {
        if self.attempt(permit).await.is_break() {
            return;
        }
        for _ in 0..self.sources.retries.0 {
            if self.retry_once().await.is_break() {
                return;
            }
        }
    }

    /// One attempt. The permit ends with it. Continue means that a source
    /// failed.
    async fn attempt(&self, _permit: OwnedSemaphorePermit) -> ControlFlow<()> {
        match self.sources.observe(self.address).await {
            Found::State(_, state) => {
                let _ = self.answers.send(AccountEvent::Answer {
                    address: self.address,
                    state,
                });
                ControlFlow::Break(())
            }
            Found::Missing => ControlFlow::Break(()),
            Found::Failed => ControlFlow::Continue(()),
        }
    }

    /// Wait, take a permit, and try again. A stopped feed ends the retry.
    async fn retry_once(&self) -> ControlFlow<()> {
        let stop = &self.sources.retry_stop;
        tokio::select! {
            biased;
            () = stop.cancelled() => return ControlFlow::Break(()),
            () = tokio::time::sleep(RETRY_AFTER) => {},
        }
        let permit = tokio::select! {
            biased;
            () = stop.cancelled() => return ControlFlow::Break(()),
            permit = self.limit.clone().acquire_owned() => permit,
        };
        let Ok(permit) = permit else {
            return ControlFlow::Break(());
        };
        tokio::select! {
            biased;
            () = stop.cancelled() => ControlFlow::Break(()),
            flow = self.attempt(permit) => flow,
        }
    }
}

impl Sources {
    async fn observe(&self, address: Address) -> Found {
        let started = Instant::now();
        let found = self.state_of(address).await;
        self.latency.since(started);
        metrics::record_nonce_lookup(self.partition, found.label());
        found
    }

    /// The state of `address` from the first source that holds it.
    async fn state_of(&self, address: Address) -> Found {
        if let Some(view) = self.from.live.get(address) {
            return Found::State("live", state_of_view(&view));
        }
        let cached = match &self.from.redis {
            Some(redis) => redis.read_account(address).await,
            None => Ok(None),
        };
        if let Ok(Some(view)) = cached {
            return Found::State("redis", state_of_view(&view));
        }
        if self.from.query.is_none() {
            return cached.map_or(Found::Failed, |_| Found::Missing);
        }
        self.executor_state(address)
            .await
            .map_or(Found::Failed, |state| Found::State("ok", state))
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
        let query = self.from.query.as_ref()?;
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
