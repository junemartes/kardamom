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
//! One address has at most one lookup at a time. A sender asks again each
//! time a parked transaction of it expires, and a lookup can retry for
//! longer than a parked transaction lives. So during an outage of every
//! source, a second lookup of an address would only add retries that
//! compete for the permits with the first attempts of other senders. A
//! request for an address with a running lookup starts no second lookup:
//! the answer of that lookup serves the new transactions too. The feed
//! remembers the request. When the lookup ends with no answer, its last
//! retry failed, and the request came after the expiry that ended the
//! retries. The feed then starts a new lookup at once, so a source that
//! returns serves the sender before its next expiry.
//!
//! Each attempt counts once in `nonce_lookups_total`, by the source that
//! answered (`live`, `redis`, `ok` for an executor), `miss`, or `error`.

use std::collections::HashMap;
use std::future::Future;
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
    /// The addresses with a running lookup, each with whether a request
    /// came for it while the lookup ran.
    running: HashMap<Address, bool>,
    /// Each lookup sends its end here.
    ended_tx: tokio::sync::mpsc::UnboundedSender<Ended>,
    ended: tokio::sync::mpsc::UnboundedReceiver<Ended>,
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
        let (ended_tx, ended) = tokio::sync::mpsc::unbounded_channel();
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
            running: HashMap::new(),
            ended_tx,
            ended,
        }
    }

    #[must_use]
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run())
    }

    async fn run(mut self) {
        while self.tick().await.is_continue() {}
    }

    /// Wait for the end of a lookup, a request or shutdown. The end of a
    /// lookup comes first, so a request after it starts a new lookup.
    async fn tick(&mut self) -> ControlFlow<()> {
        tokio::select! {
            biased;
            () = self.shutdown.cancelled() => ControlFlow::Break(()),
            Some(ended) = self.ended.recv() => self.on_ended(ended).await,
            request = self.rx.recv() => match request {
                Some(address) => self.on_request(address).await,
                None => ControlFlow::Break(()),
            },
        }
    }

    /// A request for `address`: remember it when a lookup runs for the
    /// address, else start one.
    async fn on_request(&mut self, address: Address) -> ControlFlow<()> {
        if let Some(asked) = self.running.get_mut(&address) {
            *asked = true;
            return ControlFlow::Continue(());
        }
        self.start(address).await
    }

    /// A lookup ended. Start a new one when it ended with no answer and a
    /// request came for it while it ran.
    async fn on_ended(&mut self, ended: Ended) -> ControlFlow<()> {
        let asked = self.running.remove(&ended.address) == Some(true);
        if asked && !ended.answered {
            return self.start(ended.address).await;
        }
        ControlFlow::Continue(())
    }

    /// Start the lookup of `address`.
    async fn start(&mut self, address: Address) -> ControlFlow<()> {
        self.running.insert(address, false);
        let Some(permit) = self.permit().await else {
            return ControlFlow::Break(());
        };
        let lookup = Lookup {
            address,
            sources: self.sources.clone(),
            answers: self.answers.clone(),
            limit: self.limit.clone(),
            end: LookupEnd {
                address,
                answered: false,
                ended: self.ended_tx.clone(),
            },
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
    /// Tells the feed the end of the lookup when it drops.
    end: LookupEnd,
}

/// The end of the lookup of one address.
struct Ended {
    address: Address,
    /// A source answered, or every source missed the account.
    answered: bool,
}

/// Tells the feed that the lookup of `address` ended, however it ends.
struct LookupEnd {
    address: Address,
    answered: bool,
    ended: tokio::sync::mpsc::UnboundedSender<Ended>,
}

impl Drop for LookupEnd {
    fn drop(&mut self) {
        // The feed is gone when the send fails, and with it the set.
        let _ = self.ended.send(Ended {
            address: self.address,
            answered: self.answered,
        });
    }
}

impl Lookup {
    async fn run(mut self, permit: OwnedSemaphorePermit) {
        let mut flow = self.attempt(permit).await;
        for _ in 0..self.sources.retries.0 {
            if flow.is_break() {
                break;
            }
            flow = self.retry_once().await;
        }
        self.end.answered = flow == ControlFlow::Break(true);
    }

    /// One attempt. The permit ends with it. `Break(true)` is an answer
    /// or a miss on every source; `Continue` means that a source failed.
    async fn attempt(&self, _permit: OwnedSemaphorePermit) -> ControlFlow<bool> {
        match self.sources.observe(self.address).await {
            Found::State(_, state) => {
                let _ = self.answers.send(AccountEvent::Answer {
                    address: self.address,
                    state,
                });
                ControlFlow::Break(true)
            }
            Found::Missing => ControlFlow::Break(true),
            Found::Failed => ControlFlow::Continue(()),
        }
    }

    /// Wait, take a permit, and try again. A stopped feed ends the retry
    /// with `Break(false)`.
    async fn retry_once(&self) -> ControlFlow<bool> {
        let stopped = ControlFlow::Break(false);
        if self
            .unless_stopped(tokio::time::sleep(RETRY_AFTER))
            .await
            .is_none()
        {
            return stopped;
        }
        let Some(Ok(permit)) = self
            .unless_stopped(self.limit.clone().acquire_owned())
            .await
        else {
            return stopped;
        };
        self.unless_stopped(self.attempt(permit))
            .await
            .unwrap_or(stopped)
    }

    /// The output of `work`, or `None` when the feed stops first.
    async fn unless_stopped<T>(&self, work: impl Future<Output = T>) -> Option<T> {
        tokio::select! {
            biased;
            () = self.sources.retry_stop.cancelled() => None,
            out = work => Some(out),
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
