//! The reader side of Redis: the second layer, behind [`LiveAccounts`].
//!
//! A reader never stalls on Redis. [`CacheReader::spawn`] returns at once
//! and one background task connects with a backoff, then polls the mirror
//! heads every 100 ms into an atomic. Every read before the first
//! connection, during a reconnect, past the timeout, or on an error
//! answers `None` and counts a degraded read; the caller admits. So Redis
//! makes admission faster and never blocks it.
//!
//! Freshness is a position lag, never wall clock: the newest position the
//! local layer has seen minus the highest live mirror head. An idle chain
//! is not stale.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy_primitives::Address;
use arc_swap::ArcSwapOption;
use kardamom_types::Receipt;
use tracing::{info, warn};

use crate::client::{AccountCache, AccountView};
use crate::config::CacheConfig;
use crate::error::CacheError;
use crate::live::LiveAccounts;
use crate::metrics;

/// How often the heads are polled.
const HEAD_INTERVAL: Duration = Duration::from_millis(100);
/// The pause between two connection attempts.
const CONNECT_BACKOFF: Duration = Duration::from_secs(1);

/// The Redis read side: the account projection, the receipt index, and
/// the freshness gate. Cheap to share behind an `Arc`.
pub struct CacheReader {
    cache: Arc<ArcSwapOption<AccountCache>>,
    /// The highest live mirror head, as a canonical index. Zero means no
    /// live head.
    head: Arc<AtomicU64>,
    max_stale: u64,
    live: Arc<LiveAccounts>,
    /// The connect-and-poll task. Aborted in `Drop`.
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CacheReader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CacheReader {
    /// Start the reader. `mirrors` is the number of mirror heads to poll:
    /// the mirror ids are the executor indexes. Needs a tokio runtime.
    #[must_use]
    pub fn spawn(cfg: &CacheConfig, mirrors: NonZeroU32, live: Arc<LiveAccounts>) -> Self {
        let cache = Arc::new(ArcSwapOption::empty());
        let head = Arc::new(AtomicU64::new(0));
        let poll = HeadPoll {
            cfg: cfg.clone(),
            ids: (0..mirrors.get()).collect(),
            cache: cache.clone(),
            head: head.clone(),
            live: live.clone(),
        };
        Self {
            cache,
            head,
            max_stale: cfg.max_stale_txs.get(),
            live,
            task: tokio::spawn(poll.run()),
        }
    }

    /// Whether a connection is up.
    #[must_use]
    pub fn connected(&self) -> bool {
        self.cache.load().is_some()
    }

    /// The highest live mirror head. `None` before the first poll, or
    /// when every mirror's head has aged out.
    #[must_use]
    pub fn head(&self) -> Option<u64> {
        match self.head.load(Ordering::Acquire) {
            0 => None,
            head => Some(head),
        }
    }

    /// Whether a Redis value is fresh: a live mirror head exists, the
    /// local layer has seen a receipt frame, and its newest position is
    /// within `max_stale_txs` of the mirror head. Before the first frame,
    /// for example after a restart on an idle chain, the local layer has
    /// no position to measure the lag against, so no value is fresh. Only
    /// the balance check needs this. A nonce is a lower bound at any lag.
    #[must_use]
    pub fn fresh(&self) -> bool {
        let local = self.live.head();
        local > 0
            && self
                .head()
                .is_some_and(|head| local.saturating_sub(head) <= self.max_stale)
    }

    /// The account's projected state. `None` on a miss and on every
    /// degraded outcome, each counted.
    pub async fn account(&self, address: Address) -> Option<AccountView> {
        let cache = self.usable()?;
        match cache.account(address).await {
            Ok(Some(view)) => {
                metrics::record_lookup("redis", "hit");
                Some(view)
            }
            Ok(None) => {
                metrics::record_lookup("redis", "miss");
                None
            }
            Err(e) => degrade(&e),
        }
    }

    /// The receipt of `(sender, nonce)` from the index. `None` on a
    /// miss and on every degraded outcome, each counted.
    pub async fn receipt(&self, sender: Address, nonce: u64) -> Option<Receipt> {
        let cache = self.usable()?;
        match cache.receipt(sender, nonce).await {
            Ok(Some(receipt)) => {
                metrics::record_lookup("redis", "hit");
                Some(receipt)
            }
            Ok(None) => {
                metrics::record_lookup("redis", "miss");
                None
            }
            Err(e) => degrade(&e),
        }
    }

    /// The client, when it is connected and not mid-reconnect. A read
    /// during a reconnect would pay the full timeout for nothing, and
    /// every cold submit of an outage would pay it: skip and count.
    fn usable(&self) -> Option<Arc<AccountCache>> {
        let Some(cache) = self.cache.load_full() else {
            metrics::record_degraded("disconnected");
            return None;
        };
        if cache.reconnecting() {
            metrics::record_degraded("reconnecting");
            return None;
        }
        Some(cache)
    }
}

/// Count one failed read and answer `None`.
fn degrade<T>(e: &CacheError) -> Option<T> {
    let reason = match e {
        CacheError::Timeout => "timeout",
        _ => "error",
    };
    metrics::record_lookup("redis", reason);
    metrics::record_degraded(reason);
    warn!(error = %e, "cache read failed; admitting");
    None
}

/// The background task: connect with a backoff, then poll the heads.
struct HeadPoll {
    cfg: CacheConfig,
    ids: Vec<u32>,
    cache: Arc<ArcSwapOption<AccountCache>>,
    head: Arc<AtomicU64>,
    live: Arc<LiveAccounts>,
}

impl HeadPoll {
    async fn run(self) {
        loop {
            let pause = match self.cache.load_full() {
                Some(cache) => self.poll(&cache).await,
                None => self.connect().await,
            };
            tokio::time::sleep(pause).await;
        }
    }

    /// One connection attempt. Returns the pause before the next step.
    async fn connect(&self) -> Duration {
        match AccountCache::connect(&self.cfg).await {
            Ok(cache) => {
                self.cache.store(Some(Arc::new(cache)));
                info!("cache reader connected");
                HEAD_INTERVAL
            }
            Err(e) => {
                warn!(error = %e, "cache reader connect failed; retrying");
                CONNECT_BACKOFF
            }
        }
    }

    /// One head poll. A failed poll keeps the last head; the client's
    /// own reconnect heals the connection.
    async fn poll(&self, cache: &AccountCache) -> Duration {
        if let Ok(heads) = cache.heads(&self.ids).await {
            let head = heads.into_iter().flatten().max().unwrap_or(0);
            self.head.store(head, Ordering::Release);
            metrics::set_head_lag(self.live.head().saturating_sub(head));
        }
        HEAD_INTERVAL
    }
}

#[cfg(test)]
mod tests {
    use kardamom_types::BPosition;

    use super::*;
    use crate::config::LiveAccountsConfig;

    /// A mirror head alone is not enough: before the local layer sees a
    /// receipt frame, no value is fresh. After it, a value within the
    /// bound is fresh, and one beyond it is stale.
    #[tokio::test]
    async fn no_local_head_means_stale() {
        let (live, mut writer) = LiveAccounts::new(&LiveAccountsConfig::default());
        let reader = CacheReader::spawn(&CacheConfig::default(), NonZeroU32::new(1).unwrap(), live);
        reader.head.store(10, Ordering::Release);
        assert!(!reader.fresh(), "no local head yet");
        writer.apply(BPosition::from_index(10), &[]);
        assert!(reader.fresh());
        writer.apply(BPosition::from_index(10 + reader.max_stale + 1), &[]);
        assert!(!reader.fresh(), "beyond max_stale_txs");
    }
}
