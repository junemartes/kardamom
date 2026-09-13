//! The membership watch: one blocking-query loop per subscribed service
//! filter, publishing each successful read as a [`Membership`] snapshot.
//!
//! Contract:
//! - An error keeps the last membership and retries with bounded backoff.
//!   An error is never an empty set.
//! - A successful empty read is a distinct state: it publishes an empty
//!   membership, and the reconciler applies its removal grace.
//! - An index that moves backwards (a rebuilt catalog) restarts the
//!   blocking query from index 0.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::catalog::{Catalog, Query, QueryResult};
use super::record::{PublisherRecord, ServiceEntry, ServiceId};
use crate::config::DiscoveryConfig;
use crate::error::LogError;

/// The catalog's health as the watch last saw it. Aeron transport
/// liveness is a separate observation and never reads this.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogHealth {
    /// The last read succeeded.
    Fresh,
    /// Reads are failing; the membership is the last successful read.
    Degraded { consecutive_errors: u32 },
}

/// One successful read of a service filter, parsed. Entries that fail to
/// parse are logged and left out; they never take the whole read down.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
    pub index: u64,
    pub entries: BTreeMap<ServiceId, ServiceEntry>,
    pub health: CatalogHealth,
}

impl Membership {
    /// The membership before the first read: nothing known, and not a
    /// successful empty read either.
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            index: 0,
            entries: BTreeMap::new(),
            health: CatalogHealth::Degraded {
                consecutive_errors: 0,
            },
        }
    }

    /// Whether any successful read has happened.
    #[must_use]
    pub fn is_known(&self) -> bool {
        self.index > 0
    }

    /// The publisher records of this membership, skipping entries that
    /// fail to parse.
    #[must_use]
    pub fn publishers(&self) -> BTreeMap<ServiceId, PublisherRecord> {
        self.entries
            .values()
            .filter_map(|e| match PublisherRecord::from_entry(e) {
                Ok(r) => Some((e.id.clone(), r)),
                Err(err) => {
                    warn!(error = %err, "discovery: skipping malformed publisher record");
                    None
                }
            })
            .collect()
    }
}

/// Query timing from the `[discovery]` section.
#[derive(Clone, Copy, Debug)]
pub struct WatchTiming {
    pub wait: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
}

impl WatchTiming {
    #[must_use]
    pub fn from_config(cfg: &DiscoveryConfig) -> Self {
        Self {
            wait: cfg.blocking_wait(),
            backoff_min: cfg.backoff_min(),
            backoff_max: cfg.backoff_max(),
        }
    }
}

/// The blocking-query loop over one service filter.
pub struct MembershipWatch {
    catalog: Catalog,
    query: Query,
    timing: WatchTiming,
    tx: watch::Sender<Membership>,
    cancel: CancellationToken,
    consecutive_errors: u32,
}

impl MembershipWatch {
    /// Build the watch and its receiver. Nothing runs until
    /// [`Self::run`].
    #[must_use]
    pub fn new(
        catalog: Catalog,
        service: String,
        meta_equals: BTreeMap<String, String>,
        timing: WatchTiming,
        cancel: CancellationToken,
    ) -> (Self, watch::Receiver<Membership>) {
        let (tx, rx) = watch::channel(Membership::unknown());
        let query = Query {
            service,
            meta_equals,
            index: 0,
            wait: timing.wait,
        };
        (
            Self {
                catalog,
                query,
                timing,
                tx,
                cancel,
                consecutive_errors: 0,
            },
            rx,
        )
    }

    /// Run until cancelled, or until every receiver is gone.
    pub async fn run(mut self) {
        while self.step().await {}
    }

    /// One read, its outcome applied. `false` ends the loop.
    async fn step(&mut self) -> bool {
        let read = tokio::select! {
            () = self.cancel.cancelled() => return false,
            r = self.catalog.query(&self.query) => r,
        };
        let next = match read {
            Ok(result) => self.on_success(result),
            Err(e) => self.on_error(&e),
        };
        match next {
            Step::Immediate => !self.tx.is_closed(),
            Step::After(delay) => self.pause(delay).await,
        }
    }

    /// Sleep `delay` unless cancelled first. `false` ends the loop.
    async fn pause(&self, delay: Duration) -> bool {
        tokio::select! {
            () = self.cancel.cancelled() => false,
            () = tokio::time::sleep(delay) => !self.tx.is_closed(),
        }
    }

    fn on_success(&mut self, result: QueryResult) -> Step {
        self.consecutive_errors = 0;
        if result.index == 0 {
            // A catalog index is never 0. Reading again at once would
            // spin, so this waits one backoff first.
            warn!(service = %self.query.service, "discovery: catalog answered with index 0");
            self.query.index = 0;
            return Step::After(self.timing.backoff_min);
        }
        if result.index < self.query.index {
            info!(
                service = %self.query.service,
                from = self.query.index,
                to = result.index,
                "discovery: catalog index moved backwards; restarting the blocking query"
            );
            self.query.index = 0;
            return Step::Immediate;
        }
        self.query.index = result.index;
        let entries = result
            .entries
            .into_iter()
            .map(|e| (e.id.clone(), e))
            .collect();
        self.tx.send_replace(Membership {
            index: result.index,
            entries,
            health: CatalogHealth::Fresh,
        });
        Step::Immediate
    }

    fn on_error(&mut self, error: &LogError) -> Step {
        self.consecutive_errors = self.consecutive_errors.saturating_add(1);
        let delay = self.backoff();
        warn!(
            service = %self.query.service,
            error = %error,
            consecutive_errors = self.consecutive_errors,
            retry_in_ms = delay.as_millis(),
            "discovery: catalog read failed; keeping the last membership"
        );
        let consecutive_errors = self.consecutive_errors;
        self.tx.send_modify(|m| {
            m.health = CatalogHealth::Degraded { consecutive_errors };
        });
        Step::After(delay)
    }

    /// Doubling backoff from the minimum, capped at the maximum.
    fn backoff(&self) -> Duration {
        let doublings = self.consecutive_errors.saturating_sub(1).min(16);
        let scaled = self
            .timing
            .backoff_min
            .checked_mul(1u32 << doublings)
            .unwrap_or(self.timing.backoff_max);
        scaled.min(self.timing.backoff_max)
    }
}

/// What a step does before the next read.
enum Step {
    Immediate,
    After(Duration),
}

#[cfg(test)]
mod tests;
