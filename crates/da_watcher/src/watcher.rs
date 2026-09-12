//! Watcher loop: cursor-tailed L1 polling that builds one
//! [`kardamom_types::EpochRecord`] per finalized L1 block and publishes it
//! on the `tx_deposits` Aeron channel.
//!
//! The unit is the epoch, not the deposit. Every finalized L1 block yields
//! exactly one record, including a block with no deposits. The no-skipping
//! rule only holds if every epoch appears on the stream. See
//! `docs/agents/l1-origin-deposit-derivation-spec.md`.

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::time::Duration;

use alloy_primitives::Address;

use crate::source::LockboxLog;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::metrics;
use crate::publisher::{EpochPublisher, PublishError};
use crate::source::{L1Source, L1SourceError};
use kardamom_types::epoch::{EpochError, derive_epoch};

/// Watcher configuration.
#[derive(Debug, Clone, Copy)]
pub struct DaWatcherConfig {
    /// L1 address of the `ETHLockbox` proxy this L2 chain id maps to.
    pub lockbox: Address,
    /// Polling cadence for `finalized_block_number()`.
    pub poll_interval: Duration,
}

/// Handle to a running watcher task. Drop `shutdown`, or send `()` on it, to
/// ask the loop to exit. `task` is the underlying tokio `JoinHandle`.
pub struct WatcherHandle {
    /// Underlying tokio task. `.await` this after sending `shutdown`, to join.
    pub task: JoinHandle<()>,
    /// Cooperative shutdown signal. Send `()`, or drop this, to ask the
    /// watcher loop to exit at the next tick boundary.
    pub shutdown: oneshot::Sender<()>,
}

/// Errors the watcher's tick loop reports up. A `Tip` or `Logs` error means
/// the cursor did not advance. The next tick retries the same range.
#[derive(Debug, thiserror::Error)]
pub enum MonitorError {
    /// `L1Source::finalized_block_number` failed (transport or decode error).
    #[error("failed to read L1 finalized tip: {0}")]
    Tip(L1SourceError),
    /// `L1Source::lockbox_logs` failed.
    #[error("failed to read L1 deposit logs: {0}")]
    Logs(L1SourceError),
    /// `L1Source::block_hash` failed. An epoch needs its block's hash to
    /// build the canonical id, so it cannot be built without one.
    #[error("failed to read L1 block hash: {0}")]
    BlockHash(L1SourceError),
    /// The logs fetched for a block disagree with the hash fetched for that
    /// same block. This can mean an L1 reorg happened between the two
    /// reads, or the provider served inconsistent views. Never advance the
    /// cursor past this.
    #[error("epoch derivation failed: {0}")]
    Derive(EpochError),
    /// The L1 has not yet produced a finalized block. This is a tick-level
    /// outcome, not an error, so dashboards do not alarm before finality
    /// starts on a freshly started chain.
    #[error("L1 has no finalized block yet")]
    NotFinalized,
    /// The publisher transport is permanently closed. The watcher must exit.
    #[error("deposit publisher closed")]
    PublisherClosed,
}

/// [`Tick::read_range`]'s result: the inclusive `from_block..=tip` range
/// this tick should publish, and its lockbox logs bucketed by block.
struct TickRange {
    from_block: u64,
    tip: u64,
    by_block: BTreeMap<u64, Vec<LockboxLog>>,
}

/// What [`L1Watcher::publish_one_epoch`] did with one block.
enum PublishStep {
    /// Published; the caller's count and the cursor both advance.
    Published,
    /// Backpressured or transport-failed. The range halts here; the next
    /// tick retries from this block.
    Halt,
}

/// The L1 watcher's state: the L1 source, the epoch publisher, the
/// lockbox address, the poll cadence, and the durable cursor (the last
/// L1 block whose epoch was published; `None` until the first tick seeds
/// it at the finalized tip).
pub struct L1Watcher<S, P> {
    source: S,
    publisher: P,
    lockbox: Address,
    poll_interval: Duration,
    cursor: Option<u64>,
}

impl<S: L1Source, P: EpochPublisher> L1Watcher<S, P> {
    #[must_use]
    pub fn new(publisher: P, source: S, config: DaWatcherConfig) -> Self {
        Self {
            source,
            publisher,
            lockbox: config.lockbox,
            poll_interval: config.poll_interval,
            cursor: None,
        }
    }

    /// Seed the cursor, instead of letting the first tick seed it at the
    /// finalized tip. Unit tests use this to start a pass mid-chain.
    #[must_use]
    pub fn resume_at(mut self, cursor: Option<u64>) -> Self {
        self.cursor = cursor;
        self
    }

    /// The last L1 block whose epoch was published, or `None` before the
    /// first seed.
    #[must_use]
    pub fn cursor(&self) -> Option<u64> {
        self.cursor
    }

    /// The L1 source. Unit tests script the next tick through it.
    pub fn source(&self) -> &S {
        &self.source
    }

    /// Spawn the watcher loop. Return a [`WatcherHandle`] that owns the
    /// task and a cooperative shutdown channel.
    pub fn spawn(publisher: P, source: S, config: DaWatcherConfig) -> WatcherHandle {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(Self::new(publisher, source, config).run(shutdown_rx));
        WatcherHandle {
            task,
            shutdown: shutdown_tx,
        }
    }

    /// The tick loop. `shutdown` stays outside the state, so the select
    /// in [`Self::step`] can wait on it while the pass borrows `self`.
    async fn run(mut self, mut shutdown: oneshot::Receiver<()>) {
        // `interval` fires immediately on the first `tick().await`. This
        // is what we want: it seeds the cursor as soon as the task starts.
        let mut interval = tokio::time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while let ControlFlow::Continue(()) = self.step(&mut shutdown, &mut interval).await {}
    }

    /// Wait for the next tick or the shutdown signal, then run one pass
    /// and report it. `Break` ends the loop: shutdown, or a closed
    /// publisher.
    async fn step(
        &mut self,
        shutdown: &mut oneshot::Receiver<()>,
        interval: &mut tokio::time::Interval,
    ) -> ControlFlow<()> {
        tokio::select! {
            biased;
            _ = shutdown => {
                info!(target: "da_watcher", "shutting down");
                return ControlFlow::Break(());
            }
            _ = interval.tick() => {}
        }
        Self::report(self.process_once().await)
    }

    /// Count and log one pass's outcome. Only a closed publisher stops
    /// the loop; every other error retries on the next tick.
    fn report(outcome: Result<usize, MonitorError>) -> ControlFlow<()> {
        match outcome {
            Ok(0) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "ok").increment(1);
            }
            Ok(n) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "ok").increment(1);
                info!(target: "da_watcher", published = n, "epochs published");
            }
            Err(MonitorError::NotFinalized) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "ok").increment(1);
                debug!(target: "da_watcher", "L1 has no finalized block yet");
            }
            Err(MonitorError::PublisherClosed) => {
                warn!(target: "da_watcher", "publisher closed; exiting");
                return ControlFlow::Break(());
            }
            Err(
                ref e @ (MonitorError::Tip(L1SourceError::Decode(_))
                | MonitorError::Logs(L1SourceError::Decode(_))),
            ) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "parse_error").increment(1);
                warn!(target: "da_watcher", error = %e, "tick failed (parse error)");
            }
            Err(e) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "rpc_error").increment(1);
                warn!(target: "da_watcher", error = %e, "tick failed");
            }
        }
        ControlFlow::Continue(())
    }

    /// One processing pass. This is public, so unit tests can call it
    /// directly, with no timer or thread.
    ///
    /// On success, return `Ok(n)`, where `n` is the number of epochs
    /// published this pass. `n` is zero on a seed or idle tick. The cursor
    /// advances per published block, so a partial pass resumes exactly
    /// where it stopped.
    ///
    /// On `Err`, the cursor is unchanged.
    ///
    /// # Errors
    /// Returns [`MonitorError`] on any read or publish failure; see
    /// [`Self::read_range`] and [`Self::publish_one_epoch`] for which
    /// variant means what.
    ///
    /// A publish backpressure event is logged. The cursor stays at the
    /// last block that did publish, so the next tick retries from the one
    /// that did not.
    pub async fn process_once(&mut self) -> Result<usize, MonitorError> {
        let Some(mut range) = self.read_range().await? else {
            return Ok(0);
        };

        let mut published_count = 0usize;
        for number in range.from_block..=range.tip {
            match self.publish_block(&mut range.by_block, number).await? {
                PublishStep::Published => published_count += 1,
                PublishStep::Halt => return Ok(published_count),
            }
        }

        self.cursor = Some(range.tip);
        Ok(published_count)
    }

    /// Read the finalized tip and, when the cursor already has new blocks
    /// behind it, fetch this tick's lockbox logs. `Ok(None)` means the
    /// tick only seeded the cursor or found nothing new; the caller
    /// reports `Ok(0)` for both.
    ///
    /// # Errors
    /// - [`MonitorError::Tip`] if reading the L1 finalized tip fails.
    /// - [`MonitorError::Logs`] if reading the deposit logs in `(cursor, tip]` fails.
    async fn read_range(&mut self) -> Result<Option<TickRange>, MonitorError> {
        let tip = match self.source.finalized_block_number().await {
            Ok(n) => n,
            Err(L1SourceError::NotFinalized) => return Err(MonitorError::NotFinalized),
            Err(e) => return Err(MonitorError::Tip(e)),
        };

        // Emit the finalized tip gauge on every successful tip fetch.
        // Metric value; f64 precision loss only above 2^52, never reached
        // by an L1 block number.
        #[allow(
            clippy::cast_precision_loss,
            reason = "metric value; never nears 2^52 for an L1 block number"
        )]
        ::metrics::gauge!(metrics::L1_FINALIZED).set(tip as f64);

        let from_block = match self.cursor {
            None => {
                // Seed the cursor. Skip historical deposits, per the spec's Non-Goals section.
                self.cursor = Some(tip);
                return Ok(None);
            }
            Some(c) if tip <= c => return Ok(None),
            // PROVEN: this arm is reached only when `tip > c`, so `c <
            // tip <= u64::MAX`, so `c + 1` cannot overflow. `saturating_add`
            // documents that at the call site instead of an unchecked `+`.
            Some(c) => c.saturating_add(1),
        };

        // Fetch the logs with one range query, then split by block. A query per
        // block would multiply RPC round-trips during catch-up, for no gain.
        let logs = self
            .source
            .lockbox_logs(self.lockbox, from_block, tip)
            .await
            .map_err(MonitorError::Logs)?;
        let mut by_block: BTreeMap<u64, Vec<LockboxLog>> = BTreeMap::new();
        for log in logs {
            by_block.entry(log.block_number()).or_default().push(log);
        }
        Ok(Some(TickRange {
            from_block,
            tip,
            by_block,
        }))
    }

    /// Take block `number`'s logs out of `by_block` (empty when the block
    /// had none) and publish its epoch. For [`Self::process_once`]'s loop.
    ///
    /// # Errors
    /// Same as [`Self::publish_one_epoch`].
    async fn publish_block(
        &mut self,
        by_block: &mut std::collections::BTreeMap<u64, Vec<LockboxLog>>,
        number: u64,
    ) -> Result<PublishStep, MonitorError> {
        let logs = by_block.remove(&number).unwrap_or_default();
        self.publish_one_epoch(number, logs).await
    }

    /// Derive block `number`'s epoch from `logs` and its L1 hash, and
    /// publish it. Advances the cursor to `number` on a successful
    /// publish.
    ///
    /// # Errors
    /// - [`MonitorError::BlockHash`] if reading the L1 block hash fails.
    /// - [`MonitorError::Derive`] if the logs disagree with the hash.
    /// - [`MonitorError::PublisherClosed`] if the publisher transport is shut.
    async fn publish_one_epoch(
        &mut self,
        number: u64,
        logs: Vec<LockboxLog>,
    ) -> Result<PublishStep, MonitorError> {
        // A block with no deposits has no log to carry its hash, so the hash is
        // fetched per block. `derive_epoch` then checks every log's
        // block_hash against it. This is what catches a reorg between the log
        // query and this read.
        let hash = self
            .source
            .block_hash(number)
            .await
            .map_err(MonitorError::BlockHash)?;
        let epoch = derive_epoch(number, hash, &logs).map_err(MonitorError::Derive)?;
        let deposits = epoch.deposits.len();

        match self.publisher.publish(&epoch) {
            Ok(pos) => {
                // Advance the cursor per block, not once for the whole range. A
                // failure halfway through must not re-publish already-accepted
                // epochs. Dedup would absorb a repeat, but the cursor also
                // drives the origin-lag signal, and it should not go backwards.
                self.cursor = Some(number);
                ::metrics::counter!(metrics::EPOCHS_PUBLISHED_TOTAL).increment(1);
                ::metrics::counter!(metrics::DEPOSITS_DETECTED_TOTAL).increment(deposits as u64);
                // Metric value; f64 precision loss only above 2^52,
                // never reached by an L1 block number.
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "metric value; never nears 2^52 for an L1 block number"
                )]
                ::metrics::gauge!(metrics::EPOCH_ORIGIN).set(number as f64);
                debug!(
                    target: "da_watcher",
                    l1_number = number,
                    deposits,
                    ?pos,
                    "published epoch"
                );
                Ok(PublishStep::Published)
            }
            Err(PublishError::Backpressure) => {
                // Hold the cursor at the last successful block. The next tick
                // resumes at exactly this one.
                warn!(
                    target: "da_watcher",
                    l1_number = number,
                    "epoch publish backpressured; will retry next tick"
                );
                Ok(PublishStep::Halt)
            }
            Err(PublishError::Closed) => Err(MonitorError::PublisherClosed),
            Err(PublishError::Transport(detail)) => {
                // An epoch must never be skipped: skipping one puts a permanent
                // hole in the origin sequence, and a verifier would later
                // reject the chain for it. Stop the range here and retry from
                // this block.
                warn!(
                    target: "da_watcher",
                    l1_number = number,
                    %detail,
                    "epoch publish failed; halting range (an epoch must never be skipped)"
                );
                Ok(PublishStep::Halt)
            }
        }
    }
}
