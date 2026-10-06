//! Watcher loop: cursor-tailed L1 polling that builds one
//! [`kardamom_types::EpochRecord`] per finalized L1 block and publishes it
//! on the `tx_deposits` Aeron channel.
//!
//! The unit is the epoch, not the deposit. Every finalized L1 block yields
//! exactly one record, including a block with no deposits. The no-skipping
//! rule only holds if every epoch appears on the stream. See
//! `docs/agents/l1-origin-deposit-derivation-spec.md`.

use std::collections::BTreeMap;
use std::num::{NonZeroU64, ParseIntError};
use std::ops::ControlFlow;
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use kardamom_obs::halt::{self, Halt, HaltCause};

use crate::source::LockboxLog;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::cursor::{CursorError, CursorFile};
use crate::l1_cursor::L1Cursor;
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
    /// The last L1 block whose epoch the chain holds. When set, it
    /// overrides the cursor file. `None` resumes after the block in the
    /// cursor file, or, with no file, seeds the cursor at the finalized
    /// tip on the first tick.
    pub resume_after: Option<L1ResumeAfter>,
}

/// The last L1 block whose epoch the chain already holds: the L1 origin
/// of the head that a sealer cluster starts from. The watcher then
/// publishes every finalized block after it, so no epoch between that
/// origin and the tip is lost. The value is never 0: a head at origin 0
/// holds no epoch, and its watcher starts at the finalized tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L1ResumeAfter(NonZeroU64);

impl L1ResumeAfter {
    /// The L1 block number.
    #[must_use]
    pub fn block(self) -> u64 {
        self.0.get()
    }
}

/// Why a resume block does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResumeAfterError {
    /// The text is not a `u64`.
    #[error("not an L1 block number: {0}")]
    NotANumber(#[from] ParseIntError),
    /// Block 0: the chain holds no epoch to resume after.
    #[error(
        "L1 block 0 holds no epoch; omit the resume block, and the watcher starts at the finalized tip"
    )]
    Zero,
}

impl From<NonZeroU64> for L1ResumeAfter {
    fn from(block: NonZeroU64) -> Self {
        Self(block)
    }
}

impl FromStr for L1ResumeAfter {
    type Err = ResumeAfterError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NonZeroU64::new(s.parse()?)
            .map(Self)
            .ok_or(ResumeAfterError::Zero)
    }
}

/// Handle to a running watcher task. [`WatcherHandle::join`] asks the
/// loop to exit and waits for it. Dropping the handle also asks the loop
/// to exit, without the wait. `task` is the underlying tokio `JoinHandle`.
pub struct WatcherHandle {
    /// Underlying tokio task. Tests read `is_finished` on it, and hold
    /// `shutdown` while they await it, to see a fail-stop.
    pub task: JoinHandle<()>,
    /// Cooperative shutdown signal. Dropping this asks the watcher loop
    /// to exit at the next tick boundary.
    pub shutdown: oneshot::Sender<()>,
}

impl WatcherHandle {
    /// Ask the loop to exit, then wait for the task. Ending the sender is
    /// the request; the block scope ends it before the await.
    ///
    /// # Errors
    ///
    /// Returns the join error when the watcher task panicked.
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        let Self { task, shutdown } = self;
        {
            let _request = shutdown;
        }
        task.await
    }
}

/// Errors the watcher's tick loop reports up. A `Tip` or `Logs` error means
/// the cursor did not advance. The next tick retries the same range. A
/// chain break and an L1 that does not answer also raise the watcher's
/// halt (see [`MonitorError::halt`]).
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
    /// Block `number` does not descend from the block this watcher
    /// published before it: its parent hash is not the hash of `number - 1`
    /// that the watcher holds, from this run or from its cursor file. A
    /// finalized chain never reorgs, so the provider served an
    /// inconsistent view, or it lies, or the stored hash came from a lie.
    /// Never advance the cursor past this: the next tick reads the block
    /// again, against the same anchor.
    #[error(
        "L1 block {number} does not descend from the published block {}: parent {parent}, expected {expected}",
        number - 1
    )]
    ChainBreak {
        number: u64,
        expected: B256,
        parent: B256,
    },
    /// The L1 has not yet produced a finalized block. This is a tick-level
    /// outcome, not an error, so dashboards do not alarm before finality
    /// starts on a freshly started chain.
    #[error("L1 has no finalized block yet")]
    NotFinalized,
    /// The publisher transport is permanently closed. The watcher must exit.
    #[error("deposit publisher closed")]
    PublisherClosed,
}

impl MonitorError {
    /// The halt this error puts the watcher in: `l1_chain_break` for a
    /// block that does not descend from the published one, and the halt
    /// of an L1 read that failed ([`L1SourceError::halt_cause`]). `None`
    /// for every other error. Every one of these halts clears by itself:
    /// the watcher retries the same range on every tick.
    #[must_use]
    pub fn halt(&self) -> Option<Halt> {
        let cause = match self {
            Self::ChainBreak { .. } => Some(HaltCause::L1ChainBreak),
            Self::Tip(e) | Self::Logs(e) | Self::BlockHash(e) => e.halt_cause(),
            _ => None,
        };
        cause.map(|cause| Halt::new(cause, self.to_string()))
    }
}

/// [`Tick::read_range`]'s result: the block the range descends from,
/// the inclusive `anchor.number + 1..=tip` range this tick should
/// publish, and its lockbox logs bucketed by block.
struct TickRange {
    anchor: L1Cursor,
    tip: u64,
    by_block: BTreeMap<u64, Vec<LockboxLog>>,
}

/// What [`L1Watcher::publish_one_epoch`] did with one block.
enum PublishStep {
    /// Published; the caller's count and the cursor both advance to this
    /// block.
    Published(L1Cursor),
    /// Backpressured or transport-failed. The range halts here; the next
    /// tick retries from this block.
    Halt,
}

/// Where the watcher stands on L1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Position {
    /// No block yet: the first tick anchors at the finalized tip.
    Tip,
    /// Resume after this block. The first tick reads its hash, which then
    /// anchors the next block.
    After(L1ResumeAfter),
    /// The last block published, or the block a start anchored at. The
    /// next block must name its hash as its parent.
    Anchored(L1Cursor),
}

/// The L1 watcher's state: the L1 source, the epoch publisher, the
/// lockbox address, the poll cadence, the position (the last L1 block
/// whose epoch was published), and the file that keeps the position
/// across a restart.
///
/// The position chains every block to the one before it by the parent
/// hash. Verifying each block alone would let an L1 endpoint serve any
/// hash for any number; the link forces it to fabricate a consistent chain
/// instead. The cursor file keeps the link across a restart, so the first
/// block after a restart is linked too.
pub struct L1Watcher<S, P> {
    source: S,
    publisher: P,
    lockbox: Address,
    poll_interval: Duration,
    position: Position,
    cursor_file: Option<CursorFile<L1Cursor>>,
}

impl<S: L1Source, P: EpochPublisher> L1Watcher<S, P> {
    /// A watcher at the configured resume block, or at the finalized tip.
    /// [`Self::load_cursor`] then applies the cursor file.
    #[must_use]
    pub fn new(
        publisher: P,
        source: S,
        config: DaWatcherConfig,
        cursor_file: Option<CursorFile<L1Cursor>>,
    ) -> Self {
        Self {
            source,
            publisher,
            lockbox: config.lockbox,
            poll_interval: config.poll_interval,
            position: config.resume_after.map_or(Position::Tip, Position::After),
            cursor_file,
        }
    }

    /// The last L1 block whose epoch was published, or `None` before the
    /// first seed.
    #[must_use]
    pub fn cursor(&self) -> Option<u64> {
        match self.position {
            Position::Tip => None,
            Position::After(block) => Some(block.block()),
            Position::Anchored(cursor) => Some(cursor.number),
        }
    }

    /// Apply the cursor file to the start position. The order of
    /// precedence:
    ///
    /// 1. `resume_after` wins. The operator sets it after a seed, and the
    ///    file then holds a block of the old chain. The first tick
    ///    anchors at the resume block and overwrites the file.
    /// 2. A file that parses: resume after its block, linked to its hash.
    /// 3. No file: the finalized tip, logged as a warning. The epochs
    ///    between the last publish and the tip are lost, unless this is
    ///    the chain's first start.
    ///
    /// # Errors
    /// Returns [`CursorError`] when the file exists but cannot be read or
    /// parsed. The watcher never guesses a position past that: see
    /// [`Self::cursor_halt`].
    pub fn load_cursor(&mut self) -> Result<(), CursorError> {
        let Some(file) = &self.cursor_file else {
            warn!(
                target: "da_watcher",
                "no L1 cursor file is set; a restart starts at the finalized tip and loses the \
                 epochs between the last publish and the tip"
            );
            return Ok(());
        };
        if let Position::After(block) = self.position {
            info!(
                target: "da_watcher",
                resume_after = block.block(),
                cursor_file = %file.path().display(),
                "--l1-resume-after overrides the L1 cursor file"
            );
            return Ok(());
        }
        let Some(cursor) = file.load()? else {
            warn!(
                target: "da_watcher",
                cursor_file = %file.path().display(),
                "NO L1 CURSOR FILE: starting at the finalized tip. Unless this is the chain's \
                 first start, the deposits of the L1 blocks between the last publish and the tip \
                 are lost; give --l1-resume-after to resume after a known block"
            );
            return Ok(());
        };
        info!(
            target: "da_watcher",
            l1_number = cursor.number,
            l1_hash = %cursor.hash,
            "resuming after the block in the L1 cursor file"
        );
        self.position = Position::Anchored(cursor);
        Ok(())
    }

    /// The halt for a cursor file that cannot be read. An operator
    /// clears it, after the file is restored or rewritten.
    #[must_use]
    pub fn cursor_halt(error: &CursorError) -> Halt {
        Halt::new(HaltCause::L1CursorUnreadable, error.to_string())
    }

    /// Spawn the watcher loop. Return a [`WatcherHandle`] that owns the
    /// task and a cooperative shutdown channel.
    pub fn spawn(
        publisher: P,
        source: S,
        config: DaWatcherConfig,
        cursor_file: Option<CursorFile<L1Cursor>>,
    ) -> WatcherHandle {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(Self::new(publisher, source, config, cursor_file).run(shutdown_rx));
        WatcherHandle {
            task,
            shutdown: shutdown_tx,
        }
    }

    /// Load the cursor, then run the tick loop. `shutdown` stays outside
    /// the state, so the select in [`Self::step`] can wait on it while the
    /// pass borrows `self`.
    async fn run(mut self, mut shutdown: oneshot::Receiver<()>) {
        if self.await_cursor(&mut shutdown).await.is_break() {
            return;
        }
        // `interval` fires immediately on the first `tick().await`. This
        // is what we want: it seeds the cursor as soon as the task starts.
        let mut interval = tokio::time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while let ControlFlow::Continue(()) = self.step(&mut shutdown, &mut interval).await {}
    }

    /// Load the cursor file until it reads. `Break` means shutdown came
    /// first.
    async fn await_cursor(&mut self, shutdown: &mut oneshot::Receiver<()>) -> ControlFlow<()> {
        loop {
            if let Some(flow) = self.try_cursor(shutdown).await {
                return flow;
            }
        }
    }

    /// One load of the cursor file. On a failure, raise the halt and wait
    /// for the operator's clear, then return `None` so the caller reads
    /// the file again. The watcher publishes nothing while it waits.
    async fn try_cursor(
        &mut self,
        shutdown: &mut oneshot::Receiver<()>,
    ) -> Option<ControlFlow<()>> {
        let Err(e) = self.load_cursor() else {
            return Some(ControlFlow::Continue(()));
        };
        error!(
            target: "da_watcher",
            error = %e,
            "the L1 cursor file cannot be read; halted until an operator clears the halt"
        );
        halt::raise(Self::cursor_halt(&e));
        tokio::select! {
            biased;
            _ = shutdown => {
                info!(target: "da_watcher", "shutting down");
                Some(ControlFlow::Break(()))
            }
            () = halt::cleared() => None,
        }
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
    /// the loop; every other error retries on the next tick. A chain
    /// break or an unreachable L1 raises the watcher's halt, and a good
    /// pass clears it.
    fn report(outcome: Result<usize, MonitorError>) -> ControlFlow<()> {
        kardamom_obs::ready::mark_now(metrics::LAST_TICK_UNIX_SECONDS);
        match outcome {
            Ok(0) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "ok").increment(1);
                halt::clear();
            }
            Ok(n) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "ok").increment(1);
                info!(target: "da_watcher", published = n, "epochs published");
                halt::clear();
            }
            Err(MonitorError::NotFinalized) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "ok").increment(1);
                debug!(target: "da_watcher", "L1 has no finalized block yet");
                halt::clear();
            }
            Err(MonitorError::PublisherClosed) => {
                warn!(target: "da_watcher", "publisher closed; exiting");
                return ControlFlow::Break(());
            }
            Err(ref e @ MonitorError::ChainBreak { .. }) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "chain_break").increment(1);
                warn!(target: "da_watcher", error = %e, "tick failed (the L1 view is not a chain)");
                if let Some(halt) = e.halt() {
                    halt::raise(halt);
                }
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
                if let Some(halt) = e.halt() {
                    halt::raise(halt);
                }
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
    /// After the pass, success or not, the cursor file receives the cursor
    /// when it moved. The write follows the publish it records, never
    /// precedes it: see [`crate::cursor`].
    ///
    /// # Errors
    /// Returns [`MonitorError`] on any read or publish failure; see
    /// [`Self::read_range`] and [`Self::publish_one_epoch`] for which
    /// variant means what. The cursor stays at the last block that
    /// published.
    ///
    /// A publish backpressure event is logged. The cursor stays at the
    /// last block that did publish, so the next tick retries from the one
    /// that did not.
    pub async fn process_once(&mut self) -> Result<usize, MonitorError> {
        let before = self.position;
        let outcome = self.publish_range().await;
        self.persist_cursor(before);
        outcome
    }

    /// Read this tick's range and publish its epochs in order, up to the
    /// first that does not publish.
    async fn publish_range(&mut self) -> Result<usize, MonitorError> {
        let Some(mut range) = self.read_range().await? else {
            return Ok(0);
        };
        let mut published_count = 0usize;
        // PROVEN: `read_range` returns a range only when `anchor.number <
        // tip <= u64::MAX`, so `anchor.number + 1` cannot overflow.
        for number in range.anchor.number.saturating_add(1)..=range.tip {
            match self.publish_block(&mut range, number).await? {
                PublishStep::Published(_) => published_count += 1,
                PublishStep::Halt => return Ok(published_count),
            }
        }
        Ok(published_count)
    }

    /// Write the cursor to the file when it moved since `before`. A failed
    /// write is counted and logged, not fatal: the file then holds an
    /// older block, and a restart publishes the epochs after it again,
    /// which the sealer drops.
    fn persist_cursor(&self, before: Position) {
        let (Position::Anchored(cursor), Some(file)) = (self.position, &self.cursor_file) else {
            return;
        };
        if self.position == before {
            return;
        }
        if let Err(e) = file.persist(&cursor) {
            ::metrics::counter!(metrics::L1_CURSOR_PERSIST_FAILURES_TOTAL).increment(1);
            warn!(
                target: "da_watcher",
                l1_number = cursor.number,
                error = %e,
                "L1 cursor persist failed; a restart before the next good persist publishes \
                 again from an older block (harmless: the sealer drops the repeats)"
            );
        }
    }

    /// Read the finalized tip and, when the cursor already has new blocks
    /// behind it, fetch this tick's lockbox logs. `Ok(None)` means the
    /// tick only seeded the cursor or found nothing new; the caller
    /// reports `Ok(0)` for both.
    ///
    /// # Errors
    /// - [`MonitorError::Tip`] if reading the L1 finalized tip fails.
    /// - [`MonitorError::BlockHash`] if reading the anchor block fails.
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

        let Some(anchor) = self.anchor(tip).await? else {
            return Ok(None);
        };
        if tip <= anchor.number {
            return Ok(None);
        }
        // PROVEN: `anchor.number < tip <= u64::MAX`, so `+ 1` cannot
        // overflow. `saturating_add` documents that at the call site
        // instead of an unchecked `+`.
        let from_block = anchor.number.saturating_add(1);

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
            anchor,
            tip,
            by_block,
        }))
    }

    /// The block the next epoch descends from. A position without a hash
    /// reads it from L1 here, once, and the watcher anchors there: at the
    /// finalized tip on a first start (historical deposits are skipped,
    /// per the spec's Non-Goals section), or at the resume block. `None`
    /// while the resume block is not finalized yet.
    ///
    /// # Errors
    /// [`MonitorError::BlockHash`] if reading the anchor block fails.
    async fn anchor(&mut self, tip: u64) -> Result<Option<L1Cursor>, MonitorError> {
        let number = match self.position {
            Position::Anchored(cursor) => return Ok(Some(cursor)),
            Position::After(block) if tip < block.block() => return Ok(None),
            Position::After(block) => block.block(),
            Position::Tip => tip,
        };
        let (hash, _) = self
            .source
            .block_ids(number)
            .await
            .map_err(MonitorError::BlockHash)?;
        let anchor = L1Cursor { number, hash };
        info!(
            target: "da_watcher",
            l1_number = number,
            l1_hash = %hash,
            "anchored the L1 cursor"
        );
        self.position = Position::Anchored(anchor);
        Ok(Some(anchor))
    }

    /// Take block `number`'s logs out of the range (empty when the block
    /// had none) and publish its epoch. On a publish, the range and the
    /// watcher both advance to the block. For [`Self::publish_range`]'s
    /// loop.
    ///
    /// # Errors
    /// Same as [`Self::publish_one_epoch`].
    async fn publish_block(
        &mut self,
        range: &mut TickRange,
        number: u64,
    ) -> Result<PublishStep, MonitorError> {
        let logs = range.by_block.remove(&number).unwrap_or_default();
        let step = self.publish_one_epoch(range.anchor, number, logs).await?;
        if let PublishStep::Published(cursor) = step {
            range.anchor = cursor;
            self.position = Position::Anchored(cursor);
        }
        Ok(step)
    }

    /// Derive block `number`'s epoch from `logs` and its L1 hash, and
    /// publish it. The block must descend from `anchor`, the block
    /// published before it.
    ///
    /// # Errors
    /// - [`MonitorError::BlockHash`] if reading the L1 block hash fails.
    /// - [`MonitorError::ChainBreak`] if the block's parent is not `anchor`.
    /// - [`MonitorError::Derive`] if the logs disagree with the hash.
    /// - [`MonitorError::PublisherClosed`] if the publisher transport is shut.
    async fn publish_one_epoch(
        &mut self,
        anchor: L1Cursor,
        number: u64,
        logs: Vec<LockboxLog>,
    ) -> Result<PublishStep, MonitorError> {
        // A block with no deposits has no log to carry its hash, so the hash is
        // fetched per block. `derive_epoch` then checks every log's
        // block_hash against it. This is what catches a reorg between the log
        // query and this read.
        let (hash, parent) = self
            .source
            .block_ids(number)
            .await
            .map_err(MonitorError::BlockHash)?;
        if parent != anchor.hash {
            return Err(MonitorError::ChainBreak {
                number,
                expected: anchor.hash,
                parent,
            });
        }
        let epoch = derive_epoch(number, hash, &logs).map_err(MonitorError::Derive)?;
        let deposits = epoch.deposits.len();

        match self.publisher.publish(&epoch) {
            Ok(pos) => {
                // The caller advances the cursor per block, not once for the
                // whole range. A failure halfway through must not re-publish
                // already-accepted epochs. Dedup would absorb a repeat, but
                // the cursor also drives the origin-lag signal, and it should
                // not go backwards.
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
                Ok(PublishStep::Published(L1Cursor { number, hash }))
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
