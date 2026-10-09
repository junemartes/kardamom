//! The L1 watcher: one [`kardamom_types::EpochRecord`] per finalized L1
//! block on the `tx_deposits` Aeron channel, from the records of the
//! `l1_blocks` stream.
//!
//! The unit is the epoch, not the deposit. Every finalized L1 block yields
//! exactly one record, including a block with no deposits. The no-skipping
//! rule only holds if every epoch appears on the stream.
//!
//! The watcher has no L1 access: the L1 follower reads L1, derives each
//! epoch, and publishes it inside the block's record. The watcher takes
//! the records in block order, checks each record's parent link against
//! its head, and publishes the record's epoch. With a sealer feed, it
//! follows the sealer's commit, not its own publish: see the `follow`
//! module.

use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::time::Duration;

use alloy_primitives::B256;
use kardamom_obs::halt::{self, Halt, HaltCause};
use kardamom_types::{L1Block, L1BlockDedup};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::cursor::{CursorError, CursorFile};
use crate::feed::BlockFeed;
use crate::l1_cursor::L1Cursor;
use crate::metrics;
use crate::publisher::EpochPublisher;
use crate::window::Window;

mod follow;
mod records;
mod resume;
mod upstream;

use follow::Confirmation;
pub use follow::START_WAIT;
pub use resume::{L1ResumeAfter, ResumeAfterError, WatcherHandle};
use upstream::Upstream;

/// Watcher configuration.
#[derive(Debug, Clone, Copy)]
pub struct DaWatcherConfig {
    /// The cadence of the housekeeping tick: a history read the watcher
    /// waits on, a publish to retry, the waiting log, the check of the
    /// follower, and the readiness mark.
    pub tick: Duration,
    /// How long the stream may carry no record before the watcher pauses
    /// with the follower as its root.
    pub silence: Duration,
    /// The last L1 block whose epoch the chain holds. When set, it
    /// overrides the cursor file and the start's wait for the sealer's
    /// first boundary. A later boundary outside the published range still
    /// moves the watcher to the sealer's origin. `None` resumes after the
    /// sealer's origin, or after the block in the cursor file, or, with
    /// neither, anchors at the first record of the stream.
    pub resume_after: Option<L1ResumeAfter>,
}

/// Why a record stops the watcher. A chain break and a disagreement also
/// raise the watcher's halt (see [`MonitorError::halt`]).
#[derive(Debug, thiserror::Error)]
pub enum MonitorError {
    /// The record of block `number` does not descend from the block this
    /// watcher published before it. The follower checks the chain before
    /// it publishes, so the record, or the hash the watcher holds from its
    /// cursor file, is a lie. The watcher drops the record and reads the
    /// block again from the archives, against the same anchor.
    #[error(
        "L1 block {number} does not descend from the published block {}: parent {parent}, expected {expected}",
        number - 1
    )]
    ChainBreak {
        number: u64,
        expected: B256,
        parent: B256,
    },
    /// Two records of block `number` with different hashes: one follower
    /// instance read a lie its cross-check did not catch.
    #[error(
        "two records of L1 block {number}: {first} (published) and {second}; the follower instances disagree"
    )]
    Disagreement {
        number: u64,
        first: B256,
        second: B256,
    },
    /// Two records of block `number` with the same hash and a different
    /// epoch or batches: one follower instance derived the block's content
    /// from a lie its cross-check did not catch.
    #[error(
        "two records of L1 block {number} with one hash and different contents; the follower instances disagree"
    )]
    ContentDisagreement { number: u64 },
    /// The publisher transport is permanently closed. The watcher must exit.
    #[error("deposit publisher closed")]
    PublisherClosed,
}

impl MonitorError {
    /// The halt this error puts the watcher in: `l1_chain_break` for a
    /// record that does not descend from the head, which clears by itself
    /// once a record that does arrives; `l1_follower_disagreement` for
    /// two hashes of one block, which an operator clears.
    #[must_use]
    pub fn halt(&self) -> Option<Halt> {
        let cause = match self {
            Self::ChainBreak { .. } => Some(HaltCause::L1ChainBreak),
            Self::Disagreement { .. } | Self::ContentDisagreement { .. } => {
                Some(HaltCause::L1FollowerDisagreement)
            }
            Self::PublisherClosed => None,
        };
        cause.map(|cause| Halt::new(cause, self.to_string()))
    }
}

/// Where the watcher stands on L1.
#[derive(Debug, Clone)]
enum Position {
    /// No block yet: the first record anchors the watcher.
    Tip,
    /// Resume after this block. Its record gives its hash, which then
    /// anchors the next block.
    After(L1ResumeAfter),
    /// The confirmed block, the epochs published after it, and the rule
    /// of the two follower instances at the window's head. The next record
    /// must name the head's hash as its parent.
    Anchored(Window, L1BlockDedup),
}

/// The block whose record the watcher needs next and does not have.
#[derive(Debug, Clone, Copy)]
struct Wanted {
    number: u64,
    since: Instant,
}

/// The L1 watcher's state: the record feed, the epoch publisher, the
/// position, what confirms a published epoch, and the file that keeps the
/// confirmed block across a restart.
///
/// The position chains every block to the one before it by the parent
/// hash. The cursor file keeps the link across a restart, so the first
/// block after a restart is linked too.
///
/// With a sealer feed ([`Self::following`]), the boundaries' L1 origin
/// confirms the published epochs, the file holds the confirmed origin,
/// and the epochs that no boundary confirms are published again. Without
/// one, a publish confirms its epoch, and the file holds the last
/// published block.
pub struct L1Watcher<F, P> {
    feed: F,
    publisher: P,
    tick: Duration,
    position: Position,
    confirmation: Confirmation,
    cursor_file: Option<CursorFile<L1Cursor>>,
    /// The cursor the file holds, as this run last read or wrote it.
    persisted: Option<L1Cursor>,
    /// The records that arrived and wait for their turn, in order.
    inbox: VecDeque<L1Block>,
    /// The block whose record the archives must give: the next history
    /// read starts there.
    wanted: Option<Wanted>,
    /// No history read before this instant.
    history_at: Option<Instant>,
    /// No publish before this instant: the last one failed.
    retry_at: Option<Instant>,
    /// An operator halt stands: the watcher takes no record until the
    /// clear.
    held: bool,
    upstream: Upstream,
}

impl<F: BlockFeed, P: EpochPublisher> L1Watcher<F, P> {
    /// A watcher at the configured resume block, or at the first record.
    /// [`Self::load_cursor`] then applies the cursor file.
    #[must_use]
    pub fn new(
        publisher: P,
        feed: F,
        config: DaWatcherConfig,
        cursor_file: Option<CursorFile<L1Cursor>>,
    ) -> Self {
        let mut watcher = Self {
            feed,
            publisher,
            tick: config.tick,
            position: Position::Tip,
            confirmation: Confirmation::Publish,
            cursor_file,
            persisted: None,
            inbox: VecDeque::new(),
            wanted: None,
            history_at: None,
            retry_at: None,
            held: false,
            upstream: Upstream::new(config.silence),
        };
        if let Some(after) = config.resume_after {
            watcher.resume_after(after);
        }
        watcher
    }

    /// The last L1 block whose epoch was published, or `None` before the
    /// first anchor.
    #[must_use]
    pub fn cursor(&self) -> Option<u64> {
        match &self.position {
            Position::Tip => None,
            Position::After(block) => Some(block.block()),
            Position::Anchored(window, _) => Some(window.head().number),
        }
    }

    /// The confirmed block, or the block the watcher resumed after while
    /// no boundary confirmed one. `None` before the first anchor.
    #[must_use]
    pub fn confirmed(&self) -> Option<L1Cursor> {
        match &self.position {
            Position::Anchored(window, _) => Some(window.base()),
            Position::Tip | Position::After(_) => None,
        }
    }

    /// Resume after `after`: the watcher needs its record.
    fn resume_after(&mut self, after: L1ResumeAfter) {
        self.position = Position::After(after);
        self.inbox.clear();
        self.want(after.block());
    }

    /// Anchor at `cursor`: the next record must name its hash as its
    /// parent. The watcher reads the records after it from the archives.
    fn anchor_at(&mut self, cursor: L1Cursor) {
        self.position = Position::Anchored(
            Window::new(cursor),
            L1BlockDedup::after(cursor.number, cursor.hash),
        );
    }

    /// Apply the cursor file to the start position. The order of
    /// precedence:
    ///
    /// 1. `resume_after` wins. The operator sets it after a seed, and the
    ///    file then holds a block of the old chain. The watcher anchors at
    ///    the resume block's record and overwrites the file.
    /// 2. A file that parses: resume after its block, linked to its hash.
    ///    The watcher reads the records after it from the archives.
    /// 3. No file: the first record of the stream, logged as a warning.
    ///    The epochs between the last publish and that record are lost,
    ///    unless this is the chain's first start.
    ///
    /// With a sealer feed, the first boundary then overrides 2 and 3: see
    /// [`Self::start_at`].
    ///
    /// # Errors
    /// Returns [`CursorError`] when the file exists but cannot be read or
    /// parsed. The watcher never guesses a position past that: see
    /// [`Self::cursor_halt`].
    pub fn load_cursor(&mut self) -> Result<(), CursorError> {
        let Some(file) = &self.cursor_file else {
            warn!(
                target: "da_watcher",
                "no L1 cursor file is set; a restart starts at the newest record and loses the \
                 epochs between the last publish and that record"
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
                "NO L1 CURSOR FILE: starting at the newest record of l1_blocks. Unless this is \
                 the chain's first start, the deposits of the L1 blocks between the last publish \
                 and that record are lost; give --l1-resume-after to resume after a known block"
            );
            return Ok(());
        };
        info!(
            target: "da_watcher",
            l1_number = cursor.number,
            l1_hash = %cursor.hash,
            "resuming after the block in the L1 cursor file"
        );
        self.anchor_at(cursor);
        self.want(cursor.number.saturating_add(1));
        self.persisted = Some(cursor);
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
        feed: F,
        config: DaWatcherConfig,
        cursor_file: Option<CursorFile<L1Cursor>>,
    ) -> WatcherHandle {
        Self::new(publisher, feed, config, cursor_file).start()
    }

    /// Spawn this watcher's loop. Return a [`WatcherHandle`] that owns the
    /// task and a cooperative shutdown channel.
    pub fn start(self) -> WatcherHandle {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        WatcherHandle {
            task: tokio::spawn(self.run(shutdown_rx)),
            shutdown: shutdown_tx,
        }
    }

    /// Load the cursor, follow the sealer's first boundary, then run the
    /// loop. `shutdown` stays outside the state, so the select in
    /// [`Self::step`] can wait on it while the step borrows `self`.
    async fn run(mut self, mut shutdown: oneshot::Receiver<()>) {
        if self.await_cursor(&mut shutdown).await.is_break() {
            return;
        }
        if self.follow_start(&mut shutdown).await.is_break() {
            return;
        }
        let mut housekeeping = tokio::time::interval(self.tick);
        housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while let ControlFlow::Continue(()) = self.step(&mut shutdown, &mut housekeeping).await {}
        self.persist_cursor();
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

    /// Take the work at hand: a history read that is due, then every
    /// record that waits. Then wait for the next record, a new sealer
    /// origin, a due re-publish, the operator's clear, the housekeeping
    /// tick, or the shutdown signal. `Break` ends the loop: shutdown, a
    /// closed stream, or a closed publisher. The origin comes before the
    /// record, so a stream that runs ahead cannot hide a new origin.
    async fn step(
        &mut self,
        shutdown: &mut oneshot::Receiver<()>,
        housekeeping: &mut tokio::time::Interval,
    ) -> ControlFlow<()> {
        if Self::report(&self.process_once().await).is_break() {
            return ControlFlow::Break(());
        }
        let due = self.republish_at();
        let held = self.held;
        tokio::select! {
            biased;
            _ = shutdown => {
                info!(target: "da_watcher", "shutting down");
                ControlFlow::Break(())
            }
            () = halt::cleared(), if held => {
                self.release();
                ControlFlow::Continue(())
            }
            origin = self.confirmation.changed() => {
                self.on_sealer_origin(origin);
                ControlFlow::Continue(())
            }
            () = Self::sleep_until(due) => Self::report_republish(&self.republish_due()),
            _ = housekeeping.tick() => {
                self.housekeep();
                ControlFlow::Continue(())
            }
            record = self.feed.next(), if !held => self.receive(record),
        }
    }

    /// Take one live record into the inbox. `Break` when the stream
    /// closed: the process is shutting down.
    fn receive(&mut self, record: Option<L1Block>) -> ControlFlow<()> {
        let Some(record) = record else {
            warn!(target: "da_watcher", "the l1_blocks stream closed; exiting");
            return ControlFlow::Break(());
        };
        self.arrived(record);
        ControlFlow::Continue(())
    }

    /// A live record arrived: it waits in the inbox for its turn.
    fn arrived(&mut self, record: L1Block) {
        self.upstream.saw_record();
        Self::mark_finalized(record.number);
        self.inbox.push_back(record);
    }

    /// Export the newest finalized block the stream carried, live or from
    /// the archives.
    #[allow(
        clippy::cast_precision_loss,
        reason = "metric value; never nears 2^52 for an L1 block number"
    )]
    fn mark_finalized(number: u64) {
        ::metrics::gauge!(metrics::L1_FINALIZED).set(number as f64);
    }

    /// The operator cleared the halt: read the records after the head
    /// again.
    fn release(&mut self) {
        info!(target: "da_watcher", "the operator cleared the halt; reading l1_blocks again");
        self.held = false;
        if let Some(head) = self.cursor() {
            self.want(head.saturating_add(1));
        }
    }

    /// Log the outcome of one pass and raise its halt. Only a closed
    /// publisher stops the loop.
    fn report(outcome: &Result<usize, MonitorError>) -> ControlFlow<()> {
        match outcome {
            Ok(0) => {}
            Ok(n) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "ok").increment(1);
                info!(target: "da_watcher", published = n, "epochs published");
            }
            Err(MonitorError::PublisherClosed) => {
                warn!(target: "da_watcher", "publisher closed; exiting");
                return ControlFlow::Break(());
            }
            Err(e @ MonitorError::ChainBreak { .. }) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "chain_break").increment(1);
                warn!(target: "da_watcher", error = %e, "a record does not descend from the head");
                Self::raise(e);
            }
            Err(
                e @ (MonitorError::Disagreement { .. } | MonitorError::ContentDisagreement { .. }),
            ) => {
                ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "disagreement").increment(1);
                error!(target: "da_watcher", error = %e, "the follower instances disagree; halted until an operator clears it");
                Self::raise(e);
            }
        }
        ControlFlow::Continue(())
    }

    fn raise(error: &MonitorError) {
        if let Some(halt) = error.halt() {
            halt::raise(halt);
        }
    }

    /// One pass, with no wait for a live record: the live records that
    /// already arrived, the history read when one is due (its records go
    /// before the live ones), then every record
    /// that waits, in order, up to the first that does not publish. This
    /// is public, so unit tests can call it with no timer or thread.
    ///
    /// On success, return how many epochs the pass published. After the
    /// pass, success or not, the cursor file receives the confirmed block
    /// when it moved. The write follows the confirm it records, never
    /// precedes it: see [`crate::cursor`].
    ///
    /// # Errors
    /// Returns [`MonitorError`] on a record that breaks the chain, two
    /// records that disagree, or a closed publisher. The head stays at the
    /// last block that published.
    pub async fn process_once(&mut self) -> Result<usize, MonitorError> {
        self.take_arrived();
        self.read_history().await;
        let outcome = self.process();
        self.persist_cursor();
        self.record_window();
        outcome
    }

    /// Move the live records that already arrived into the inbox.
    fn take_arrived(&mut self) {
        while let Some(record) = self.feed.try_next() {
            self.arrived(record);
        }
    }

    /// Ask the archives for the records from `number` on, at the next
    /// history read. The watcher waits for that record from now.
    fn want(&mut self, number: u64) {
        let since = match self.wanted {
            Some(wanted) if wanted.number == number => wanted.since,
            _ => Instant::now(),
        };
        self.wanted = Some(Wanted { number, since });
    }

    /// The history read, when the watcher wants a record and the last
    /// read is one tick back: the records go before the live ones that
    /// wait. The watcher keeps wanting the block until a record of it is
    /// taken, so a read that comes back short is repeated every tick.
    async fn read_history(&mut self) {
        let now = Instant::now();
        let Some(wanted) = self.wanted else {
            return;
        };
        if self.history_at.is_some_and(|at| at > now) {
            return;
        }
        self.history_at = now.checked_add(self.tick);
        let history = self.feed.history(wanted.number).await;
        if let Some(newest) = history.iter().map(|record| record.number).max() {
            self.upstream.saw_record();
            Self::mark_finalized(newest);
        }
        let live = std::mem::take(&mut self.inbox);
        self.inbox = history.into_iter().chain(live).collect();
    }

    /// Write the confirmed block to the file when it moved since the last
    /// write. A failed write is counted and logged, not fatal: the file
    /// then holds an older block, and a restart publishes the epochs
    /// after it again, which the sealer drops. The next pass tries again.
    fn persist_cursor(&mut self) {
        let (Some(cursor), Some(file)) = (self.confirmed(), &self.cursor_file) else {
            return;
        };
        if self.persisted == Some(cursor) {
            return;
        }
        match file.persist(&cursor) {
            Ok(()) => self.persisted = Some(cursor),
            Err(e) => Self::persist_failed(cursor, &e),
        }
    }

    /// Count and log a failed write of the cursor file.
    fn persist_failed(cursor: L1Cursor, e: &CursorError) {
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
