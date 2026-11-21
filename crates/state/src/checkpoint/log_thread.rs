//! [`BlockLogThread`]: the thread that writes the block log behind the
//! state writer.
//!
//! The state writer offers the batch of each committed block on a bounded
//! queue, and never waits for it. A full queue drops the batch, and the log
//! has a gap there. So a slow disk costs restore points, but never a state
//! commit. The thread appends each batch, and syncs the log on a fixed
//! period. The state database stays the source of truth: the log is
//! durable only so that a restore after a crash finds it.
//!
//! At start the thread matches the log to the last committed block of the
//! state database. It cuts the records above that block, because the
//! executor executes those blocks again. A log that ends below that block
//! continues after a gap.
//!
//! The thread also shows the newest restore point of the checkpoint
//! directory in the gauges of [`super::newest`], at start and after each
//! sync.

use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use tracing::warn;

use super::block_log::{BlockLogWriter, LogIdentity, SegmentSize};
use super::manifest::stored_genesis_digest;
use super::newest::{LogEnd, NewestCheckpoint};
use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::SCHEMA_VERSION;
use crate::recovery::read_recovery_point;
use crate::writer::WriteBatch;

/// The batches that the full queue dropped.
const LOG_DROPPED: &str = "kardamom_checkpoint_log_dropped_total";
/// The appends and syncs of the log that failed.
const LOG_FAILED: &str = "kardamom_checkpoint_log_failed_total";

/// The settings of the block log.
#[derive(Clone, Debug)]
pub struct LogConfig {
    /// The directory of the segments.
    pub dir: PathBuf,
    /// The size at which a segment ends.
    pub segment_bytes: NonZeroU64,
    /// The batches that may wait for the thread.
    pub queue: NonZeroUsize,
    /// The period of the sync.
    pub sync_every: Duration,
}

/// The side of the block log that the state writer holds.
pub struct LogFeed {
    tx: Sender<WriteBatch>,
}

impl LogFeed {
    /// Offer the batch of a committed block. A full queue drops it, and a
    /// stopped thread takes nothing.
    pub(crate) fn offer(&self, batch: WriteBatch) {
        if self.tx.try_send(batch).is_err() {
            metrics::counter!(LOG_DROPPED).increment(1);
        }
    }
}

/// The thread that writes the block log.
pub struct BlockLogThread {
    log: BlockLogWriter,
    rx: Receiver<WriteBatch>,
    sync_every: Duration,
    synced_at: Instant,
    newest: NewestGauges,
}

/// What the gauges of the newest checkpoint need from the log thread.
pub(crate) struct NewestGauges {
    /// The checkpoint directory: the parent of the log directory.
    checkpoints_dir: PathBuf,
    segment_bytes: NonZeroU64,
    /// The time of the sync that made the last block of the log durable.
    /// At start, the time of the last write to the log on disk.
    written: Option<SystemTime>,
    /// Whether a block was appended after the last sync.
    appended: bool,
}

impl BlockLogThread {
    /// Open the log in `cfg.dir`, match it to the last committed block of
    /// `env`, and start the thread. The thread ends when the feed is gone.
    ///
    /// # Errors
    ///
    /// Returns an error when `env` has no genesis digest, or when the log
    /// does not open or cut.
    pub fn spawn(env: &StateEnv, cfg: &LogConfig) -> Result<(LogFeed, JoinHandle<()>), StateError> {
        let identity = LogIdentity {
            genesis: stored_genesis_digest(env)?,
            schema: SCHEMA_VERSION,
        };
        let committed = read_recovery_point(env)?.last_committed_block;
        // The open and the cut change the modification time of the last
        // segment, so the gauges read it first.
        let newest = NewestGauges::new(cfg)?;
        let mut log = BlockLogWriter::open(&cfg.dir, identity, cfg.segment_bytes)?;
        log.cut_after(committed)?;
        let (tx, rx) = crossbeam_channel::bounded(cfg.queue.get());
        let thread = Self {
            log,
            rx,
            sync_every: cfg.sync_every,
            synced_at: Instant::now(),
            newest,
        };
        let join = thread::Builder::new()
            .name("kardamom-block-log".into())
            .spawn(move || thread.run())?;
        Ok((LogFeed { tx }, join))
    }

    fn run(mut self) {
        self.newest.publish(&self.log);
        while self.step().is_continue() {}
    }

    /// Append the next batch, or wait for the sync period, then sync when
    /// the period is over. `Break` once the feed is gone and the last
    /// batches are synced.
    fn step(&mut self) -> ControlFlow<()> {
        match self.rx.recv_timeout(self.sync_every) {
            Ok(batch) => self.append(&batch),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                self.sync();
                return ControlFlow::Break(());
            }
        }
        if self.synced_at.elapsed() >= self.sync_every {
            self.sync();
        }
        ControlFlow::Continue(())
    }

    fn append(&mut self, batch: &WriteBatch) {
        let result = self.log.append(batch);
        self.newest.appended |= result.is_ok();
        self.on_failure(result, "append");
    }

    fn sync(&mut self) {
        let result = self.log.sync();
        self.synced_at = Instant::now();
        if result.is_ok() {
            self.newest.synced(&self.log);
        }
        self.on_failure(result, "sync");
    }

    /// Count and log a failed write, and give up the open segment: the
    /// next batch starts a new one after the gap.
    fn on_failure(&mut self, result: Result<(), StateError>, step: &'static str) {
        if let Err(e) = result {
            warn!(error = %e, step, "the block log failed; the next batch starts a new segment");
            metrics::counter!(LOG_FAILED, "step" => step).increment(1);
            self.log.abandon();
        }
    }
}

impl NewestGauges {
    /// The gauges of the log in `cfg.dir`, before the writer opens it.
    pub(crate) fn new(cfg: &LogConfig) -> Result<Self, StateError> {
        // The log directory is `<checkpoints>/log`, as a restore reads it.
        let checkpoints_dir = cfg
            .dir
            .parent()
            .map_or_else(|| cfg.dir.clone(), Path::to_path_buf);
        Ok(Self {
            checkpoints_dir,
            segment_bytes: cfg.segment_bytes,
            written: SegmentSize::last_write(&cfg.dir)?,
            appended: false,
        })
    }

    /// A sync of `log` succeeded: the blocks appended before it are
    /// durable now.
    fn synced(&mut self, log: &BlockLogWriter) {
        if std::mem::take(&mut self.appended) {
            self.written = Some(SystemTime::now());
        }
        self.publish(log);
    }

    fn publish(&self, log: &BlockLogWriter) {
        NewestCheckpoint::show(self.newest(log));
    }

    /// The newest restore point of the checkpoint directory, with the end
    /// of `log`. With no open segment, the newest base.
    pub(crate) fn newest(
        &self,
        log: &BlockLogWriter,
    ) -> Result<Option<NewestCheckpoint>, StateError> {
        let end = log
            .next_block()
            .and_then(|next| next.checked_sub(1))
            .zip(self.written)
            .map(|(block, written)| LogEnd {
                block,
                written,
                segment_bytes: self.segment_bytes,
            });
        match end {
            Some(end) => end.newest(&self.checkpoints_dir),
            None => NewestCheckpoint::base(&self.checkpoints_dir),
        }
    }
}

#[cfg(test)]
#[path = "log_thread_tests.rs"]
mod tests;
