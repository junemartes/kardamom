//! State backend startup: checkpoint restore, env open, the durable
//! cursor, the periodic checkpointer task, and the resume decision.

use std::ops::ControlFlow;

use anyhow::{Context, Result};
use kardamom_engine::ResumePoint;
use kardamom_engine::reader::carried::LocalHistory;
use kardamom_state::checkpoint::{create_checkpoint, prune_checkpoints};
use kardamom_state::{BlockRange, StateEnv, StateEnvBuilder, StateSnapshot, read_recovery_point};
use tokio_util::sync::CancellationToken;

use crate::args::Args;

/// Everything `main` needs from the state side before streams open.
pub(crate) struct PreparedState {
    pub(crate) env: StateEnv,
    /// The persisted cursor. It has a genesis value on a fresh DB. The
    /// cluster client replays the canonical stream from it, and the reader
    /// and exec threads seed their absolute counters from it (see
    /// `ResumePoint`).
    pub(crate) start: ResumePoint,
    /// The periodic checkpoint task, when one runs. It ends on the
    /// shutdown token, and the process joins it before a repair: a late
    /// round could otherwise publish a checkpoint of a diverged state, or
    /// prune the checkpoint that a rollback chose.
    pub(crate) checkpointer: Option<tokio::task::JoinHandle<()>>,
}

impl PreparedState {
    /// The committed history of the last `window` blocks, which the reader
    /// of the member feed compares with the stream before it delivers.
    /// `None` at genesis.
    ///
    /// # Errors
    ///
    /// Returns `Err` when a read of the state DB fails.
    pub(crate) fn history(&self, window: std::num::NonZeroU64) -> Result<Option<LocalHistory>> {
        let cursor = kardamom_engine::bin_support::cluster_replay_cursor(&self.start);
        let snapshot = StateSnapshot::open(&self.env).context("open a snapshot for the history")?;
        LocalHistory::read(&snapshot, cursor.position(), window)
            .context("read the committed history")
    }
}

/// Fast cold-start recovery, env open, cursor read, checkpointer spawn.
///
/// Restore runs before the env opens. If the state dir is empty (a fresh
/// or wiped node) and a checkpoint in `restore` is available locally or
/// from a peer, startup then sees a populated DB. It replays only the
/// tail, instead of re-syncing from genesis. A rollback limits `restore`
/// to the blocks below its divergence point.
pub(crate) fn prepare_state(
    args: &Args,
    expected_genesis: Option<alloy_primitives::B256>,
    restore: BlockRange,
    shutdown: CancellationToken,
) -> Result<PreparedState> {
    if let Some(ckpt_dir) = args.checkpoint_dir.as_ref() {
        restore_if_fresh(args, ckpt_dir, restore, expected_genesis)?;
    }

    // Open the libmdbx state env and read the durable cursor.
    let env = StateEnvBuilder::new(&args.state_dir)
        .durability(args.state_durability.into())
        .open()
        .with_context(|| format!("open state env at {}", args.state_dir.display()))?;
    let recovery = read_recovery_point(&env).context("read state recovery point")?;

    let checkpointer = spawn_checkpointer(args, &env, shutdown);

    // Crash-recovery cursor. A non-genesis cursor means the node restarted
    // mid-chain. A fresh start is just a resume from the genesis cursor.
    let start = ResumePoint::from(&recovery);
    if start.is_resume() {
        tracing::info!(
            resume_block = start.block,
            resume_record_count = start.record_count,
            "resuming from persisted state cursor via cluster canonical replay"
        );
    }

    Ok(PreparedState {
        env,
        start,
        checkpointer,
    })
}

/// Restores from a checkpoint, but only on a fresh state dir. A non-fresh
/// dir already has a chain to resume, so a restore here would overwrite
/// it.
fn restore_if_fresh(
    args: &Args,
    ckpt_dir: &std::path::Path,
    restore: BlockRange,
    expected_genesis: Option<alloy_primitives::B256>,
) -> Result<()> {
    // The state dir is fresh only if it has no mdbx data file. Check this
    // without opening the env: opening would create the data file itself
    // and defeat the restore.
    let fresh =
        !kardamom_state::checkpoint::has_state_db(&args.state_dir).context("probe state dir")?;
    if !fresh {
        return Ok(());
    }
    let restored = kardamom_engine::bin_support::restore_or_fetch_checkpoint(
        ckpt_dir,
        &args.state_dir,
        &args.checkpoint_peers,
        restore,
        expected_genesis,
    )?;
    log_restore_outcome(ckpt_dir, restored);
    Ok(())
}

/// Logs whether the fresh-start restore found a checkpoint to replay from.
fn log_restore_outcome(ckpt_dir: &std::path::Path, restored: Option<(u64, std::path::PathBuf)>) {
    let Some((block, path)) = restored else {
        tracing::info!(
            checkpoint_dir = %ckpt_dir.display(),
            "no checkpoint available locally or from peers; fresh start will \
             replay from genesis (refused if the chain outgrew the cluster \
             retention window — then a peer checkpoint or rebuild-from-L1 is required)"
        );
        return;
    };
    tracing::info!(
        restored_block = block,
        checkpoint = %path.display(),
        "restored state from checkpoint; will replay tail from here"
    );
}

/// Start periodic checkpointing, if the args ask for it. It gives fast
/// recovery for other nodes, and for this node after a future wipe. Call
/// this inside a tokio runtime.
fn spawn_checkpointer(
    args: &Args,
    env: &StateEnv,
    shutdown: CancellationToken,
) -> Option<tokio::task::JoinHandle<()>> {
    let (Some(ckpt_dir), Some(interval_secs)) =
        (args.checkpoint_dir.clone(), args.checkpoint_interval_secs.0)
    else {
        return None;
    };
    let round = CheckpointRound {
        env: env.clone(),
        dir: ckpt_dir,
        keep: args.checkpoint_keep,
    };
    let interval = std::time::Duration::from_secs(interval_secs.get());
    Some(Checkpointer::new(round, interval, shutdown).spawn())
}

/// One checkpoint round's fixed inputs: the state env, the checkpoint
/// directory, and how many of the newest checkpoints to keep. Cloned into each
/// round's blocking task, so the mdbx transaction stays on one thread for
/// the whole call.
#[derive(Clone)]
struct CheckpointRound {
    env: StateEnv,
    dir: std::path::PathBuf,
    keep: std::num::NonZeroUsize,
}

impl CheckpointRound {
    /// One checkpoint + prune round; failures are logged, never fatal.
    fn once(&self) {
        match create_checkpoint(&self.env, &self.dir) {
            Ok(_) => self.prune_stale(),
            Err(e) => tracing::warn!(error = %e, "checkpoint creation failed"),
        }
    }

    /// Prunes all checkpoints except the newest `self.keep`, whatever
    /// their block spacing. Failures are logged, never fatal.
    fn prune_stale(&self) {
        if let Err(e) = prune_checkpoints(&self.dir, self.keep) {
            tracing::warn!(error = %e, "checkpoint prune failed");
        }
    }
}

/// The periodic checkpoint task's state: the round inputs, the interval
/// ticker, and the shutdown signal. `compact_to` runs against an online
/// read-only snapshot, so it never blocks the writer.
struct Checkpointer {
    round: CheckpointRound,
    ticker: tokio::time::Interval,
    shutdown: CancellationToken,
}

impl Checkpointer {
    fn new(
        round: CheckpointRound,
        interval: std::time::Duration,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            round,
            ticker: tokio::time::interval(interval),
            shutdown,
        }
    }

    /// Start the interval task. It stops when `shutdown` is cancelled.
    fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run())
    }

    /// The interval loop. The first tick fires immediately and is
    /// skipped.
    async fn run(mut self) {
        self.ticker.tick().await;
        loop {
            let ControlFlow::Continue(()) = self.tick().await else {
                return;
            };
        }
    }

    /// Wait for the next checkpoint tick, or the shutdown signal, then
    /// run one checkpoint round if it was a tick. Returns
    /// [`ControlFlow::Break`] once `shutdown` fires, so the caller's
    /// loop stops.
    async fn tick(&mut self) -> ControlFlow<()> {
        tokio::select! {
            () = self.shutdown.cancelled() => return ControlFlow::Break(()),
            _ = self.ticker.tick() => {}
        }
        let round = self.round.clone();
        let ran = tokio::task::spawn_blocking(move || round.once()).await;
        if let Err(e) = ran {
            tracing::warn!(error = %e, "checkpointer task panicked");
        }
        ControlFlow::Continue(())
    }
}
