//! State backend startup: checkpoint serve/restore, env open, the durable
//! cursor, the periodic checkpointer task, and the resume decision.

use std::ops::ControlFlow;

use anyhow::{Context, Result};
use kardamom_engine::ResumePoint;
use kardamom_state::checkpoint::{create_checkpoint, prune_checkpoints};
use kardamom_state::{StateEnv, StateEnvBuilder, read_recovery_point};
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
}

/// Fast cold-start recovery, env open, cursor read, checkpointer spawn.
///
/// Restore runs before the env opens. If the state dir is empty (a fresh
/// or wiped node) and a checkpoint is available locally or from a peer,
/// startup then sees a populated DB. It replays only the tail, instead of
/// re-syncing from genesis.
pub(crate) fn prepare_state(
    args: &Args,
    expected_genesis: Option<alloy_primitives::B256>,
    shutdown: CancellationToken,
) -> Result<PreparedState> {
    if let Some(ckpt_dir) = args.checkpoint_dir.as_ref() {
        // Serve this node's checkpoints to peers (the other side of the peer
        // fetch below). This is best-effort infrastructure. But a bad bind
        // address is a deploy bug, so fail startup loudly.
        // Runs as a tokio task for the life of the process (called inside
        // the runtime; the handle is not needed).
        if let Some(addr) = args.checkpoint_serve_addr {
            kardamom_state::serve_checkpoints(addr, ckpt_dir.clone())
                .context("bind checkpoint serve address")?;
        }
        // The state dir is fresh only if it has no mdbx data file. Check this
        // without opening the env: opening would create the data file itself
        // and defeat the restore.
        let fresh = !kardamom_state::checkpoint::has_state_db(&args.state_dir)
            .context("probe state dir")?;
        if fresh {
            let restored = kardamom_engine::bin_support::restore_or_fetch_checkpoint(
                ckpt_dir,
                &args.state_dir,
                &args.checkpoint_peers,
                expected_genesis,
            )?;
            if let Some((block, path)) = restored {
                tracing::info!(
                    restored_block = block,
                    checkpoint = %path.display(),
                    "restored state from checkpoint; will replay tail from here"
                );
            } else {
                tracing::info!(
                    checkpoint_dir = %ckpt_dir.display(),
                    "no checkpoint available locally or from peers; fresh start will \
                     replay from genesis (refused if the chain outgrew the cluster \
                     retention window — then a peer checkpoint or rebuild-from-L1 is required)"
                );
            }
        }
    }

    // Open the libmdbx state env and read the durable cursor.
    let env = StateEnvBuilder::new(&args.state_dir)
        .durability(args.state_durability.into())
        .open()
        .with_context(|| format!("open state env at {}", args.state_dir.display()))?;
    let recovery = read_recovery_point(&env).context("read state recovery point")?;

    spawn_checkpointer(args, &env, shutdown);

    // Crash-recovery cursor. A non-genesis cursor means the node restarted
    // mid-chain. A fresh start is just a resume from the genesis cursor.
    let start = ResumePoint {
        block: recovery.last_committed_block,
        record_count: recovery.last_fsynced_b_position.as_index(),
        l2_timestamp: recovery.last_committed_l2_timestamp,
    };
    if start.is_resume() {
        tracing::info!(
            resume_block = start.block,
            resume_record_count = start.record_count,
            "resuming from persisted state cursor via cluster canonical replay"
        );
    }

    Ok(PreparedState { env, start })
}

/// Start periodic checkpointing, if the args ask for it. It gives fast
/// recovery for other nodes, and for this node after a future wipe. Call
/// this inside a tokio runtime.
fn spawn_checkpointer(args: &Args, env: &StateEnv, shutdown: CancellationToken) {
    let (Some(ckpt_dir), Some(interval_secs)) =
        (args.checkpoint_dir.clone(), args.checkpoint_interval_secs.0)
    else {
        return;
    };
    let round = CheckpointRound {
        env: env.clone(),
        dir: ckpt_dir,
        keep: args.checkpoint_keep.get(),
    };
    let interval = std::time::Duration::from_secs(interval_secs.get());
    Checkpointer::new(round, interval, shutdown).spawn();
}

/// One checkpoint round's fixed inputs: the state env, the checkpoint
/// directory, and how many past checkpoints to retain. Cloned into each
/// round's blocking task, so the mdbx transaction stays on one thread for
/// the whole call.
#[derive(Clone)]
struct CheckpointRound {
    env: StateEnv,
    dir: std::path::PathBuf,
    keep: u64,
}

impl CheckpointRound {
    /// One checkpoint + prune round; failures are logged, never fatal.
    fn once(&self) {
        match create_checkpoint(&self.env, &self.dir) {
            Ok(info) => {
                if info.block > self.keep
                    && let Err(e) = prune_checkpoints(&self.dir, info.block - self.keep + 1)
                {
                    tracing::warn!(error = %e, "checkpoint prune failed");
                }
            }
            Err(e) => tracing::warn!(error = %e, "checkpoint creation failed"),
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
    fn spawn(self) {
        tokio::spawn(self.run());
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
