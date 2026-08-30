//! State backend startup: checkpoint serve/restore, env open, the durable
//! cursor, the periodic checkpointer task, and the resume decision.

use anyhow::{Context, Result};
use kardamom_engine::ResumePoint;
use kardamom_state::checkpoint::{create_checkpoint, prune_checkpoints};
use kardamom_state::{StateEnv, StateEnvBuilder, read_recovery_point};
use tokio_util::sync::CancellationToken;

use crate::args::Args;

/// Everything `main` needs from the state side before streams open.
pub(crate) struct PreparedState {
    pub(crate) env: StateEnv,
    /// The persisted cursor (GENESIS-valued on a fresh DB): the cluster
    /// client replays the canonical stream from it and the reader/exec
    /// threads seed their absolute counters from it (see `ResumePoint`).
    pub(crate) start: ResumePoint,
}

/// Fast cold-start recovery, env open, cursor read, checkpointer spawn.
///
/// Restore runs BEFORE opening the env: if the state dir is empty (a
/// fresh/wiped node) and a checkpoint is available locally or from peers,
/// startup then sees a populated DB and replays only the tail instead of
/// re-syncing from genesis.
pub(crate) fn prepare_state(
    args: &Args,
    expected_genesis: Option<alloy_primitives::B256>,
    shutdown: CancellationToken,
) -> Result<PreparedState> {
    if let Some(ckpt_dir) = args.checkpoint_dir.as_ref() {
        // Serve this node's checkpoints to peers (the other side of the peer
        // fetch below). Best-effort infrastructure, but a bad bind address is
        // a deploy bug — fail startup loudly.
        // Runs as a tokio task for the life of the process (called inside
        // the runtime; the handle is not needed).
        if let Some(addr) = args.checkpoint_serve_addr {
            kardamom_state::serve_checkpoints(addr, ckpt_dir.clone())
                .context("bind checkpoint serve address")?;
        }
        // Fresh iff the state dir has no mdbx data file — checked WITHOUT opening
        // the env (opening would itself create the data file and defeat restore).
        // (Observed motivation for the ladder's quarantine rung: a copy that
        // raced the writer's prune had no MANIFEST; every restart refused it
        // and the fleet sat at 2/3.)
        let fresh = !kardamom_state::checkpoint::has_state_db(&args.state_dir)
            .context("probe state dir")?;
        if fresh {
            let restored = kardamom_engine::bin_support::restore_or_fetch_checkpoint(
                ckpt_dir,
                &args.state_dir,
                &args.checkpoint_peers,
                expected_genesis,
            )?;
            match restored {
                Some((block, path)) => {
                    tracing::info!(
                        restored_block = block,
                        checkpoint = %path.display(),
                        "restored state from checkpoint; will replay tail from here"
                    );
                }
                None => tracing::info!(
                    checkpoint_dir = %ckpt_dir.display(),
                    "no checkpoint available locally or from peers; fresh start will \
                     replay from genesis (refused if the chain outgrew the cluster \
                     retention window — then a peer checkpoint or rebuild-from-L1 is required)"
                ),
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

    // Crash-recovery cursor. A non-genesis cursor means we restarted
    // mid-chain; a fresh start IS a resume from the genesis cursor.
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

/// Periodic checkpointing (fast recovery for OTHER nodes, and for this node
/// on a future wipe). `compact_to` runs against an online RO snapshot, so it
/// never blocks the writer. A tokio interval task; each tick runs the mdbx
/// compaction on `spawn_blocking` (the txn stays on one thread for the whole
/// call). Prunes to `checkpoint_keep`. Stops when `shutdown` is cancelled.
/// Must be called inside a tokio runtime.
fn spawn_checkpointer(args: &Args, env: &StateEnv, shutdown: CancellationToken) {
    let (Some(ckpt_dir), true) = (
        args.checkpoint_dir.clone(),
        args.checkpoint_interval_secs > 0,
    ) else {
        return;
    };
    let ckpt_env = env.clone();
    let interval = std::time::Duration::from_secs(args.checkpoint_interval_secs);
    let keep = args.checkpoint_keep;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately; the old thread slept first.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let env = ckpt_env.clone();
            let dir = ckpt_dir.clone();
            let ran = tokio::task::spawn_blocking(move || checkpoint_once(&env, &dir, keep)).await;
            if let Err(e) = ran {
                tracing::warn!(error = %e, "checkpointer task panicked");
            }
        }
    });
}

/// One checkpoint + prune round; failures are logged, never fatal.
fn checkpoint_once(env: &StateEnv, ckpt_dir: &std::path::Path, keep: u64) {
    match create_checkpoint(env, ckpt_dir) {
        Ok(info) => {
            if info.block > keep
                && let Err(e) = prune_checkpoints(ckpt_dir, info.block - keep + 1)
            {
                tracing::warn!(error = %e, "checkpoint prune failed");
            }
        }
        Err(e) => tracing::warn!(error = %e, "checkpoint creation failed"),
    }
}
