//! State backend startup: checkpoint serve/restore, env open, the durable
//! cursor, the periodic checkpointer thread, and the resume decision.

use anyhow::{Context, Result};
use kardamom_executor::ResumePoint;
use kardamom_state::checkpoint::{create_checkpoint, prune_checkpoints};
use kardamom_state::{StateEnv, StateEnvBuilder, read_recovery_point};

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
) -> Result<PreparedState> {
    if let Some(ckpt_dir) = args.checkpoint_dir.as_ref() {
        // Serve this node's checkpoints to peers (the other side of the peer
        // fetch below). This is best-effort infrastructure. But a bad bind
        // address is a deploy bug, so fail startup loudly.
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

    spawn_checkpointer(args, &env)?;

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

/// Periodic checkpointing. This gives fast recovery for other nodes, and
/// for this node after a future wipe. `compact_to` runs against an online
/// read-only snapshot, so it never blocks the writer. An interval guards
/// it, and it prunes to `checkpoint_keep`.
fn spawn_checkpointer(args: &Args, env: &StateEnv) -> Result<()> {
    let (Some(ckpt_dir), true) = (
        args.checkpoint_dir.clone(),
        args.checkpoint_interval_secs > 0,
    ) else {
        return Ok(());
    };
    let ckpt_env = env.clone();
    let interval = std::time::Duration::from_secs(args.checkpoint_interval_secs);
    let keep = args.checkpoint_keep;
    std::thread::Builder::new()
        .name("kardamom-checkpointer".into())
        .spawn(move || {
            loop {
                std::thread::sleep(interval);
                match create_checkpoint(&ckpt_env, &ckpt_dir) {
                    Ok(info) => {
                        if info.block > keep
                            && let Err(e) = prune_checkpoints(&ckpt_dir, info.block - keep + 1)
                        {
                            tracing::warn!(error = %e, "checkpoint prune failed");
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "checkpoint creation failed"),
                }
            }
        })
        .context("spawn checkpointer thread")?;
    Ok(())
}
