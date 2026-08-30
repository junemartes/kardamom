//! The checkpoint-trust lifecycle, gathered in one place: cold-start
//! checkpoint adoption (with its explicit marker), the post-open trie
//! bootstrap the marker gates, and the at-exit replay-unavailable resync
//! fallback. The executor-shared mechanics (restore ladder, peer fetch,
//! stale-DB parking) live in `kardamom_engine::bin_support`. This module owns
//! the validator-specific trust bookkeeping. Blocks at or below an adopted
//! checkpoint are unverified by this validator, and the frozen-at-genesis
//! trie an executor checkpoint carries must be rebuilt before the
//! incremental walker can extend it.

use std::path::Path;

use anyhow::{Context, Result};
use kardamom_engine::ExecutorError;
use kardamom_engine::bin_support;
use kardamom_state::StateEnv;
use kardamom_validator::metrics;

/// Marker file in the state dir: a checkpoint was adopted and the trie
/// bootstrap has not yet committed. See [`bootstrap_trie_if_adopted`].
const ADOPTION_MARKER: &str = ".adopted-needs-trie-bootstrap";

/// Checkpoint adoption, cold-start half: a fresh validator joining a
/// chain that outgrew the cluster retention window cannot re-execute
/// from genesis, since REPLAY_FROM(genesis) is refused. So it adopts the
/// newest staged or peer checkpoint before opening the env. Startup then
/// resumes from its cursor, and only the tail replays. Blocks through
/// the adopted checkpoint are unverified by this validator. The
/// trustless alternative is a rebuild from L1.
pub fn adopt_checkpoint_if_fresh(
    checkpoint_dir: Option<&Path>,
    state_dir: &Path,
    checkpoint_peers: &[String],
    expected_genesis: Option<alloy_primitives::B256>,
) -> Result<()> {
    let Some(ckpt_dir) = checkpoint_dir else {
        return Ok(());
    };
    let fresh = !kardamom_state::checkpoint::has_state_db(state_dir)
        .context("probe validator state dir")?;
    if !fresh {
        return Ok(());
    }
    let restored = bin_support::restore_or_fetch_checkpoint(
        ckpt_dir,
        state_dir,
        checkpoint_peers,
        expected_genesis,
    )?;
    if let Some((block, ckpt_path)) = restored {
        // Record adoption explicitly. Executor checkpoints carry the
        // genesis-seeded mirror and trie, built for every env by
        // seed_genesis and never updated by the trie-off writer. So a
        // "trie present?" probe passes on an image whose trie is frozen
        // at genesis, and the incremental walker would extend that stale
        // base into silently wrong roots. The shadow-check cannot catch
        // this, since it rebuilds from the same stale mirror.
        // The marker survives a crash between restore and bootstrap; the
        // bootstrap below is idempotent.
        std::fs::write(state_dir.join(ADOPTION_MARKER), b"").context("write adoption marker")?;
        tracing::info!(
            restored_block = block,
            checkpoint = %ckpt_path.display(),
            "adopted state from checkpoint (UNVERIFIED through this block); \
             will re-execute + verify the tail from here"
        );
    }
    Ok(())
}

/// Rebuild the hashed mirror and trie from the plain state tables when
/// the adoption marker, or a truly trie-less image, says to. Must run
/// against the open env, before the trie-aware writer spawns.
///
/// Adopted executor checkpoints carry a trie frozen at genesis, seeded
/// into every env and never updated by the trie-off writer. So adoption
/// is signaled by the explicit marker, not a presence probe, and the
/// bootstrap rebuilds the mirror and trie from the plain state tables as
/// a whole. This is idempotent and crash-safe: one read-write txn, and
/// the marker is removed only after commit. `has_trie` stays as a
/// backup check for a truly mirror-less image, such as an
/// operator-copied directory.
pub fn bootstrap_trie_if_adopted(state_dir: &Path, env: &StateEnv) -> Result<()> {
    let adoption_marker = state_dir.join(ADOPTION_MARKER);
    if adoption_marker.exists() || !kardamom_state::has_trie(env).context("probe state trie")? {
        tracing::info!("adopted state image — bootstrapping hashed mirror + trie");
        let started = std::time::Instant::now();
        let root = kardamom_state::bootstrap_trie_from_state(env)
            .context("bootstrap trie from adopted state")?;
        tracing::info!(
            state_root = %root,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "trie bootstrap complete"
        );
        if adoption_marker.exists() {
            std::fs::remove_file(&adoption_marker).context("clear adoption marker")?;
        }
    }
    Ok(())
}

/// Replay-window overrun: repair before exiting, the same way as the
/// executor's recovery-D path. Fetch a peer checkpoint at or above the
/// retention floor, and park the stale DB, so the next restart takes the
/// ordinary fresh-start restore path instead of a deterministic crash
/// loop re-requesting the same refused REPLAY_FROM.
///
/// The adopted state is unverified by this validator through the
/// checkpoint block. This is an accepted tradeoff: the divergence latch
/// only ever covers blocks this validator actually verified. The
/// trustless alternative remains kardamom-reconstruct, a rebuild from L1,
/// into --state-dir.
pub fn resync_after_engine_error(
    cause: Option<&ExecutorError>,
    checkpoint_dir: Option<&Path>,
    checkpoint_peers: &[String],
    state_dir: &Path,
    expected_genesis: Option<alloy_primitives::B256>,
) -> Result<()> {
    if let Some(outcome) = bin_support::replay_unavailable_fallback(
        cause,
        checkpoint_dir,
        checkpoint_peers,
        state_dir,
        expected_genesis,
        true,
    )? {
        metrics::resync_counter(outcome).increment(1);
    }
    Ok(())
}
