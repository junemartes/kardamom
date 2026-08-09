//! The checkpoint-trust lifecycle (#143), gathered in one place: cold-start
//! checkpoint adoption (with its explicit marker), the post-open trie
//! bootstrap the marker gates, and the at-exit replay-unavailable resync
//! fallback. The executor-shared mechanics (restore ladder, peer fetch,
//! stale-DB parking) live in `kardamom_engine::bin_support`; this module owns
//! the validator-specific trust bookkeeping — blocks at or below an adopted
//! checkpoint are UNVERIFIED by this validator (the #78 catch-up trust
//! class), and the frozen-at-genesis trie an executor checkpoint carries must
//! be rebuilt before the incremental walker may extend it.

use std::path::Path;

use anyhow::{Context, Result};
use kardamom_engine::ExecutorError;
use kardamom_engine::bin_support;
use kardamom_state::StateEnv;
use kardamom_validator::metrics;

/// Marker file in the state dir: a checkpoint was adopted and the trie
/// bootstrap has not yet committed. See [`bootstrap_trie_if_adopted`].
const ADOPTION_MARKER: &str = ".adopted-needs-trie-bootstrap";

/// Checkpoint adoption, cold-start half (#143): a fresh validator joining
/// a chain that outgrew the cluster retention window can NOT re-execute
/// from genesis (REPLAY_FROM(genesis) is refused), so adopt the newest
/// staged/peer checkpoint BEFORE opening the env — startup then resumes
/// from its cursor and only the tail replays. Blocks through the adopted
/// checkpoint are UNVERIFIED by this validator (trust class of #78
/// catch-up); the trustless alternative is rebuild-from-L1.
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
        // Adoption is recorded EXPLICITLY: executor checkpoints carry
        // the genesis-seeded mirror + trie (built for every env by
        // seed_genesis, then never updated by the trie-off writer),
        // so a "trie present?" probe passes on an image whose trie is
        // frozen at genesis — and the incremental walker would extend
        // that stale base into silently wrong roots the shadow-check
        // cannot catch (it rebuilds from the SAME stale mirror).
        // Caught by the validator-join chaos case's non-vacuity grep.
        // The marker survives a crash between restore and bootstrap;
        // the bootstrap below is idempotent.
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

/// Rebuild the hashed mirror + trie from the plain state tables when the
/// adoption marker (or a truly trie-less image) says so. Must run against
/// the OPEN env, before the trie-aware writer spawns.
///
/// Adopted executor checkpoints carry a trie FROZEN AT GENESIS (seeded
/// into every env, never updated by the trie-off writer) — so adoption is
/// signaled by the explicit marker, not a presence probe, and the
/// bootstrap rebuilds the mirror + trie from the plain state tables
/// wholesale. Idempotent and crash-safe (one RW txn; the marker is
/// removed only after commit). `has_trie` remains as a belt-and-braces
/// net for a truly mirror-less image (e.g. an operator-copied dir).
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

/// Replay-window overrun: repair BEFORE exiting, exactly like the
/// executor's recovery-D path (#94) — fetch a peer checkpoint at/above
/// the retention floor and park the stale DB, so the next restart takes
/// the ordinary fresh-start restore path instead of a deterministic
/// crash loop re-requesting the same refused REPLAY_FROM (#143).
/// Adoption trust class: the adopted state is unverified BY THIS
/// validator through the checkpoint block — the same accepted tradeoff
/// as #78's BAL catch-up; the divergence latch only ever covers blocks
/// this validator actually verified. The trustless alternative remains
/// kardamom-reconstruct (rebuild-from-L1) into --state-dir.
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
