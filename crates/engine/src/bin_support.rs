//! Shared wiring for the engine-driven node binaries (`kardamom-executor`
//! and `kardamom-validator`).
//!
//! Both binaries build the same scaffolding around [`crate::Executor::run`]:
//!   - the durability CLI mirror
//!   - genesis loading and allocation
//!   - the per-shard tx_data and tx_deposits async-to-sync bridges (live
//!     multicast on a fresh start, archive replay-merge on crash recovery)
//!   - tracing init and signal handling
//!
//! This code used to be copied between the two binaries, and had begun to
//! drift. This module is now the single copy. Only role-specific seam
//! construction stays in each binary: receipt publication vs. cross-check
//! sink, BAL tee vs. BAL cross-check, trie-aware vs. plain writer.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};

use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::{AeronConfig, ChannelsConfig};
use kardamom_log::refetch::{ArchiveRefetcher, RefetchConfig};
use kardamom_state::Durability;
use kardamom_types::{AccountChange, BPosition, CodeEntry, Deposit, TxDataLoc, TxEnvelope};

use crate::error::ExecutorError;
use crate::reader::{JoinRecovery, JoinRecoveryFactory, TxDataSubscription};

/// CLI mirror of [`kardamom_state::Durability`]. Clap renders the variants
/// as `durable` and `safe-no-sync`.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum StateDurabilityArg {
    Durable,
    SafeNoSync,
}

impl From<StateDurabilityArg> for Durability {
    fn from(a: StateDurabilityArg) -> Self {
        match a {
            StateDurabilityArg::Durable => Durability::Durable,
            StateDurabilityArg::SafeNoSync => Durability::SafeNoSync,
        }
    }
}

/// Load a kardamom genesis TOML, and run its semantic validation:
/// chain_id must not be 0, and alloc addresses must not repeat.
pub fn load_genesis(path: &Path) -> Result<kardamom_types::Genesis> {
    let raw = std::fs::read_to_string(path).context("read genesis TOML")?;
    let genesis: kardamom_types::Genesis = toml::from_str(&raw).context("parse genesis TOML")?;
    genesis.validate().context("validate genesis")?;
    Ok(genesis)
}

/// Resolve the effective genesis and chain id from the `--chain` and
/// `--chain-id` flags. If a genesis file is present, its chain id applies,
/// and it must agree with an explicit `--chain-id`.
pub fn resolve_genesis(
    chain: Option<&Path>,
    chain_id_flag: u64,
) -> Result<(Option<kardamom_types::Genesis>, u64)> {
    let genesis = match chain {
        Some(path) => Some(load_genesis(path)?),
        None => None,
    };
    let chain_id = genesis
        .as_ref()
        .map(|g| g.chain_id)
        .unwrap_or(chain_id_flag);
    if let Some(g) = &genesis
        && chain_id_flag != 1
        && chain_id_flag != g.chain_id
    {
        anyhow::bail!(
            "--chain-id {} conflicts with genesis chain_id {}",
            chain_id_flag,
            g.chain_id
        );
    }
    Ok((genesis, chain_id))
}

/// Build the genesis allocation set (accounts and code) from a `Genesis`,
/// ready for `kardamom_state::seed_genesis`. Each `AllocEntry` becomes one
/// `AccountChange`, with its balance, nonce, and the keccak256 hash of its
/// code (if any). Code bytes become a `CodeEntry`, retrievable through
/// `code_by_hash`. This returns empty vecs when no genesis is given.
pub fn build_genesis_alloc(
    genesis: Option<&kardamom_types::Genesis>,
) -> (Vec<AccountChange>, Vec<CodeEntry>) {
    use alloy_primitives::{B256, keccak256};
    let mut accounts = Vec::new();
    let mut code = Vec::new();
    let Some(g) = genesis else {
        return (accounts, code);
    };
    for entry in &g.alloc {
        let nonce = entry.nonce.unwrap_or(0);
        let code_hash = entry
            .code
            .as_ref()
            .map(|c| keccak256(c.as_ref()))
            .unwrap_or(B256::ZERO);
        tracing::info!(
            address = ?entry.address,
            balance = %entry.balance,
            nonce,
            has_code = entry.code.is_some(),
            "seeding genesis account"
        );
        accounts.push(AccountChange {
            address: entry.address,
            nonce,
            balance: entry.balance,
            code_hash,
        });
        if let Some(c) = entry.code.as_ref() {
            // `AllocEntry.code` is `alloy_primitives::Bytes`. `CodeEntry.code`
            // is `bytes::Bytes`. Both wrap the same buffer (`c.0`).
            code.push(CodeEntry {
                code_hash,
                code: c.0.clone(),
            });
        }
    }
    (accounts, code)
}

/// Reader join-timeout policy: always bounded, even on a fresh start. An
/// unbounded join wait would freeze a replica silently if an envelope is
/// lost (a multicast image raced a publisher restart, or lapsed under a load
/// burst). Failing loudly hands recovery to the normal loop: the supervisor
/// restarts the task, and crash recovery replays the gap from the archive.
///
/// The bounds are fresh(60s) > resume(30s). This may look backward from
/// "relax generously while resuming", but the reason is different. A fresh
/// start must ride out full bring-up races: multicast images still forming,
/// deploy ordering, this subscriber joining mid-burst. Nothing bounds it
/// yet when the first envelopes appear. A resume reads the archive
/// replay-merge, whose streams are already local and only catch up at
/// different rates. The tight 100ms live default would still fire
/// spuriously there, hence 30s, but no bring-up slack is needed on top.
pub fn bounded_join_timeout(resuming: bool) -> Duration {
    if resuming {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(60)
    }
}

// ---------------------------------------------------------------------------
// This maps async log handles to sync engine traits.
// ---------------------------------------------------------------------------

/// The live tx_data subscription both role binaries run. It is one Aeron
/// subscriber channel per shard. The Aeron reader thread sends into a
/// tokio channel. The engine's reader thread blocks on the channel, with
/// `blocking_recv`. No pump task sits between them. This is public so a
/// binary's `EngineWiring` can name it as its `TxData` type.
pub struct LiveTxDataSub {
    sequencer_id: u8,
    rx: tokio::sync::mpsc::UnboundedReceiver<(TxDataLoc, TxEnvelope)>,
}

impl TxDataSubscription for LiveTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    fn next(&mut self) -> Result<(TxDataLoc, TxEnvelope), ExecutorError> {
        self.rx.blocking_recv().ok_or(ExecutorError::TxDataClosed {
            sequencer_id: self.sequencer_id,
        })
    }
}

/// Open the M per-shard tx_data subscriptions. Each subscription hands
/// the engine's reader thread its tokio receiver directly. The reader
/// thread blocks on it, off the tokio runtime. When the [`AeronRuntime`]
/// drops, every subscription's sender closes. Then `next()` returns
/// `TxDataClosed`.
///
/// This always uses live multicast, even on a crash-recovery resume. The
/// old resume path opened an archive replay-merge against the local node's
/// archive instead. But no consumer node records tx_data; the durability
/// recordings live on the ingress nodes. So that merge waited forever for a
/// recording that never appeared, and a resuming process had no tx_data
/// source at all. Envelopes the live subscription missed (a down window,
/// an image lapse, a blackout) are recovered in-band by the reader's
/// join-miss refetch against the remote durability archives. See
/// [`archive_join_recovery`].
pub fn open_tx_data_subs(
    rt: &AeronRuntime,
    channels: &ChannelsConfig,
    shards: u8,
) -> Result<Vec<LiveTxDataSub>> {
    (0..shards)
        .map(|shard_id| {
            let rx = rt
                .open_tx_data_subscription(
                    &channels.tx_data_channel(shard_id),
                    channels.tx_data_stream_id(shard_id),
                )
                .with_context(|| format!("open tx_data subscription shard={shard_id}"))?;
            Ok(LiveTxDataSub {
                sequencer_id: shard_id,
                rx,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Join-miss archive refetch wiring.
// ---------------------------------------------------------------------------

/// [`JoinRecovery`] over the remote durability archives, via
/// [`kardamom_log::refetch::ArchiveRefetcher`].
struct ArchiveJoinRecovery {
    refetcher: ArchiveRefetcher,
    tx_data_stream_base: i32,
    tx_deposits_stream_id: i32,
}

impl JoinRecovery for ArchiveJoinRecovery {
    fn recover_tx_data(
        &mut self,
        shard_id: u8,
        session_id: i32,
        from: BPosition,
        sink: &mut dyn FnMut(TxDataLoc, TxEnvelope),
    ) -> Result<u64, String> {
        self.refetcher
            .fetch_tx_data(
                self.tx_data_stream_base + shard_id as i32,
                session_id,
                from,
                sink,
            )
            .map_err(|e| e.to_string())
    }

    fn recover_deposits(
        &mut self,
        from: BPosition,
        sink: &mut dyn FnMut(BPosition, Deposit),
    ) -> Result<u64, String> {
        self.refetcher
            .fetch_deposits(self.tx_deposits_stream_id, from, sink)
            .map_err(|e| e.to_string())
    }
}

/// Build the join-miss refetch factory from config. Return `None` when no
/// durability-archive endpoints are configured (single-host or IPC runs),
/// or the node-local transport endpoints are missing. The reader thread
/// calls the factory, because the refetcher's Aeron resources are
/// thread-bound. The refetcher itself is fully lazy: no Aeron resources
/// exist until the first join miss.
pub fn archive_join_recovery(
    channels: &ChannelsConfig,
    aeron_cfg: &AeronConfig,
    aeron_dir: Option<&Path>,
    response_endpoint: Option<&str>,
    replay_endpoint: Option<&str>,
) -> Option<JoinRecoveryFactory> {
    if aeron_cfg.tx_data_archive_endpoints.is_empty()
        && aeron_cfg.tx_deposits_archive_endpoints.is_empty()
    {
        return None;
    }
    let (Some(response_endpoint), Some(replay_endpoint)) = (response_endpoint, replay_endpoint)
    else {
        tracing::warn!(
            "durability-archive endpoints configured but no local refetch endpoints \
             (--archive-control-response-endpoint / --replay-destination-endpoint); \
             join-miss refetch DISABLED — a lost envelope will be fatal"
        );
        return None;
    };
    let cfg = RefetchConfig {
        tx_data_endpoints: aeron_cfg.tx_data_archive_endpoints.clone(),
        tx_deposits_endpoints: aeron_cfg.tx_deposits_archive_endpoints.clone(),
        response_endpoint: response_endpoint.to_string(),
        replay_endpoint: replay_endpoint.to_string(),
        aeron_dir: aeron_dir.map(|p| p.to_path_buf()),
        aeron: aeron_cfg.clone(),
    };
    let tx_data_stream_base = channels.tx_data_stream_id_base;
    let tx_deposits_stream_id = channels.tx_deposits_stream_id;
    Some(Box::new(move || {
        Some(Box::new(ArchiveJoinRecovery {
            refetcher: ArchiveRefetcher::new(cfg),
            tx_data_stream_base,
            tx_deposits_stream_id,
        }) as Box<dyn JoinRecovery>)
    }))
}

// ---------------------------------------------------------------------------
// Cold-start checkpoint ladder and replay-window-overrun repair, for the
// executor and validator. This code used to be copied between the two
// binaries, and had begun to drift. It is safety-critical recovery logic,
// so it must stay one copy.
// ---------------------------------------------------------------------------

/// Chain identity for checkpoint adoption: the digest of the genesis this
/// node is configured with. This refuses a checkpoint from another chain,
/// or with corrupt bytes, instead of adopting it. See `CheckpointManifest`.
pub fn expected_genesis_digest(
    genesis: Option<&kardamom_types::Genesis>,
) -> Option<alloy_primitives::B256> {
    let (a, c) = build_genesis_alloc(genesis);
    Some(kardamom_state::genesis_digest(&a, &c))
}

/// The cold-start restore ladder. It tries the newest local checkpoint
/// first, and quarantines it on verification failure (a torn or foreign
/// checkpoint costs one rung of the ladder; it must never crash-loop the
/// node). Then it tries a one-shot peer fetch and retry. A fresh node
/// joining an old chain cannot re-sync from genesis: the cluster's
/// canonical stream is retained only over a bounded window, so
/// `REPLAY_FROM(genesis)` would be refused once lifetime traffic exceeds
/// it.
///
/// This returns the restored `(block, checkpoint_path)`, or `None` for a
/// genesis start. Role-specific follow-up, such as the validator's
/// adoption marker and log wording, stays at the call site.
pub fn restore_or_fetch_checkpoint(
    ckpt_dir: &Path,
    state_dir: &Path,
    peers: &[String],
    expected_genesis: Option<alloy_primitives::B256>,
) -> Result<Option<(u64, std::path::PathBuf)>> {
    let restore = || kardamom_state::restore_best_checkpoint(ckpt_dir, state_dir, expected_genesis);
    let mut restored = restore().context("restore local checkpoint")?;
    if restored.is_none()
        && !peers.is_empty()
        && kardamom_state::fetch_best_checkpoint(peers, ckpt_dir, 1, expected_genesis).is_some()
    {
        restored = restore().context("restore fetched checkpoint")?;
    }
    Ok(restored)
}

/// Replay cursor for the cluster canonical stream: it resumes from the
/// persisted state cursor. The cluster re-offers retained frames from the
/// cursor on every session start. So tx_ordering has no gaps across
/// restarts and session loss.
/// [`ResumePoint::GENESIS`](crate::ResumePoint::GENESIS) gives
/// `ReplayCursor::genesis()`: no records seen, first boundary is block 1.
/// So a fresh node needs no separate case.
pub fn cluster_replay_cursor(start: &crate::ResumePoint) -> crate::reader::cluster::ReplayCursor {
    crate::reader::cluster::ReplayCursor::new(start.record_count, start.block + 1)
}

/// The live tx_ordering subscription both role binaries run: the Aeron
/// Cluster (Raft) egress, behind the replay and dedup adapter. This is
/// public so a binary's `EngineWiring` can name it as its `TxOrdering`
/// type, without a direct `kardamom-cluster-adapter` dependency.
pub type LiveTxOrderingSub =
    crate::reader::cluster::ClusterTxOrderingSubscription<kardamom_cluster_adapter::LiveEgress>;

/// Spawn a dedicated cluster Aeron runtime, on its own thread, using the
/// same aeron dir. The cluster session must never contend with tx_data or
/// receipts work on the main runtimes. Connect the cluster tx_ordering
/// subscription from `cursor`. The returned `LiveCluster` guard must
/// outlive the engine loop.
pub fn connect_cluster_ordering(
    aeron_dir: Option<&Path>,
    cfg: kardamom_cluster_adapter::LiveClusterConfig,
    cursor: crate::reader::cluster::ReplayCursor,
) -> Result<(kardamom_cluster_adapter::LiveCluster, LiveTxOrderingSub)> {
    let cluster_rt = AeronRuntime::spawn(aeron_dir).context("spawn cluster AeronRuntime")?;
    crate::reader::cluster::cluster_tx_ordering_subscription(cluster_rt, cfg, cursor)
        .context("connect cluster tx_ordering subscription")
}

/// Replay-window overrun repair, run at exit. The durable cursor fell
/// below the cluster's retention floor, so resuming from it can never
/// succeed. Every restart would re-request the same refused `REPLAY_FROM`,
/// a deterministic crash loop. Repair before exiting: fetch a peer
/// checkpoint at or above the floor, and park the stale DB. The next
/// restart then takes the ordinary fresh-start restore path, and resumes
/// from the fetched checkpoint. Repairing at exit, instead of looping
/// in-process, keeps a single startup path and stays crash-safe at every
/// step. The fetch is an atomic rename, and the DB is parked only after a
/// qualifying checkpoint is already on disk.
///
/// `adopted_unverified` selects the validator's log wording, for its
/// catch-up trust class (its adopted state is unverified through the
/// checkpoint block). This returns the resync outcome label
/// (`"peer-checkpoint"` or `"unrecoverable"`) for the caller's per-service
/// metric, or `None` when `err` is not a `ClusterReplayUnavailable`.
pub fn replay_unavailable_fallback(
    err: Option<&ExecutorError>,
    checkpoint_dir: Option<&Path>,
    checkpoint_peers: &[String],
    state_dir: &Path,
    expected_genesis: Option<alloy_primitives::B256>,
    adopted_unverified: bool,
) -> Result<Option<&'static str>> {
    let Some(ExecutorError::ClusterReplayUnavailable {
        from_index,
        oldest_index,
        oldest_block,
    }) = err
    else {
        return Ok(None);
    };
    let oldest_block = *oldest_block;
    tracing::warn!(
        from_index,
        oldest_index,
        oldest_block,
        "cluster replay unavailable — attempting peer-checkpoint fallback"
    );
    match (checkpoint_dir, checkpoint_peers.is_empty()) {
        (Some(ckpt_dir), false) => {
            match kardamom_state::fetch_best_checkpoint(
                checkpoint_peers,
                ckpt_dir,
                oldest_block,
                expected_genesis,
            ) {
                Some(ckpt) => {
                    kardamom_state::park_state_db(state_dir).context("park stale state DB")?;
                    if adopted_unverified {
                        tracing::info!(
                            checkpoint_block = ckpt.block,
                            "resync prepared: peer checkpoint staged, stale state parked; \
                             restart will adopt it (blocks through the checkpoint are \
                             UNVERIFIED by this validator)"
                        );
                    } else {
                        tracing::info!(
                            checkpoint_block = ckpt.block,
                            "resync prepared: peer checkpoint staged, stale state parked; \
                             restart will restore and resume from it"
                        );
                    }
                    Ok(Some("peer-checkpoint"))
                }
                None => {
                    tracing::error!(
                        oldest_block,
                        "resync fallback failed: no peer checkpoint at or above the \
                         retention floor — operator action required (restore a \
                         checkpoint into --checkpoint-dir, or rebuild-from-L1 with \
                         kardamom-reconstruct into --state-dir)"
                    );
                    Ok(Some("unrecoverable"))
                }
            }
        }
        _ => {
            tracing::error!(
                "resync fallback unavailable (--checkpoint-dir/--checkpoint-peers not \
                 configured) — operator action required (rebuild-from-L1 with \
                 kardamom-reconstruct, or restore a peer checkpoint manually)"
            );
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Process scaffolding.
// ---------------------------------------------------------------------------

// This moved to `kardamom_obs::bin`, so every service, not only the
// engine-shaped ones, shares one copy. This re-export keeps old callers
// working.
pub use kardamom_obs::bin::{init_tracing, wait_for_shutdown};

#[cfg(test)]
mod tests {
    use super::*;

    // The factory gates on config. No archive endpoints means no recovery,
    // just a plain bounded join. Endpoints without local transport means it
    // is disabled, with a loud warning. It never builds a half-configured
    // client.
    #[test]
    fn recovery_factory_gates_on_config() {
        let channels = ChannelsConfig::default();
        let mut aeron = AeronConfig::default();
        assert!(
            archive_join_recovery(
                &channels,
                &aeron,
                None,
                Some("10.0.0.1:40140"),
                Some("10.0.0.1:40130")
            )
            .is_none(),
            "no endpoints configured ⇒ None"
        );
        aeron.tx_data_archive_endpoints = vec!["192.168.56.31:8010".into()];
        assert!(
            archive_join_recovery(&channels, &aeron, None, None, None).is_none(),
            "endpoints but no local transport ⇒ None"
        );
        let f = archive_join_recovery(
            &channels,
            &aeron,
            None,
            Some("10.0.0.1:40140"),
            Some("10.0.0.1:40130"),
        );
        assert!(f.is_some(), "fully configured ⇒ factory");
        // The factory is safe to run without Aeron; it is fully lazy.
        assert!(f.unwrap()().is_some());
    }
}
