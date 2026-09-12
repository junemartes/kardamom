//! Shared wiring for the engine-driven node binaries (`kardamom-executor`
//! and `kardamom-validator`).
//!
//! Both binaries build the same scaffolding around [`crate::Executor::run`]:
//!   - the durability CLI mirror
//!   - genesis loading and allocation
//!   - the per-shard `tx_data` async-to-sync bridge, always live multicast,
//!     with join-miss gaps recovered in-band by archive refetch
//!   - tracing init and signal handling
//!
//! This module is the single copy for both binaries. Only role-specific
//! seam construction stays in each binary: receipt publication vs.
//! cross-check sink, BAL tee vs. BAL cross-check, trie-aware vs. plain
//! writer.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};

use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::{AeronConfig, ChannelsConfig};
use kardamom_log::refetch::RefetchConfig;
use kardamom_state::Durability;
use kardamom_types::{AccountChange, CodeEntry, TxDataLoc, TxEnvelope};

use crate::error::ExecutorError;
use crate::reader::{JoinRecoveryFactory, TxDataSubscription};

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
/// `chain_id` must not be 0, and alloc addresses must not repeat.
fn load_genesis(path: &Path) -> Result<kardamom_types::Genesis> {
    let raw = std::fs::read_to_string(path).context("read genesis TOML")?;
    let genesis: kardamom_types::Genesis = toml::from_str(&raw).context("parse genesis TOML")?;
    genesis.validate().context("validate genesis")?;
    Ok(genesis)
}

/// Resolve the effective genesis and chain id from the `--chain` and
/// `--chain-id` flags. If a genesis file is present, its chain id applies,
/// and it must agree with an explicit `--chain-id`.
///
/// # Errors
///
/// Returns `Err` when `--chain` names a file that does not exist, does not
/// parse as genesis TOML, or fails semantic validation; when
/// `--chain-id` disagrees with the genesis file's `chain_id`; or when the
/// resolved chain id is zero.
pub fn resolve_genesis(
    chain: Option<&Path>,
    chain_id_flag: u64,
) -> Result<(Option<kardamom_types::Genesis>, std::num::NonZeroU64)> {
    let genesis = match chain {
        Some(path) => Some(load_genesis(path)?),
        None => None,
    };
    let chain_id = genesis.as_ref().map_or(chain_id_flag, |g| g.chain_id);
    let chain_id = std::num::NonZeroU64::new(chain_id).context("chain id must not be zero")?;
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
#[must_use]
pub fn build_genesis_alloc(
    genesis: Option<&kardamom_types::Genesis>,
) -> (Vec<AccountChange>, Vec<CodeEntry>) {
    let Some(g) = genesis else {
        return (Vec::new(), Vec::new());
    };
    for entry in &g.alloc {
        tracing::info!(
            address = ?entry.address,
            balance = %entry.balance,
            nonce = entry.nonce.unwrap_or(0),
            has_code = entry.code.is_some(),
            "seeding genesis account"
        );
    }
    // `Genesis::to_alloc` is the shared builder `kardamom-types` already
    // has: it keeps this binary's genesis identical, byte for byte, to the
    // rebuild-from-L1 reconstructor's genesis, so their state roots match.
    g.to_alloc()
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
#[must_use]
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

/// The live `tx_data` subscription both role binaries run. It is one Aeron
/// subscriber channel per shard. The Aeron reader thread sends into a
/// tokio channel. The engine's reader thread blocks on the channel, with
/// `blocking_recv`. No pump task sits between them. This is public so a
/// binary's `EngineWiring` can name it as its `TxData` type.
pub struct LiveTxDataSub {
    sequencer_id: u8,
    rx: kardamom_log::aeron_live::TxDataSubscription,
}

impl TxDataSubscription for LiveTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    fn next(&mut self) -> Result<(TxDataLoc, TxEnvelope), ExecutorError> {
        #[allow(
            clippy::cast_precision_loss,
            reason = "a queue depth stays far below 2^52"
        )]
        metrics::gauge!(
            crate::metrics::TX_DATA_QUEUE_DEPTH,
            "shard" => self.sequencer_id.to_string()
        )
        .set(self.rx.len() as f64);
        self.rx.blocking_recv().ok_or(ExecutorError::TxDataClosed {
            sequencer_id: self.sequencer_id,
        })
    }
}

/// Open one `tx_data` subscription per lane of the lane plane
/// (`LANE_COUNT`, today 8). A consumer does not know the active shard
/// count. It reads every lane, and an idle lane costs one handle and one
/// blocked reader thread. Each subscription hands the engine's reader
/// thread its tokio receiver directly. The reader thread blocks on it,
/// off the tokio runtime. When the [`AeronRuntime`] drops, every
/// subscription's sender closes. Then `next()` returns `TxDataClosed`.
///
/// This always uses live multicast, even on a crash-recovery resume: no
/// consumer node records `tx_data`, so an archive replay-merge against the
/// local node's archive would wait forever for a recording that never
/// appears. Envelopes the live subscription missed (a down window, an
/// image lapse, a blackout) are recovered in-band by the reader's
/// join-miss refetch against the remote durability archives. See
/// [`archive_join_recovery`].
///
/// # Errors
///
/// Returns `Err` when the Aeron subscription for a shard fails to open.
pub fn open_tx_data_subs(
    rt: &AeronRuntime,
    plane: &mut kardamom_log::discovery::StreamPlane,
) -> Result<Vec<LiveTxDataSub>> {
    (0..kardamom_types::shard_map::LANE_COUNT)
        .map(|shard_id| {
            let rx = plane
                .tx_data_subscription(rt, shard_id)
                .with_context(|| format!("open tx_data subscription lane={shard_id}"))?;
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

/// Build the join-miss refetch factory from config. Return `None` when no
/// durability-archive endpoints are configured (single-host or IPC runs),
/// or the node-local transport endpoints are missing. The reader thread
/// builds the [`JoinRecovery`](crate::reader::JoinRecovery) from the
/// factory, because its Aeron resources are thread-bound. Those resources
/// are fully lazy: none exist until the first join miss.
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
        aeron_dir: aeron_dir.map(std::path::Path::to_path_buf),
        aeron: aeron_cfg.clone(),
    };
    Some(JoinRecoveryFactory {
        cfg,
        tx_data_stream_base: channels.tx_data_stream_id_base,
        tx_deposits_stream_id: channels.tx_deposits_stream_id,
    })
}

// ---------------------------------------------------------------------------
// Cold-start checkpoint ladder and replay-window-overrun repair, for the
// executor and validator. This is safety-critical recovery logic, so it
// stays one copy, shared by both binaries.
// ---------------------------------------------------------------------------

/// Chain identity for checkpoint adoption: the digest of the genesis this
/// node is configured with. This refuses a checkpoint from another chain,
/// or with corrupt bytes, instead of adopting it. See `CheckpointManifest`.
#[must_use]
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
///
/// # Errors
///
/// Returns `Err` when a local or fetched checkpoint exists but fails to
/// restore (a torn write, or bytes from a different chain).
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
/// cursor on every session start. So `tx_ordering` has no gaps across
/// restarts and session loss.
/// [`ResumePoint::GENESIS`](crate::ResumePoint::GENESIS) gives
/// `ReplayCursor::genesis()`: no records seen, first boundary is block 1.
/// So a fresh node needs no separate case.
#[must_use]
pub fn cluster_replay_cursor(start: &crate::ResumePoint) -> crate::reader::cluster::ReplayCursor {
    // `start.block` is read from the persisted state cursor; a corrupt
    // value near `u64::MAX` must not wrap the replay cursor back to block 0.
    crate::reader::cluster::ReplayCursor::new(start.record_count, start.block.saturating_add(1))
}

/// The live `tx_ordering` subscription both role binaries run: the Aeron
/// Cluster (Raft) egress, behind the replay and dedup adapter. This is
/// public so a binary's `EngineWiring` can name it as its `TxOrdering`
/// type, without a direct `kardamom-cluster-adapter` dependency.
pub type LiveTxOrderingSub =
    crate::reader::cluster::ClusterTxOrderingSubscription<kardamom_cluster_adapter::LiveEgress>;

/// Spawn a dedicated cluster Aeron runtime, on its own thread, using the
/// same aeron dir. The cluster session must never contend with `tx_data` or
/// receipts work on the main runtimes. Connect the cluster `tx_ordering`
/// subscription from `cursor`. The returned `LiveCluster` guard must
/// outlive the engine loop.
///
/// # Errors
///
/// Returns `Err` when the cluster Aeron runtime fails to spawn, or the
/// cluster session fails to connect.
pub fn connect_cluster_ordering(
    aeron_dir: Option<&Path>,
    cfg: kardamom_cluster_adapter::LiveClusterConfig,
    cursor: crate::reader::cluster::ReplayCursor,
) -> Result<(kardamom_cluster_adapter::LiveCluster, LiveTxOrderingSub)> {
    let cluster_rt = AeronRuntime::spawn(aeron_dir).context("spawn cluster AeronRuntime")?;
    crate::reader::cluster::cluster_tx_ordering_subscription(cluster_rt, cfg, cursor)
        .context("connect cluster tx_ordering subscription")
}

/// Everything [`open_inbound`] needs, gathered so the function reads no
/// eight-argument list.
pub struct InboundConfig<'a> {
    pub rt: &'a AeronRuntime,
    /// The plane the `tx_data` lanes open through. Its channels also feed
    /// the refetch wiring.
    pub plane: &'a mut kardamom_log::discovery::StreamPlane,
    pub aeron_cfg: &'a AeronConfig,
    pub aeron_dir: Option<&'a Path>,
    pub archive_control_response_endpoint: Option<&'a str>,
    pub replay_destination_endpoint: Option<&'a str>,
    pub cluster_cfg: kardamom_cluster_adapter::LiveClusterConfig,
    pub cursor: crate::reader::cluster::ReplayCursor,
    /// Names this binary in the `tx_ordering via Aeron Cluster` log line.
    pub bin_name: &'a str,
    /// The executor is the chosen emitter of the `kardamom_sealer_*`
    /// re-export; the validator suppresses its own copy to avoid a
    /// second, lagging series.
    pub suppress_sealer_metrics: bool,
}

/// Open the shared inbound side of the engine, both role binaries run
/// unchanged: the M `tx_data` streams (bridged async-to-sync, always live
/// multicast, join-miss gaps recovered in-band by archive refetch), and
/// the one `tx_ordering` subscription (always the Aeron Cluster egress).
/// Returns the ready-to-run [`Inbound`] plus the
/// [`kardamom_cluster_adapter::LiveCluster`] guard, which the caller
/// holds until shutdown.
///
/// # Errors
///
/// Returns `Err` when opening the `tx_data` subscriptions or the cluster
/// ordering connection fails.
pub fn open_inbound<W>(
    cfg: InboundConfig<'_>,
) -> Result<(crate::Inbound<W>, kardamom_cluster_adapter::LiveCluster)>
where
    W: crate::EngineWiring<TxData = LiveTxDataSub, TxOrdering = LiveTxOrderingSub>,
{
    let tx_data = open_tx_data_subs(cfg.rt, cfg.plane)?;
    let join_recovery = archive_join_recovery(
        cfg.plane.channels(),
        cfg.aeron_cfg,
        cfg.aeron_dir,
        cfg.archive_control_response_endpoint,
        cfg.replay_destination_endpoint,
    );
    let (cluster_guard, cluster_sub) =
        connect_cluster_ordering(cfg.aeron_dir, cfg.cluster_cfg, cfg.cursor)?;
    tracing::info!("{}: tx_ordering via Aeron Cluster", cfg.bin_name);
    let tx_ordering = if cfg.suppress_sealer_metrics {
        cluster_sub.suppress_sealer_metrics()
    } else {
        cluster_sub
    };
    Ok((
        crate::Inbound {
            tx_data,
            tx_ordering,
            join_recovery,
        },
        cluster_guard,
    ))
}

/// Resync-fallback context: everything [`ResyncFallback::stage_peer_checkpoint`]
/// and [`ResyncFallback::log_resume_prepared`] need, gathered once so
/// neither takes it as loose parameters.
struct ResyncFallback<'a> {
    checkpoint_peers: &'a [String],
    state_dir: &'a Path,
    expected_genesis: Option<alloy_primitives::B256>,
    /// Selects the validator's trust-class wording in the resume-prepared
    /// log line: its adopted state is unverified through the checkpoint
    /// block.
    adopted_unverified: bool,
}

impl ResyncFallback<'_> {
    /// Fetch a peer checkpoint at or above `oldest_block` and park the
    /// stale state DB, so the next restart adopts it. Returns the
    /// checkpoint's block on success, or `None` when no peer has a
    /// qualifying checkpoint.
    ///
    /// # Errors
    ///
    /// Returns `Err` when a fetched checkpoint exists but fails to park (a
    /// filesystem failure moving the stale state DB aside).
    fn stage_peer_checkpoint(&self, ckpt_dir: &Path, oldest_block: u64) -> Result<Option<u64>> {
        let Some(ckpt) = kardamom_state::fetch_best_checkpoint(
            self.checkpoint_peers,
            ckpt_dir,
            oldest_block,
            self.expected_genesis,
        ) else {
            return Ok(None);
        };
        kardamom_state::park_state_db(self.state_dir).context("park stale state DB")?;
        Ok(Some(ckpt.block))
    }

    /// Log the resync-prepared line.
    fn log_resume_prepared(&self, checkpoint_block: u64) {
        if self.adopted_unverified {
            tracing::info!(
                checkpoint_block,
                "resync prepared: peer checkpoint staged, stale state parked; \
                 restart will adopt it (blocks through the checkpoint are \
                 UNVERIFIED by this validator)"
            );
        } else {
            tracing::info!(
                checkpoint_block,
                "resync prepared: peer checkpoint staged, stale state parked; \
                 restart will restore and resume from it"
            );
        }
    }
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
///
/// # Errors
///
/// Returns `Err` when a fetched peer checkpoint exists but fails to park.
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
    if let (Some(ckpt_dir), false) = (checkpoint_dir, checkpoint_peers.is_empty()) {
        let fallback = ResyncFallback {
            checkpoint_peers,
            state_dir,
            expected_genesis,
            adopted_unverified,
        };
        if let Some(checkpoint_block) = fallback.stage_peer_checkpoint(ckpt_dir, oldest_block)? {
            fallback.log_resume_prepared(checkpoint_block);
            Ok(Some("peer-checkpoint"))
        } else {
            tracing::error!(
                oldest_block,
                "resync fallback failed: no peer checkpoint at or above the \
                 retention floor — operator action required (restore a \
                 checkpoint into --checkpoint-dir, or rebuild-from-L1 with \
                 kardamom-reconstruct into --state-dir)"
            );
            Ok(Some("unrecoverable"))
        }
    } else {
        tracing::error!(
            "resync fallback unavailable (--checkpoint-dir/--checkpoint-peers not \
             configured) — operator action required (rebuild-from-L1 with \
             kardamom-reconstruct, or restore a peer checkpoint manually)"
        );
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Process scaffolding.
// ---------------------------------------------------------------------------

pub use kardamom_obs::bin::{init_tracing, wait_for_shutdown};

/// The Aeron runtime and the cluster-session guard that must outlive the
/// engine loop. Field order is drop order: `rt` ends first, then
/// `cluster_guard`.
pub struct LiveStreams {
    pub rt: AeronRuntime,
    pub cluster_guard: kardamom_cluster_adapter::LiveCluster,
}

/// End both streams. Dropping `streams` at the end of this function
/// already does the work, in field-declaration order; the function
/// exists so the call site names the point at which both streams end,
/// instead of a bare `drop`.
fn stop_streams(_streams: LiveStreams) {}

/// The state [`EngineShutdown::wait`] needs, gathered so `wait` reads no
/// argument list of its own.
///
/// `bin_name` names this binary in the `shutdown signal received` log
/// line. `before_drop` runs after the wait resolves, but before `streams`
/// ends; a caller with extra state to release first (the validator
/// cancels its pumps here, so the `tx_bal` pump releases its
/// `AeronRuntime` clone before `streams.rt` drops) passes that step. A
/// caller with nothing extra passes `|| {}`.
pub struct EngineShutdown<'a, B: FnOnce()> {
    pub bin_name: &'a str,
    pub join: tokio::task::JoinHandle<Result<(), ExecutorError>>,
    pub streams: LiveStreams,
    pub before_drop: B,
}

impl<B: FnOnce()> EngineShutdown<'_, B> {
    /// Wait for whichever comes first: an operator shutdown signal, or the
    /// engine loop finishing on its own. Exiting on the first of the two,
    /// instead of only on SIGTERM, avoids an errored or halted node
    /// looking "alive": metrics up, pipeline dead or frozen, instead of
    /// exiting so the orchestrator restarts it into the crash-recovery
    /// path.
    ///
    /// Returns the joined engine result once the loop has actually
    /// finished, whether that happened before or after the shutdown
    /// signal.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the engine loop's task panicked, so no join
    /// result survives. The inner `Result` carries the engine loop's own
    /// error, if it returned one cleanly.
    pub async fn wait(
        self,
    ) -> std::result::Result<Result<(), ExecutorError>, tokio::task::JoinError> {
        let Self {
            bin_name,
            mut join,
            streams,
            before_drop,
        } = self;
        let engine_result = tokio::select! {
            () = wait_for_shutdown() => {
                tracing::info!("{bin_name}: shutdown signal received; dropping runtime");
                None
            }
            res = &mut join => Some(res),
        };
        before_drop();
        stop_streams(streams);
        match engine_result {
            Some(r) => r,
            None => join.await,
        }
    }
}

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
        // The factory is safe to build without Aeron; it is fully lazy.
        let _recovery = f.unwrap().build();
    }
}
