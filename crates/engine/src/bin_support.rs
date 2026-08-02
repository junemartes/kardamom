//! Shared wiring for the engine-driven node binaries (`kardamom-executor`,
//! `kardamom-validator`).
//!
//! Both binaries build the same scaffolding around [`crate::Executor::run`]:
//! the durability CLI mirror, genesis loading/allocation, the per-shard
//! tx_data + tx_deposits async→sync bridges (live multicast on a fresh start,
//! archive replay-merge on crash recovery), tracing init and signal handling.
//! It used to be copy-pasted between the two binaries and had already begun to
//! drift; this module is the single copy. Only role-specific seam construction
//! (receipt publication vs cross-check sink, BAL tee vs BAL cross-check,
//! trie-aware vs plain writer) stays in each binary.

use std::path::Path;
use std::sync::mpsc as sync_mpsc;
use std::time::Duration;

use anyhow::{Context, Result};

use kardamom_log::aeron_live::{AeronRuntime, TxDataSubscriberHandle, TxDepositsSubscriberHandle};
use kardamom_log::config::{AeronConfig, ChannelsConfig};
use kardamom_log::refetch::{ArchiveRefetcher, RefetchConfig};
use kardamom_state::Durability;
use kardamom_types::{AccountChange, BPosition, CodeEntry, Deposit, TxDataLoc, TxEnvelope};

use crate::error::ExecutorError;
use crate::reader::{DepositSubscription, JoinRecovery, JoinRecoveryFactory, TxDataSubscription};

/// CLI mirror of [`kardamom_state::Durability`] (clap renders the variants as
/// `durable` / `safe-no-sync`).
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

/// Load a kardamom genesis TOML and run its semantic validation
/// (chain_id != 0, no duplicate alloc addresses).
pub fn load_genesis(path: &Path) -> Result<kardamom_types::Genesis> {
    let raw = std::fs::read_to_string(path).context("read genesis TOML")?;
    let genesis: kardamom_types::Genesis = toml::from_str(&raw).context("parse genesis TOML")?;
    genesis.validate().context("validate genesis")?;
    Ok(genesis)
}

/// Resolve the effective genesis + chain id from the `--chain` / `--chain-id`
/// flags: the genesis file's chain id is adopted when present and must agree
/// with an explicitly-set `--chain-id`.
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

/// Build the genesis allocation set (accounts + code) from a `Genesis`, ready
/// for `kardamom_state::seed_genesis`. Every `AllocEntry` becomes one
/// `AccountChange` with the declared balance/nonce and the keccak256 hash of
/// its code (if any); the code bytes become a `CodeEntry` retrievable via
/// `code_by_hash`. Returns empty vecs when no genesis was supplied.
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
            // `AllocEntry.code` is `alloy_primitives::Bytes`; `CodeEntry.code`
            // is `bytes::Bytes`. They wrap the same buffer (`c.0`).
            code.push(CodeEntry {
                code_hash,
                code: c.0.clone(),
            });
        }
    }
    (accounts, code)
}

/// Reader join-timeout policy: ALWAYS bounded, fresh starts included. An
/// unbounded join wait freezes a replica silently when an envelope is lost
/// (multicast image raced a publisher restart / lapsed under a load burst);
/// failing loudly hands recovery to the designed loop — the supervisor
/// restarts the task and crash recovery replays the gap from the archive.
///
/// The bounds are deliberately fresh(60s) > resume(30s), which reads backwards
/// against "relaxed generously while resuming" — the rationale is: a FRESH
/// start must ride out full bring-up races (multicast images still forming,
/// deploy ordering, this subscriber joining mid-burst) where nothing has yet
/// bounded when the first envelopes appear, while a RESUME reads the archive
/// replay-merge, whose streams are already materialized locally and merely
/// catch up at different rates (the tight 100ms LIVE default would still fire
/// spuriously there, hence 30s, but no bring-up slack is needed on top).
pub fn bounded_join_timeout(resuming: bool) -> Duration {
    if resuming {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(60)
    }
}

// ---------------------------------------------------------------------------
// Async log handles → sync engine traits, with the per-shard pump tasks.
// ---------------------------------------------------------------------------

struct LiveTxDataSub {
    sequencer_id: u8,
    rx: sync_mpsc::Receiver<(TxDataLoc, kardamom_log::TxFrame)>,
}

impl TxDataSubscription for LiveTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    fn next(&mut self) -> Result<(TxDataLoc, kardamom_log::TxFrame), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::TxDataClosed {
            sequencer_id: self.sequencer_id,
        })
    }
}

struct LiveTxDepositsSub {
    rx: sync_mpsc::Receiver<(BPosition, Deposit)>,
}

impl DepositSubscription for LiveTxDepositsSub {
    fn next(&mut self) -> Result<(BPosition, Deposit), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::DepositsClosed)
    }
}

/// Open the M per-shard tx_data subscriptions, bridging each handle's async
/// `recv()` to the synchronous `next()` the engine's reader threads expect
/// (dedicated tokio pump task per shard; must be called inside a tokio
/// runtime).
///
/// ALWAYS live multicast — including on a crash-recovery resume. The old
/// resume path opened an archive replay-merge against the LOCAL node's
/// archive instead, but no consumer node records tx_data (the durability
/// recordings live on the ingress nodes), so that merge waited forever for a
/// recording that never materialises and a resuming process had NO tx_data
/// source at all. Envelopes the live subscription missed (down-window, image
/// lapse, blackout) are recovered in-band by the reader's join-miss refetch
/// against the REMOTE durability archives — see [`archive_join_recovery`].
pub fn open_tx_data_subs(
    rt: &AeronRuntime,
    channels: &ChannelsConfig,
    shards: u8,
) -> Result<Vec<Box<dyn TxDataSubscription>>> {
    let mut a_subs: Vec<Box<dyn TxDataSubscription>> = Vec::with_capacity(shards as usize);
    for shard_id in 0..shards {
        let (tx, rx) = sync_mpsc::channel::<(TxDataLoc, kardamom_log::TxFrame)>();
        let mut handle = TxDataSubscriberHandle::open(rt, channels, shard_id)
            .with_context(|| format!("open TxDataSubscriberHandle shard={shard_id}"))?;
        tokio::spawn(async move {
            while let Some(item) = handle.recv().await {
                if tx.send(item).is_err() {
                    break;
                }
            }
        });
        a_subs.push(Box::new(LiveTxDataSub {
            sequencer_id: shard_id,
            rx,
        }));
    }
    Ok(a_subs)
}

/// Open the tx_deposits subscription (async→sync bridged) — the deposit-path
/// mirror of [`open_tx_data_subs`], always live for the same reason.
pub fn open_tx_deposits_sub(
    rt: &AeronRuntime,
    channels: &ChannelsConfig,
) -> Result<Box<dyn DepositSubscription>> {
    let (d_tx, d_rx) = sync_mpsc::channel::<(BPosition, Deposit)>();
    let mut handle = TxDepositsSubscriberHandle::open(rt, channels)
        .context("open TxDepositsSubscriberHandle")?;
    tokio::spawn(async move {
        while let Some(item) = handle.recv().await {
            if d_tx.send(item).is_err() {
                break;
            }
        }
    });
    Ok(Box::new(LiveTxDepositsSub { rx: d_rx }))
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
        sink: &mut dyn FnMut(TxDataLoc, kardamom_log::TxFrame),
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

/// Build the join-miss refetch factory from config, or `None` when no
/// durability-archive endpoints are configured (single-host/IPC runs) or the
/// node-local transport endpoints weren't supplied. The factory is invoked
/// inside the reader thread (the refetcher's Aeron resources are
/// thread-bound), and the refetcher itself is fully lazy — no Aeron resources
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
// Process scaffolding.
// ---------------------------------------------------------------------------

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

pub async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler; falling back to Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
            _ = tokio::signal::ctrl_c() => tracing::info!("Ctrl-C received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Ctrl-C received");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The factory gates on config: no archive endpoints ⇒ no recovery (plain
    // bounded join); endpoints without local transport ⇒ disabled with a loud
    // warn (never a half-configured client).
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
        // The factory itself is safe to run without Aeron (fully lazy).
        assert!(f.unwrap()().is_some());
    }
}
