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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};

use kardamom_log::aeron_live::{AeronRuntime, TxDataSubscriberHandle, TxDepositsSubscriberHandle};
use kardamom_log::config::{AeronConfig, ChannelsConfig};
use kardamom_log::replay;
use kardamom_state::Durability;
use kardamom_types::{AccountChange, BPosition, CodeEntry, Deposit, TxDataLoc, TxEnvelope};

use crate::error::ExecutorError;
use crate::reader::{DepositSubscription, TxDataSubscription};

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
// Replay-merge failure surfacing (F13.4): a failed crash recovery must exit
// non-zero, not look like a clean end-of-stream.
// ---------------------------------------------------------------------------

/// Shared slot recording the first fatal replay-merge failure observed by any
/// of the bridge pump tasks. The replay subscriber reports a fatal poll-thread
/// error (archive error, coverage gap, decode failure, stuck merge) as a
/// closed channel plus [`kardamom_log::replay::ReplayMergeSubscriber::take_failure`];
/// without this slot the closed channel is indistinguishable from a clean
/// shutdown and the process exits 0 on a FAILED crash recovery. The binary's
/// main loop selects on [`failed`](Self::failed) and exits non-zero.
///
/// Designed for a single waiter (the binary main); any number of recorders.
#[derive(Clone, Default)]
pub struct ReplayFailure {
    slot: Arc<Mutex<Option<String>>>,
    notify: Arc<tokio::sync::Notify>,
}

impl ReplayFailure {
    /// Record a fatal replay failure (first one wins) and wake the waiter.
    pub fn record(&self, stream: &str, error: impl std::fmt::Display) {
        let msg = format!("{stream}: {error}");
        tracing::error!(error = %msg, "replay-merge recovery failed");
        let mut slot = self.slot.lock().expect("replay failure lock");
        if slot.is_none() {
            *slot = Some(msg);
        }
        drop(slot);
        // `notify_one` stores a permit if the waiter has not parked yet, so a
        // failure recorded before `failed()` is polled is never lost.
        self.notify.notify_one();
    }

    /// The recorded failure, if any.
    pub fn get(&self) -> Option<String> {
        self.slot.lock().expect("replay failure lock").clone()
    }

    /// Wait until a failure is recorded, then return it.
    pub async fn failed(&self) -> String {
        loop {
            if let Some(msg) = self.get() {
                return msg;
            }
            self.notify.notified().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Async log handles → sync engine traits, with the per-shard pump tasks.
// ---------------------------------------------------------------------------

struct LiveTxDataSub {
    sequencer_id: u8,
    rx: sync_mpsc::Receiver<(TxDataLoc, TxEnvelope)>,
}

impl TxDataSubscription for LiveTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    fn next(&mut self) -> Result<(TxDataLoc, TxEnvelope), ExecutorError> {
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
/// Live multicast when `replay_dst` is `None`; archive replay-merge (replay
/// from the recording's origin, then follow live) when `Some` — both paths
/// yield `(TxDataLoc, TxEnvelope)` with the ORIGINAL publisher session (replay
/// is frame-faithful), so the reader's session-keyed join works either way.
/// A replay pump that ends abnormally records on `failure` so the binary
/// exits non-zero instead of treating a failed recovery as a clean stop.
pub fn open_tx_data_subs(
    rt: &AeronRuntime,
    channels: &ChannelsConfig,
    aeron_cfg: &AeronConfig,
    shards: u8,
    replay_dst: Option<&str>,
    failure: &ReplayFailure,
) -> Result<Vec<Box<dyn TxDataSubscription>>> {
    let mut a_subs: Vec<Box<dyn TxDataSubscription>> = Vec::with_capacity(shards as usize);
    for shard_id in 0..shards {
        let (tx, rx) = sync_mpsc::channel::<(TxDataLoc, TxEnvelope)>();
        if let Some(replay_dst) = replay_dst {
            let mut replay =
                replay::open_tx_data_replay(channels, aeron_cfg, shard_id, replay_dst.to_string())
                    .with_context(|| format!("open tx_data replay-merge shard={shard_id}"))?;
            let failure = failure.clone();
            tokio::spawn(async move {
                while let Some(item) = replay.recv().await {
                    if tx.send(item).is_err() {
                        return; // consumer gone (shutdown)
                    }
                }
                if let Some(e) = replay.take_failure() {
                    failure.record(&format!("tx_data[{shard_id}] replay-merge"), e);
                }
            });
        } else {
            let mut handle = TxDataSubscriberHandle::open(rt, channels, shard_id)
                .with_context(|| format!("open TxDataSubscriberHandle shard={shard_id}"))?;
            tokio::spawn(async move {
                while let Some(item) = handle.recv().await {
                    if tx.send(item).is_err() {
                        break;
                    }
                }
            });
        }
        a_subs.push(Box::new(LiveTxDataSub {
            sequencer_id: shard_id,
            rx,
        }));
    }
    Ok(a_subs)
}

/// Open the tx_deposits subscription (async→sync bridged), live or archive
/// replay-merge — the deposit-path mirror of [`open_tx_data_subs`].
pub fn open_tx_deposits_sub(
    rt: &AeronRuntime,
    channels: &ChannelsConfig,
    aeron_cfg: &AeronConfig,
    replay_dst: Option<&str>,
    failure: &ReplayFailure,
) -> Result<Box<dyn DepositSubscription>> {
    let (d_tx, d_rx) = sync_mpsc::channel::<(BPosition, Deposit)>();
    if let Some(replay_dst) = replay_dst {
        let mut replay =
            replay::open_tx_deposits_replay(channels, aeron_cfg, replay_dst.to_string())
                .context("open tx_deposits archive replay-merge subscriber")?;
        let failure = failure.clone();
        tokio::spawn(async move {
            while let Some(item) = replay.recv().await {
                if d_tx.send(item).is_err() {
                    return;
                }
            }
            if let Some(e) = replay.take_failure() {
                failure.record("tx_deposits replay-merge", e);
            }
        });
    } else {
        let mut handle = TxDepositsSubscriberHandle::open(rt, channels)
            .context("open TxDepositsSubscriberHandle")?;
        tokio::spawn(async move {
            while let Some(item) = handle.recv().await {
                if d_tx.send(item).is_err() {
                    break;
                }
            }
        });
    }
    Ok(Box::new(LiveTxDepositsSub { rx: d_rx }))
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

    // F13.4 regression (unit level): a failure recorded BEFORE the waiter
    // parks must still complete `failed()` — the permit is stored.
    #[tokio::test]
    async fn replay_failure_recorded_before_wait_is_not_lost() {
        let f = ReplayFailure::default();
        f.record("tx_data[3] replay-merge", "archive gone");
        let msg = f.failed().await;
        assert!(msg.contains("tx_data[3]"), "{msg}");
        assert!(msg.contains("archive gone"), "{msg}");
        // First failure wins.
        f.record("tx_deposits replay-merge", "later");
        assert!(f.get().unwrap().contains("tx_data[3]"));
    }
}
