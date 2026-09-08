//! Withdrawal attester: the validator's L1-facing seam.
//!
//! After the validator commits a block range and advances its MPT state
//! root, the attester does three things. It collects the withdrawals
//! started in that range from the re-executed block receipts. It builds
//! the output root `keccak(VERSION ++ stateRoot ++ withdrawalsRoot)`. It
//! posts the root to the L1 `WithdrawalOutputOracle`. It runs off the hot
//! path and posts with its own L1 key. A permissioned challenger catches a
//! dishonest attester today; a ZK challenge is a later step.
//!
//! This module is split so the pure parts (leaf collection, output-root
//! computation, and the cadence state machine) are unit-tested in
//! isolation, and the [`oracle::OutputPoster`] is tested against anvil by
//! the integration test.
//!
//! The production driver is [`AttesterLoop::run`], spawned by
//! [`spawn_attester`] as a background task. The binary feeds it per-block
//! withdrawal leaves ([`AttesterHandle::submit_leaves`], through
//! [`sinks::AttestingReceiptSink`]) and per-block committed state roots
//! ([`AttesterHandle::submit_root`], from the existing snapshot poller).
//! Every `post_interval_blocks`, it builds and posts one output that
//! covers all pending leaves. Leaves are discarded only after a
//! successful post, so a failed proposal (an L1 hiccup, or re-proposing a
//! range whose output was deleted by a challenge) retries with the
//! accumulated set instead of dropping it.
//!
//! Challenge recovery: on startup, the driver resumes below the latest
//! non-deleted on-chain output, so a challenged and deleted output is
//! re-attested from the leaves the validator re-collects on replay.
//! Reacting to a deletion of an older range mid-run without a restart is a
//! follow-up.

mod oracle;
mod sinks;
mod state;

use std::num::NonZeroU64;

use alloy_network::Ethereum;
use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;

pub use oracle::{AttesterError, OutputPoster};
pub use sinks::{AttesterHandle, AttestingReceiptSink};
pub use state::Output;

use sinks::AttesterMsg;
use state::{AttestState, build_output};

/// Configuration for the background attestation task ([`spawn_attester`]).
/// `l1_rpc_url` and `signer` are already resolved: the caller parses the
/// `--l1-rpc-url` flag and resolves the `--attester-key` flag (raw hex or
/// `env:VAR`) before building this, so nothing in the attester's own
/// startup path can fail on malformed config.
#[derive(Debug, Clone)]
pub struct AttesterConfig {
    /// L1 JSON-RPC endpoint the outputs are posted to.
    pub l1_rpc_url: reqwest::Url,
    /// Address of the deployed `WithdrawalOutputOracle` proxy.
    pub oracle: Address,
    /// The attester, the oracle's permissioned proposer.
    pub signer: PrivateKeySigner,
    /// Post one output per this many L2 blocks.
    pub post_interval_blocks: NonZeroU64,
}

/// The result of [`spawn_attester`]: the feed handle, and the background
/// task's `JoinHandle`.
pub struct SpawnedAttester {
    pub handle: AttesterHandle,
    pub task: tokio::task::JoinHandle<()>,
}

/// Spawn the background attestation task. Call this from within a tokio
/// runtime. The task runs until every [`AttesterHandle`] clone is dropped.
///
/// On startup, it resumes from the latest non-deleted on-chain output (see
/// [`OutputPoster::latest_attested_block`]). So a deleted, challenged,
/// latest output is re-attested from the leaves the validator re-collects
/// on replay. A failed `proposeOutput` keeps its leaves pending and
/// retries at the next cadence point.
#[must_use]
pub fn spawn_attester(cfg: &AttesterConfig) -> SpawnedAttester {
    let poster = build_poster(cfg);
    let (handle, rx) = AttesterHandle::channel();
    let task = tokio::spawn(AttesterLoop::new(poster, rx, cfg.post_interval_blocks).run());
    SpawnedAttester { handle, task }
}

/// Build the output poster from the already-resolved signer and L1 URL.
fn build_poster(cfg: &AttesterConfig) -> OutputPoster<impl Provider<Ethereum> + Clone + use<>> {
    let provider = ProviderBuilder::new()
        .wallet(cfg.signer.clone())
        .connect_http(cfg.l1_rpc_url.clone());
    OutputPoster::new(provider, cfg.oracle)
}

/// The attester task: resumes from the latest on-chain output, then drives
/// the cadence state machine from the feed channel until every
/// [`AttesterHandle`] clone drops.
struct AttesterLoop<P: Provider<Ethereum> + Clone> {
    poster: OutputPoster<P>,
    rx: tokio::sync::mpsc::UnboundedReceiver<AttesterMsg>,
    state: AttestState,
    interval: NonZeroU64,
}

impl<P: Provider<Ethereum> + Clone> AttesterLoop<P> {
    fn new(
        poster: OutputPoster<P>,
        rx: tokio::sync::mpsc::UnboundedReceiver<AttesterMsg>,
        interval: NonZeroU64,
    ) -> Self {
        // `last_attested` starts at 0; `run` overwrites it with the
        // resumed on-chain value once the async lookup returns, before
        // any message can be read from `rx`.
        Self {
            poster,
            rx,
            state: AttestState::new(0, interval),
            interval,
        }
    }

    async fn run(mut self) {
        let resume = match self.poster.latest_attested_block().await {
            Ok(b) => b,
            Err(e) => {
                // Not fatal: start from genesis. The oracle's monotonicity
                // check rejects any overlap with already-attested ranges.
                tracing::warn!(error = %e, "attester: could not read resume point; starting at 0");
                None
            }
        };
        if let Some(b) = resume {
            self.state = AttestState::new(b, self.interval);
        }
        tracing::info!(
            last_attested = self.state.last_attested(),
            post_interval_blocks = self.interval.get(),
            "attester task started"
        );
        while let Some(msg) = self.rx.recv().await {
            match msg {
                AttesterMsg::Leaves { block, leaves } => self.state.on_leaves(block, leaves),
                AttesterMsg::Root { block, state_root } => self.state.on_root(block, state_root),
            }
            self.post_due().await;
        }
        tracing::info!("attester task stopping (all handles dropped)");
    }

    /// Post an output if a block is attestable now. A root is attestable
    /// only once the receipt stream confirms its block's receipts are
    /// complete, so either message (leaves or a root) can be the one that
    /// releases it.
    async fn post_due(&mut self) {
        let Some((block, state_root)) = self.state.next_attestable() else {
            return;
        };
        let leaves = self.state.leaves_through(block);
        let output = build_output(state_root, &leaves);
        match self.poster.propose_output(output.output_root, block).await {
            Ok(tx_hash) => {
                tracing::info!(
                    l2_block = block,
                    leaves = leaves.len(),
                    output_root = %output.output_root,
                    l1_tx = %tx_hash,
                    "attester posted output"
                );
                self.state.mark_attested(block);
            }
            Err(e) => {
                // Keep the leaves and the root pending, carried forward, and
                // retry at the next cadence point.
                tracing::warn!(
                    l2_block = block,
                    error = %e,
                    "attester failed to post output; will retry with accumulated leaves"
                );
            }
        }
    }
}
