//! `kardamom-validator`: monolithic validator node.
//!
//! It follows the sequencer by subscribing to the same canonical streams
//! the executor reads (`tx_data` x 8 lanes, `tx_ordering` from the Aeron
//! Cluster (Raft) egress, `tx_deposits`). It re-executes every block
//! through the shared `kardamom-engine` pipeline, and commits to its own
//! libmdbx state through the trie-aware writer, advancing a canonical
//! Ethereum MPT state root per block. It also subscribes to the
//! executor's `tx_receipts` and per-block `tx_bal` (BAL) streams, checks
//! its independent re-execution against them, and stops on any proven
//! divergence. It has no HA and runs off the hot path.
//!
//! The default run re-executes from genesis, or resumes through the same
//! archive replay-merge the executor uses, produces roots, and checks
//! them. It publishes nothing on the L2 streams.
//!
//! L1 output attestation (optional): when `--l1-rpc-url`,
//! `--output-oracle`, and `--attester-key` are all given, a background
//! attester collects each committed block's `MessagePassed` withdrawal
//! leaves, builds the per-output withdrawals root, and posts one output
//! per `--attester-post-interval` blocks to the L1
//! `WithdrawalOutputOracle`. The key must be the oracle's permissioned
//! `attester`. Without all three flags, the validator does no automatic
//! attestation.
//!
//! Structure: CLI and file config in [`args`]; the checkpoint-trust
//! lifecycle (adoption marker, trie bootstrap, resync fallback) in
//! [`adoption`]; the verification-stream pump tasks in [`pumps`]; the
//! startup builder and shutdown helpers in [`wiring`].

mod adoption;
mod args;
mod pumps;
mod wiring;

use anyhow::Result;
use clap::Parser;

use args::Args;
use wiring::Startup;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let args = Args::parse();
    Startup::init(args)
        .await?
        .open_state()?
        .open_streams()?
        .spawn_pumps()?
        .spawn_writer()?
        .spawn_attester()?
        .build_sink()
        .run()
        .await
}
