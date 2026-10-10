//! The transaction canary against the local stack.
//!
//! - [`probes_succeed`]: the `kardamom-canary` binary runs its probes on
//!   the stack's ingress and notifier. Every `transfer`, `read` and
//!   `contract` run succeeds, and the feed stages arrive.
//! - [`ring_resolves_in_flight`]: the ring's nonce owner, driven through
//!   the library. An accepted submit whose answer was lost, and a signed
//!   transaction that never left before a restart, both resolve from the
//!   journal before their nonce takes other work; concurrent probes on
//!   one account get distinct, contiguous nonces.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::Path;
use std::time::Duration;

use alloy_consensus::TxEip1559;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use anyhow::{Context, Result};
use kardamom_canary::config::Endpoint;
use kardamom_canary::feed::{Board, BoardHandle};
use kardamom_canary::outcome::Outcome;
use kardamom_canary::ring::journal::{InFlight, Journal};
use kardamom_canary::ring::{self, Call, DerivedSigner, Lease, Ring, Sent};
use kardamom_canary::rpc::Rpc;
use kardamom_canary::wait::Poll;

use super::Target;
use crate::harness::l2::DEV_MNEMONIC;
use crate::harness::metrics;

/// The canary's metric of probe runs.
const PROBE_TOTAL: &str = "kardamom_canary_probe_total";
/// The patience for one landed transaction on the local stack.
const LAND: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(100);

/// The probes that run on short timers in the scenario.
const PROBES: [&str; 6] = [
    "probe=\"transfer\"",
    "probe=\"read\"",
    "probe=\"contract\"",
    "probe=\"fees\"",
    "probe=\"rwa\"",
    "probe=\"swap\"",
];
/// The probes that must succeed once in the scenario's time: the
/// liquidity add and the deposit.
const ONCE: [&str; 2] = ["probe=\"liquidity\"", "probe=\"deposit\""];
/// How many transfers [`receipts_match_their_blocks`] checks.
const BLOCK_CHECKS: usize = 5;
/// The L1 key of the canary in the scenario: anvil account #9, which no
/// other part of the stack uses.
pub const L1_KEY: &str = "0x2a871d0798f97d79848a013d4936a73bf4cc922c825d33c1cf7073dff6d409c6";

/// Wait until every probe has `runs` successes (the liquidity and the
/// deposit probes one) on the canary at `canary`, then require that no
/// probe but `read` failed, and that the feed reported the `executed`
/// stage. A `read` run can see the head not move between two runs on an
/// idle local chain.
///
/// # Errors
/// Returns an error when the successes do not come within the patience,
/// or when a failure outcome or a missing stage shows.
pub async fn probes_succeed(canary: SocketAddr, runs: f64) -> Result<()> {
    let success = |probe: &'static str| [probe, "outcome=\"success\""];
    metrics::poll_until(
        "every canary probe succeeds",
        Duration::from_secs(300),
        Duration::from_millis(500),
        async || {
            // The exporter binds a moment after the process starts.
            let Ok(s) = metrics::scrape(canary).await else {
                return Ok(None);
            };
            let count = |probe| {
                s.value_where_all(PROBE_TOTAL, &success(probe))
                    .unwrap_or(0.0)
            };
            let done =
                PROBES.iter().all(|p| count(p) >= runs) && ONCE.iter().all(|p| count(p) >= 1.0);
            Ok(done.then_some(()))
        },
    )
    .await?;
    let s = metrics::scrape(canary).await?;
    let raw = kardamom_obs::testkit::scrape(&format!("http://{canary}/metrics")).await;
    let failures: Vec<&str> = raw
        .lines()
        .filter(|l| l.starts_with(PROBE_TOTAL) && !l.contains("outcome=\"success\""))
        .collect();
    for probe in PROBES
        .iter()
        .chain(ONCE.iter())
        .filter(|p| !p.contains("read"))
    {
        let all = s.value_where_all(PROBE_TOTAL, &[probe]).unwrap_or(0.0);
        let ok = s
            .value_where_all(PROBE_TOTAL, &success(probe))
            .unwrap_or(0.0);
        anyhow::ensure!(
            (all - ok).abs() < f64::EPSILON,
            "{probe}: {} of {all} runs failed: {failures:#?}",
            all - ok
        );
    }
    let executed = s
        .value_where_all(
            "kardamom_canary_stage_seconds_count",
            &["probe=\"transfer\"", "stage=\"executed\""],
        )
        .unwrap_or(0.0);
    anyhow::ensure!(executed > 0.0, "no transfer saw the executed stage");
    Ok(())
}

/// Mine an L1 block every second until the task is aborted: the
/// da-watcher and the `deposit` probe follow L1 finality.
#[must_use]
pub fn keep_mining(rpc_url: String) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Ok(client) = jsonrpsee::http_client::HttpClientBuilder::default().build(&rpc_url)
        else {
            return;
        };
        loop {
            let _: Result<serde_json::Value, _> = jsonrpsee::core::client::ClientT::request(
                &client,
                "evm_mine",
                jsonrpsee::rpc_params![],
            )
            .await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}

/// The ring's nonce owner on the stack: a lost answer, a restart with an
/// unsent transaction, and concurrent leases on one account.
///
/// # Errors
/// Returns an error when a nonce is reused, skipped, or never resolves.
pub async fn ring_resolves_in_flight(t: &Target, dir: &Path, first: u32) -> Result<()> {
    let rpc = Rpc::new(
        t.rpc.url.parse::<Endpoint>().map_err(anyhow::Error::msg)?,
        LAND,
    )?;
    let one = NonZeroU32::MIN;
    let lost = ring::signers(DEV_MNEMONIC, first, one)?;
    lost_answer(t, &rpc, &dir.join("lost"), lost, true).await?;
    let unsent = ring::signers(DEV_MNEMONIC, first.saturating_add(1), one)?;
    lost_answer(t, &rpc, &dir.join("unsent"), unsent, false).await?;
    let shared = ring::signers(DEV_MNEMONIC, first.saturating_add(2), one)?;
    concurrent(t, &rpc, &dir.join("shared"), shared).await
}

/// Write a signed transfer at the committed nonce to the journal as a
/// probe does before it submits. With `submitted`, submit it too and drop
/// the answer: the accepted submit whose response is lost. Without, the
/// process stops before the submit. A ring opened on the journal (the
/// restart) must land that transaction and then sign the next nonce.
async fn lost_answer(
    t: &Target,
    rpc: &Rpc,
    dir: &Path,
    signers: Vec<DerivedSigner>,
    submitted: bool,
) -> Result<()> {
    let signer = signers.first().context("one signer")?.clone();
    let nonce = rpc.nonce(signer.address).await?;
    let tx = TxEip1559 {
        chain_id: t.chain_id,
        nonce,
        gas_limit: 21_000,
        max_fee_per_gas: 10_000_000_000,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(Address::repeat_byte(0x5c)),
        value: U256::from(1),
        access_list: alloy_eips::eip2930::AccessList::default(),
        input: Bytes::new(),
    };
    let journaled = signer.sign_raw(tx)?;
    tokio::fs::create_dir_all(dir).await?;
    Journal::new(dir, signer.address)
        .store(&InFlight {
            nonce,
            hash: journaled.hash,
            raw: journaled.raw.clone(),
            published: submitted,
        })
        .await?;
    if submitted {
        let _ = rpc.send(&journaled.raw).await;
    }
    let ring = Ring::open(dir, signers, t.chain_id, board()).await?;
    let lease = lease_when_free(&ring, rpc).await?;
    anyhow::ensure!(
        rpc.receipt(journaled.hash).await.ok().flatten().is_some(),
        "the journal's transaction did not land before the lease"
    );
    let next = transfer(rpc, lease).await?.nonce;
    anyhow::ensure!(
        Some(next) == nonce.checked_add(1),
        "after nonce {nonce} the ring signed {next}"
    );
    Ok(())
}

/// Six tasks lease one account at once. Each lands one transfer; the
/// nonces are distinct and contiguous.
async fn concurrent(t: &Target, rpc: &Rpc, dir: &Path, signers: Vec<DerivedSigner>) -> Result<()> {
    let start = rpc.nonce(signers[0].address).await?;
    let ring = std::sync::Arc::new(Ring::open(dir, signers, t.chain_id, board()).await?);
    let tasks: Vec<_> = (0..6)
        .map(|_| {
            let ring = std::sync::Arc::clone(&ring);
            let rpc = rpc.clone();
            tokio::spawn(async move {
                let lease = lease_when_free(&ring, &rpc).await?;
                transfer(&rpc, lease).await.map(|sent| sent.nonce)
            })
        })
        .collect();
    let mut nonces = Vec::new();
    for task in tasks {
        nonces.push(task.await??);
    }
    nonces.sort_unstable();
    let expected: Vec<u64> = (start..start.saturating_add(6)).collect();
    anyhow::ensure!(
        nonces == expected,
        "nonces {nonces:?}, expected {expected:?}"
    );
    Ok(())
}

/// The ground truth of a receipt's block. The ring account `index` lands
/// [`BLOCK_CHECKS`] transfers, one at a time, so they fall in different
/// blocks while the base fee moves. For each one, these must agree:
///
/// - the receipt's `blockNumber` on the ingress, and in the executor's
///   committed receipt;
/// - the committed block of that number, which lists the transaction;
/// - the base fee of that block's header row;
/// - the receipt's `effectiveGasPrice`, the base fee it paid;
/// - the base fee that `eth_feeHistory` gives for that block.
///
/// # Errors
/// Returns an error when a transfer does not land, or when two of these
/// values disagree.
pub async fn receipts_match_their_blocks(
    t: &Target,
    state_dir: &Path,
    dir: &Path,
    index: u32,
) -> Result<()> {
    let rpc = Rpc::new(
        t.rpc.url.parse::<Endpoint>().map_err(anyhow::Error::msg)?,
        LAND,
    )?;
    let signers = ring::signers(DEV_MNEMONIC, index, NonZeroU32::MIN)?;
    let ring = Ring::open(dir, signers, t.chain_id, board()).await?;
    let mut hashes = Vec::with_capacity(BLOCK_CHECKS);
    for _ in 0..BLOCK_CHECKS {
        hashes.push(
            transfer(&rpc, lease_when_free(&ring, &rpc).await?)
                .await?
                .hash,
        );
    }
    let env = super::open_state_ro(state_dir)?;
    for hash in hashes {
        BlockCheck {
            rpc: &rpc,
            env: &env,
            hash,
        }
        .run()
        .await?;
    }
    Ok(())
}

/// One transaction of [`receipts_match_their_blocks`], with the ingress
/// and the executor's state.
struct BlockCheck<'a> {
    rpc: &'a Rpc,
    env: &'a kardamom_state::StateEnv,
    hash: B256,
}

impl BlockCheck<'_> {
    async fn run(&self) -> Result<()> {
        let receipt = self
            .rpc
            .receipt(self.hash)
            .await?
            .with_context(|| format!("no receipt for {:#x}", self.hash))?;
        let number = receipt.block();
        let price = receipt
            .effective_gas_price
            .context("the receipt has no effectiveGasPrice")?;
        let listed = self.committed_block(number).await?;
        anyhow::ensure!(
            listed.refs.iter().any(|r| r.tx_hash == self.hash),
            "block {number} does not list {:#x}",
            self.hash
        );
        let committed = kardamom_state::committed_receipt(self.env, self.hash)?
            .receipt
            .context("no committed receipt")?;
        let header = kardamom_state::read_all_headers(self.env)?
            .into_iter()
            .find_map(|(n, h)| (n == number).then_some(h))
            .with_context(|| format!("no header row for block {number}"))?;
        let history = self.rpc.base_fee(number).await.context("eth_feeHistory")?;
        anyhow::ensure!(
            committed.block_number == number,
            "the ingress gives block {number}, the state gives {}",
            committed.block_number
        );
        anyhow::ensure!(
            U256::from(header.base_fee) == price && history == price,
            "block {number}: header base fee {}, fee history {history}, \
             effectiveGasPrice {price}",
            header.base_fee
        );
        Ok(())
    }

    /// The references of block `number`, once the executor commits it.
    async fn committed_block(&self, number: u64) -> Result<kardamom_state::BlockRefs> {
        Poll::within(LAND, POLL)
            .until(|| async {
                kardamom_state::committed_block_refs(self.env, number)
                    .ok()
                    .and_then(|c| c.refs)
            })
            .await
            .with_context(|| format!("block {number} is not committed"))
    }
}

/// A status board with no feed: the ring cases resolve by receipt and
/// nonce alone.
fn board() -> BoardHandle {
    let (board, handle) = Board::new();
    tokio::spawn(board.run());
    handle
}

/// Lease the ring's one account once it is free and resolved.
async fn lease_when_free(ring: &Ring, rpc: &Rpc) -> Result<Lease> {
    Poll::within(LAND, POLL)
        .until(|| async {
            match ring.lease(rpc, Poll::within(LAND, POLL)).await {
                Ok(lease) => Some(Ok(lease)),
                Err(Outcome::AccountStalled) => None,
                Err(other) => Some(Err(anyhow::anyhow!("lease: {other:?}"))),
            }
        })
        .await
        .context("the account never came free")?
}

/// Send one transfer on `lease`, wait for its receipt, settle, and
/// return what was sent.
async fn transfer(rpc: &Rpc, mut lease: Lease) -> Result<Sent> {
    let sent = lease
        .send(
            rpc,
            Call::new(
                TxKind::Call(Address::repeat_byte(0x5d)),
                U256::from(1),
                Bytes::new(),
                21_000,
            ),
        )
        .await
        .map_err(|o| anyhow::anyhow!("send: {o:?}"))?;
    landed(rpc, sent.hash).await?;
    lease.settle().await;
    Ok(sent)
}

async fn landed(rpc: &Rpc, hash: B256) -> Result<()> {
    Poll::within(LAND, POLL)
        .until(|| async { rpc.receipt(hash).await.ok().flatten() })
        .await
        .map(|_| ())
        .with_context(|| format!("{hash:#x} did not land"))
}
