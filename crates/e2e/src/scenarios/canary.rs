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
use kardamom_canary::outcome::Outcome;
use kardamom_canary::ring::journal::{InFlight, Journal};
use kardamom_canary::ring::{self, Call, DerivedSigner, Lease, Ring};
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

/// Wait until every probe has `runs` successes on the canary at
/// `canary`, then require that no `transfer` or `contract` run failed
/// and that the feed reported the `executed` stage.
///
/// # Errors
/// Returns an error when the successes do not come within the patience,
/// or when a failure outcome or a missing stage shows.
pub async fn probes_succeed(canary: SocketAddr, runs: f64) -> Result<()> {
    let success = |probe: &'static str| [probe, "outcome=\"success\""];
    metrics::poll_until(
        "every canary probe succeeds",
        Duration::from_secs(180),
        Duration::from_millis(500),
        async || {
            // The exporter binds a moment after the process starts.
            let Ok(s) = metrics::scrape(canary).await else {
                return Ok(None);
            };
            let done = ["probe=\"transfer\"", "probe=\"read\"", "probe=\"contract\""]
                .into_iter()
                .all(|p| s.value_where_all(PROBE_TOTAL, &success(p)).unwrap_or(0.0) >= runs);
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
    for probe in ["probe=\"transfer\"", "probe=\"contract\""] {
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
    let ring = Ring::open(dir, signers, t.chain_id).await?;
    let lease = lease_when_free(&ring, rpc).await?;
    anyhow::ensure!(
        rpc.receipt(journaled.hash).await.ok().flatten().is_some(),
        "the journal's transaction did not land before the lease"
    );
    let next = transfer(rpc, lease).await?;
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
    let ring = std::sync::Arc::new(Ring::open(dir, signers, t.chain_id).await?);
    let tasks: Vec<_> = (0..6)
        .map(|_| {
            let ring = std::sync::Arc::clone(&ring);
            let rpc = rpc.clone();
            tokio::spawn(async move {
                let lease = lease_when_free(&ring, &rpc).await?;
                transfer(&rpc, lease).await
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
/// return its nonce.
async fn transfer(rpc: &Rpc, mut lease: Lease) -> Result<u64> {
    let sent = lease
        .send(
            rpc,
            Call {
                to: TxKind::Call(Address::repeat_byte(0x5d)),
                value: U256::from(1),
                input: Bytes::new(),
                gas_limit: 21_000,
            },
        )
        .await
        .map_err(|o| anyhow::anyhow!("send: {o:?}"))?;
    landed(rpc, sent.hash).await?;
    lease.settle().await;
    Ok(sent.nonce)
}

async fn landed(rpc: &Rpc, hash: B256) -> Result<()> {
    Poll::within(LAND, POLL)
        .until(|| async { rpc.receipt(hash).await.ok().flatten() })
        .await
        .map(|_| ())
        .with_context(|| format!("{hash:#x} did not land"))
}
