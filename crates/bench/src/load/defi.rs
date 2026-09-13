//! This is the `DeFi` bench workload: CLOB updates, Uniswap-style swaps,
//! and vault flows.
//!
//! The contracts live in `bench-contracts/src/BenchDefi.sol`, its own
//! foundry project, kept apart from the pinned CREATE2-sensitive one.
//! `bench-contracts/embed.sh` embeds the creation bytecode into
//! `defi_bytecode.rs`. The mix is chosen for its write-set profile as
//! much as its gas profile:
//!
//! - A swap writes to two hot reserve slots, whose attribution can
//!   collapse into a chunk.
//! - A vault operation writes to two hot aggregate slots and one
//!   unique per-user slot.
//! - A CLOB place allocates a fresh order struct, in a unique slot that
//!   chunking cannot compress, behind a hot ID counter and best-price
//!   slots.
//!
//! Deployment is deterministic: the first load sender deploys all
//! three contracts, at nonces `nonce_start` through `nonce_start + 2`.
//! This lets every other sender compute the addresses without an RPC
//! round trip, and sender 0's operation queue simply starts three
//! nonces later.

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use alloy_consensus::TxLegacy;
use alloy_primitives::{Address, Bytes, TxKind, U256, keccak256};
use anyhow::Context as _;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;

use crate::load::hex_u64;
use crate::load::plan::{PlannedTx, TxPlanParams};
use crate::signers::{DerivedSigner, SignerSet};

include!("defi_bytecode.rs");

/// The gas limit for every workload call. This covers the CLOB worst
/// case, cold order slots plus a crossing fill, with headroom. Unused
/// gas is refunded; only `gasUsed` counts toward the gas/s metrics.
const CALL_GAS_LIMIT: u64 = 400_000;
const CREATE_GAS_LIMIT: u64 = 1_500_000;

#[derive(Debug, Clone, Copy)]
pub struct DefiContracts {
    pub pool: Address,
    pub vault: Address,
    pub clob: Address,
}

impl DefiContracts {
    /// The addresses when `deployer` creates the pool, vault, and CLOB
    /// at `nonce_start`, `nonce_start + 1`, and `nonce_start + 2`.
    ///
    /// # Errors
    ///
    /// Returns an error if `nonce_start + 2` overflows `u64`.
    pub fn at(deployer: Address, nonce_start: u64) -> anyhow::Result<Self> {
        let n1 = nonce_start
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("nonce_start + 1 overflows u64"))?;
        let n2 = nonce_start
            .checked_add(2)
            .ok_or_else(|| anyhow::anyhow!("nonce_start + 2 overflows u64"))?;
        Ok(Self {
            pool: deployer.create(nonce_start),
            vault: deployer.create(n1),
            clob: deployer.create(n2),
        })
    }
}

fn selector(sig: &str) -> [u8; 4] {
    keccak256(sig.as_bytes())[..4].try_into().unwrap()
}

fn call(selector_sig: &str, args: &[U256]) -> Bytes {
    let mut data = Vec::with_capacity(4 + 32 * args.len());
    data.extend_from_slice(&selector(selector_sig));
    for a in args {
        data.extend_from_slice(&a.to_be_bytes::<32>());
    }
    Bytes::from(data)
}

/// A previous order ID to cancel, for churn. ID `0` never exists (the
/// CLOB assigns IDs starting at 1), so a request for it floors to ID 1:
/// still a cheap no-op cancel, never an out-of-range one.
fn churn_id(seq: u64) -> U256 {
    let id = if seq == 0 { 1 } else { seq };
    U256::from(id)
}

/// The target contract and calldata one operation calls.
struct OpCall {
    to: Address,
    input: Bytes,
}

/// The first operation in every sender's queue: `pool.seed()`, so swaps
/// have balances to move. Every operation after that comes from [`op`].
fn seed_or_op(contracts: &DefiContracts, sender: usize, i: u64) -> OpCall {
    if i == 0 {
        OpCall {
            to: contracts.pool,
            input: call("seed()", &[]),
        }
    } else {
        op(contracts, sender, i)
    }
}

/// The deterministic operation for `(sender, seq)`: the target contract
/// and calldata. The mix is about 50% swaps, 25% vault operations
/// (deposit and withdraw alternating), and 25% CLOB operations (7
/// places for every 1 cancel).
fn op(contracts: &DefiContracts, sender: usize, seq: u64) -> OpCall {
    // This is a cheap deterministic mixer, not a hash. It only decorrelates
    // the mix from the sequence, so every sender exercises all operations
    // in all phases.
    let h = (sender as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(seq.wrapping_mul(0xBF58_476D_1CE4_E5B9));
    match h % 4 {
        0 | 1 => {
            let zero_for_one = U256::from(seq & 1);
            let amount_in = U256::from(10u128.pow(17) + u128::from(h % 100) * 10u128.pow(15));
            OpCall {
                to: contracts.pool,
                input: call("swap(bool,uint256)", &[zero_for_one, amount_in]),
            }
        }
        2 => {
            if seq & 1 == 0 {
                let assets = U256::from(10u128.pow(18) + u128::from(h % 1000) * 10u128.pow(15));
                OpCall {
                    to: contracts.vault,
                    input: call("deposit(uint256)", &[assets]),
                }
            } else {
                let shares = U256::from(5u128 * 10u128.pow(17));
                OpCall {
                    to: contracts.vault,
                    input: call("withdraw(uint256)", &[shares]),
                }
            }
        }
        _ => {
            if h % 8 == 7 {
                // Cancel a recent-ish ID. A cancel of another user's order,
                // or of a filled order, is a cheap no-op. This is realistic
                // book churn.
                OpCall {
                    to: contracts.clob,
                    input: call("cancel(uint256)", &[churn_id(seq.saturating_sub(1))]),
                }
            } else {
                let bid = U256::from(seq & 1);
                let price = U256::from(1_000 + h % 64);
                let size = U256::from(1_000_000 + h % 1_000_000);
                OpCall {
                    to: contracts.clob,
                    input: call("place(bool,uint256,uint96)", &[bid, price, size]),
                }
            }
        }
    }
}

/// The per-transaction fields `sign` needs, beyond the signer and the
/// chain ID both callers already have in scope.
struct SignSpec {
    nonce: u64,
    gas_price: u128,
    gas_limit: u64,
    to: TxKind,
    input: Bytes,
    sender: usize,
}

fn sign(s: &DerivedSigner, chain_id: u64, spec: SignSpec) -> anyhow::Result<PlannedTx> {
    let SignSpec {
        nonce,
        gas_price,
        gas_limit,
        to,
        input,
        sender,
    } = spec;
    let tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price,
        gas_limit,
        to,
        value: U256::ZERO,
        input,
    };
    let signed = s
        .sign_raw(tx)
        .map_err(|e| anyhow::anyhow!("signing defi tx (sender {sender} nonce {nonce}): {e}"))?;
    Ok(PlannedTx {
        raw: signed.raw,
        hash: signed.hash,
        sender,
        nonce,
    })
}

fn creation_bytes(hex: &str) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(
        alloy_primitives::hex::decode(hex).context("embedded bytecode hex")?,
    ))
}

/// The three deployment transactions, and the contract addresses they
/// create.
pub struct Deployment {
    pub txs: Vec<PlannedTx>,
    pub contracts: DefiContracts,
}

/// The three deployment transactions, signed by the first signer at
/// nonces `nonce_start` through `nonce_start + 2`. Submit and confirm
/// these before starting load: every workload call targets their
/// computed addresses.
///
/// # Errors
///
/// Returns an error if signing a deployment transaction fails.
pub fn deployment_txs(signers: &SignerSet, params: TxPlanParams) -> anyhow::Result<Deployment> {
    let deployer = signers.deployer();
    let contracts = DefiContracts::at(deployer.signer.address(), params.nonce_start)?;
    let txs = [SWAPPOOL_CREATION_HEX, VAULT_CREATION_HEX, CLOB_CREATION_HEX]
        .iter()
        .enumerate()
        .map(|(i, hex)| {
            let nonce = params
                .nonce_start
                .checked_add(i as u64)
                .ok_or_else(|| anyhow::anyhow!("nonce_start + {i} overflows u64"))?;
            sign(
                deployer,
                params.chain_id,
                SignSpec {
                    nonce,
                    gas_price: params.gas_price,
                    gas_limit: CREATE_GAS_LIMIT,
                    to: TxKind::Create,
                    input: creation_bytes(hex)?,
                    sender: 0,
                },
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Deployment { txs, contracts })
}

// This is a liveness bound, expressed as one. `deploy_and_confirm`'s
// wait stage starts the moment the transfer soak's verdict lands, so
// the chain is still draining that backlog, with the deploy queued
// behind it. How long that takes is a property of the runner, not of
// the code under test. A fixed wall-clock deadline would race the
// drain instead.
//
// So `deploy_and_confirm` waits as long as the chain is advancing, and
// fails only when it stops. A stalled pipeline is caught in seconds; a
// merely slow one is waited out. The overall cap stays as a backstop
// against waiting forever on a chain that advances but never includes
// the transaction.
const STALL_LIMIT: Duration = Duration::from_secs(60);
const HARD_CAP: Duration = Duration::from_secs(600);

async fn head_block(client: &HttpClient) -> Option<u64> {
    client
        .request::<String, _>("eth_blockNumber", rpc_params![])
        .await
        .ok()
        .and_then(|h| hex_u64(&h))
}

/// Submit the deployment transactions and wait until each is mined
/// successfully. Land these before any load starts: every workload
/// call targets their computed addresses, so a call that arrives
/// before its contract exists would revert and spoil the verdict.
///
/// # Errors
/// Returns an error if a submit is rejected, a deployment reverts, the
/// chain stops advancing while a deployment is unmined, or an accepted
/// deployment is never included within the hard cap.
pub(crate) async fn deploy_and_confirm(
    client: &HttpClient,
    deploys: &[PlannedTx],
) -> anyhow::Result<()> {
    for d in deploys {
        let _: alloy_primitives::B256 = client
            .request("eth_sendRawTransaction", rpc_params![d.raw.clone()])
            .await
            .map_err(|e| anyhow::anyhow!("defi deploy submit (nonce {}): {e}", d.nonce))?;
    }
    let started = Instant::now();
    for d in deploys {
        confirm_deploy(client, d, started).await?;
    }
    tracing::info!("defi contracts deployed + confirmed");
    Ok(())
}

/// [`confirm_deploy`]'s carried state across polls: the last seen head
/// block, and when it last changed.
struct ConfirmState {
    last_block: Option<u64>,
    last_progress: Instant,
}

/// Poll until `d`'s receipt lands, or fail if the chain stalls or the
/// deploy takes too long overall from `started`.
async fn confirm_deploy(
    client: &HttpClient,
    d: &PlannedTx,
    started: Instant,
) -> anyhow::Result<()> {
    let mut state = ConfirmState {
        last_block: head_block(client).await,
        last_progress: Instant::now(),
    };
    loop {
        let ControlFlow::Continue(()) =
            confirm_tick_or_wait(client, d, started, &mut state).await?
        else {
            return Ok(());
        };
    }
}

/// One [`confirm_deploy`] poll. Returns [`ControlFlow::Break`] once
/// [`confirm_tick`] sees the receipt land; otherwise sleeps the poll
/// interval and returns [`ControlFlow::Continue`], so the caller's loop
/// tries again.
async fn confirm_tick_or_wait(
    client: &HttpClient,
    d: &PlannedTx,
    started: Instant,
    state: &mut ConfirmState,
) -> anyhow::Result<ControlFlow<()>> {
    let cf = confirm_tick(client, d, started, state).await?;
    if cf.is_continue() {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(cf)
}

/// One [`confirm_tick_or_wait`] poll: check for a landed receipt, else
/// update the stall clock and check both deadlines.
async fn confirm_tick(
    client: &HttpClient,
    d: &PlannedTx,
    started: Instant,
    state: &mut ConfirmState,
) -> anyhow::Result<ControlFlow<()>> {
    let v: Option<serde_json::Value> = client
        .request("eth_getTransactionReceipt", rpc_params![d.hash])
        .await
        .unwrap_or(None);
    if let Some(r) = v {
        anyhow::ensure!(
            r["status"].as_str() == Some("0x1"),
            "defi deploy reverted (nonce {}): {r}",
            d.nonce
        );
        return Ok(ControlFlow::Break(()));
    }
    let now = head_block(client).await;
    if now.is_some() && now != state.last_block {
        state.last_block = now;
        state.last_progress = Instant::now();
    }
    anyhow::ensure!(
        state.last_progress.elapsed() < STALL_LIMIT,
        "defi deploy not mined (nonce {}): chain STOPPED advancing — no new \
         block for {}s while waiting (head {:?})",
        d.nonce,
        STALL_LIMIT.as_secs(),
        state.last_block
    );
    anyhow::ensure!(
        started.elapsed() < HARD_CAP,
        "defi deploy not mined (nonce {}) within {}s although the chain kept \
         advancing to {:?} — the tx was accepted but never included",
        d.nonce,
        HARD_CAP.as_secs(),
        state.last_block
    );
    Ok(ControlFlow::Continue(()))
}

/// Pre-sign per-sender queues of `DeFi` calls. Sender 0's nonces start
/// after the three deployments. Every sender's first operation is
/// `pool.seed()`, so swaps have balances to move.
///
/// # Errors
///
/// Returns an error if signing a call fails.
pub fn pregenerate_defi(
    signers: &SignerSet,
    contracts: &DefiContracts,
    per_sender: usize,
    params: TxPlanParams,
) -> anyhow::Result<Vec<Vec<PlannedTx>>> {
    signers
        .iter()
        .enumerate()
        .map(|(sender, s)| {
            let base = if sender == 0 {
                // This is after the deployments.
                params
                    .nonce_start
                    .checked_add(3)
                    .ok_or_else(|| anyhow::anyhow!("nonce_start + 3 overflows u64"))?
            } else {
                params.nonce_start
            };
            (0..per_sender)
                .map(|i| {
                    let nonce = base
                        .checked_add(i as u64)
                        .ok_or_else(|| anyhow::anyhow!("base + {i} overflows u64"))?;
                    let OpCall { to, input } = seed_or_op(contracts, sender, i as u64);
                    sign(
                        s,
                        params.chain_id,
                        SignSpec {
                            nonce,
                            gas_price: params.gas_price,
                            gas_limit: CALL_GAS_LIMIT,
                            to: TxKind::Call(to),
                            input,
                            sender,
                        },
                    )
                })
                .collect::<anyhow::Result<Vec<PlannedTx>>>()
        })
        .collect()
}

/// Pre-sign per-sender queues of a single operation family, for
/// allocation profiling: per-family numbers separate contract-execution
/// cost from engine fixed cost. A family that needs state, such as
/// withdraw needing shares or cancel needing orders, interleaves a
/// setup operation every 4th transaction, so the measured operation
/// dominates.
///
/// # Errors
///
/// Returns an error if signing a call fails.
pub fn pregenerate_family(
    signers: &SignerSet,
    chain_id: u64,
    contracts: &DefiContracts,
    fam: &str,
    per_sender: usize,
    nonce_start: u64,
    gas_price: u128,
) -> anyhow::Result<Vec<Vec<PlannedTx>>> {
    signers
        .iter()
        .enumerate()
        .map(|(sender, s)| {
            let base = if sender == 0 {
                nonce_start
                    .checked_add(3)
                    .ok_or_else(|| anyhow::anyhow!("nonce_start + 3 overflows u64"))?
            } else {
                nonce_start
            };
            let ctx = FamilyCtx {
                contracts,
                fam,
                chain_id,
                base,
                gas_price,
                sender,
            };
            (0..per_sender).map(|i| family_op_tx(s, &ctx, i)).collect()
        })
        .collect()
}

/// The per-sender constants [`family_op_tx`] needs.
struct FamilyCtx<'a> {
    contracts: &'a DefiContracts,
    fam: &'a str,
    chain_id: u64,
    base: u64,
    gas_price: u128,
    sender: usize,
}

/// One profiling-family operation, at position `i` in the sender's
/// queue: `i == 0` always seeds the pool, so every measured operation
/// (`i >= 1`) has balances to move.
fn family_op_tx(s: &DerivedSigner, ctx: &FamilyCtx<'_>, i: usize) -> anyhow::Result<PlannedTx> {
    let nonce = ctx
        .base
        .checked_add(i as u64)
        .ok_or_else(|| anyhow::anyhow!("base + {i} overflows u64"))?;
    let seq = i as u64;
    let (to, input, gas) = match (ctx.fam, i) {
        (_, 0) => (ctx.contracts.pool, call("seed()", &[]), CALL_GAS_LIMIT),
        ("swap", _) => (
            ctx.contracts.pool,
            call(
                "swap(bool,uint256)",
                &[U256::from(seq & 1), U256::from(10u128.pow(17))],
            ),
            CALL_GAS_LIMIT,
        ),
        ("vault_deposit", _) => (
            ctx.contracts.vault,
            call("deposit(uint256)", &[U256::from(10u128.pow(18))]),
            CALL_GAS_LIMIT,
        ),
        ("vault_withdraw", n) if n % 4 == 1 => (
            ctx.contracts.vault,
            call("deposit(uint256)", &[U256::from(4u128 * 10u128.pow(18))]),
            CALL_GAS_LIMIT,
        ),
        ("vault_withdraw", _) => (
            ctx.contracts.vault,
            call("withdraw(uint256)", &[U256::from(10u128.pow(17))]),
            CALL_GAS_LIMIT,
        ),
        ("clob_place", _) => (
            ctx.contracts.clob,
            call(
                "place(bool,uint256,uint96)",
                &[
                    U256::from(seq & 1),
                    U256::from(1_000 + seq % 64),
                    U256::from(1_000_000u64),
                ],
            ),
            CALL_GAS_LIMIT,
        ),
        ("clob_cancel", n) if n % 2 == 1 => (
            ctx.contracts.clob,
            call(
                "place(bool,uint256,uint96)",
                &[
                    U256::from(0u64),
                    U256::from(1_000u64),
                    U256::from(1_000_000u64),
                ],
            ),
            CALL_GAS_LIMIT,
        ),
        ("clob_cancel", _) => (
            ctx.contracts.clob,
            call("cancel(uint256)", &[churn_id(seq)]),
            CALL_GAS_LIMIT,
        ),
        ("transfer", _) => (Address::repeat_byte(0xEE), Bytes::new(), 21_000),
        (other, _) => anyhow::bail!("unknown profile family {other:?}"),
    };
    sign(
        s,
        ctx.chain_id,
        SignSpec {
            nonce,
            gas_price: ctx.gas_price,
            gas_limit: gas,
            to: TxKind::Call(to),
            input,
            sender: ctx.sender,
        },
    )
}

#[cfg(test)]
pub(crate) mod tests;
