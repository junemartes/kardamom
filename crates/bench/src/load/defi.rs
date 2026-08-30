//! This is the DeFi bench workload: CLOB updates, Uniswap-style swaps,
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

use std::time::{Duration, Instant};

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256, keccak256};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;

use crate::load::hex_u64;
use crate::load::plan::PlannedTx;
use crate::signers::DerivedSigner;

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
    pub fn at(deployer: Address, nonce_start: u64) -> Self {
        Self {
            pool: deployer.create(nonce_start),
            vault: deployer.create(nonce_start + 1),
            clob: deployer.create(nonce_start + 2),
        }
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

/// The deterministic operation for `(sender, seq)`: the target contract
/// and calldata. The mix is about 50% swaps, 25% vault operations
/// (deposit and withdraw alternating), and 25% CLOB operations (7
/// places for every 1 cancel).
fn op(contracts: &DefiContracts, sender: usize, seq: u64) -> (Address, Bytes) {
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
            (
                contracts.pool,
                call("swap(bool,uint256)", &[zero_for_one, amount_in]),
            )
        }
        2 => {
            if seq & 1 == 0 {
                let assets = U256::from(10u128.pow(18) + u128::from(h % 1000) * 10u128.pow(15));
                (contracts.vault, call("deposit(uint256)", &[assets]))
            } else {
                let shares = U256::from(5u128 * 10u128.pow(17));
                (contracts.vault, call("withdraw(uint256)", &[shares]))
            }
        }
        _ => {
            if h % 8 == 7 {
                // Cancel a recent-ish ID. A cancel of another user's order,
                // or of a filled order, is a cheap no-op. This is realistic
                // book churn.
                let id = U256::from((seq.saturating_sub(1)).max(1));
                (contracts.clob, call("cancel(uint256)", &[id]))
            } else {
                let bid = U256::from(seq & 1);
                let price = U256::from(1_000 + h % 64);
                let size = U256::from(1_000_000 + h % 1_000_000);
                (
                    contracts.clob,
                    call("place(bool,uint256,uint96)", &[bid, price, size]),
                )
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn sign(
    s: &DerivedSigner,
    chain_id: u64,
    nonce: u64,
    gas_price: u128,
    gas_limit: u64,
    to: TxKind,
    input: Bytes,
    sender: usize,
) -> anyhow::Result<PlannedTx> {
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price,
        gas_limit,
        to,
        value: U256::ZERO,
        input,
    };
    let sig = s
        .signer
        .sign_transaction_sync(&mut tx)
        .map_err(|e| anyhow::anyhow!("signing defi tx (sender {sender} nonce {nonce}): {e}"))?;
    let signed = tx.into_signed(sig);
    let hash = *signed.hash();
    let envelope: TxEnvelope = signed.into();
    let mut bytes = Vec::with_capacity(200);
    envelope.encode_2718(&mut bytes);
    Ok(PlannedTx {
        raw: Bytes::from(bytes),
        hash,
        sender,
        nonce,
    })
}

fn creation_bytes(hex: &str) -> Bytes {
    Bytes::from(alloy_primitives::hex::decode(hex).expect("embedded bytecode hex"))
}

/// The three deployment transactions, signed by `signers[0]` at nonces
/// `nonce_start` through `nonce_start + 2`. Submit and confirm these
/// before starting load: every workload call targets their computed
/// addresses.
pub fn deployment_txs(
    signers: &[DerivedSigner],
    chain_id: u64,
    nonce_start: u64,
    gas_price: u128,
) -> anyhow::Result<(Vec<PlannedTx>, DefiContracts)> {
    let deployer = signers
        .first()
        .ok_or_else(|| anyhow::anyhow!("at least one signer required"))?;
    let contracts = DefiContracts::at(deployer.signer.address(), nonce_start);
    let txs = [SWAPPOOL_CREATION_HEX, VAULT_CREATION_HEX, CLOB_CREATION_HEX]
        .iter()
        .enumerate()
        .map(|(i, hex)| {
            sign(
                deployer,
                chain_id,
                nonce_start + i as u64,
                gas_price,
                CREATE_GAS_LIMIT,
                TxKind::Create,
                creation_bytes(hex),
                0,
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((txs, contracts))
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
pub async fn deploy_and_confirm(client: &HttpClient, deploys: &[PlannedTx]) -> anyhow::Result<()> {
    for d in deploys {
        let _: alloy_primitives::B256 = client
            .request("eth_sendRawTransaction", rpc_params![d.raw.clone()])
            .await
            .map_err(|e| anyhow::anyhow!("defi deploy submit (nonce {}): {e}", d.nonce))?;
    }
    // This is a liveness bound, expressed as one. This stage starts the
    // moment the transfer soak's verdict lands, so the chain is still
    // draining that backlog, with the deploy queued behind it. How long
    // that takes is a property of the runner, not of the code under test.
    // A fixed wall-clock deadline would race the drain instead.
    //
    // So this code waits as long as the chain is advancing, and fails
    // only when it stops. A stalled pipeline is caught in seconds; a
    // merely slow one is waited out. The overall cap stays as a backstop
    // against waiting forever on a chain that advances but never
    // includes this transaction.
    const STALL_LIMIT: Duration = Duration::from_secs(60);
    const HARD_CAP: Duration = Duration::from_secs(600);
    async fn head_block(client: &HttpClient) -> Option<u64> {
        client
            .request::<String, _>("eth_blockNumber", rpc_params![])
            .await
            .ok()
            .and_then(|h| hex_u64(&h))
    }
    let started = Instant::now();
    for d in deploys {
        let mut last_block = head_block(client).await;
        let mut last_progress = Instant::now();
        loop {
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
                break;
            }
            let now = head_block(client).await;
            if now.is_some() && now != last_block {
                last_block = now;
                last_progress = Instant::now();
            }
            anyhow::ensure!(
                last_progress.elapsed() < STALL_LIMIT,
                "defi deploy not mined (nonce {}): chain STOPPED advancing — no new \
                 block for {}s while waiting (head {:?})",
                d.nonce,
                STALL_LIMIT.as_secs(),
                last_block
            );
            anyhow::ensure!(
                started.elapsed() < HARD_CAP,
                "defi deploy not mined (nonce {}) within {}s although the chain kept \
                 advancing to {:?} — the tx was accepted but never included",
                d.nonce,
                HARD_CAP.as_secs(),
                last_block
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    tracing::info!("defi contracts deployed + confirmed");
    Ok(())
}

/// Pre-sign per-sender queues of DeFi calls. Sender 0's nonces start
/// after the three deployments. Every sender's first operation is
/// `pool.seed()`, so swaps have balances to move.
pub fn pregenerate_defi(
    signers: &[DerivedSigner],
    chain_id: u64,
    contracts: &DefiContracts,
    per_sender: usize,
    nonce_start: u64,
    gas_price: u128,
) -> anyhow::Result<Vec<Vec<PlannedTx>>> {
    if signers.is_empty() {
        anyhow::bail!("at least one signer is required");
    }
    let mut out = Vec::with_capacity(signers.len());
    for (sender, s) in signers.iter().enumerate() {
        let base = if sender == 0 {
            nonce_start + 3 // This is after the deployments.
        } else {
            nonce_start
        };
        let mut queue = Vec::with_capacity(per_sender);
        for i in 0..per_sender {
            let nonce = base + i as u64;
            let (to, input) = if i == 0 {
                (contracts.pool, call("seed()", &[]))
            } else {
                op(contracts, sender, i as u64)
            };
            queue.push(sign(
                s,
                chain_id,
                nonce,
                gas_price,
                CALL_GAS_LIMIT,
                TxKind::Call(to),
                input,
                sender,
            )?);
        }
        out.push(queue);
    }
    Ok(out)
}

/// Pre-sign per-sender queues of a single operation family, for
/// allocation profiling: per-family numbers separate contract-execution
/// cost from engine fixed cost. A family that needs state, such as
/// withdraw needing shares or cancel needing orders, interleaves a
/// setup operation every 4th transaction, so the measured operation
/// dominates.
pub fn pregenerate_family(
    signers: &[DerivedSigner],
    chain_id: u64,
    contracts: &DefiContracts,
    fam: &str,
    per_sender: usize,
    nonce_start: u64,
    gas_price: u128,
) -> anyhow::Result<Vec<Vec<PlannedTx>>> {
    let mut out = Vec::with_capacity(signers.len());
    for (sender, s) in signers.iter().enumerate() {
        let base = if sender == 0 {
            nonce_start + 3
        } else {
            nonce_start
        };
        let mut queue = Vec::with_capacity(per_sender);
        for i in 0..per_sender {
            let nonce = base + i as u64;
            let seq = i as u64;
            let (to, input, gas) = match (fam, i) {
                (_, 0) => (contracts.pool, call("seed()", &[]), CALL_GAS_LIMIT),
                ("swap", _) => (
                    contracts.pool,
                    call(
                        "swap(bool,uint256)",
                        &[U256::from(seq & 1), U256::from(10u128.pow(17))],
                    ),
                    CALL_GAS_LIMIT,
                ),
                ("vault_deposit", _) => (
                    contracts.vault,
                    call("deposit(uint256)", &[U256::from(10u128.pow(18))]),
                    CALL_GAS_LIMIT,
                ),
                ("vault_withdraw", n) if n % 4 == 1 => (
                    contracts.vault,
                    call("deposit(uint256)", &[U256::from(4u128 * 10u128.pow(18))]),
                    CALL_GAS_LIMIT,
                ),
                ("vault_withdraw", _) => (
                    contracts.vault,
                    call("withdraw(uint256)", &[U256::from(10u128.pow(17))]),
                    CALL_GAS_LIMIT,
                ),
                ("clob_place", _) => (
                    contracts.clob,
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
                    contracts.clob,
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
                    contracts.clob,
                    call("cancel(uint256)", &[U256::from(seq.max(1))]),
                    CALL_GAS_LIMIT,
                ),
                ("transfer", _) => (Address::repeat_byte(0xEE), Bytes::new(), 21_000),
                (other, _) => anyhow::bail!("unknown profile family {other:?}"),
            };
            queue.push(sign(
                s,
                chain_id,
                nonce,
                gas_price,
                gas,
                TxKind::Call(to),
                input,
                sender,
            )?);
        }
        out.push(queue);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mnemonic;

    const ANVIL_PHRASE: &str = "test test test test test test test test test test test junk";

    #[test]
    fn op_mix_covers_all_contracts_and_is_deterministic() {
        let c = DefiContracts::at(Address::repeat_byte(9), 0);
        let mut hit = std::collections::HashSet::new();
        for sender in 0..4 {
            for seq in 1..64 {
                let (to, data) = op(&c, sender, seq);
                assert_eq!(op(&c, sender, seq), (to, data.clone()), "deterministic");
                hit.insert(to);
                assert!(data.len() >= 4);
            }
        }
        assert!(hit.contains(&c.pool) && hit.contains(&c.vault) && hit.contains(&c.clob));
    }

    #[test]
    fn deployment_addresses_match_planned_nonces() {
        let signers = mnemonic::derive_signers(ANVIL_PHRASE, 2).unwrap();
        let (txs, contracts) = deployment_txs(&signers, 412_346, 5, 1_000_000_000).unwrap();
        assert_eq!(txs.len(), 3);
        assert_eq!(txs[0].nonce, 5);
        assert_eq!(txs[2].nonce, 7);
        let expect = DefiContracts::at(signers[0].signer.address(), 5);
        assert_eq!(contracts.pool, expect.pool);
        assert_eq!(contracts.clob, expect.clob);
    }

    #[test]
    fn sender_zero_queue_starts_after_deployments() {
        let signers = mnemonic::derive_signers(ANVIL_PHRASE, 2).unwrap();
        let c = DefiContracts::at(signers[0].signer.address(), 0);
        let q = pregenerate_defi(&signers, 412_346, &c, 4, 0, 1_000_000_000).unwrap();
        assert_eq!(q[0][0].nonce, 3, "sender 0 shifted past deployments");
        assert_eq!(q[1][0].nonce, 0, "other senders start at nonce_start");
        // Every sender's first operation is the pool seed, which funds
        // swap balances.
        for queue in &q {
            assert!(!queue.is_empty());
        }
    }
}
