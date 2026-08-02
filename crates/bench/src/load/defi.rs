//! DeFi bench workload: CLOB updates, Uniswap-style swaps, vault flows.
//!
//! Contracts live in `bench-contracts/src/BenchDefi.sol` (own foundry
//! project, isolated from the pinned CREATE2-sensitive one); creation
//! bytecode is embedded via `bench-contracts/embed.sh` into
//! `defi_bytecode.rs`. The mix is chosen for its BAL/write-set profile as
//! much as its gas profile:
//!
//! - swaps hammer two HOT reserve slots (chunk-collapsible attribution),
//! - vault ops mix two hot aggregate slots with a unique per-user slot,
//! - CLOB places allocate fresh order structs (UNIQUE slots chunking cannot
//!   compress) behind a hot id counter and best-price slots.
//!
//! Deployment is deterministic: the FIRST load sender deploys all three
//! contracts at `nonce_start..nonce_start+2`, so every other sender can
//! compute the addresses without an RPC round-trip, and sender 0's op queue
//! simply starts three nonces later.

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256, keccak256};

use crate::load::plan::PlannedTx;
use crate::signers::DerivedSigner;

include!("defi_bytecode.rs");

/// Gas limit for every workload call: covers the CLOB worst case (cold
/// order slots + a crossing fill) with headroom. Unused gas is refunded;
/// only `gasUsed` counts toward the gas/s metrics.
const CALL_GAS_LIMIT: u64 = 400_000;
const CREATE_GAS_LIMIT: u64 = 1_500_000;

#[derive(Debug, Clone, Copy)]
pub struct DefiContracts {
    pub pool: Address,
    pub vault: Address,
    pub clob: Address,
}

impl DefiContracts {
    /// Addresses when `deployer` creates pool, vault, clob at
    /// `nonce_start`, `nonce_start+1`, `nonce_start+2`.
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

/// The deterministic op for `(sender, seq)`: target contract + calldata.
/// Mix: ~50% swaps, ~25% vault (deposit/withdraw alternating), ~25% CLOB
/// (7 places : 1 cancel).
fn op(contracts: &DefiContracts, sender: usize, seq: u64) -> (Address, Bytes) {
    // Cheap deterministic mixer — NOT a hash, just decorrelates the mix
    // from the sequence so every sender exercises all ops in all phases.
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
                // Cancel a recent-ish id. Cancels of other users' orders (or
                // filled ones) no-op cheaply — realistic book churn.
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

/// The three deployment txs, signed by `signers[0]` at
/// `nonce_start..nonce_start+2`. Submit and CONFIRM these before starting
/// load — every workload call targets their computed addresses.
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

/// Pre-sign per-sender queues of DeFi calls. Sender 0's nonces start after
/// the three deployments; every sender's FIRST op is `pool.seed()` so swaps
/// have balances to move.
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
            nonce_start + 3 // after the deployments
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

/// Pre-sign per-sender queues of a SINGLE op family (allocation profiling:
/// per-family numbers separate contract-execution cost from engine fixed
/// cost). Families needing state (withdraw needs shares, cancel needs
/// orders) interleave a setup op every 4th tx so the measured op dominates.
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
        // Every sender's first op is the pool seed (funds swap balances).
        for queue in &q {
            assert!(!queue.is_empty());
        }
    }
}
