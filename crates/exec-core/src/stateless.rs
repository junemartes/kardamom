//! The `no_std` block driver + the hardened stateless entry (spec:
//! no-std-exec-core, phase 3).
//!
//! [`execute_block`] is the single-scope sequential driver both live roles
//! already used (hoisted verbatim from the validator so the zk guest links
//! the exact production code path): one [`ExecScope`] per block, deposits
//! folded into the scope cache so later txs observe their writes.
//!
//! [`execute_block_stateless`] is the GUEST shape: the same driver over a
//! fail-closed [`WitnessDb`], with the trust boundary CLOSED. The live
//! pipeline trusts `TxEnvelope.sender` / `tx_hash` from the proxy (S0); a
//! proof cannot. Before executing, every tx record's identity is re-derived
//! in-guest — `tx_hash = keccak256(raw_tx)` and `sender` recovered from the
//! secp256k1 signature (pure-Rust k256) — and any mismatch aborts with
//! [`ExecutorError::RecordIdentity`].
//!
//! Known gaps, deliberately NOT closed here (documented in the spec):
//! - Deposits: `source_hash` derives from L1 data the guest does not yet
//!   carry (deposit-derivation phases D/E) — deposit identity remains a
//!   trusted input until the witness is L1-anchored.
//! - The witness itself is unanchored until phase 3b (MPT proofs against
//!   `pre_state_root`).

use alloc::format;
use alloc::vec::Vec;

use alloy_consensus::transaction::SignerRecoverable;
use alloy_primitives::keccak256;
use kardamom_types::{BPosition, ExecutionWitness, Receipt, StateDatabase, TxEnvelope};

use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::executor::{ExecScope, decode_alloy_envelope, execute_deposit_tx};
use crate::witness::WitnessDb;

/// One canonical record of a block, in execution order. Clone is cheap:
/// envelope/deposit byte payloads are refcounted `Bytes` (the validator's
/// flight ring keeps recent blocks' records for receipt-divergence dumps).
#[derive(Clone)]
pub enum BufferedRecord {
    Tx {
        tx_idx: TxIndex,
        envelope: kardamom_types::TxEnvelope,
        position: BPosition,
    },
    Deposit {
        tx_idx: TxIndex,
        deposit: kardamom_types::Deposit,
        position: BPosition,
    },
}

/// What a block-execution strategy returns: the block's receipts in block
/// order (block-cumulative gas already correct) and its merged writes.
pub struct BlockExecOutput {
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
}

/// Execute a block's records sequentially over `snapshot ∘ parent`: ONE
/// scope for the whole block, deposit writes folded into the scope cache so
/// later txs observe them. This IS the validator's sequential re-execution
/// path (`execute_block_sequential` delegates here) — and therefore the
/// exact semantics a stateless replay must reproduce.
pub fn execute_block<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<BlockExecOutput, ExecutorError> {
    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(records.len());
    let mut cumulative = 0u64;
    let mut scope = ExecScope::new(snapshot, parent, env)?;
    for (i, rec) in records.iter().enumerate() {
        let idx_in_block = i as u64;
        let (receipt, ws) = match rec {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => scope.execute_tx(*tx_idx, *position, envelope, idx_in_block, cumulative, None)?,
            BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => {
                let out = execute_deposit_tx(
                    snapshot,
                    parent,
                    &delta,
                    env,
                    *tx_idx,
                    *position,
                    deposit,
                    idx_in_block,
                    cumulative,
                    None,
                )?;
                // Fold deposit writes into the scope cache (mirrors the
                // actor's streaming path) so later txs observe them.
                let mut layer = PendingDelta::new();
                layer.apply(out.1.clone());
                scope.seed_layer(&layer)?;
                out
            }
        };
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    Ok(BlockExecOutput { receipts, delta })
}

/// Re-derive a tx record's identity from its raw bytes — the in-guest
/// closure of the S0 trust boundary. The live pipeline takes
/// `sender`/`tx_hash` from the proxy on faith; a proof must not.
pub fn verify_record_identity(envelope: &TxEnvelope) -> Result<(), ExecutorError> {
    let computed_hash = keccak256(&envelope.raw_tx);
    if computed_hash != envelope.tx_hash {
        return Err(ExecutorError::RecordIdentity(format!(
            "tx_hash mismatch: envelope carries {}, keccak256(raw_tx) = {computed_hash}",
            envelope.tx_hash
        )));
    }
    let decoded = decode_alloy_envelope(&envelope.raw_tx, TxIndex::ZERO)?;
    let recovered = decoded
        .recover_signer()
        .map_err(|e| ExecutorError::RecordIdentity(format!("sender recovery failed: {e}")))?;
    if recovered != envelope.sender {
        return Err(ExecutorError::RecordIdentity(format!(
            "sender mismatch: envelope carries {}, signature recovers {recovered}",
            envelope.sender
        )));
    }
    Ok(())
}

/// The zk-guest execution shape: verify every tx record's identity, then
/// execute the block over NOTHING but the witness. Fail-closed twice over —
/// on identity forgery and on witness incompleteness.
pub fn execute_block_stateless(
    witness: &ExecutionWitness,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<BlockExecOutput, ExecutorError> {
    for rec in records {
        if let BufferedRecord::Tx { envelope, .. } = rec {
            verify_record_identity(envelope)?;
        }
        // Deposits: identity stays a trusted input until the witness is
        // L1-anchored (see module docs).
    }
    let db = WitnessDb::from_witness(witness);
    execute_block(&db, parent, records, env)
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256};

    // A well-formed envelope for identity tests, signed with the anvil #0
    // dev key (public, dev only).
    fn honest_envelope() -> TxEnvelope {
        use alloy_consensus::{SignableTransaction, TxLegacy};
        use alloy_eips::eip2718::Encodable2718;
        use alloy_network::TxSignerSync;
        use alloy_signer_local::PrivateKeySigner;
        let signer: PrivateKeySigner =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                .parse()
                .unwrap();
        let mut tx = TxLegacy {
            chain_id: Some(412346),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: alloy_primitives::TxKind::Call(Address::repeat_byte(0xdd)),
            value: alloy_primitives::U256::from(1u64),
            input: Default::default(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let env = alloy_consensus::TxEnvelope::Legacy(tx.into_signed(sig));
        let mut raw = Vec::new();
        env.encode_2718(&mut raw);
        TxEnvelope {
            correlation_id: 0,
            raw_tx: bytes::Bytes::from(raw),
            sender: signer.address(),
            tx_hash: *env.tx_hash(),
        }
    }

    #[test]
    fn honest_identity_verifies() {
        verify_record_identity(&honest_envelope()).unwrap();
    }

    #[test]
    fn forged_tx_hash_is_rejected() {
        let mut e = honest_envelope();
        e.tx_hash = B256::repeat_byte(0x66);
        assert!(matches!(
            verify_record_identity(&e),
            Err(ExecutorError::RecordIdentity(_))
        ));
    }

    #[test]
    fn forged_sender_is_rejected() {
        let mut e = honest_envelope();
        e.sender = Address::repeat_byte(0x66);
        assert!(matches!(
            verify_record_identity(&e),
            Err(ExecutorError::RecordIdentity(_))
        ));
    }

    #[test]
    fn corrupted_raw_bytes_are_rejected() {
        let mut e = honest_envelope();
        let mut raw = e.raw_tx.to_vec();
        let last = raw.len() - 1;
        raw[last] ^= 0x01; // flip one signature bit
        e.raw_tx = bytes::Bytes::from(raw);
        // Either the hash check or recovery must refuse it.
        assert!(verify_record_identity(&e).is_err());
    }
}
