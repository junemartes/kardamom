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
#[derive(Debug)]
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
    execute_block_inner(snapshot, parent, records, env, None).map(|(out, _)| out)
}

/// [`execute_block`] with EIP-7928 capture: also returns the block's raw
/// (granularity-1) access list, built through the SAME per-tx capture hooks
/// the live executor publishes from — `Bal::update_account` for txs,
/// the synthetic-WriteSet path for deposits.
pub fn execute_block_with_bal<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<(BlockExecOutput, alloy_eip7928::BlockAccessList), ExecutorError> {
    let mut bal = revm::state::bal::Bal::new();
    let (out, _) = execute_block_inner(snapshot, parent, records, env, Some(&mut bal))?;
    Ok((out, bal.into_alloy_bal()))
}

fn execute_block_inner<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
    mut bal: Option<&mut revm::state::bal::Bal>,
) -> Result<(BlockExecOutput, ()), ExecutorError> {
    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(records.len());
    let mut cumulative = 0u64;
    let mut scope = ExecScope::new(snapshot, parent, env)?;
    for (i, rec) in records.iter().enumerate() {
        let idx_in_block = i as u64;
        // revm's Bal convention: index 0 = pre-execution, 1..=n = txs in
        // block order (same as the actor's `tx_index_in_block + 1`).
        let bal_arg = bal.as_deref_mut().map(|b| (b, idx_in_block + 1));
        let (receipt, ws) = execute_record_in_scope(
            &mut scope,
            snapshot,
            parent,
            &delta,
            env,
            rec,
            idx_in_block,
            cumulative,
            bal_arg,
        )?;
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    Ok((BlockExecOutput { receipts, delta }, ()))
}

/// Execute ONE canonical record inside an existing block scope — the
/// Tx-vs-Deposit dispatch every whole-block strategy shares. A Tx runs in
/// the scope; a Deposit runs outside it (rare, own commit semantics) against
/// `snapshot ∘ parent ∘ delta`, and its writes are folded into the scope
/// cache so later records observe them. `delta` is the caller's accumulated
/// block/batch delta; `parent` its seed layer.
///
/// This is the single home of consensus-critical record dispatch: the
/// sequential driver above, the validator's parallel batches, and (through
/// the driver) the zk guest all execute records through here.
#[allow(clippy::too_many_arguments)] // mirrors execute_tx/execute_deposit_tx;
// a params struct would rename the same nine fields without removing any.
pub fn execute_record_in_scope<'a, S: StateDatabase>(
    scope: &mut ExecScope<&'a S>,
    snapshot: &'a S,
    parent: Option<&PendingDelta>,
    delta: &PendingDelta,
    env: ExecEnv,
    rec: &BufferedRecord,
    idx_in_block: u64,
    cumulative: u64,
    bal: Option<(&mut revm::state::bal::Bal, u64)>,
) -> Result<(Receipt, crate::delta::WriteSet), ExecutorError> {
    match rec {
        BufferedRecord::Tx {
            tx_idx,
            envelope,
            position,
        } => scope.execute_tx(
            *tx_idx,
            *position,
            envelope,
            idx_in_block,
            cumulative,
            bal,
            None,
        ),
        BufferedRecord::Deposit {
            tx_idx,
            deposit,
            position,
        } => {
            let out = execute_deposit_tx(
                snapshot,
                parent,
                delta,
                env,
                *tx_idx,
                *position,
                deposit,
                idx_in_block,
                cumulative,
                bal,
            )?;
            // Fold deposit writes into the scope cache (mirrors the
            // actor's streaming path) so later txs observe them.
            let mut layer = PendingDelta::new();
            layer.apply(out.1.clone());
            scope.seed_layer(&layer)?;
            Ok(out)
        }
    }
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

/// The zk-guest execution shape, with the published BAL as a PROOF INPUT:
/// verify every tx record's identity, execute the block over NOTHING but
/// the witness, re-derive the access list through the live capture path,
/// quantize it at the frame's granularity through the shared
/// [`crate::bal_ladder`], and require structural equality with the input.
///
/// Fail-closed three times over — identity forgery
/// ([`ExecutorError::RecordIdentity`]), witness incompleteness
/// ([`crate::witness::WitnessError`] surfaced through execution), and BAL
/// inequality ([`ExecutorError::Divergence`], the same class the live
/// validator fail-stops on). On success the proof may bind
/// [`bal_commitment`]`(expected_bal)` as a public output: the recomputed
/// list is structurally equal, so the commitment attests the PUBLISHED
/// artifact.
pub fn execute_block_stateless(
    witness: &ExecutionWitness,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
    expected_bal: &alloy_eip7928::BlockAccessList,
    granularity: u16,
) -> Result<BlockExecOutput, ExecutorError> {
    for rec in records {
        if let BufferedRecord::Tx { envelope, .. } = rec {
            verify_record_identity(envelope)?;
        }
        // Deposits: identity stays a trusted input until the witness is
        // L1-anchored (see module docs).
    }
    let db = WitnessDb::from_witness(witness);
    let (out, raw_bal) = execute_block_with_bal(&db, parent, records, env)?;
    let recomputed = crate::bal_ladder::quantize(raw_bal, granularity);
    if &recomputed != expected_bal {
        return Err(ExecutorError::Divergence(format!(
            "stateless BAL mismatch at block {}: recomputed {} account entr{} vs published {} \
             (granularity {granularity}); first differing address: {}",
            env.block_number,
            recomputed.len(),
            if recomputed.len() == 1 { "y" } else { "ies" },
            expected_bal.len(),
            first_bal_difference(&recomputed, expected_bal),
        )));
    }
    Ok(out)
}

/// The 3b proof shape: a stateless execution ANCHORED to the chain's root
/// history. These are the proof's public outputs — an inductive chain from
/// genesis: the L1 verifier holds the running root, checks
/// `pre_state_root` continuity, `bal_commitment` against the posted frame,
/// and advances to `post_state_root`.
#[derive(Debug)]
pub struct AnchoredBlockOutput {
    pub out: BlockExecOutput,
    pub pre_state_root: alloy_primitives::B256,
    pub post_state_root: alloy_primitives::B256,
    pub bal_commitment: alloy_primitives::B256,
    pub block_number: u64,
}

/// The FULL guest entry (spec: no-std-exec-core, phase 3b):
/// [`execute_block_stateless`]'s three fail-closed layers (identity,
/// witness completeness, BAL equality) plus the MPT anchor on both ends —
/// the witness is proven against `pre_state_root` BEFORE the first EVM
/// step, and the post-state root is recomputed from the carried node set
/// after the last. A prover that fabricates state now has nowhere left to
/// stand: the witness must hash-link into a root the L1 already holds.
pub fn execute_block_anchored(
    witness: &ExecutionWitness,
    proofs: &kardamom_types::WitnessProofs,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
    expected_bal: &alloy_eip7928::BlockAccessList,
    granularity: u16,
) -> Result<AnchoredBlockOutput, ExecutorError> {
    let pre = crate::anchor::verify_witness_anchored(witness, proofs)?;
    let pre_state_root = witness
        .pre_state_root
        .expect("verify_witness_anchored requires the root");
    let out = execute_block_stateless(witness, parent, records, env, expected_bal, granularity)?;
    let post_state_root = crate::anchor::recompute_post_root(witness, proofs, &pre, &out.delta)?;
    Ok(AnchoredBlockOutput {
        pre_state_root,
        post_state_root,
        bal_commitment: bal_commitment(expected_bal),
        block_number: env.block_number,
        out,
    })
}

/// Canonical commitment to a (quantized) access list: keccak256 of its RLP
/// encoding — the SAME bytes the executor publishes in `BalFrame.bal_rlp`,
/// so an L1 verifier can check the proof's public output against the posted
/// frame without re-encoding.
pub fn bal_commitment(bal: &alloy_eip7928::BlockAccessList) -> alloy_primitives::B256 {
    use alloy_rlp::Encodable;
    let mut rlp = Vec::new();
    bal.encode(&mut rlp);
    keccak256(&rlp)
}

/// Name the first address where two access lists disagree (or the shape
/// difference) — receipt-mismatch-grade diagnosability (#159's lesson).
fn first_bal_difference(
    a: &alloy_eip7928::BlockAccessList,
    b: &alloy_eip7928::BlockAccessList,
) -> alloc::string::String {
    use alloc::string::ToString;
    for (x, y) in a.iter().zip(b.iter()) {
        if x.address != y.address {
            return format!("{} vs {}", x.address, y.address);
        }
        if x != y {
            return x.address.to_string();
        }
    }
    "(entry-count mismatch)".to_string()
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
