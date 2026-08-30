//! The `no_std` block driver, and the hardened stateless entry.
//!
//! [`execute_block`] is the single-scope sequential driver both live
//! roles already use. It is hoisted verbatim from the validator, so the
//! zk guest links the exact production code path: one [`ExecScope`] per
//! block, with deposits folded into the scope cache so later txs observe
//! their writes.
//!
//! [`execute_block_stateless`] is the guest shape: the same driver over a
//! fail-closed [`WitnessDb`], with the trust boundary closed. The live
//! pipeline trusts `TxEnvelope.sender` and `tx_hash` from the proxy; a
//! proof cannot. Before executing, this re-derives every tx record's
//! identity in-guest: `tx_hash = keccak256(raw_tx)`, and `sender`
//! recovered from the secp256k1 signature (pure-Rust k256). Any mismatch
//! aborts with [`ExecutorError::RecordIdentity`].
//!
//! Known gaps, left open on purpose (documented in the spec):
//!
//! - Deposits: `source_hash` derives from L1 data the guest does not yet
//!   carry (deposit-derivation phases D and E). Deposit identity stays a
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

/// One canonical record of a block, in execution order. Clone is cheap.
/// Envelope and deposit byte payloads are reference-counted `Bytes` (the
/// validator's flight ring keeps recent blocks' records for
/// receipt-divergence dumps).
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
/// order (with block-cumulative gas already correct), and its merged
/// writes.
#[derive(Debug)]
pub struct BlockExecOutput {
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
    /// EIP-7928 capture, when the strategy produced one. The executor's
    /// parallel path captures per-tx fragments and folds them through
    /// [`crate::bal_ladder::merge_bal_fragments`], so BAL publication
    /// survives a strategy swap. This is `None` for strategies that never
    /// publish (the validator verifies BALs; it does not emit them), and
    /// for the sequential drivers, whose callers capture through
    /// [`execute_block_with_bal`] instead.
    pub bal: Option<revm::state::bal::Bal>,
}

/// Execute a block's records sequentially, over `snapshot` composed with
/// `parent`. This uses one scope for the whole block, with deposit writes
/// folded into the scope cache so later txs observe them. This is the
/// validator's sequential re-execution path (`execute_block_sequential`
/// delegates here), so it defines the exact semantics a stateless replay
/// must reproduce.
pub fn execute_block<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<BlockExecOutput, ExecutorError> {
    execute_block_inner(snapshot, parent, records, env, None).map(|(out, _)| out)
}

/// [`execute_block`] with EIP-7928 capture, kept in revm form. The
/// output's `bal` field carries the block's `Bal` (granularity 1), ready
/// for the executor's boundary handoff to the BAL publisher. This is the
/// executor strategy's sequential arm: same driver and capture hooks as
/// [`execute_block_with_bal`], just a different output shape.
pub fn execute_block_capture<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<BlockExecOutput, ExecutorError> {
    let mut bal = revm::state::bal::Bal::new();
    let (mut out, _) = execute_block_inner(snapshot, parent, records, env, Some(&mut bal))?;
    out.bal = Some(bal);
    Ok(out)
}

/// [`execute_block`] with EIP-7928 capture. This also returns the block's
/// raw (granularity-1) access list, built through the same per-tx capture
/// hooks the live executor publishes from: `Bal::update_account` for txs,
/// and the synthetic-WriteSet path for deposits.
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
    Ok((
        BlockExecOutput {
            receipts,
            delta,
            bal: None,
        },
        (),
    ))
}

/// Execute one canonical record inside an existing block scope. This is
/// the tx-versus-deposit dispatch that every whole-block strategy shares.
/// A tx runs in the scope. A deposit runs outside it (rare, with its own
/// commit semantics), against `snapshot` composed with `parent` and
/// `delta`, and its writes are folded into the scope cache so later
/// records observe them. `delta` is the caller's accumulated block or
/// batch delta; `parent` is its seed layer.
///
/// This is the single home of consensus-critical record dispatch. The
/// sequential driver above, the validator's parallel batches, and (through
/// the driver) the zk guest all execute records through here.
#[allow(clippy::too_many_arguments)] // mirrors execute_tx and execute_deposit_tx;
// a params struct would just rename these nine fields, not reduce them.
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
            // Fold deposit writes into the scope cache, mirroring the
            // actor's streaming path, so later txs observe them.
            let mut layer = PendingDelta::new();
            layer.apply(out.1.clone());
            scope.seed_layer(&layer)?;
            Ok(out)
        }
    }
}

/// Re-derive a tx record's identity from its raw bytes. This closes the
/// trust boundary in-guest. The live pipeline takes `sender` and
/// `tx_hash` from the proxy on faith; a proof must not.
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

/// The zk-guest execution shape, with the published BAL as a proof input.
/// This verifies every tx record's identity, executes the block using
/// only the witness, re-derives the access list through the live capture
/// path, quantizes it at the frame's granularity through the shared
/// [`crate::bal_ladder`], and requires structural equality with the
/// input.
///
/// This fails closed in three ways: identity forgery
/// ([`ExecutorError::RecordIdentity`]), witness incompleteness
/// ([`crate::witness::WitnessError`] surfaced through execution), and BAL
/// inequality ([`ExecutorError::Divergence`], the same error class the
/// live validator fail-stops on). On success, the proof may bind
/// [`bal_commitment`]`(expected_bal)` as a public output. The recomputed
/// list is structurally equal, so the commitment attests to the
/// published artifact.
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
        // L1-anchored. See the module docs.
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

/// The phase-3b proof shape: a stateless execution anchored to the
/// chain's root history. These fields are the proof's public outputs, an
/// inductive chain from genesis. The L1 verifier holds the running root,
/// checks `pre_state_root` continuity and `bal_commitment` against the
/// posted frame, and advances to `post_state_root`.
#[derive(Debug)]
pub struct AnchoredBlockOutput {
    pub out: BlockExecOutput,
    pub pre_state_root: alloy_primitives::B256,
    pub post_state_root: alloy_primitives::B256,
    pub bal_commitment: alloy_primitives::B256,
    pub block_number: u64,
}

/// The full guest entry. This adds the
/// MPT anchor on both ends to [`execute_block_stateless`]'s three
/// fail-closed layers (identity, witness completeness, BAL equality). The
/// witness is proven against `pre_state_root` before the first EVM step,
/// and the post-state root is recomputed from the carried node set after
/// the last step. A prover that fabricates state has nowhere left to
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

/// Canonical commitment to a (quantized) access list: keccak256 of its
/// RLP encoding. These are the same bytes the executor publishes in
/// `BalFrame.bal_rlp`, so an L1 verifier can check the proof's public
/// output against the posted frame without re-encoding.
pub fn bal_commitment(bal: &alloy_eip7928::BlockAccessList) -> alloy_primitives::B256 {
    use alloy_rlp::Encodable;
    let mut rlp = Vec::new();
    bal.encode(&mut rlp);
    keccak256(&rlp)
}

/// Name the first address where two access lists disagree, or describe
/// the shape difference. This gives the same level of detail as a
/// receipt-mismatch diagnostic.
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

    // A well-formed envelope for identity tests. It is signed with
    // anvil's dev key #0, which is public and for development only.
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
