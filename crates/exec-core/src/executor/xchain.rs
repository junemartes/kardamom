//! Cross-chain message delivery ([`execute_xchain_tx`]): a fee-free inner
//! EVM call into the Inbox predeploy, with the message's `source_hash` as
//! the receipt's canonical id.

use kardamom_types::xchain::{self, XChainMessage};
use kardamom_types::{BPosition, Receipt, SkipReason, StateDatabase, WireLog};
use revm::context::result::{EVMError, ExecutionResult};
use revm::database::CacheDB;
use revm::primitives::Log;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use crate::block_env::{ExecEnv, SPEC_ID};
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;

use super::db::{SnapshotRef, seed_cache_layer};
use super::scope::{derived_tx_rejection, skip_reason_of_tx};
use super::tx_env::tx_env_from_xchain;
use super::write_set::{retain_changed, write_set_from_cache};

/// Extra gas for a cross-chain delivery, on top of the inner-call budget
/// and the calldata intrinsic gas.
///
/// `message.gas_limit` pays only for the inner `target` call. The Inbox's
/// own work (a delivery-status write, an event, and a callback enqueue
/// through the local Outbox) needs gas too. Without this headroom, that
/// work would use up the app's budget.
///
/// The Inbox forwards `min(gasLimit, 63/64 of the gas it has left)` to
/// the inner call (EIP-150). With this overhead, the inner call gets the
/// full `gasLimit` only up to about 4.9M gas. Above that, the 63/64 rule
/// withholds part of it. The delivery still executes; only the inner
/// call's budget is smaller than requested.
pub const XCHAIN_DELIVERY_OVERHEAD: u64 = 150_000;

/// The tx gas limit of one delivery: the message's inner-call budget, the
/// Inbox overhead, and the intrinsic gas of `calldata` under the pinned
/// spec. The intrinsic part is `max(standard, floor)`, where `standard`
/// is the 21 000 base plus the per-byte calldata cost, and `floor` is the
/// EIP-7623 calldata floor. Both come from revm's own helper, so the
/// budget always covers what revm's validation demands.
///
/// The sum stays under the EIP-7825 cap for every honest message: the
/// origin Outbox bounds `gas_limit` and `data`
/// ([`xchain::MAX_MESSAGE_GAS`], [`xchain::MAX_DATA_BYTES`]), and the
/// `budget_fits_the_tx_gas_cap` test pins the worst case.
pub fn xchain_gas_budget(message_gas_limit: u64, calldata: &[u8]) -> u64 {
    let gas = revm::context_interface::cfg::gas::calculate_initial_tx_gas(
        SPEC_ID, calldata, false, 0, 0, 0,
    );
    let intrinsic = gas.initial_total_gas.max(gas.floor_gas);
    message_gas_limit
        .saturating_add(XCHAIN_DELIVERY_OVERHEAD)
        .saturating_add(intrinsic)
}

/// The deterministic failed receipt for a delivery that revm rejects at
/// validation, or that fails the value pre-check. Both delivery paths
/// build it here, so the artifact is identical by construction.
///
/// The state it leaves: no state change (an empty write set), `status =
/// false`, `gas_used = 0`, no logs, and `skip_reason = Some(reason)`. The
/// pair `status = false, gas_used = 0` is the wire-visible skip marker
/// ([`Receipt::is_invalid_skip`]). It is loud by design: an honest origin
/// can never produce this case, because [`xchain::derive_remote_epoch`]
/// mirrors the Outbox bounds.
#[allow(clippy::too_many_arguments)] // the receipt's own field list.
#[cfg_attr(not(feature = "std"), allow(unused_variables))]
pub(super) fn failed_xchain_receipt(
    reason: SkipReason,
    detail: &str,
    tx_position: BPosition,
    origin_chain_id: u64,
    message: &XChainMessage,
    block_number: u64,
    tx_index_in_block: u64,
    cumulative_gas_used_before: u64,
) -> (Receipt, WriteSet) {
    #[cfg(feature = "std")]
    {
        tracing::error!(
            source_hash = ?message.source_hash,
            origin_chain_id,
            seq = message.seq,
            block = block_number,
            reason = reason.as_str(),
            detail,
            "INVALID xchain delivery FAILED (deterministic; the origin bounds were bypassed — investigate)"
        );
        crate::metrics::record_invalid_tx_skipped(reason);
    }
    let ws = WriteSet::default();
    let write_set_hash = ws.hash();
    let receipt = Receipt {
        tx_idx: tx_position,
        tx_hash: message.source_hash,
        tx_type: kardamom_types::TX_TYPE_XCHAIN,
        status: false,
        gas_used: 0,
        logs: Vec::new(),
        write_set_hash,
        nonce: 0,
        from: xchain::xchain_tx_sender(origin_chain_id),
        to: Some(xchain::INBOX),
        contract_address: None,
        effective_gas_price: 0,
        block_number,
        transaction_index: tx_index_in_block,
        cumulative_gas_used: cumulative_gas_used_before,
        skip_reason: Some(reason),
    };
    (receipt, ws)
}

/// The value pre-check, shared by both delivery paths. v1 carries no
/// value. A nonzero `message.value` cannot come from an honest origin (the
/// Outbox and [`xchain::derive_remote_epoch`] both reject it). It becomes
/// a failed receipt, never an engine error: one fabricated field must not
/// halt every replica.
pub(super) fn xchain_value_rejection(
    origin_chain_id: u64,
    message: &XChainMessage,
) -> Option<(SkipReason, String)> {
    (message.value != 0).then(|| {
        (
            SkipReason::OtherTransaction,
            format!(
                "xchain message (origin {origin_chain_id}, seq {}) carries value {} but v1 \
                 delivery is value-free",
                message.seq, message.value
            ),
        )
    })
}

/// Execute one derived cross-chain message. It runs against a snapshot
/// and the current `PendingDelta`. It returns the receipt, plus a fresh
/// per-tx `WriteSet`.
///
/// This uses the deposit shape ([`super::execute_deposit_tx`]), minus the
/// mint: the call is fee-free (`gas_price = 0`), the nonce check is off,
/// and the receipt's `tx_hash` is the message's `source_hash`. The EVM
/// caller is the aliased origin Outbox ([`xchain::xchain_tx_sender`]) —
/// the only address `Inbox.deliver` accepts — and the callee is always
/// the Inbox predeploy. The call reaches `target` only through the
/// Inbox's own inner call.
///
/// A message that revm rejects at validation, or that carries a nonzero
/// value, becomes a deterministic failed receipt
/// (`failed_xchain_receipt`). Only local failures (database errors) are
/// engine errors.
#[allow(clippy::too_many_arguments)] // matches execute_tx's shape; see the
// equivalent allow on execute_tx for the rationale.
pub fn execute_xchain_tx<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    delta: &PendingDelta,
    env: ExecEnv,
    tx_idx: TxIndex,
    tx_position: BPosition,
    origin_chain_id: u64,
    message: &XChainMessage,
    tx_index_in_block: u64,
    cumulative_gas_used_before: u64,
    // See `execute_deposit_tx`. Cross-chain claims are writes only,
    // through the same constructed-account path. This keeps executor and
    // validator claims symmetric.
    bal: Option<(&mut revm::state::bal::Bal, u64)>,
) -> Result<(Receipt, WriteSet), ExecutorError> {
    if let Some((reason, detail)) = xchain_value_rejection(origin_chain_id, message) {
        return Ok(failed_xchain_receipt(
            reason,
            &detail,
            tx_position,
            origin_chain_id,
            message,
            env.block_number,
            tx_index_in_block,
            cumulative_gas_used_before,
        ));
    }

    // Layer the running delta on top of the snapshot through CacheDB, so
    // revm sees writes from earlier txs in the same block. This mirrors
    // execute_tx.
    let snap_ref = SnapshotRef { inner: snapshot };
    let mut cache: CacheDB<SnapshotRef<'_, S>> = CacheDB::new(snap_ref);
    for layer in parent.into_iter().chain(core::iter::once(delta)) {
        seed_cache_layer(&mut cache, layer).map_err(|detail| ExecutorError::Execution {
            idx: tx_idx,
            detail,
        })?;
    }

    // Like deposits, the derived tx carries no nonce.
    let mut cfg = env.cfg_env();
    cfg.disable_nonce_check = true;

    let sender = xchain::xchain_tx_sender(origin_chain_id);
    let tx_env = tx_env_from_xchain(origin_chain_id, message);
    let mut evm = Context::mainnet()
        .with_db(&mut cache)
        .with_block(env.block_env())
        .with_cfg(cfg)
        .build_mainnet();
    let result = match evm.transact_commit(tx_env) {
        Ok(r) => r,
        Err(EVMError::Transaction(e)) => {
            return Ok(failed_xchain_receipt(
                skip_reason_of_tx(&e),
                &format!("{e:?}"),
                tx_position,
                origin_chain_id,
                message,
                env.block_number,
                tx_index_in_block,
                cumulative_gas_used_before,
            ));
        }
        Err(e) => return Err(derived_tx_rejection(e, tx_idx)),
    };

    let gas_used = result.gas().tx_gas_used();
    // A revert inside deliver (or the inner call bubbling one up) marks
    // the receipt failed, but it is not an engine error. This is the
    // deposit posture.
    let (status_success, logs) = match &result {
        ExecutionResult::Success { logs, .. } => (true, logs.clone()),
        ExecutionResult::Revert { .. } => (false, Vec::<Log>::new()),
        ExecutionResult::Halt { .. } => (false, Vec::<Log>::new()),
    };

    let ws = write_set_from_cache(&cache.cache);
    // Same discipline as deposits: only true changes survive, so this
    // capture is a pure function of execution, not of what the pipelined
    // commit happened to leave in the seeded layers.
    let ws = retain_changed(ws, snapshot, parent, delta, tx_idx)?;
    if let Some((bal, bal_index)) = bal {
        ws.record_into_bal(bal, bal_index);
    }

    let write_set_hash = ws.hash();
    let wire_logs: Vec<WireLog> = logs.iter().map(WireLog::from).collect();
    let cumulative_gas_used = cumulative_gas_used_before + gas_used;

    let receipt = Receipt {
        tx_idx: tx_position,
        // The canonical id stamped at derivation (source_hash), not a
        // 2718 keccak. Same posture as deposits.
        tx_hash: message.source_hash,
        tx_type: kardamom_types::TX_TYPE_XCHAIN,
        status: status_success,
        gas_used,
        logs: wire_logs,
        write_set_hash,
        // Origin-derived: consumes no local nonce. The filler `nonce: 0`
        // is never a real nonce; consumers branch on tx_type, not on it.
        nonce: 0,
        from: sender,
        to: Some(xchain::INBOX),
        contract_address: None,
        // Destination-side delivery pays no fee (quota-gated instead).
        effective_gas_price: 0,
        block_number: env.block_number,
        transaction_index: tx_index_in_block,
        cumulative_gas_used,
        skip_reason: None,
    };
    Ok((receipt, ws))
}

// -----------------------------------------------------------------
// Cross-chain (0x7D) execution tests. These are the delivery analogue of
// the deposit scenarios in `deposit.rs`.
// -----------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Executor;
    use crate::executor::test_support::{boundary, pos};
    use crate::state::MockStateDatabase;
    use alloy_primitives::Bytes as AlloyBytes;
    use alloy_primitives::{Address, B256, U256};
    use bytes::Bytes;
    use kardamom_types::xchain::{Callback, MAX_DATA_BYTES, MAX_MESSAGE_GAS};
    use revm::primitives::KECCAK_EMPTY;
    use revm::state::Bytecode;

    const ORIGIN_CHAIN: u64 = 412_346;

    fn xmsg(seq: u64, data: Bytes) -> XChainMessage {
        XChainMessage {
            source_hash: xchain::remote_source_hash(ORIGIN_CHAIN, seq),
            seq,
            origin_sender: Address::from([0x77u8; 20]),
            target: Address::from([0x88u8; 20]),
            value: 0,
            gas_limit: 200_000,
            input: data,
            callback: None,
        }
    }

    /// Run the free function with the common test arguments.
    fn run_free(
        snap: &MockStateDatabase,
        delta: &PendingDelta,
        m: &XChainMessage,
        cumulative_before: u64,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        execute_xchain_tx(
            snap,
            None,
            delta,
            ExecEnv::new(1, &boundary(1)),
            TxIndex(0),
            pos(0),
            ORIGIN_CHAIN,
            m,
            0,
            cumulative_before,
            bal,
        )
    }

    /// The runtime bytecode of one interop predeploy, read from the pinned
    /// dev-interop genesis. The deployer's drift test keeps that file in
    /// step with the Solidity artifacts.
    fn predeploy_code(addr: Address) -> Vec<u8> {
        const GENESIS: &str = include_str!("../../../../chains/dev-interop.toml");
        let text = GENESIS.to_ascii_lowercase();
        let needle = format!("address = \"{}\"", addr.to_string().to_ascii_lowercase());
        let at = text
            .find(&needle)
            .expect("predeploy address in dev-interop.toml");
        let rest = &text[at + needle.len()..];
        let start = rest.find("\"0x").expect("code hex") + 3;
        let end = start + rest[start..].find('"').expect("closing quote");
        alloy_primitives::hex::decode(&rest[start..end]).expect("valid hex")
    }

    #[test]
    fn xchain_delivery_produces_a_0x7d_receipt_keyed_by_source_hash() {
        let snap = MockStateDatabase::builder().build();
        let delta = PendingDelta::new();
        let m = xmsg(4, Bytes::from_static(&[0xCA, 0xFE]));

        let mut bal = revm::state::bal::Bal::new();
        let (receipt, ws) = run_free(&snap, &delta, &m, 33, Some((&mut bal, 1))).expect("execute");

        // Identity comes from the derivation, not from any local encoding.
        assert_eq!(receipt.tx_hash, m.source_hash);
        assert_eq!(receipt.tx_type, kardamom_types::TX_TYPE_XCHAIN);
        assert!(receipt.status);
        let sender = xchain::xchain_tx_sender(ORIGIN_CHAIN);
        assert_eq!(
            receipt.from, sender,
            "EVM caller is the aliased origin Outbox"
        );
        assert_eq!(
            receipt.to,
            Some(xchain::INBOX),
            "the derived tx only ever calls the Inbox"
        );
        assert_eq!(receipt.nonce, 0);
        assert_eq!(receipt.effective_gas_price, 0);
        assert_eq!(receipt.block_number, 1);
        assert_eq!(receipt.cumulative_gas_used, 33 + receipt.gas_used);
        assert_eq!(receipt.skip_reason, None);

        // The delivery bumps the caller's nonce. This is the minimal
        // state effect. It must appear in the WriteSet and in the BAL,
        // or the validator has nothing to check.
        assert_eq!(ws.account(&sender).expect("caller touched").0, 1);
        let alloy = bal.into_alloy_bal();
        assert!(
            alloy
                .iter()
                .any(|a| a.address == sender && !a.nonce_changes.is_empty()),
            "caller claim missing from BAL: {alloy:?}"
        );
    }

    #[test]
    fn xchain_nonzero_value_is_a_failed_receipt_not_an_error() {
        let snap = MockStateDatabase::builder().build();
        let delta = PendingDelta::new();
        let mut m = xmsg(0, Bytes::new());
        m.value = 5;

        let (receipt, ws) =
            run_free(&snap, &delta, &m, 40, None).expect("a failed receipt, not an error");
        assert!(receipt.is_invalid_skip());
        assert!(!receipt.status);
        assert_eq!(receipt.gas_used, 0);
        assert_eq!(receipt.cumulative_gas_used, 40);
        assert!(receipt.logs.is_empty());
        assert_eq!(receipt.skip_reason, Some(SkipReason::OtherTransaction));
        assert_eq!(receipt.tx_hash, m.source_hash);
        assert_eq!(ws, WriteSet::default(), "no state change");
        assert_eq!(receipt.write_set_hash, WriteSet::default().hash());
    }

    #[test]
    fn xchain_inner_revert_yields_failed_receipt_not_error() {
        // This revert bytecode at the INBOX stands in for a deliver()
        // call that reverts. The receipt records the failure, and the
        // engine keeps running. This is the deposit posture, minus the
        // mint.
        let revert_code = AlloyBytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]);
        let bytecode = Bytecode::new_raw(revert_code);
        let code_hash = bytecode.hash_slow();
        let snap = MockStateDatabase::builder()
            .account(xchain::INBOX, U256::ZERO, 1, code_hash)
            .code(
                code_hash,
                Bytes::copy_from_slice(bytecode.original_bytes().as_ref()),
            )
            .build();
        let delta = PendingDelta::new();

        let m = xmsg(9, Bytes::new());
        let (receipt, _ws) =
            run_free(&snap, &delta, &m, 0, None).expect("revert is OK at the executor layer");
        assert!(!receipt.status, "inner revert yields status=false");
        assert!(receipt.gas_used > 0, "a revert still charges intrinsic gas");
        assert!(!receipt.is_invalid_skip());
        assert_eq!(receipt.tx_hash, m.source_hash);
        assert_eq!(receipt.tx_type, kardamom_types::TX_TYPE_XCHAIN);
    }

    /// The budget formula, pinned byte by byte under the Osaka rules:
    /// 21 000 base, 4 per zero byte and 16 per nonzero byte standard;
    /// floor = 21 000 + 10 tokens, one token per zero byte and four per
    /// nonzero byte. The larger of the two is what revm demands.
    #[test]
    fn budget_covers_the_calldata_intrinsic_gas() {
        assert_eq!(xchain_gas_budget(0, &[]), XCHAIN_DELIVERY_OVERHEAD + 21_000);
        // One zero byte: standard 21 004, floor 21 010.
        assert_eq!(
            xchain_gas_budget(0, &[0]),
            XCHAIN_DELIVERY_OVERHEAD + 21_010
        );
        // One nonzero byte: standard 21 016, floor 21 040.
        assert_eq!(
            xchain_gas_budget(0, &[1]),
            XCHAIN_DELIVERY_OVERHEAD + 21_040
        );
        // The message budget adds linearly.
        assert_eq!(
            xchain_gas_budget(7, &[1]),
            XCHAIN_DELIVERY_OVERHEAD + 21_040 + 7
        );
        // Saturating at the top of the range, never wrapping.
        assert_eq!(xchain_gas_budget(u64::MAX, &[1]), u64::MAX);
    }

    /// The worst honest message stays under the EIP-7825 tx gas cap:
    /// `MAX_MESSAGE_GAS + XCHAIN_DELIVERY_OVERHEAD + intrinsic(deliver
    /// calldata for MAX_DATA_BYTES nonzero bytes) <= TX_GAS_LIMIT_CAP`.
    #[test]
    fn budget_fits_the_tx_gas_cap() {
        let mut m = xmsg(u64::MAX, Bytes::from(vec![0xFFu8; MAX_DATA_BYTES]));
        m.gas_limit = MAX_MESSAGE_GAS;
        m.origin_sender = Address::repeat_byte(0xFF);
        m.target = Address::repeat_byte(0xFF);
        m.callback = Some(Callback {
            target: Address::repeat_byte(0xFF),
            gas_limit: u64::MAX,
            context: B256::repeat_byte(0xFF),
        });
        let calldata = xchain::deliver_calldata(u64::MAX, &m);
        let budget = xchain_gas_budget(m.gas_limit, &calldata);
        assert!(
            budget <= revm::primitives::eip7825::TX_GAS_LIMIT_CAP,
            "budget {budget} is above the cap"
        );
        // And under the block gas limit, or the delivery could never seal.
        assert!(budget <= crate::block_env::BLOCK_GAS_LIMIT);
    }

    /// A large payload with a zero inner-call budget. The calldata floor
    /// alone is far above the old fixed overhead. The budget now covers
    /// it, so the delivery executes: `Ok`, and not a skip marker.
    #[test]
    fn max_payload_with_zero_gas_limit_delivers() {
        let snap = MockStateDatabase::builder().build();
        let delta = PendingDelta::new();
        for len in [MAX_DATA_BYTES, 3_200] {
            let mut m = xmsg(1, Bytes::from(vec![0xFFu8; len]));
            m.gas_limit = 0;
            let (receipt, _ws) = run_free(&snap, &delta, &m, 0, None)
                .unwrap_or_else(|e| panic!("{len} bytes: {e:?}"));
            assert!(
                !receipt.is_invalid_skip(),
                "{len} bytes: the delivery must execute, got {receipt:?}"
            );
            // No Inbox code in this snapshot: the call to an empty account
            // succeeds and charges the floor.
            assert!(receipt.status, "{len} bytes");
            assert!(
                receipt.gas_used >= 21_000 + 10 * 4 * len as u64,
                "{len} bytes"
            );
        }
    }

    /// A message revm rejects at validation (the tx gas limit above the
    /// EIP-7825 cap) becomes a failed receipt on both paths, with no state
    /// change. An honest origin cannot produce this: the derivation caps
    /// `gas_limit` at `MAX_MESSAGE_GAS`.
    #[test]
    fn xchain_validation_failure_is_a_failed_receipt_on_both_paths() {
        let snap = MockStateDatabase::builder().build();
        let delta = PendingDelta::new();
        let mut m = xmsg(2, Bytes::from_static(&[0xCA]));
        m.gas_limit = u64::MAX;

        let mut old_bal = revm::state::bal::Bal::new();
        let (receipt, ws) =
            run_free(&snap, &delta, &m, 55, Some((&mut old_bal, 1))).expect("failed receipt");
        assert!(receipt.is_invalid_skip());
        assert_eq!(receipt.gas_used, 0);
        assert_eq!(receipt.cumulative_gas_used, 55);
        assert_eq!(receipt.skip_reason, Some(SkipReason::GasLimit));
        assert!(receipt.logs.is_empty());
        assert_eq!(ws, WriteSet::default());

        let env = ExecEnv::new(1, &boundary(1));
        let mut scope = Executor::new(&snap, None, env).expect("scope");
        let mut new_bal = revm::state::bal::Bal::new();
        let (new_r, new_ws) = scope
            .execute_xchain(
                TxIndex(0),
                pos(0),
                ORIGIN_CHAIN,
                &m,
                0,
                55,
                Some((&mut new_bal, 1)),
            )
            .expect("failed receipt");
        assert_eq!(new_r, receipt);
        assert_eq!(new_ws, ws);
        assert!(old_bal.into_alloy_bal().is_empty());
        assert!(new_bal.into_alloy_bal().is_empty());
    }

    /// THE EQUIVALENCE GATE for the on-scope delivery path, the mirror of
    /// `old_and_new_deposit_paths_agree`: the free function (the streaming
    /// executor and the offline replay) and `Executor::execute_xchain`
    /// (the validator and the stateless driver) must produce
    /// byte-identical artifacts — receipt (`write_set_hash` included),
    /// WriteSet, and BAL claims — through the real Inbox and Outbox
    /// bytecode, across every shape that can diverge.
    #[test]
    fn old_and_new_xchain_paths_agree() {
        let inbox = Bytecode::new_raw(AlloyBytes::from(predeploy_code(xchain::INBOX)));
        let outbox = Bytecode::new_raw(AlloyBytes::from(predeploy_code(xchain::OUTBOX)));
        // Target A: SSTORE(0x01, 0x42) — a writing call.
        let write_code = AlloyBytes::from(vec![0x60, 0x42, 0x60, 0x01, 0x55, 0x00]);
        // Target B: SLOAD(0x01); POP; STOP — a reading call.
        let read_code = AlloyBytes::from(vec![0x60, 0x01, 0x54, 0x50, 0x00]);
        // Target C: REVERT(0,0).
        let revert_code = AlloyBytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]);
        let mk = |code: &AlloyBytes| {
            let bc = Bytecode::new_raw(code.clone());
            (bc.hash_slow(), bc)
        };
        let (wh, wb) = mk(&write_code);
        let (rh, rb) = mk(&read_code);
        let (vh, vb) = mk(&revert_code);
        let (a_write, a_read, a_rev) = (
            Address::from([0xA1u8; 20]),
            Address::from([0xA2u8; 20]),
            Address::from([0xA3u8; 20]),
        );
        let plain_to = Address::from([0x88u8; 20]);

        let code_bytes = |b: &Bytecode| Bytes::copy_from_slice(b.original_bytes().as_ref());
        let snap = MockStateDatabase::builder()
            .account(xchain::INBOX, U256::ZERO, 1, inbox.hash_slow())
            .code(inbox.hash_slow(), code_bytes(&inbox))
            .account(xchain::OUTBOX, U256::ZERO, 1, outbox.hash_slow())
            .code(outbox.hash_slow(), code_bytes(&outbox))
            .account(a_write, U256::ZERO, 1, wh)
            .code(wh, code_bytes(&wb))
            .account(a_read, U256::ZERO, 1, rh)
            .code(rh, code_bytes(&rb))
            .account(a_rev, U256::ZERO, 1, vh)
            .code(vh, code_bytes(&vb))
            .storage(a_read, B256::with_last_byte(1), U256::from(77u64))
            .build();
        // The local chain id must differ from the origin, or the Outbox
        // rejects the callback enqueue as a self-send.
        let env = ExecEnv::new(1, &boundary(1));

        let with_target = |seq: u64, target: Address| {
            let mut m = xmsg(seq, Bytes::from_static(&[0xCA, 0xFE]));
            m.target = target;
            m
        };
        let mut callback = with_target(5, plain_to);
        callback.callback = Some(Callback {
            target: Address::from([0xC1u8; 20]),
            gas_limit: 50_000,
            context: B256::repeat_byte(0x0C),
        });
        let mut over_cap = with_target(6, plain_to);
        over_cap.gas_limit = u64::MAX;
        let mut valued = with_target(7, plain_to);
        valued.value = 1;

        // (name, message, deliver it once before the measured run)
        let cases = [
            ("plain success", with_target(0, plain_to), false),
            ("target writes storage", with_target(1, a_write), false),
            ("target reads storage", with_target(2, a_read), false),
            ("target reverts", with_target(3, a_rev), false),
            ("duplicate delivery", with_target(4, plain_to), true),
            ("callback enqueue", callback, false),
            ("validation failure", over_cap, false),
            ("nonzero value", valued, false),
        ];

        // A prior write on the delta/scope, so the on-scope path runs over
        // a non-empty cache exactly like a mid-block delivery does.
        let mut base_prior = PendingDelta::new();
        {
            let mut ws0 = WriteSet::default();
            ws0.accounts.push((
                Address::from([0x77u8; 20]),
                (3, U256::from(9u64), KECCAK_EMPTY),
            ));
            ws0.finish();
            base_prior.apply(ws0);
        }

        for (name, m, deliver_first) in cases {
            let mut prior = base_prior.clone();
            let mut scope = Executor::new(&snap, None, env).expect("scope");
            scope.seed_layer(&prior).expect("seed");
            if deliver_first {
                let (r, ws) = run_free(&snap, &prior, &m, 0, None)
                    .unwrap_or_else(|e| panic!("{name}: first delivery failed: {e:?}"));
                assert!(r.status, "{name}: the first delivery succeeds");
                prior.apply(ws);
                let (r, _) = scope
                    .execute_xchain(TxIndex(0), pos(0), ORIGIN_CHAIN, &m, 0, 0, None)
                    .unwrap_or_else(|e| panic!("{name}: first delivery failed: {e:?}"));
                assert!(r.status, "{name}: the first delivery succeeds");
            }

            // OLD: fresh cache over snapshot ∘ prior.
            let mut old_bal = revm::state::bal::Bal::new();
            let (old_r, old_ws) = run_free(&snap, &prior, &m, 0, Some((&mut old_bal, 1)))
                .unwrap_or_else(|e| panic!("{name}: old path failed: {e:?}"));

            // NEW: on a scope seeded with the same prior layer.
            let mut new_bal = revm::state::bal::Bal::new();
            let (new_r, new_ws) = scope
                .execute_xchain(
                    TxIndex(0),
                    pos(0),
                    ORIGIN_CHAIN,
                    &m,
                    0,
                    0,
                    Some((&mut new_bal, 1)),
                )
                .unwrap_or_else(|e| panic!("{name}: new path failed: {e:?}"));

            assert_eq!(new_ws, old_ws, "{name}: WriteSet diverges");
            assert_eq!(new_r, old_r, "{name}: Receipt diverges");
            let (mut o, mut n) = (Vec::new(), Vec::new());
            use alloy_rlp::Encodable;
            old_bal.into_alloy_bal().encode(&mut o);
            new_bal.into_alloy_bal().encode(&mut n);
            assert_eq!(n, o, "{name}: BAL claims diverge");

            // Shape checks, so a silently degenerate case cannot pass.
            match name {
                "plain success" | "target reads storage" | "target reverts" => {
                    assert!(old_r.status, "{name}");
                    assert!(
                        old_ws.storage.iter().any(|((a, _), _)| *a == xchain::INBOX),
                        "{name}: Inbox bookkeeping"
                    );
                }
                "target writes storage" => {
                    assert!(old_r.status, "{name}");
                    assert!(
                        old_ws.storage.iter().any(|((a, _), _)| *a == a_write),
                        "{name}"
                    );
                }
                "duplicate delivery" => {
                    assert!(!old_r.status, "{name}: the Inbox rejects a repeat");
                    assert!(!old_r.is_invalid_skip(), "{name}: executed, not skipped");
                }
                "callback enqueue" => {
                    assert!(old_r.status, "{name}");
                    assert!(
                        old_ws
                            .storage
                            .iter()
                            .any(|((a, _), _)| *a == xchain::OUTBOX),
                        "{name}: Outbox enqueue"
                    );
                    assert!(
                        old_r.logs.iter().any(|l| l.address == xchain::OUTBOX),
                        "{name}: MessageSent"
                    );
                }
                "validation failure" | "nonzero value" => {
                    assert!(old_r.is_invalid_skip(), "{name}");
                    assert!(old_r.skip_reason.is_some(), "{name}");
                    assert_eq!(old_ws, WriteSet::default(), "{name}");
                }
                _ => unreachable!(),
            }
        }
    }
}
