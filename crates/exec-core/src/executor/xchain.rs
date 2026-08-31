//! Cross-chain message delivery ([`execute_xchain_tx`]): a fee-free inner
//! EVM call into the Inbox predeploy, with the message's `source_hash` as
//! the receipt's canonical id.

use alloy_primitives::Bytes as AlloyBytes;
use alloy_primitives::U256;
use kardamom_types::xchain::{self, XChainMessage};
use kardamom_types::{BPosition, Receipt, StateDatabase, WireLog};
use revm::context::TxEnv;
use revm::context::result::ExecutionResult;
use revm::database::CacheDB;
use revm::primitives::{Log, TxKind};
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

use alloc::format;
use alloc::vec::Vec;

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;

use super::db::{SnapshotRef, seed_cache_layer};
use super::write_set::write_set_from_cache;

/// Extra gas for a cross-chain delivery, on top of the inner-call budget.
///
/// `message.gas_limit` pays only for the inner `target` call, and the
/// Inbox forwards exactly that amount. Intrinsic gas, plus the Inbox's own
/// work (a delivery-status write, an event, and a callback enqueue through
/// the local Outbox), needs gas too. Without this headroom, that work
/// would use up the app's budget.
pub const XCHAIN_DELIVERY_OVERHEAD: u64 = 150_000;

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
/// v1 carries no value. A nonzero `message.value` is a chain fault, not a
/// dropped message: the wire format keeps the field so it stays stable
/// once value transfer ships, but minting before the anchored-tier value
/// rules exist would inflate supply. So this case fails the engine.
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
    if message.value != 0 {
        return Err(ExecutorError::Execution {
            idx: tx_idx,
            detail: format!(
                "xchain message (origin {origin_chain_id}, seq {}) carries value {} but v1 \
                 delivery is value-free — chain fault, not droppable",
                message.seq, message.value
            ),
        });
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
    let tx_env = TxEnv {
        caller: sender,
        kind: TxKind::Call(xchain::INBOX),
        value: U256::ZERO,
        data: AlloyBytes::from(xchain::deliver_calldata(origin_chain_id, message)),
        gas_limit: message.gas_limit.saturating_add(XCHAIN_DELIVERY_OVERHEAD),
        gas_price: 0,
        nonce: 0,
        chain_id: None,
        ..Default::default()
    };
    let mut evm = Context::mainnet()
        .with_db(&mut cache)
        .with_block(env.block_env())
        .with_cfg(cfg)
        .build_mainnet();
    let result = evm
        .transact_commit(tx_env)
        .map_err(|e| ExecutorError::Execution {
            idx: tx_idx,
            detail: format!("{e:?}"),
        })?;

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
    use crate::executor::test_support::{boundary, pos};
    use crate::state::MockStateDatabase;
    use alloy_primitives::Address;
    use bytes::Bytes;
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

    #[test]
    fn xchain_delivery_produces_a_0x7d_receipt_keyed_by_source_hash() {
        let snap = MockStateDatabase::builder().build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));
        let m = xmsg(4, Bytes::from_static(&[0xCA, 0xFE]));

        let mut bal = revm::state::bal::Bal::new();
        let (receipt, ws) = execute_xchain_tx(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            ORIGIN_CHAIN,
            &m,
            0,
            33,
            Some((&mut bal, 1)),
        )
        .expect("execute");

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
    fn xchain_nonzero_value_is_a_chain_fault_not_a_drop() {
        let snap = MockStateDatabase::builder().build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));
        let mut m = xmsg(0, Bytes::new());
        m.value = 5;

        let err = execute_xchain_tx(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            ORIGIN_CHAIN,
            &m,
            0,
            0,
            None,
        )
        .unwrap_err();
        assert!(
            matches!(err, ExecutorError::Execution { ref detail, .. } if detail.contains("value")),
            "got {err:?}"
        );
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
        let env = ExecEnv::new(1, &boundary(1));

        let m = xmsg(9, Bytes::new());
        let (receipt, _ws) = execute_xchain_tx(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            ORIGIN_CHAIN,
            &m,
            0,
            0,
            None,
        )
        .expect("revert is OK at the executor layer");
        assert!(!receipt.status, "inner revert yields status=false");
        assert_eq!(receipt.tx_hash, m.source_hash);
        assert_eq!(receipt.tx_type, kardamom_types::TX_TYPE_XCHAIN);
    }
}
