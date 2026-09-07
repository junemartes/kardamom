//! Cross-chain message delivery ([`execute_xchain_tx`]): a fee-free inner
//! EVM call into the Inbox predeploy, with the message's `source_hash` as
//! the receipt's canonical id.

use kardamom_types::xchain::{self, XChainMessage};
use kardamom_types::{Receipt, StateDatabase};

use alloc::format;

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::{TxIndex, TxSlot};

use super::db::seed_composed_cache;
use super::derived::{DerivedCall, DerivedIdentity};
use super::write_set::{retain_changed, write_set_from_cache};

/// Extra gas for a cross-chain delivery, on top of the inner-call budget.
///
/// `message.gas_limit` pays only for the inner `target` call, and the
/// Inbox forwards exactly that amount. Intrinsic gas, plus the Inbox's own
/// work (a delivery-status write, an event, and a callback enqueue through
/// the local Outbox), needs gas too. Without this headroom, that work
/// would use up the app's budget.
pub(super) const XCHAIN_DELIVERY_OVERHEAD: u64 = 150_000;

/// One cross-chain message's origin and payload. `execute_xchain_tx`
/// also carries `snapshot`/`parent`/`delta` (the deposit path's fresh-
/// cache shape), so bundling `origin_chain_id` with `message` here keeps
/// it within the argument-count bound `TxSlot` alone does not clear for
/// this one entry point.
#[derive(Clone, Copy)]
pub struct XChainDelivery<'a> {
    pub origin_chain_id: u64,
    pub message: &'a XChainMessage,
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
/// v1 carries no value. A nonzero `message.value` is a chain fault, not a
/// dropped message: the wire format keeps the field so it stays stable
/// once value transfer ships, but minting before the anchored-tier value
/// rules exist would inflate supply. So this case fails the engine.
///
/// # Errors
///
/// Returns [`ExecutorError::Execution`] when `message.value` is nonzero,
/// the inner call fails non-deterministically, or a database read fails.
pub fn execute_xchain_tx<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    delta: &PendingDelta,
    env: ExecEnv,
    slot: TxSlot,
    delivery: XChainDelivery<'_>,
    // See `execute_deposit_tx`. Cross-chain claims are writes only,
    // through the same constructed-account path. This keeps executor and
    // validator claims symmetric.
    bal: Option<(&mut revm::state::bal::Bal, u64)>,
) -> Result<(Receipt, WriteSet), ExecutorError> {
    let origin_chain_id = delivery.origin_chain_id;
    let message = ValuelessMessage::new(slot.tx_idx, origin_chain_id, delivery.message)?;

    // Layer the running delta on top of the snapshot through CacheDB, so
    // revm sees writes from earlier txs in the same block. This mirrors
    // execute_tx.
    let mut cache = seed_composed_cache(snapshot, parent, delta, slot.tx_idx)?;
    let sender = xchain::xchain_tx_sender(origin_chain_id);
    let mut call = DerivedCall::new(&mut cache, &env, slot);
    let tx_env = super::tx_env::tx_env_from_xchain(origin_chain_id, &message);
    let outcome = call.transact_commit(tx_env)?;

    let ws = write_set_from_cache(&cache.cache);
    // Same discipline as deposits: only true changes survive, so this
    // capture is a pure function of execution, not of what the pipelined
    // commit happened to leave in the seeded layers.
    let ws = retain_changed(ws, snapshot, parent, delta, slot.tx_idx)?;
    if let Some((bal, bal_index)) = bal {
        ws.record_into_bal(bal, bal_index);
    }

    let receipt = DerivedCall::new(&mut cache, &env, slot).derived_receipt(
        DerivedIdentity {
            tx_hash: message.source_hash,
            tx_type: kardamom_types::TX_TYPE_XCHAIN,
            from: sender,
            to: Some(xchain::INBOX),
        },
        &outcome,
        ws.hash(),
    )?;
    Ok((receipt, ws))
}

/// A cross-chain message proven value-free and gas-bounded, at the point
/// where it enters execution. v1 carries no value: a nonzero
/// `message.value` is a chain fault, not a droppable message. The wire
/// format keeps the field so it stays stable once value transfer ships,
/// but minting before the anchored-tier value rules exist would inflate
/// supply. `gas_limit` is bounded to the block gas limit here too, so
/// [`tx_env_from_xchain`](super::tx_env::tx_env_from_xchain)'s
/// `+ XCHAIN_DELIVERY_OVERHEAD` can be a plain add: an untrusted wire
/// value near `u64::MAX` never reaches it. Every entry point
/// ([`execute_xchain_tx`] and [`super::scope::Executor::execute_xchain`])
/// derives one of these before touching the cache, so downstream code
/// ([`DerivedCall::transact_commit`], [`DerivedCall::derived_receipt`])
/// never re-checks either bound.
pub(super) struct ValuelessMessage<'a>(&'a XChainMessage);

impl<'a> ValuelessMessage<'a> {
    pub(super) fn new(
        tx_idx: TxIndex,
        origin_chain_id: u64,
        message: &'a XChainMessage,
    ) -> Result<Self, ExecutorError> {
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
        if message.gas_limit > crate::block_env::BLOCK_GAS_LIMIT {
            return Err(ExecutorError::Execution {
                idx: tx_idx,
                detail: format!(
                    "xchain message (origin {origin_chain_id}, seq {}) claims gas_limit {} \
                     above the block gas limit {} — chain fault, not droppable",
                    message.seq,
                    message.gas_limit,
                    crate::block_env::BLOCK_GAS_LIMIT
                ),
            });
        }
        Ok(Self(message))
    }
}

impl core::ops::Deref for ValuelessMessage<'_> {
    type Target = XChainMessage;

    fn deref(&self) -> &XChainMessage {
        self.0
    }
}

// -----------------------------------------------------------------
// Cross-chain (0x7D) execution tests. These are the delivery analogue of
// the deposit scenarios in `deposit.rs`.
// -----------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::{boundary, slot};
    use crate::state::MockStateDatabase;
    use alloy_primitives::{Address, Bytes as AlloyBytes, U256};
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
            slot(0, 0, 0, 33),
            XChainDelivery {
                origin_chain_id: ORIGIN_CHAIN,
                message: &m,
            },
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
            slot(0, 0, 0, 0),
            XChainDelivery {
                origin_chain_id: ORIGIN_CHAIN,
                message: &m,
            },
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
            slot(0, 0, 0, 0),
            XChainDelivery {
                origin_chain_id: ORIGIN_CHAIN,
                message: &m,
            },
            None,
        )
        .expect("revert is OK at the executor layer");
        assert!(!receipt.status, "inner revert yields status=false");
        assert_eq!(receipt.tx_hash, m.source_hash);
        assert_eq!(receipt.tx_type, kardamom_types::TX_TYPE_XCHAIN);
    }
}
