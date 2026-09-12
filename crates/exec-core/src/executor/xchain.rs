//! Cross-chain message delivery ([`execute_xchain_tx`]): a fee-free inner
//! EVM call into the Inbox predeploy, with the message's `source_hash` as
//! the receipt's canonical id.

use kardamom_types::xchain::{self, XChainMessage};
use kardamom_types::{Receipt, SkipReason, StateDatabase};

use alloc::format;

use crate::block_env::{ExecEnv, SPEC_ID};
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::TxSlot;

use super::db::seed_composed_cache;
use super::derived::{CallOutcome, DerivedCall, DerivedOutcome};
use super::scope::Executor;
use super::skip::{DerivedTxIdentity, Rejection, SkipContext};
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
///
/// This stays a plain function, not an `Executor<S>` method: its only
/// caller, `tx_env_from_xchain`, builds a `TxEnv` before any `Executor`
/// or snapshot type is in scope.
#[must_use]
pub fn xchain_gas_budget(message_gas_limit: u64, calldata: &[u8]) -> u64 {
    let gas = revm::context_interface::cfg::gas::calculate_initial_tx_gas(
        SPEC_ID, calldata, false, 0, 0, 0,
    );
    let intrinsic = gas.initial_total_gas.max(gas.floor_gas);
    message_gas_limit
        .saturating_add(XCHAIN_DELIVERY_OVERHEAD)
        .saturating_add(intrinsic)
}

impl<S: StateDatabase> Executor<S> {
    /// The deterministic failed receipt for a delivery that revm rejects
    /// at validation, or that fails the value pre-check. Both delivery
    /// paths build it here, so the artifact is identical by
    /// construction.
    ///
    /// The state it leaves: no state change (an empty write set),
    /// `status = false`, `gas_used = 0`, no logs, and `skip_reason =
    /// Some(reason)`. The pair `status = false, gas_used = 0` is the
    /// wire-visible skip marker ([`Receipt::is_invalid_skip`]). It is
    /// loud by design: an honest origin can never produce this case,
    /// because [`xchain::derive_remote_epoch`] mirrors the Outbox
    /// bounds.
    pub(super) fn failed_xchain_receipt(
        reason: SkipReason,
        detail: &str,
        ctx: SkipContext,
        origin_chain_id: u64,
        message: &XChainMessage,
    ) -> (Receipt, WriteSet) {
        super::skip::log_invalid(
            reason,
            detail,
            ctx.block_number,
            super::skip::InvalidTx::XChain {
                message,
                origin_chain_id,
            },
        );
        Self::failed_derived_receipt(
            DerivedTxIdentity {
                tx_hash: message.source_hash,
                tx_type: kardamom_types::TX_TYPE_XCHAIN,
                from: xchain::xchain_tx_sender(origin_chain_id),
                to: Some(xchain::INBOX),
                nonce: 0,
            },
            ctx,
            WriteSet::default(),
            None,
            reason,
        )
    }

    /// The value pre-check, shared by both delivery paths. v1 carries no
    /// value. A nonzero `message.value` cannot come from an honest origin
    /// (the Outbox and [`xchain::derive_remote_epoch`] both reject it).
    /// It becomes a failed receipt, never an engine error: one
    /// fabricated field must not halt every replica.
    pub(super) fn xchain_value_rejection(
        origin_chain_id: u64,
        message: &XChainMessage,
    ) -> Result<(), Rejection> {
        if message.value == 0 {
            return Ok(());
        }
        Err(Rejection {
            reason: SkipReason::OtherTransaction,
            detail: format!(
                "xchain message (origin {origin_chain_id}, seq {}) carries value {} but v1 \
                 delivery is value-free",
                message.seq, message.value
            ),
        })
    }
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
/// A message that revm rejects at validation (for example a `gas_limit`
/// above the EIP-7825 cap), or that carries a nonzero value, becomes a
/// deterministic failed receipt (`failed_xchain_receipt`). Only local
/// failures (database errors) are engine errors.
///
/// # Errors
///
/// Returns [`ExecutorError::Execution`] when the inner call fails
/// non-deterministically or a database read fails.
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
    if let Err(Rejection { reason, detail }) =
        Executor::<S>::xchain_value_rejection(origin_chain_id, delivery.message)
    {
        return Ok(Executor::<S>::failed_xchain_receipt(
            reason,
            &detail,
            SkipContext::new(slot, env.block_number),
            origin_chain_id,
            delivery.message,
        ));
    }
    let message = ValuelessMessage::new(delivery.message);

    // Layer the running delta on top of the snapshot through CacheDB, so
    // revm sees writes from earlier txs in the same block. This mirrors
    // execute_tx.
    let mut cache = seed_composed_cache(snapshot, parent, delta, slot.tx_idx)?;
    let sender = xchain::xchain_tx_sender(origin_chain_id);
    let mut call = DerivedCall::new(&mut cache, &env, slot);
    let tx_env = tx_env_from_xchain(origin_chain_id, &message);
    let outcome = match call.transact_or_skip(tx_env)? {
        DerivedOutcome::Ran(o) => o,
        // Deterministic input invalidity: never an engine error.
        DerivedOutcome::Rejected(Rejection { reason, detail }) => {
            return Ok(Executor::<S>::failed_xchain_receipt(
                reason,
                &detail,
                SkipContext::new(slot, env.block_number),
                origin_chain_id,
                &message,
            ));
        }
    };
    let call_outcome = CallOutcome::from(&outcome.result);
    revm::DatabaseCommit::commit(&mut cache, outcome.state);

    let ws = write_set_from_cache(&cache.cache);
    // Same discipline as deposits: only true changes survive, so this
    // capture is a pure function of execution, not of what the pipelined
    // commit happened to leave in the seeded layers.
    let ws = retain_changed(ws, snapshot, parent, delta, slot.tx_idx)?;
    if let Some((bal, bal_index)) = bal {
        ws.record_into_bal(bal, bal_index);
    }

    let receipt = DerivedCall::new(&mut cache, &env, slot).derived_receipt(
        DerivedTxIdentity {
            tx_hash: message.source_hash,
            tx_type: kardamom_types::TX_TYPE_XCHAIN,
            from: sender,
            to: Some(xchain::INBOX),
            nonce: 0,
        },
        &call_outcome,
        ws.hash(),
    )?;
    Ok((receipt, ws))
}

/// A cross-chain message proven value-free, at the point where it enters
/// execution. v1 carries no value: a nonzero `message.value` is a chain
/// fault, not a droppable message. The wire format keeps the field so it
/// stays stable once value transfer ships, but minting before the
/// anchored-tier value rules exist would inflate supply. Every entry
/// point ([`execute_xchain_tx`] and
/// [`super::scope_derived::Executor::execute_xchain`]) checks
/// `message.value` through `xchain_value_rejection` before building one
/// of these, so the constructor itself never needs to re-check it.
///
/// An over-cap `gas_limit` is not checked here: [`xchain_gas_budget`]
/// saturates rather than overflows, so the untrusted wire value reaches
/// revm's own validation unharmed, and a cap violation there becomes a
/// deterministic failed receipt (`SkipReason::GasLimit`) like any other
/// validation rejection.
pub(super) struct ValuelessMessage<'a>(&'a XChainMessage);

impl<'a> ValuelessMessage<'a> {
    pub(super) fn new(message: &'a XChainMessage) -> Self {
        Self(message)
    }
}

impl core::ops::Deref for ValuelessMessage<'_> {
    type Target = XChainMessage;

    fn deref(&self) -> &XChainMessage {
        self.0
    }
}

#[cfg(test)]
#[path = "xchain_tests.rs"]
mod tests;
