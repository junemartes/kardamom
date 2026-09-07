//! The deterministic skip path: classifying a revm rejection into a
//! `SkipReason`, and building the skip receipt. These share no EVM
//! state with the execution scope.

use alloy_primitives::Address;
use kardamom_types::{Receipt, SkipReason, StateDatabase, TxEnvelope};

use alloc::vec::Vec;

use crate::delta::WriteSet;
use crate::exec_types::TxSlot;

use super::scope::Executor;

/// Classify a revm tx-validation rejection into the wire [`SkipReason`].
/// This is part of the state transition: every replica maps the same
/// rejection to the same reason. This function stands in for
/// `From<&InvalidTransaction>`, which the orphan rule blocks here (both
/// types are foreign to this crate).
#[must_use]
pub fn skip_reason_of_tx(err: &revm::context::result::InvalidTransaction) -> SkipReason {
    use revm::context::result::InvalidTransaction as E;
    match err {
        E::NonceTooLow { .. } => SkipReason::NonceTooLow,
        E::NonceTooHigh { .. } => SkipReason::NonceTooHigh,
        E::LackOfFundForMaxFee { .. } | E::OverflowPaymentInTransaction => {
            SkipReason::InsufficientFunds
        }
        E::CallerGasLimitMoreThanBlock
        | E::CallGasCostMoreThanGasLimit { .. }
        | E::GasFloorMoreThanGasLimit { .. }
        | E::TxGasLimitGreaterThanCap { .. } => SkipReason::GasLimit,
        E::PriorityFeeGreaterThanMaxFee
        | E::GasPriceLessThanBasefee
        | E::BlobGasPriceGreaterThanMax { .. } => SkipReason::Fee,
        E::CreateInitCodeSizeLimit => SkipReason::InitCodeSize,
        E::RejectCallerWithCode => SkipReason::SenderHasCode,
        _ => SkipReason::OtherTransaction,
    }
}

/// The deterministic skip receipt, as an associated constructor. It
/// needs no state access. The Block-STM engine builds skip receipts
/// inside its own worker EVM, and both paths must keep one definition
/// of the skip artifact.
impl<S: StateDatabase> Executor<S> {
    /// Build the deterministic skip receipt for a canonical record that is
    /// invalid at execution: `status=false, gas_used=0` (the wire-visible
    /// marker, since real execution always charges intrinsic gas), empty
    /// logs, an empty write set (`WriteSet::default().hash()` on both the
    /// live and re-exec sides), and unchanged gas accounting. This is loud
    /// by design: a log plus a counter. A skip existing at all means an
    /// upstream guard (sequencer nonce fence, cluster dedup, or resync
    /// floor) let an invalid record reach the canonical log.
    #[cfg_attr(not(feature = "std"), allow(unused_variables))]
    pub fn skip_receipt(
        reason: SkipReason,
        detail: &str,
        slot: TxSlot,
        inbound_envelope: &TxEnvelope,
        nonce: u64,
        to: Option<Address>,
        block_number: u64,
    ) -> (Receipt, WriteSet) {
        // Loudness is a `std`-side concern. The skip receipt itself is
        // the consensus artifact, and guest builds produce it
        // identically. `detail` keeps the full revm rejection next to
        // the coarse enum.
        #[cfg(feature = "std")]
        {
            tracing::error!(
                tx_hash = ?inbound_envelope.tx_hash,
                from = ?inbound_envelope.sender,
                nonce,
                block = block_number,
                reason = reason.as_str(),
                detail,
                "INVALID canonical tx SKIPPED (deterministic; upstream guard failed — investigate)"
            );
            crate::metrics::record_invalid_tx_skipped(reason);
        }
        let ws = WriteSet::default();
        let write_set_hash = ws.hash();
        let receipt = Receipt {
            tx_idx: slot.tx_position,
            tx_hash: inbound_envelope.tx_hash,
            tx_type: kardamom_types::tx_type_of(&inbound_envelope.raw_tx),
            status: false,
            gas_used: 0,
            logs: Vec::new(),
            write_set_hash,
            nonce,
            from: inbound_envelope.sender,
            to,
            contract_address: None,
            effective_gas_price: 0,
            block_number,
            transaction_index: slot.tx_index_in_block,
            cumulative_gas_used: slot.cumulative_gas_used_before,
            skip_reason: Some(reason),
        };
        (receipt, ws)
    }
}
