//! The deterministic skip path: classifying a revm rejection into a
//! `SkipReason`, and building the skip receipt. These share no EVM
//! state with the execution scope.

use alloy_primitives::{Address, B256};
use kardamom_types::xchain::XChainMessage;
use kardamom_types::{BPosition, Deposit, Receipt, SkipReason, StateDatabase, TxEnvelope};

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use revm::context::result::{EVMError, InvalidTransaction};
use revm::state::EvmState;

use crate::delta::WriteSet;
use crate::error::ExecutorError;
use crate::exec_types::TxSlot;

use super::burn::NonceBurn;
use super::scope::Executor;

/// A transaction's receipt identity: the fields a [`Receipt`] states
/// about the transaction that a skip or a derived-tx path (a deposit or
/// a cross-chain message) reads directly, with no 2718-encoded envelope
/// to supply them. A canonical-record skip carries the envelope's real
/// nonce; a derived tx carries no nonce, so its constructors pass 0.
#[derive(Clone, Copy)]
pub(super) struct DerivedTxIdentity {
    pub(super) tx_hash: B256,
    pub(super) tx_type: u8,
    pub(super) from: Address,
    pub(super) to: Option<Address>,
    pub(super) nonce: u64,
}

/// Where a receipt sits in the block and the canonical stream. Shared by
/// every skip and failed-receipt constructor.
#[derive(Clone, Copy)]
pub(super) struct SkipContext {
    pub(super) tx_position: BPosition,
    pub(super) block_number: u64,
    pub(super) tx_index_in_block: u64,
    pub(super) cumulative_gas_used_before: u64,
}

impl SkipContext {
    /// Build a context from one execution slot and the block it runs in.
    /// `TxSlot` carries every field this needs except `block_number`,
    /// which lives on the block env instead.
    pub(super) fn new(slot: TxSlot, block_number: u64) -> Self {
        Self {
            tx_position: slot.tx_position,
            block_number,
            tx_index_in_block: slot.tx_index_in_block,
            cumulative_gas_used_before: slot.cumulative_gas_used_before,
        }
    }
}

/// A rejected cross-chain message, pre-revm: the deterministic reason and
/// a human-readable detail for the error log. See
/// [`super::xchain::Executor::xchain_value_rejection`].
pub(super) struct Rejection {
    pub(super) reason: SkipReason,
    pub(super) detail: String,
}

/// The fields one invalid-tx log line needs, beyond the reason and the
/// detail every kind shares. The log text and the extra fields differ
/// by kind, so each site names its own variant instead of writing out
/// its own `tracing::error!` call.
#[derive(Clone, Copy)]
#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "the no_std build of log_invalid ignores every field"
    )
)]
pub(super) enum InvalidTx<'a> {
    /// A canonical record an upstream guard let through, but that fails
    /// execution.
    CanonicalSkip {
        inbound_envelope: &'a TxEnvelope,
        nonce: u64,
    },
    /// A deposit whose inner call revm rejects at validation.
    Deposit { deposit: &'a Deposit },
    /// A cross-chain delivery that revm rejects at validation, or that
    /// fails the value pre-check.
    XChain {
        message: &'a XChainMessage,
        origin_chain_id: u64,
    },
}

/// Log one invalid-tx skip or derived-tx failure, and count it. This is
/// loud by design: a log plus a counter. Every kind is a deterministic,
/// input-derived rejection; an honest producer never triggers one, so
/// each occurrence is worth an operator's attention.
///
/// Loudness is a `std`-side concern. The receipt itself is the
/// consensus artifact, and a `no_std` guest build produces it
/// identically with no log and no counter.
#[cfg(feature = "std")]
pub(super) fn log_invalid(reason: SkipReason, detail: &str, block_number: u64, tx: InvalidTx<'_>) {
    match tx {
        InvalidTx::CanonicalSkip {
            inbound_envelope,
            nonce,
        } => {
            tracing::error!(
                tx_hash = ?inbound_envelope.tx_hash,
                from = ?inbound_envelope.sender,
                nonce,
                block = block_number,
                reason = reason.as_str(),
                detail,
                "INVALID canonical tx SKIPPED (deterministic; upstream guard failed — investigate)"
            );
        }
        InvalidTx::Deposit { deposit } => {
            tracing::error!(
                source_hash = ?deposit.source_hash,
                from = ?deposit.from,
                gas_limit = deposit.gas_limit,
                block = block_number,
                reason = reason.as_str(),
                detail,
                "INVALID deposit FAILED (deterministic; the mint stays, the inner call never ran)"
            );
        }
        InvalidTx::XChain {
            message,
            origin_chain_id,
        } => {
            tracing::error!(
                source_hash = ?message.source_hash,
                origin_chain_id,
                seq = message.seq,
                block = block_number,
                reason = reason.as_str(),
                detail,
                "INVALID xchain delivery FAILED (deterministic; the origin bounds were bypassed — investigate)"
            );
        }
    }
    crate::metrics::record_invalid_tx_skipped(reason);
}

/// A `no_std` build produces the deterministic artifact with no log and
/// no counter. Every parameter still names its purpose, so a caller
/// reads the same signature either way.
#[cfg(not(feature = "std"))]
pub(super) fn log_invalid(
    _reason: SkipReason,
    _detail: &str,
    _block_number: u64,
    _tx: InvalidTx<'_>,
) {
}

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

/// A canonical record that execution skips: its block slot, its envelope,
/// and the block it runs in. [`Self::undecodable`] and [`Self::rejected`]
/// build its [`Skip`].
#[derive(Clone, Copy)]
pub struct SkippedRecord<'a> {
    pub slot: TxSlot,
    pub inbound_envelope: &'a TxEnvelope,
    pub block_number: u64,
}

impl<'a> SkippedRecord<'a> {
    /// The skip of a record whose bytes do not decode. It carries no
    /// nonce, so it uses none.
    #[must_use]
    pub fn undecodable(self, detail: String) -> Skip<'a> {
        Skip {
            reason: SkipReason::Undecodable,
            detail,
            burnable: false,
            record: self,
            nonce: 0,
            to: None,
        }
    }

    /// The skip of a decoded transaction at `nonce` to `to` that
    /// `transact` refused with `e`. A validation rejection is
    /// deterministic: every replica computes the same one from the same
    /// state and transaction, so the record skips.
    ///
    /// The skip can use its nonce only when the rejection comes from the
    /// transaction against the chain state, and leaves the chain nonce of
    /// the sender possibly at `nonce`. A reason that cannot use a nonce
    /// ([`SkipReason::can_use_nonce`]) and a chain-id rejection (a
    /// transaction for another chain) never do.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] for a database or a custom
    /// failure. It is local, not derivable from the input, so execution
    /// halts, and crash recovery replays cleanly.
    pub fn rejected<E: core::fmt::Debug>(
        self,
        e: EVMError<E>,
        nonce: u64,
        to: Option<Address>,
    ) -> Result<Skip<'a>, ExecutorError> {
        let (reason, detail, other_chain) = match e {
            EVMError::Transaction(e) => (
                skip_reason_of_tx(&e),
                format!("{e:?}"),
                matches!(
                    e,
                    InvalidTransaction::InvalidChainId | InvalidTransaction::MissingChainId
                ),
            ),
            EVMError::Header(e) => (SkipReason::Header, format!("{e:?}"), false),
            e => {
                return Err(ExecutorError::Execution {
                    idx: self.slot.tx_idx,
                    detail: format!("{e:?}"),
                });
            }
        };
        Ok(Skip {
            reason,
            detail,
            burnable: reason.can_use_nonce() && !other_chain,
            record: self,
            nonce,
            to,
        })
    }
}

/// A skipped canonical record: what its skip receipt states, what its
/// error log names, and whether its rejection can use the nonce.
pub struct Skip<'a> {
    reason: SkipReason,
    detail: String,
    burnable: bool,
    record: SkippedRecord<'a>,
    /// The nonce of the transaction, 0 for undecodable bytes.
    nonce: u64,
    to: Option<Address>,
}

impl Skip<'_> {
    /// The state change of the nonce burn of this skip, read through `db`
    /// at the block timestamp `timestamp_ms`. `None` when the skip uses
    /// no nonce. See [`NonceBurn`].
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::State`] when a read of `db` fails.
    pub fn burn_state<DB: revm::Database>(
        &self,
        db: &mut DB,
        timestamp_ms: u64,
    ) -> Result<Option<EvmState>, ExecutorError>
    where
        DB::Error: core::fmt::Debug,
    {
        if !self.burnable {
            return Ok(None);
        }
        NonceBurn {
            sender: self.record.inbound_envelope.sender,
            nonce: self.nonce,
        }
        .state(db, timestamp_ms)
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
    /// logs, and unchanged gas accounting. `ws` is the empty write set,
    /// or the nonce burn of the sender ([`NonceBurn::state`]). Its hash is
    /// the receipt's `write_set_hash` on the live and the re-exec sides.
    /// This is loud by design: a log plus a counter. A skip existing at
    /// all means an upstream guard (sequencer nonce fence, cluster dedup,
    /// or resync floor) let an invalid record reach the canonical log.
    pub fn skip_receipt(skip: Skip<'_>, ws: WriteSet) -> (Receipt, WriteSet) {
        let Skip {
            reason,
            detail,
            record:
                SkippedRecord {
                    slot,
                    inbound_envelope,
                    block_number,
                },
            nonce,
            to,
            ..
        } = skip;
        log_invalid(
            reason,
            &detail,
            block_number,
            InvalidTx::CanonicalSkip {
                inbound_envelope,
                nonce,
            },
        );
        Self::failed_derived_receipt(
            DerivedTxIdentity {
                tx_hash: inbound_envelope.tx_hash,
                tx_type: kardamom_types::tx_type_of(&inbound_envelope.raw_tx),
                from: inbound_envelope.sender,
                to,
                nonce,
            },
            SkipContext::new(slot, block_number),
            ws,
            None,
            reason,
        )
    }

    /// The deterministic failed receipt for a derived transaction (a
    /// deposit or a cross-chain delivery) that revm rejects at
    /// validation, or that fails a pre-check before it reaches revm.
    /// `ws` carries whatever state survives the rejection: a deposit's
    /// mint pre-credit, or an empty set for a cross-chain delivery.
    /// `identity.nonce` is 0 for a deposit or a cross-chain delivery
    /// (neither carries one) and the real envelope nonce for a skipped
    /// canonical record.
    pub(super) fn failed_derived_receipt(
        identity: DerivedTxIdentity,
        ctx: SkipContext,
        ws: WriteSet,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
        reason: SkipReason,
    ) -> (Receipt, WriteSet) {
        if let Some((bal, bal_index)) = bal {
            ws.record_into_bal(bal, bal_index);
        }
        let write_set_hash = ws.hash();
        let receipt = Receipt {
            tx_idx: ctx.tx_position,
            tx_hash: identity.tx_hash,
            tx_type: identity.tx_type,
            status: false,
            gas_used: 0,
            logs: Vec::new(),
            write_set_hash,
            nonce: identity.nonce,
            from: identity.from,
            to: identity.to,
            contract_address: None,
            effective_gas_price: 0,
            priority_fee_per_gas: 0,
            priority_fee_paid: 0,
            block_number: ctx.block_number,
            transaction_index: ctx.tx_index_in_block,
            cumulative_gas_used: ctx.cumulative_gas_used_before,
            skip_reason: Some(reason),
        };
        (receipt, ws)
    }
}
