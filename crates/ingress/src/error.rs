//! Public error type for the ingress proxy.
//!
//! `From<IngressError> for ErrorObjectOwned` maps each variant to a
//! JSON-RPC error code.

use alloy_primitives::Address;
use jsonrpsee::types::ErrorObjectOwned;

#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    #[error("rate limit exceeded for client {0}")]
    RateLimited(String),
    #[error("failed to decode transaction: {0}")]
    Decode(String),
    #[error("signature verification failed")]
    SignatureInvalid,
    #[error("sequencer partition unavailable: {0}")]
    PartitionUnavailable(String),
    #[error("timed out waiting for receipt or watermark")]
    Timeout,
    #[error("internal server error: {0}")]
    Internal(String),
    #[error("duplicate (sender, nonce): {0:?}")]
    Duplicate((Address, u64)),
    #[error(
        "evicted by sequencer overload shed (sender, nonce): {0:?} — resubmit \
         once the nonce is within the reorder window"
    )]
    Evicted((Address, u64)),
    #[error(
        "expired: the sequencer dropped (sender, nonce) {0:?} after it waited on a \
         nonce gap for tx_ttl — resubmit once the gap fills"
    )]
    Expired((Address, u64)),
    #[error(
        "past deadline: the sealer refused (sender, nonce) {sender_nonce:?} at block \
         {at_block}, past the transaction's max inclusion block {max_inclusion_block} — \
         resubmit"
    )]
    PastDeadline {
        sender_nonce: (Address, u64),
        max_inclusion_block: u64,
        at_block: u64,
    },
    #[error("ingress overloaded: {0} submissions pending — retry with backoff")]
    Overloaded(usize),
    #[error("ingress draining for shutdown — retry on another replica")]
    Draining,
    #[error(
        "transaction gas limit {0} exceeds the EIP-7825 per-tx cap of \
         {cap} — the tx can never execute",
        cap = kardamom_types::limits::TX_GAS_LIMIT_CAP
    )]
    GasLimitExceedsCap(u64),
    #[error(
        "unsupported transaction type {0:#04x}: blob (EIP-4844) transactions are not supported"
    )]
    UnsupportedTxType(u8),
    /// The sender's latest known balance does not cover
    /// `gas_limit * max_fee_per_gas + value`. geth's message shape, so a
    /// wallet handles it as on L1.
    #[error(
        "insufficient funds for gas * price + value: address {address} have {have} want {want}"
    )]
    InsufficientFunds {
        address: Address,
        have: alloy_primitives::U256,
        want: alloy_primitives::U256,
    },
    /// The tip rate is above the fee cap: the transaction can never pay
    /// its bid. geth's message shape.
    #[error(
        "max priority fee per gas higher than max fee per gas: address {address}, \
         maxPriorityFeePerGas: {max_priority_fee_per_gas}, maxFeePerGas: {max_fee_per_gas}"
    )]
    FeeInvalid {
        address: Address,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
    },
    /// The fee cap is under the base fee of the latest block the
    /// sequencer saw. geth's message shape.
    #[error(
        "max fee per gas less than block base fee: address {address}, maxFeePerGas: \
         {max_fee_per_gas}, baseFee: {base_fee}"
    )]
    FeeTooLow {
        address: Address,
        max_fee_per_gas: u128,
        base_fee: u128,
    },
    /// No read layer could answer an account query: the local layer
    /// missed and no executor query is configured, or every endpoint
    /// failed.
    #[error("account state unavailable: {0}")]
    StateUnavailable(String),
    /// The ingress pauses submits on a root halt upstream: the sealer
    /// (a DA lag or a lost quorum), or every executor. The message and
    /// the data name the root's service, its cause, and its runbook; the
    /// pause ends by itself when the root clears.
    #[error(
        "chain halted: {} at {} ({detail}); the ingress pauses submits until it clears; \
         cause and recovery at /halt and kardamom_chainStatus, runbook {}",
        .root.cause.id(),
        .root.service,
        .root.cause.recovery().runbook()
    )]
    ChainHalted {
        root: kardamom_types::service::HaltRef,
        detail: String,
    },
    /// An operator paused this ingress, for example for maintenance.
    #[error("ingress paused by an operator ({note}); resubmit after the operator resumes it")]
    OperatorPaused { note: String },
}

/// The JSON-RPC error code of a halted chain. Its own code, so a client
/// tells a halt from the generic server error and waits instead of
/// retrying at once.
pub const CHAIN_HALTED_CODE: i32 = -32010;

impl IngressError {
    /// The sealer refused a record on its DA-lag guard: the chain is
    /// halted on `da_lag`, and the sealer is the root.
    #[must_use]
    pub fn da_lag(sealed_head: u64, posted_head: u64, budget_blocks: u64) -> Self {
        Self::ChainHalted {
            root: kardamom_types::service::HaltRef::sealer(
                kardamom_types::service::HaltCause::DaLag,
            ),
            detail: format!(
                "sealed head {sealed_head}, posted head {posted_head}, budget {budget_blocks} blocks"
            ),
        }
    }

    /// Builds an `Internal` error from a context label and the
    /// underlying error's `Display`. The many `.map_err(|e|
    /// IngressError::Internal(format!("...: {e}")))` call sites across
    /// this crate, wrapping a bind, open, or merge failure, share this
    /// one format.
    pub(crate) fn internal(ctx: impl std::fmt::Display, e: impl std::fmt::Display) -> Self {
        Self::Internal(format!("{ctx}: {e}"))
    }
}

impl From<IngressError> for ErrorObjectOwned {
    fn from(err: IngressError) -> Self {
        let code = match &err {
            // Limit exceeded, a server-specific and retryable overload class.
            IngressError::RateLimited(_) | IngressError::Overloaded(_) | IngressError::Draining => {
                -32005
            }
            // Invalid params.
            IngressError::Decode(_)
            | IngressError::SignatureInvalid
            | IngressError::Duplicate(_)
            | IngressError::GasLimitExceedsCap(_)
            | IngressError::UnsupportedTxType(_) => -32602,
            // Generic server error. Evicted is retryable once the sender's
            // nonce is back within the reorder window. Expired is
            // retryable once the nonce gap fills. PastDeadline is
            // retryable at once: the resubmission carries a new deadline.
            IngressError::PartitionUnavailable(_)
            | IngressError::Timeout
            | IngressError::Evicted(_)
            | IngressError::Expired(_)
            | IngressError::PastDeadline { .. }
            | IngressError::InsufficientFunds { .. }
            | IngressError::FeeInvalid { .. }
            | IngressError::FeeTooLow { .. }
            | IngressError::StateUnavailable(_) => -32000,
            // Internal error.
            IngressError::Internal(_) => -32603,
            IngressError::ChainHalted { .. } | IngressError::OperatorPaused { .. } => {
                CHAIN_HALTED_CODE
            }
        };
        let data = match &err {
            // The typed cause, so a client finds the record and the
            // runbook without parsing the message.
            IngressError::ChainHalted { root, .. } => Some(serde_json::json!({
                "cause": root.cause.id(),
                "root_service": root.service,
                "root_instance": root.instance,
                "halt": "/halt",
                "runbook": root.cause.recovery().runbook(),
            })),
            IngressError::OperatorPaused { .. } => Some(serde_json::json!({
                "cause": "operator",
                "halt": "/halt",
            })),
            _ => None,
        };
        ErrorObjectOwned::owned(code, err.to_string(), data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limited_maps_to_minus_32005() {
        let err = IngressError::RateLimited("10.0.0.1".into());
        let rpc: ErrorObjectOwned = err.into();
        assert_eq!(rpc.code(), -32005);
    }

    #[test]
    fn signature_invalid_maps_to_invalid_params() {
        let rpc: ErrorObjectOwned = IngressError::SignatureInvalid.into();
        assert_eq!(rpc.code(), -32602);
    }

    #[test]
    fn timeout_maps_to_server_error() {
        let rpc: ErrorObjectOwned = IngressError::Timeout.into();
        assert_eq!(rpc.code(), -32000);
    }

    #[test]
    fn insufficient_funds_maps_to_server_error_in_geth_shape() {
        let rpc: ErrorObjectOwned = IngressError::InsufficientFunds {
            address: Address::repeat_byte(0x11),
            have: alloy_primitives::U256::from(5u64),
            want: alloy_primitives::U256::from(9u64),
        }
        .into();
        assert_eq!(rpc.code(), -32000);
        assert!(
            rpc.message()
                .starts_with("insufficient funds for gas * price + value"),
            "{}",
            rpc.message()
        );
        assert!(rpc.message().contains("have 5 want 9"), "{}", rpc.message());
    }

    #[test]
    fn chain_halted_has_its_own_code_and_names_the_root() {
        let rpc: ErrorObjectOwned = IngressError::da_lag(160, 100, 50).into();
        assert_eq!(rpc.code(), CHAIN_HALTED_CODE);
        assert!(
            rpc.message()
                .starts_with("chain halted: da_lag at sealer (sealed head 160"),
            "{}",
            rpc.message()
        );
        assert!(rpc.message().contains("/halt"), "{}", rpc.message());
        assert!(
            rpc.message().contains("docs/runbooks/da_lag.md"),
            "{}",
            rpc.message()
        );
        let data = rpc.data().expect("the typed cause").get();
        assert!(data.contains("\"cause\":\"da_lag\""), "{data}");
        assert!(data.contains("\"root_service\":\"sealer\""), "{data}");
        assert!(data.contains("docs/runbooks/da_lag.md"), "{data}");

        let rpc: ErrorObjectOwned = IngressError::OperatorPaused {
            note: "disk swap".into(),
        }
        .into();
        assert_eq!(rpc.code(), CHAIN_HALTED_CODE);
        assert!(rpc.message().contains("disk swap"), "{}", rpc.message());
    }

    #[test]
    fn gas_cap_and_tx_type_map_to_invalid_params() {
        let rpc: ErrorObjectOwned = IngressError::GasLimitExceedsCap(30_000_000).into();
        assert_eq!(rpc.code(), -32602);
        assert!(rpc.message().contains("16777216"), "{}", rpc.message());
        let rpc: ErrorObjectOwned = IngressError::UnsupportedTxType(0x03).into();
        assert_eq!(rpc.code(), -32602);
        assert!(rpc.message().contains("0x03"), "{}", rpc.message());
    }
}
