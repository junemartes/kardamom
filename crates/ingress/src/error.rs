//! Public error type for the ingress proxy.
//!
//! Variants are mapped to JSON-RPC error codes via
//! `From<IngressError> for ErrorObjectOwned`.

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
    #[error("ingress overloaded: {0} submissions pending — retry with backoff")]
    Overloaded(usize),
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
}

impl From<IngressError> for ErrorObjectOwned {
    fn from(err: IngressError) -> Self {
        let code = match &err {
            // limit exceeded (server-specific): retryable overload class
            IngressError::RateLimited(_) | IngressError::Overloaded(_) => -32005,
            // invalid params
            IngressError::Decode(_)
            | IngressError::SignatureInvalid
            | IngressError::Duplicate(_)
            | IngressError::GasLimitExceedsCap(_)
            | IngressError::UnsupportedTxType(_) => -32602,
            // generic server error; Evicted is retryable once the sender's
            // nonce is back within the reorder window
            IngressError::PartitionUnavailable(_)
            | IngressError::Timeout
            | IngressError::Evicted(_) => -32000,
            // internal
            IngressError::Internal(_) => -32603,
        };
        ErrorObjectOwned::owned::<()>(code, err.to_string(), None)
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
    fn gas_cap_and_tx_type_map_to_invalid_params() {
        let rpc: ErrorObjectOwned = IngressError::GasLimitExceedsCap(30_000_000).into();
        assert_eq!(rpc.code(), -32602);
        assert!(rpc.message().contains("16777216"), "{}", rpc.message());
        let rpc: ErrorObjectOwned = IngressError::UnsupportedTxType(0x03).into();
        assert_eq!(rpc.code(), -32602);
        assert!(rpc.message().contains("0x03"), "{}", rpc.message());
    }
}
