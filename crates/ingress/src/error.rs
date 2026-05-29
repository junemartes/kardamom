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
}

impl From<IngressError> for ErrorObjectOwned {
    fn from(err: IngressError) -> Self {
        let code = match &err {
            // limit exceeded (server-specific)
            IngressError::RateLimited(_) => -32005,
            // invalid params
            IngressError::Decode(_)
            | IngressError::SignatureInvalid
            | IngressError::Duplicate(_) => -32602,
            // generic server error
            IngressError::PartitionUnavailable(_) | IngressError::Timeout => -32000,
            // internal
            IngressError::Internal(_) => -32603,
        };
        ErrorObjectOwned::owned::<()>(code, err.to_string(), None)
    }
}

/// JSON-RPC error code returned by write methods while the node is still
/// bootstrapping. Falls in the implementation-defined -32000..-32099 range.
pub const NODE_BOOTSTRAPPING_CODE: i32 = -32099;

pub fn node_bootstrapping_error() -> ErrorObjectOwned {
    ErrorObjectOwned::owned(
        NODE_BOOTSTRAPPING_CODE,
        "node bootstrapping: not yet caught up to canonical log",
        Option::<()>::None,
    )
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
}
