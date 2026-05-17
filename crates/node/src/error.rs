use alloy_primitives::B256;
use jsonrpsee::types::ErrorObjectOwned;

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("failed to decode transaction: {0}")]
    Decode(String),
    #[error("could not recover transaction signer")]
    SignatureRecovery,
    #[error("transaction execution failed: {0}")]
    Execution(String),
    #[error("unknown transaction {0}")]
    UnknownTransaction(B256),
    #[error("only the \"latest\" block tag is supported")]
    UnsupportedBlockTag,
    #[error("server error: {0}")]
    Server(String),
    #[error("deposit source_hash already applied")]
    DuplicateDeposit,
    #[error("deposit mint would overflow account balance")]
    MintOverflow,
    #[error("invalid deposit envelope: {0}")]
    InvalidDepositEnvelope(String),
}

impl From<NodeError> for ErrorObjectOwned {
    fn from(err: NodeError) -> Self {
        let code = match err {
            NodeError::Decode(_)
            | NodeError::SignatureRecovery
            | NodeError::UnknownTransaction(_)
            | NodeError::UnsupportedBlockTag
            | NodeError::DuplicateDeposit
            | NodeError::InvalidDepositEnvelope(_) => -32602, // invalid params
            NodeError::Execution(_) | NodeError::MintOverflow => -32000, // server error
            NodeError::Server(_) => -32603,                              // internal
        };
        ErrorObjectOwned::owned::<()>(code, err.to_string(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_error_displays() {
        let err = NodeError::Decode("bad rlp".into());
        assert_eq!(err.to_string(), "failed to decode transaction: bad rlp");
    }

    #[test]
    fn unsupported_block_tag_maps_to_invalid_params() {
        let err = NodeError::UnsupportedBlockTag;
        let rpc: ErrorObjectOwned = err.into();
        assert_eq!(rpc.code(), -32602);
    }

    #[test]
    fn decode_error_maps_to_invalid_params() {
        let err = NodeError::Decode("x".into());
        let rpc: ErrorObjectOwned = err.into();
        assert_eq!(rpc.code(), -32602);
    }

    #[test]
    fn duplicate_deposit_displays() {
        let err = NodeError::DuplicateDeposit;
        assert_eq!(err.to_string(), "deposit source_hash already applied");
    }

    #[test]
    fn duplicate_deposit_maps_to_invalid_params() {
        let rpc: ErrorObjectOwned = NodeError::DuplicateDeposit.into();
        assert_eq!(rpc.code(), -32602);
    }

    #[test]
    fn mint_overflow_displays() {
        let err = NodeError::MintOverflow;
        assert_eq!(
            err.to_string(),
            "deposit mint would overflow account balance"
        );
    }

    #[test]
    fn mint_overflow_maps_to_server_error() {
        let rpc: ErrorObjectOwned = NodeError::MintOverflow.into();
        assert_eq!(rpc.code(), -32000);
    }

    #[test]
    fn invalid_deposit_envelope_displays() {
        let err = NodeError::InvalidDepositEnvelope("expected deposit tx, got Eip1559".into());
        assert_eq!(
            err.to_string(),
            "invalid deposit envelope: expected deposit tx, got Eip1559"
        );
    }

    #[test]
    fn invalid_deposit_envelope_maps_to_invalid_params() {
        let rpc: ErrorObjectOwned = NodeError::InvalidDepositEnvelope("x".into()).into();
        assert_eq!(rpc.code(), -32602);
    }
}
