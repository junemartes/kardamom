use revm::database::DBErrorMarker;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("mdbx: {0}")]
    Mdbx(#[from] libmdbx::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("codec: {0}")]
    Codec(String),

    #[error("schema version mismatch: env has {found}, code expects {expected}")]
    SchemaVersionMismatch { found: u32, expected: u32 },

    #[error("chain id mismatch: env has {found}, code expects {expected}")]
    ChainIdMismatch { found: u64, expected: u64 },

    #[error("missing required meta key: {0}")]
    MissingMeta(&'static str),

    #[error("genesis already initialized with a different allocation")]
    GenesisMismatch,
}

impl StateError {
    pub fn codec(msg: impl Into<String>) -> Self {
        StateError::Codec(msg.into())
    }
}

impl DBErrorMarker for StateError {}
