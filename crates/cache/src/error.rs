//! The cache error. Every reader treats any variant as "unknown" and
//! admits; the mirror logs and retries.

/// Why a cache call gave no answer.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("redis command timed out")]
    Timeout,
    #[error("cache is not configured")]
    Disabled,
    #[error("password variable {0} is not set")]
    MissingPassword(String),
    #[error("stored value does not decode: {0}")]
    Codec(String),
}
