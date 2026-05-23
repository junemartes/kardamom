//! Receipt-cache channel.
//!
//! `CachedReceipt` messages flow executor → proxy/RPC frontend over a
//! dedicated Aeron stream (RAM only, no Archive). Consumers use this channel
//! to keep a hot nonce cache without round-tripping through libmdbx.
//!
//! Real publishers/subscribers live behind the `aeron-live` feature. In-memory
//! fakes (used by other crates' unit tests) live in [`crate::testing`].
