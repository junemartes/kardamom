//! The account-state projection and the receipt index in Redis, and the
//! local layer in front of them.
//!
//! Three read layers serve the ingress and the sequencer:
//!
//! 1. [`LiveAccounts`], an in-process map fed by the account rows on the
//!    `tx_receipts` batch frame. A warm sender never touches the network.
//! 2. [`AccountCache`], the Redis projection the state mirror writes.
//! 3. The executor query, on a Redis miss. The reader owns that call.
//!
//! Every layer applies one monotone write rule: a row applies only when
//! its batch end position is greater than the stored one. On an equal
//! position the content must agree. mdbx on the executors stays the
//! source of truth; the mirror rebuilds Redis from a checkpoint. See
//! `docs/specs/2026-09-13-redis-account-cache-design.md`.

pub mod config;
pub mod error;
pub mod keys;
pub mod live;
pub mod metrics;
pub mod script;

mod client;

pub use client::{AccountCache, AccountView, RowsWritten};
pub use config::{CacheConfig, LiveAccountsConfig};
pub use error::CacheError;
pub use live::{LiveAccounts, LiveAccountsWriter};
