//! Redis-backed nonce registry — the pre-validation gate for ingest.
//!
//! The sequencer queries this registry before publishing a tx to channel A/B:
//!
//! - If the sender's current `next_nonce` in Redis equals the incoming tx's
//!   nonce, the registry atomically increments and returns `Accepted` —
//!   sequencer can proceed.
//! - If it disagrees, the registry returns `Rejected { expected }` —
//!   sequencer drops the tx; no ack to the client (proxy will time out and
//!   retry, possibly to a different sequencer).
//! - If the sender is not cached, the registry returns `CacheMiss` —
//!   caller is expected to fetch the canonical value (from the executor's
//!   state DB or by replaying channel B from executor's commit position
//!   forward), seed the registry via [`NonceRegistry::seed`], and retry.
//!
//! All operations against Redis are atomic via a single Lua `EVAL` (one
//! round-trip per check). This makes the registry safe under racing
//! sequencers: only one of them can win the increment for any given
//! `(sender, nonce)` pair. The losers see `Rejected` and drop their tx.
//!
//! The registry is *not* the source of truth — the executor's state DB is.
//! The registry is a hot cache; it can be lost (e.g. Redis cold start) and
//! lazily repopulates from the canonical source on cache miss. No upfront
//! rebuild required.

use alloy_primitives::Address;
use redis::{AsyncCommands, Script};
use thiserror::Error;
use tracing::trace;

/// Outcome of a single [`NonceRegistry::check_and_increment`] call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckOutcome {
    /// Sender's current next-nonce matched; registry incremented to
    /// `new_next_nonce`. Sequencer should publish the tx.
    Accepted { new_next_nonce: u64 },
    /// Sender's current next-nonce was something else. Sequencer should
    /// drop the tx; client will retry with the correct nonce (or proxy
    /// will retry to a different sequencer).
    Rejected { expected: u64 },
    /// Sender is not cached in Redis. Caller should fetch the canonical
    /// next-nonce (from executor state DB or channel B replay), call
    /// [`NonceRegistry::seed`], then retry.
    CacheMiss,
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("registry returned malformed reply: {0}")]
    Malformed(String),
}

/// Static configuration for the registry.
#[derive(Clone, Debug)]
pub struct RegistryConfig {
    /// Redis connection URL, e.g. `redis://127.0.0.1:6379` or
    /// `redis://:password@host:6379/0`.
    pub url: String,
    /// Prefix applied to every Redis key the registry writes. Defaults to
    /// `"kn:"` (kardamom-nonce). Lets multiple environments share a Redis
    /// cluster without collisions.
    pub key_prefix: String,
}

impl RegistryConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            key_prefix: "kn:".into(),
        }
    }
}

/// Redis-backed nonce registry. Cheaply clonable via the underlying
/// multiplexed connection.
#[derive(Clone)]
pub struct NonceRegistry {
    conn: redis::aio::MultiplexedConnection,
    cas_script: Script,
    key_prefix: String,
}

impl NonceRegistry {
    /// Connect to Redis and prepare the atomic CAS script.
    pub async fn connect(cfg: RegistryConfig) -> Result<Self, RegistryError> {
        let client = redis::Client::open(cfg.url.as_str())?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            conn,
            cas_script: Script::new(CAS_SCRIPT),
            key_prefix: cfg.key_prefix,
        })
    }

    /// Atomic check-and-increment.
    ///
    /// Single Redis round-trip via Lua: if current value equals
    /// `expected_nonce`, increment and return `Accepted`; otherwise return
    /// `Rejected` or `CacheMiss`.
    pub async fn check_and_increment(
        &self,
        sender: Address,
        expected_nonce: u64,
    ) -> Result<CheckOutcome, RegistryError> {
        let key = self.key_for(sender);
        let mut conn = self.conn.clone();
        // Script returns Vec<i64> of [status, value].
        //   status =  1: accepted; value = new next_nonce
        //   status =  0: rejected; value = current next_nonce
        //   status = -1: cache miss; value = 0
        let raw: Vec<i64> = self
            .cas_script
            .key(key)
            .arg(expected_nonce)
            .invoke_async(&mut conn)
            .await?;
        let outcome = parse_cas_reply(&raw)?;
        trace!(
            sender = ?sender,
            expected = expected_nonce,
            ?outcome,
            "nonce-registry check_and_increment"
        );
        Ok(outcome)
    }

    /// Return the current cached next-nonce, or `None` on cache miss.
    pub async fn get(&self, sender: Address) -> Result<Option<u64>, RegistryError> {
        let mut conn = self.conn.clone();
        let v: Option<u64> = conn.get(self.key_for(sender)).await?;
        Ok(v)
    }

    /// Seed the registry with a sender's next-nonce. Used after a cache
    /// miss to populate from the canonical source. Uses `SET` (not
    /// `SETNX`) so an authoritative reseed can correct stale entries —
    /// callers must ensure the value comes from a fresh canonical read.
    pub async fn seed(&self, sender: Address, next_nonce: u64) -> Result<(), RegistryError> {
        let mut conn = self.conn.clone();
        let _: () = conn.set(self.key_for(sender), next_nonce).await?;
        Ok(())
    }

    /// Delete a sender's entry. Useful for tests and admin paths.
    pub async fn forget(&self, sender: Address) -> Result<(), RegistryError> {
        let mut conn = self.conn.clone();
        let _: () = conn.del(self.key_for(sender)).await?;
        Ok(())
    }

    fn key_for(&self, sender: Address) -> String {
        // 0x + 40 hex chars. Using checksum-less hex keeps Redis keys small
        // and case-stable.
        format!("{}{:x}", self.key_prefix, sender)
    }
}

fn parse_cas_reply(raw: &[i64]) -> Result<CheckOutcome, RegistryError> {
    match raw {
        [1, new_value] => Ok(CheckOutcome::Accepted {
            new_next_nonce: *new_value as u64,
        }),
        [0, current] => Ok(CheckOutcome::Rejected {
            expected: *current as u64,
        }),
        [-1, _] => Ok(CheckOutcome::CacheMiss),
        other => Err(RegistryError::Malformed(format!(
            "expected [status, value] tuple, got {other:?}"
        ))),
    }
}

/// Atomic check-and-increment Lua script.
///
/// KEYS[1] — sender key
/// ARGV[1] — expected next nonce
///
/// Returns a two-element table `{status, value}`:
///   status =  1  → accepted; value = new next_nonce  (current + 1)
///   status =  0  → rejected; value = current next_nonce
///   status = -1  → cache miss; value = 0
const CAS_SCRIPT: &str = r#"
local current = redis.call('GET', KEYS[1])
if current == false then
    return {-1, 0}
end
current = tonumber(current)
local expected = tonumber(ARGV[1])
if current == expected then
    redis.call('SET', KEYS[1], current + 1)
    return {1, current + 1}
else
    return {0, current}
end
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cas_reply_accepted() {
        let r = parse_cas_reply(&[1, 42]).unwrap();
        assert_eq!(r, CheckOutcome::Accepted { new_next_nonce: 42 });
    }

    #[test]
    fn parse_cas_reply_rejected() {
        let r = parse_cas_reply(&[0, 10]).unwrap();
        assert_eq!(r, CheckOutcome::Rejected { expected: 10 });
    }

    #[test]
    fn parse_cas_reply_cache_miss() {
        let r = parse_cas_reply(&[-1, 0]).unwrap();
        assert_eq!(r, CheckOutcome::CacheMiss);
    }

    #[test]
    fn parse_cas_reply_malformed_errors() {
        assert!(parse_cas_reply(&[42]).is_err());
        assert!(parse_cas_reply(&[1, 2, 3]).is_err());
    }
}
