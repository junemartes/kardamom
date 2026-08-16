//! L1 source trait — the seam between the watcher and the L1 RPC.
//!
//! Two reads:
//!   * `finalized_block_number()` — the latest L1 block tagged `finalized`.
//!   * `deposit_logs(lockbox, from, to)` — `DepositInitiated` events emitted
//!     by `lockbox` in the inclusive range `[from, to]`.
//!
//! Errors split between transport/decode failures ([`L1SourceError`]) and
//! "L1 has no finalized block yet" — the latter is classified separately so
//! the watcher can debug-log instead of marking the tick as an error.

use alloy_primitives::{Address, B256};
use async_trait::async_trait;

// `DepositLog` — the decoded `DepositInitiated` event — lives in
// `kardamom_types::epoch` alongside the derivation rule that consumes it, so
// producer and verifier share one definition.
pub use kardamom_types::epoch::DepositLog;

/// Errors that can surface from an `L1Source`. Exclusively transport- or
/// decode-level; semantic deposit failures (overflow, dedup) come back from
/// downstream consumers once the deposit reaches the executor.
#[derive(Debug, thiserror::Error)]
pub enum L1SourceError {
    /// Provider/transport error (HTTP failure, connection reset, etc).
    #[error("L1 provider error: {0}")]
    Provider(String),
    /// ABI/RLP/etc decode failure of a log returned by the provider.
    #[error("L1 log decode error: {0}")]
    Decode(String),
    /// The L1 has not yet produced a finalized block. Expected on a freshly-
    /// started chain (e.g. anvil before the first 128 blocks); distinct from
    /// a transport failure so the watcher can log at `debug` instead of
    /// inflating the `err` tick counter.
    #[error("L1 has no finalized block yet")]
    NotFinalized,
}

/// The L1 view the watcher needs. All methods async and fallible.
#[async_trait]
pub trait L1Source: Send + Sync + 'static {
    /// Latest finalized L1 block number.
    async fn finalized_block_number(&self) -> Result<u64, L1SourceError>;

    /// `(hash, parent_hash)` of L1 block `number`, from ONE round trip.
    ///
    /// The hash is needed because an epoch must be emitted for EVERY finalized
    /// L1 block, including ones with no deposits — and a block with no logs has
    /// no log to carry its hash. The hash is what the epoch's canonical id
    /// derives from, so it cannot be skipped or synthesised.
    ///
    /// The parent hash rides along because the verifier CHAINS consecutive
    /// origins: block N's parent must be block N-1's hash. Both live in the
    /// same header, so chaining costs no extra request — and it forces a lying
    /// L1 endpoint to fabricate a consistent chain rather than isolated
    /// blocks. See issue #163.
    async fn block_ids(&self, number: u64) -> Result<(B256, B256), L1SourceError>;

    /// Hash of L1 block `number`. Convenience over [`Self::block_ids`].
    async fn block_hash(&self, number: u64) -> Result<B256, L1SourceError> {
        Ok(self.block_ids(number).await?.0)
    }

    /// `DepositInitiated` logs emitted by `lockbox` in the inclusive block
    /// range `[from_block, to_block]`. Order within the response is the
    /// canonical (block, log_index) order.
    async fn deposit_logs(
        &self,
        lockbox: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<DepositLog>, L1SourceError>;
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    /// In-memory `L1Source` driven by a scripted queue. Tests push expected
    /// `(tip, logs)` pairs in order; each `process_once` consumes one pair.
    pub struct MockL1Source {
        /// Pre-scripted outcomes for `finalized_block_number()` calls
        /// (FIFO). `Ok(tip)` returns the tip; `Err` returns the error.
        pub tips: Mutex<VecDeque<Result<u64, L1SourceError>>>,
        /// Pre-scripted outcomes for `deposit_logs(...)` calls (FIFO).
        pub logs: Mutex<VecDeque<Result<Vec<DepositLog>, L1SourceError>>>,
        /// Hash returned for a given block number. Unlisted numbers get a
        /// deterministic filler (`repeat_byte(number)`) so tests that don't
        /// care about hashes don't have to populate this.
        pub hashes: Mutex<std::collections::BTreeMap<u64, B256>>,
        /// If set, `block_hash` fails with this provider error instead.
        pub block_hash_fails: Mutex<bool>,
    }

    impl MockL1Source {
        /// Deterministic filler hash for a block number, used when `hashes`
        /// has no entry. Tests building expected epochs use it too.
        pub fn filler_hash(number: u64) -> B256 {
            B256::repeat_byte(number as u8)
        }
    }

    impl MockL1Source {
        pub fn new() -> Self {
            Self {
                tips: Mutex::new(VecDeque::new()),
                logs: Mutex::new(VecDeque::new()),
                hashes: Mutex::new(std::collections::BTreeMap::new()),
                block_hash_fails: Mutex::new(false),
            }
        }

        pub fn push_tip(&self, r: Result<u64, L1SourceError>) {
            self.tips.lock().unwrap().push_back(r);
        }

        pub fn push_logs(&self, r: Result<Vec<DepositLog>, L1SourceError>) {
            self.logs.lock().unwrap().push_back(r);
        }
    }

    impl Default for MockL1Source {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl L1Source for MockL1Source {
        async fn finalized_block_number(&self) -> Result<u64, L1SourceError> {
            self.tips
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(L1SourceError::NotFinalized))
        }

        async fn block_ids(&self, number: u64) -> Result<(B256, B256), L1SourceError> {
            if *self.block_hash_fails.lock().unwrap() {
                return Err(L1SourceError::Provider(
                    "scripted block_hash failure".into(),
                ));
            }
            let hashes = self.hashes.lock().unwrap();
            let at = |n: u64| {
                hashes
                    .get(&n)
                    .copied()
                    .unwrap_or_else(|| Self::filler_hash(n))
            };
            // Filler hashes chain by construction — block N's parent is the
            // filler for N-1 — so a mock chain is self-consistent unless a
            // test deliberately breaks it.
            Ok((at(number), at(number.saturating_sub(1))))
        }

        async fn deposit_logs(
            &self,
            _lockbox: Address,
            _from_block: u64,
            _to_block: u64,
        ) -> Result<Vec<DepositLog>, L1SourceError> {
            self.logs
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(Vec::new()))
        }
    }
}
