//! L1 source trait: the seam between the watcher and the L1 RPC.
//!
//! It provides two reads:
//!   * `finalized_block_number()`: the latest L1 block tagged `finalized`.
//!   * `lockbox_logs(lockbox, from, to)`: the `DepositInitiated` and
//!     `UpgradeInitiated` events that `lockbox` emits in the inclusive range
//!     `[from, to]`, in canonical `(block, log_index)` order.
//!
//! Errors split into transport/decode failures ([`L1SourceError`]) and "L1
//! has no finalized block yet". The second case gets its own variant, so the
//! watcher can log at `debug` level instead of marking the tick as an error.

use alloy_primitives::{Address, B256};
use async_trait::async_trait;

// The decoded log shapes live in `kardamom_types::epoch` alongside the
// derivation rule that consumes them, so producer and verifier share one
// definition.
pub use kardamom_types::epoch::{DepositLog, LockboxLog, UpgradeLog};

/// Errors that can come from an `L1Source`. These are only transport- or
/// decode-level errors. A semantic deposit failure, such as overflow or a
/// duplicate, comes back from a downstream consumer once the deposit
/// reaches the executor.
#[derive(Debug, thiserror::Error)]
pub enum L1SourceError {
    /// Provider/transport error (HTTP failure, connection reset, etc).
    #[error("L1 provider error: {0}")]
    Provider(String),
    /// Decode failure (ABI, RLP, or similar) for a log the provider returns.
    #[error("L1 log decode error: {0}")]
    Decode(String),
    /// The L1 has not yet produced a finalized block. This is expected on a
    /// freshly started chain, for example anvil before its first 128
    /// blocks. It is a separate variant from a transport failure, so the
    /// watcher can log at `debug` level instead of inflating the `err` tick
    /// counter.
    #[error("L1 has no finalized block yet")]
    NotFinalized,
}

/// The L1 view the watcher needs. All methods async and fallible.
#[async_trait]
pub trait L1Source: Send + Sync + 'static {
    /// Latest finalized L1 block number.
    async fn finalized_block_number(&self) -> Result<u64, L1SourceError>;

    /// `(hash, parent_hash)` of L1 block `number`, from one round trip.
    ///
    /// The hash is needed because the watcher must emit an epoch for every
    /// finalized L1 block, including a block with no deposits. A block with
    /// no logs has no log to carry its hash. The hash is what the epoch's
    /// canonical id derives from, so it cannot be skipped or made up.
    ///
    /// The parent hash comes along because the verifier chains consecutive
    /// origins: block N's parent must be block N-1's hash. Both values live
    /// in the same header, so chaining costs no extra request. It also
    /// forces a lying L1 endpoint to fabricate a consistent chain, instead
    /// of isolated blocks.
    async fn block_ids(&self, number: u64) -> Result<(B256, B256), L1SourceError>;

    /// Hash of L1 block `number`. Convenience over [`Self::block_ids`].
    async fn block_hash(&self, number: u64) -> Result<B256, L1SourceError> {
        Ok(self.block_ids(number).await?.0)
    }

    /// Lockbox logs (`DepositInitiated` and `UpgradeInitiated`) that
    /// `lockbox` emits in the inclusive block range `[from_block, to_block]`.
    /// The response order is the canonical (block, log_index) order.
    ///
    /// Both event kinds must come back from one query. Fetching them
    /// separately and merging the results could let a partial failure drop
    /// one kind without an error. Then the producer and the verifier would
    /// derive different epochs from the same L1 block. `derive_epoch` exists
    /// to make that divergence impossible.
    async fn lockbox_logs(
        &self,
        lockbox: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<LockboxLog>, L1SourceError>;
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    /// In-memory `L1Source` driven by a scripted queue. Tests push expected
    /// `(tip, logs)` pairs in order. Each `process_once` call consumes one
    /// pair.
    pub struct MockL1Source {
        /// Pre-scripted outcomes for `finalized_block_number()` calls, in
        /// FIFO order. `Ok(tip)` returns the tip; `Err` returns the error.
        pub tips: Mutex<VecDeque<Result<u64, L1SourceError>>>,
        /// Pre-scripted outcomes for `lockbox_logs(...)` calls, in FIFO order.
        pub logs: Mutex<VecDeque<Result<Vec<LockboxLog>, L1SourceError>>>,
        /// Hash to return for a given block number. An unlisted number gets
        /// a deterministic filler (`repeat_byte(number)`), so a test that
        /// does not care about hashes does not have to fill this in.
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

        pub fn push_logs(&self, r: Result<Vec<LockboxLog>, L1SourceError>) {
            self.logs.lock().unwrap().push_back(r);
        }

        /// Convenience for the common case: script a round of deposit logs.
        pub fn push_deposit_logs(&self, logs: Vec<DepositLog>) {
            self.push_logs(Ok(logs.into_iter().map(LockboxLog::Deposit).collect()));
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
            // Filler hashes chain by construction: block N's parent is the
            // filler for N-1. So a mock chain stays self-consistent unless
            // a test deliberately breaks it.
            Ok((at(number), at(number.saturating_sub(1))))
        }

        async fn lockbox_logs(
            &self,
            _lockbox: Address,
            _from_block: u64,
            _to_block: u64,
        ) -> Result<Vec<LockboxLog>, L1SourceError> {
            self.logs
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(Vec::new()))
        }
    }
}
