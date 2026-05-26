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

use alloy_primitives::{Address, B256, Bytes};
use async_trait::async_trait;

/// A `DepositInitiated` event decoded from L1.
///
/// `from` is the un-aliased L1 sender; the watcher applies the OP-style
/// alias ([`crate::alias_l1_address`]) before publishing onto `tx_deposits`,
/// so the L2 executor never observes the bare L1 address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepositLog {
    /// Hash of the L1 block this log was emitted in. Feeds `source_hash`.
    pub block_hash: B256,
    /// Position of this log within the L1 block. Feeds `source_hash`.
    pub log_index: u64,
    /// L1 sender (un-aliased).
    pub from: Address,
    /// L2 recipient of the credited mint.
    pub to: Address,
    /// Amount minted on L2 (and forwarded as `value` in the inner EVM call).
    /// Wire type on L1 is `uint256`; we reject `mint > u128::MAX` at decode.
    pub mint: u128,
    /// Gas limit for the inner EVM call.
    pub gas_limit: u64,
    /// Optional calldata for the inner EVM call.
    pub data: Bytes,
}

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

/// The L1 view the watcher needs. Two methods, both async, both fallible.
#[async_trait]
pub trait L1Source: Send + Sync + 'static {
    /// Latest finalized L1 block number.
    async fn finalized_block_number(&self) -> Result<u64, L1SourceError>;

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
    }

    impl MockL1Source {
        pub fn new() -> Self {
            Self {
                tips: Mutex::new(VecDeque::new()),
                logs: Mutex::new(VecDeque::new()),
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
