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
use alloy_rpc_types_eth::{Filter, Log as RpcLog};
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
    /// The provider refused the request with HTTP 429. A source set
    /// rotates the source out for its backoff; the rest keep going.
    #[error("L1 provider rate-limited the request (HTTP 429)")]
    RateLimited,
    /// The source set cannot serve the read: a disagreement no light
    /// client settles, or too few sources in the set. The follower halts
    /// on it every tick, with the cause.
    #[error(transparent)]
    Halt(#[from] crate::sources::SourceHalt),
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

    /// The logs that match `filter`, as the provider returns them. The
    /// indexer reads the settlement's `BatchPosted` logs through this, so
    /// they pass the same cross-check as the lockbox logs.
    async fn logs(&self, filter: &Filter) -> Result<Vec<RpcLog>, L1SourceError>;

    /// Lockbox logs (`DepositInitiated` and `UpgradeInitiated`) that
    /// `lockbox` emits in the inclusive block range `[from_block, to_block]`.
    /// The response order is the canonical (block, `log_index`) order.
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
    ) -> Result<Vec<LockboxLog>, L1SourceError> {
        use alloy_sol_types::SolEvent;
        let filter = Filter::new()
            .address(lockbox)
            .event_signature(vec![
                crate::rpc_source::DepositInitiated::SIGNATURE_HASH,
                crate::rpc_source::UpgradeInitiated::SIGNATURE_HASH,
            ])
            .from_block(from_block)
            .to_block(to_block);
        self.logs(&filter)
            .await?
            .iter()
            .map(crate::rpc_source::decode_lockbox_log)
            .collect()
    }
}

/// A shared source is a source: a test keeps its handle on a mock after
/// the set takes it, and one provider serves two sets.
#[async_trait]
impl<S: L1Source> L1Source for std::sync::Arc<S> {
    async fn finalized_block_number(&self) -> Result<u64, L1SourceError> {
        (**self).finalized_block_number().await
    }

    async fn block_ids(&self, number: u64) -> Result<(B256, B256), L1SourceError> {
        (**self).block_ids(number).await
    }

    async fn logs(&self, filter: &Filter) -> Result<Vec<RpcLog>, L1SourceError> {
        (**self).logs(filter).await
    }

    async fn lockbox_logs(
        &self,
        lockbox: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<LockboxLog>, L1SourceError> {
        (**self).lockbox_logs(lockbox, from_block, to_block).await
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{Address, B256, Filter, L1Source, L1SourceError, LockboxLog, RpcLog, async_trait};

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
        /// Blocks whose parent hash the mock reports wrong: a provider
        /// that does not serve a chain.
        pub parent_lies: Mutex<std::collections::BTreeSet<u64>>,
        /// If set, every read fails with `RateLimited`: a public endpoint
        /// that answers HTTP 429.
        pub rate_limited: Mutex<bool>,
        /// Pre-scripted outcomes for `logs(...)` calls, in FIFO order. An
        /// empty queue answers no log.
        pub raw_logs: Mutex<VecDeque<Result<Vec<RpcLog>, L1SourceError>>>,
        /// Reads served, so a test can prove which source a set asked.
        pub calls: AtomicU64,
    }

    impl MockL1Source {
        /// Deterministic filler hash for a block number, used when `hashes`
        /// has no entry. Tests building expected epochs use it too.
        #[must_use]
        #[allow(
            clippy::cast_possible_truncation,
            reason = "deliberate: repeats number mod 256"
        )]
        pub fn filler_hash(number: u64) -> B256 {
            B256::repeat_byte(number as u8)
        }
    }

    impl MockL1Source {
        #[must_use]
        pub fn new() -> Self {
            Self {
                tips: Mutex::new(VecDeque::new()),
                logs: Mutex::new(VecDeque::new()),
                hashes: Mutex::new(std::collections::BTreeMap::new()),
                block_hash_fails: Mutex::new(false),
                parent_lies: Mutex::new(std::collections::BTreeSet::new()),
                rate_limited: Mutex::new(false),
                raw_logs: Mutex::new(VecDeque::new()),
                calls: AtomicU64::new(0),
            }
        }

        /// Count one read, and fail it when the mock is rate-limited.
        fn serve(&self) -> Result<(), L1SourceError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if *self.rate_limited.lock().unwrap() {
                return Err(L1SourceError::RateLimited);
            }
            Ok(())
        }

        /// Reads served so far.
        #[must_use]
        pub fn calls(&self) -> u64 {
            self.calls.load(Ordering::Relaxed)
        }

        /// # Panics
        /// Panics if the internal lock is poisoned (a prior panic while
        /// holding it), which only happens after the test has already
        /// failed.
        pub fn push_tip(&self, r: Result<u64, L1SourceError>) {
            self.tips.lock().unwrap().push_back(r);
        }

        /// # Panics
        /// Panics if the internal lock is poisoned (a prior panic while
        /// holding it), which only happens after the test has already
        /// failed.
        pub fn push_logs(&self, r: Result<Vec<LockboxLog>, L1SourceError>) {
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
            self.serve()?;
            self.tips
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(L1SourceError::NotFinalized))
        }

        async fn block_ids(&self, number: u64) -> Result<(B256, B256), L1SourceError> {
            self.serve()?;
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
            if self.parent_lies.lock().unwrap().contains(&number) {
                return Ok((at(number), Self::filler_hash(number + 1_000_000)));
            }
            Ok((at(number), at(number.saturating_sub(1))))
        }

        async fn logs(&self, _filter: &Filter) -> Result<Vec<RpcLog>, L1SourceError> {
            self.serve()?;
            self.raw_logs
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(Vec::new()))
        }

        async fn lockbox_logs(
            &self,
            _lockbox: Address,
            _from_block: u64,
            _to_block: u64,
        ) -> Result<Vec<LockboxLog>, L1SourceError> {
            self.serve()?;
            self.logs
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(Vec::new()))
        }
    }
}
