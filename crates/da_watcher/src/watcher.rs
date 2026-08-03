//! Watcher loop: cursor-tailed L1 polling that builds one [`EpochRecord`] per
//! finalized L1 block and publishes it on the `tx_deposits` Aeron channel.
//!
//! The unit is the EPOCH, not the deposit. Every finalized L1 block yields
//! exactly one record — including blocks with no deposits — because the
//! no-skipping rule is only enforceable if every epoch appears on the stream.
//! See `docs/agents/l1-origin-deposit-derivation-spec.md`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;

use crate::source::DepositLog;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use kardamom_types::EpochRecord;

use crate::metrics;
use crate::publisher::{EpochPublisher, PublishError};
use crate::source::{L1Source, L1SourceError};
use kardamom_types::epoch::{EpochError, derive_epoch};

/// Watcher configuration.
#[derive(Debug, Clone, Copy)]
pub struct DaWatcherConfig {
    /// L1 address of the `ETHLockbox` proxy this L2 chain id maps to.
    pub lockbox: Address,
    /// Polling cadence for `finalized_block_number()`.
    pub poll_interval: Duration,
}

/// Handle to a running watcher task. Drop `shutdown` (or send `()`) to ask
/// the loop to exit; `task` is the underlying tokio `JoinHandle`.
pub struct WatcherHandle {
    /// Underlying tokio task. `.await` after sending `shutdown` to join.
    pub task: JoinHandle<()>,
    /// Cooperative shutdown signal. `send(())` (or dropping it) asks the
    /// watcher loop to exit at the next tick boundary.
    pub shutdown: oneshot::Sender<()>,
}

/// Errors the watcher's tick loop reports up. A `Tip` or `Logs` error means
/// the cursor was NOT advanced; the next tick will retry the same range.
#[derive(Debug, thiserror::Error)]
pub enum MonitorError {
    /// `L1Source::finalized_block_number` failed (transport/decode).
    #[error("failed to read L1 finalized tip: {0}")]
    Tip(L1SourceError),
    /// `L1Source::deposit_logs` failed.
    #[error("failed to read L1 deposit logs: {0}")]
    Logs(L1SourceError),
    /// `L1Source::block_hash` failed. An epoch cannot be built without its
    /// block's hash — the canonical id derives from it.
    #[error("failed to read L1 block hash: {0}")]
    BlockHash(L1SourceError),
    /// The logs fetched for a block disagree with the hash fetched for that
    /// same block — an L1 reorg between the two reads, or a provider serving
    /// inconsistent views. Never advance the cursor past this.
    #[error("epoch derivation failed: {0}")]
    Derive(EpochError),
    /// The L1 has not yet produced a finalized block. Classified as a tick-
    /// level outcome (not an err) so dashboards don't light up before
    /// finality kicks in on a freshly-started chain.
    #[error("L1 has no finalized block yet")]
    NotFinalized,
    /// The publisher transport is permanently closed; the watcher must
    /// exit.
    #[error("deposit publisher closed")]
    PublisherClosed,
}

/// One processing pass. Public so unit tests can exercise it directly
/// without any timer or thread.
///
/// Returns `Ok(n)` on success (`n` = number of epochs successfully published
/// this pass; zero on seed/idle ticks). The `cursor` advances per published
/// block, so a partial pass resumes exactly where it stopped.
///
/// On `Err`, the cursor is unchanged.
///
/// # Errors
/// - [`MonitorError::Tip`] if reading the L1 finalized tip fails.
/// - [`MonitorError::Logs`] if reading the deposit logs in `(cursor, tip]` fails.
/// - [`MonitorError::BlockHash`] if reading an L1 block hash fails.
/// - [`MonitorError::Derive`] if a block's logs disagree with its hash.
/// - [`MonitorError::PublisherClosed`] if the publisher transport is shut.
///
/// Publish backpressure is logged and the cursor is left at the last block
/// that DID publish, so the next tick retries from the one that did not.
pub async fn process_once<S, P>(
    publisher: &P,
    source: &S,
    lockbox: Address,
    cursor: &mut Option<u64>,
) -> Result<usize, MonitorError>
where
    S: L1Source,
    P: EpochPublisher,
{
    let tip = match source.finalized_block_number().await {
        Ok(n) => n,
        Err(L1SourceError::NotFinalized) => return Err(MonitorError::NotFinalized),
        Err(e) => return Err(MonitorError::Tip(e)),
    };

    // Emit the finalized tip gauge on every successful tip fetch.
    ::metrics::gauge!(metrics::L1_FINALIZED).set(tip as f64);

    let from_block = match cursor {
        None => {
            // Seed: skip historical deposits per spec Non-Goals.
            *cursor = Some(tip);
            return Ok(0);
        }
        Some(c) if tip <= *c => return Ok(0),
        Some(c) => *c + 1,
    };

    // One range query for the logs, then split by block: the alternative
    // (a query per block) multiplies RPC round-trips on catch-up for no gain.
    let logs = source
        .deposit_logs(lockbox, from_block, tip)
        .await
        .map_err(MonitorError::Logs)?;
    let mut by_block: BTreeMap<u64, Vec<DepositLog>> = BTreeMap::new();
    for log in logs {
        by_block.entry(log.block_number).or_default().push(log);
    }

    let mut published = 0usize;
    for number in from_block..=tip {
        // Blocks with no deposits have no log to carry their hash, so it is
        // fetched per block. `derive_epoch` then cross-checks every log's
        // block_hash against it, which is what catches a reorg landing
        // between the log query and this read.
        let hash = source
            .block_hash(number)
            .await
            .map_err(MonitorError::BlockHash)?;
        let logs = by_block.remove(&number).unwrap_or_default();
        let epoch = derive_epoch(number, hash, &logs).map_err(MonitorError::Derive)?;
        let deposits = epoch.deposits.len();

        match publisher.publish(&epoch) {
            Ok(pos) => {
                published += 1;
                // Advance PER BLOCK, not once for the whole range: a failure
                // halfway through must not re-publish the epochs already
                // accepted (dedup would absorb it, but the cursor is also the
                // origin-lag signal and should not go backwards).
                *cursor = Some(number);
                ::metrics::counter!(metrics::EPOCHS_PUBLISHED_TOTAL).increment(1);
                ::metrics::counter!(metrics::DEPOSITS_DETECTED_TOTAL).increment(deposits as u64);
                ::metrics::gauge!(metrics::EPOCH_ORIGIN).set(number as f64);
                debug!(
                    target: "da_watcher",
                    l1_number = number,
                    deposits,
                    ?pos,
                    "published epoch"
                );
            }
            Err(PublishError::Backpressure) => {
                // Hold the cursor at the last SUCCESSFUL block: re-tick
                // resumes at exactly this one.
                warn!(
                    target: "da_watcher",
                    l1_number = number,
                    "epoch publish backpressured; will retry next tick"
                );
                return Ok(published);
            }
            Err(PublishError::Closed) => return Err(MonitorError::PublisherClosed),
            Err(PublishError::Transport(detail)) => {
                // Deliberately NOT the old per-deposit "log and carry on".
                // Skipping an epoch puts a permanent hole in the origin
                // sequence, which is precisely what the no-skipping rule
                // forbids and what a verifier would later reject the chain
                // for. Stop the range here and retry from this block.
                warn!(
                    target: "da_watcher",
                    l1_number = number,
                    %detail,
                    "epoch publish failed; halting range (an epoch must never be skipped)"
                );
                return Ok(published);
            }
        }
    }

    *cursor = Some(tip);
    Ok(published)
}

/// Spawn the watcher loop. Returns a [`WatcherHandle`] that owns the task
/// and a cooperative shutdown channel.
pub fn spawn<S, P>(publisher: P, source: S, config: DaWatcherConfig) -> WatcherHandle
where
    S: L1Source + 'static,
    P: EpochPublisher + 'static,
{
    let publisher = Arc::new(publisher);
    let source = Arc::new(source);
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        // `interval` fires immediately on the first `tick().await`, which
        // is what we want: seed the cursor as soon as the task starts.
        let mut interval = tokio::time::interval(config.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut cursor: Option<u64> = None;

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => {
                    info!(target: "da_watcher", "shutting down");
                    break;
                }
                _ = interval.tick() => {
                    match process_once(publisher.as_ref(), source.as_ref(), config.lockbox, &mut cursor).await {
                        Ok(0) => {
                            ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "ok").increment(1);
                        }
                        Ok(n) => {
                            ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "ok").increment(1);
                            info!(target: "da_watcher", published = n, "epochs published");
                        }
                        Err(MonitorError::NotFinalized) => {
                            ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "ok").increment(1);
                            debug!(target: "da_watcher", "L1 has no finalized block yet");
                        }
                        Err(MonitorError::PublisherClosed) => {
                            warn!(target: "da_watcher", "publisher closed; exiting");
                            break;
                        }
                        Err(ref e @ MonitorError::Tip(L1SourceError::Decode(_)))
                        | Err(ref e @ MonitorError::Logs(L1SourceError::Decode(_))) => {
                            ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "parse_error").increment(1);
                            warn!(target: "da_watcher", error = %e, "tick failed (parse error)");
                        }
                        Err(e) => {
                            ::metrics::counter!(metrics::TICK_TOTAL, "outcome" => "rpc_error").increment(1);
                            warn!(target: "da_watcher", error = %e, "tick failed");
                        }
                    }
                }
            }
        }
    });
    WatcherHandle {
        task,
        shutdown: shutdown_tx,
    }
}

// Helper: `Arc<T>: EpochPublisher` so the spawned task can share the
// publisher without forcing the trait to require `Clone`.
impl<P: EpochPublisher + ?Sized> EpochPublisher for Arc<P> {
    fn publish(&self, epoch: &EpochRecord) -> Result<kardamom_types::BPosition, PublishError> {
        (**self).publish(epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::fakes::InMemoryEpochPublisher;
    use crate::source::fakes::MockL1Source;
    use alloy_primitives::U256;
    use alloy_primitives::{Address, B256, address};
    use kardamom_types::epoch::{DepositLog, alias_l1_address, source_hash};

    fn lockbox() -> Address {
        address!("0000000000000000000000000000000000C0DE01")
    }

    /// A log in L1 block `number`, whose hash is the mock's filler for that
    /// number — i.e. what the watcher will independently fetch.
    fn dep_log(number: u64, log_index: u64, mint: u128) -> DepositLog {
        DepositLog {
            block_number: number,
            block_hash: MockL1Source::filler_hash(number),
            log_index,
            from: Address::repeat_byte(0x11),
            to: Address::repeat_byte(0x22),
            mint,
            gas_limit: 200_000,
            data: alloy_primitives::Bytes::new(),
        }
    }

    #[tokio::test]
    async fn seed_call_returns_zero_and_advances_cursor() {
        let pub_ = InMemoryEpochPublisher::default();
        let src = MockL1Source::new();
        src.push_tip(Ok(100));
        let mut cursor = None;
        let n = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap();
        assert_eq!(n, 0);
        assert_eq!(cursor, Some(100));
        assert!(pub_.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn one_epoch_per_l1_block_with_deposits_grouped_by_block() {
        let pub_ = InMemoryEpochPublisher::default();
        let src = MockL1Source::new();
        src.push_tip(Ok(153));
        // A single range query spanning three blocks: two deposits in 151,
        // none in 152, one in 153.
        src.push_logs(Ok(vec![
            dep_log(151, 0, 100),
            dep_log(151, 1, 200),
            dep_log(153, 5, 300),
        ]));
        let mut cursor = Some(150);

        let n = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap();

        assert_eq!(n, 3, "one epoch per block in (150, 153]");
        assert_eq!(cursor, Some(153));
        let v = pub_.published.lock().unwrap();
        assert_eq!(
            v.iter().map(|e| e.l1_number).collect::<Vec<_>>(),
            vec![151, 152, 153],
            "epochs are emitted in L1 order with no gaps"
        );
        assert_eq!(v[0].deposits.len(), 2);
        assert!(v[1].deposits.is_empty(), "block 152 had no deposits");
        assert_eq!(v[2].deposits.len(), 1);

        // Deposit content is derived, not passed through: sender aliased,
        // source_hash from the L1 (block_hash, log_index).
        assert_eq!(
            v[0].deposits[0].from,
            alias_l1_address(Address::repeat_byte(0x11))
        );
        assert_eq!(
            v[0].deposits[0].source_hash,
            source_hash(MockL1Source::filler_hash(151), 0)
        );
        assert_eq!(v[0].deposits[0].to, Some(Address::repeat_byte(0x22)));
        assert_eq!(v[0].deposits[0].mint, 100);
        assert_eq!(v[0].deposits[0].value, U256::from(100u64));
    }

    /// The no-skipping rule in miniature: a stretch of L1 with no deposits at
    /// all must still produce one epoch per block, or the origin sequence
    /// gets a hole a verifier would reject.
    #[tokio::test]
    async fn depositless_range_still_emits_every_epoch() {
        let pub_ = InMemoryEpochPublisher::default();
        let src = MockL1Source::new();
        src.push_tip(Ok(105));
        src.push_logs(Ok(vec![]));
        let mut cursor = Some(100);

        let n = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap();

        assert_eq!(n, 5);
        let v = pub_.published.lock().unwrap();
        assert_eq!(
            v.iter().map(|e| e.l1_number).collect::<Vec<_>>(),
            vec![101, 102, 103, 104, 105]
        );
        assert!(v.iter().all(|e| e.deposits.is_empty()));
    }

    #[tokio::test]
    async fn not_finalized_surfaces_distinct_error_no_cursor_advance() {
        let pub_ = InMemoryEpochPublisher::default();
        let src = MockL1Source::new();
        src.push_tip(Err(L1SourceError::NotFinalized));
        let mut cursor = None;
        let err = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap_err();
        assert!(matches!(err, MonitorError::NotFinalized));
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn tip_below_cursor_is_noop() {
        let pub_ = InMemoryEpochPublisher::default();
        let src = MockL1Source::new();
        src.push_tip(Ok(50));
        let mut cursor = Some(100);
        let n = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap();
        assert_eq!(n, 0);
        assert_eq!(cursor, Some(100));
    }

    #[tokio::test]
    async fn backpressure_holds_cursor_so_next_tick_retries() {
        let pub_ = InMemoryEpochPublisher::default();
        *pub_.fail_with_backpressure.lock().unwrap() = true;
        let src = MockL1Source::new();
        src.push_tip(Ok(200));
        src.push_logs(Ok(vec![dep_log(200, 0, 100)]));
        let mut cursor = Some(150);
        let n = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap();
        assert_eq!(n, 0); // first publish backpressured; loop returned early
        assert_eq!(cursor, Some(150)); // cursor NOT advanced
    }

    /// A partially-published range must resume at the first block that did
    /// NOT publish — not re-emit the ones that did, and not skip past them.
    #[tokio::test]
    async fn partial_range_resumes_at_the_first_unpublished_block() {
        let pub_ = InMemoryEpochPublisher::default();
        let src = MockL1Source::new();
        src.push_tip(Ok(104));
        src.push_logs(Ok(vec![]));
        let mut cursor = Some(100);

        // Let 101 and 102 through, then jam the transport.
        let flag = pub_.fail_with_backpressure.clone();
        let published = pub_.published.clone();
        std::thread::spawn(move || {
            loop {
                if published.lock().unwrap().len() >= 2 {
                    *flag.lock().unwrap() = true;
                    return;
                }
                std::thread::yield_now();
            }
        });
        let n = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap();

        assert!((1..=4).contains(&n));
        // Whatever it managed, the cursor names the last block it published.
        assert_eq!(cursor, Some(100 + n as u64));
    }

    /// A log whose block hash disagrees with the hash fetched for that block
    /// number means the two reads saw different L1s. Publishing that epoch
    /// would bake a wrong `source_hash` into the chain.
    #[tokio::test]
    async fn log_disagreeing_with_the_block_hash_is_rejected() {
        let pub_ = InMemoryEpochPublisher::default();
        let src = MockL1Source::new();
        src.push_tip(Ok(151));
        src.push_logs(Ok(vec![DepositLog {
            block_number: 151,
            block_hash: B256::repeat_byte(0xFF), // not the filler for 151
            log_index: 0,
            from: Address::repeat_byte(0x11),
            to: Address::repeat_byte(0x22),
            mint: 1,
            gas_limit: 100,
            data: alloy_primitives::Bytes::new(),
        }]));
        let mut cursor = Some(150);

        let err = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap_err();

        assert!(matches!(err, MonitorError::Derive(_)));
        assert_eq!(cursor, Some(150), "cursor must not pass a bad epoch");
        assert!(pub_.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn block_hash_failure_stops_the_range() {
        let pub_ = InMemoryEpochPublisher::default();
        let src = MockL1Source::new();
        src.push_tip(Ok(151));
        src.push_logs(Ok(vec![]));
        *src.block_hash_fails.lock().unwrap() = true;
        let mut cursor = Some(150);

        let err = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap_err();

        assert!(matches!(err, MonitorError::BlockHash(_)));
        assert_eq!(cursor, Some(150));
    }
}
