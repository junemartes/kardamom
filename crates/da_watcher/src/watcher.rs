//! Watcher loop: cursor-tailed L1 polling that builds [`Deposit`] envelopes
//! from `DepositInitiated` events and publishes them on the `tx_deposits`
//! Aeron channel.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use bytes::Bytes;
use kardamom_types::Deposit;

use crate::derive::{alias_l1_address, source_hash};
use crate::metrics;
use crate::publisher::{DepositPublisher, PublishError};
use crate::source::{DepositLog, L1Source, L1SourceError};

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
/// Returns `Ok(n)` on success (`n` = number of deposits successfully
/// published this pass; zero on seed/idle ticks). The `cursor` is advanced
/// to the latest finalized tip on every successful return — including the
/// seed call (where no logs are fetched).
///
/// On `Err`, the cursor is unchanged.
///
/// # Errors
/// - [`MonitorError::Tip`] if reading the L1 finalized tip fails.
/// - [`MonitorError::Logs`] if reading the deposit logs in `(cursor, tip]` fails.
/// - [`MonitorError::PublisherClosed`] if the publisher transport is shut.
///
/// Per-log publish backpressure is logged and the cursor is NOT advanced,
/// so the next tick retries the same range — same semantics as PR #10's
/// "broken-deposit doesn't stall the monitor" guarantee, applied to the
/// publish path.
pub async fn process_once<S, P>(
    publisher: &P,
    source: &S,
    lockbox: Address,
    cursor: &mut Option<u64>,
) -> Result<usize, MonitorError>
where
    S: L1Source,
    P: DepositPublisher,
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

    let logs = source
        .deposit_logs(lockbox, from_block, tip)
        .await
        .map_err(MonitorError::Logs)?;

    let mut published = 0usize;
    for log in &logs {
        let dep = deposit_from_log(log);
        match publisher.publish(&dep) {
            Ok(pos) => {
                published += 1;
                ::metrics::counter!(metrics::DEPOSITS_DETECTED_TOTAL).increment(1);
                debug!(
                    target: "da_watcher",
                    source_hash = ?dep.source_hash,
                    ?pos,
                    "published deposit"
                );
            }
            Err(PublishError::Backpressure) => {
                // Hold the cursor: re-tick will retry this range.
                warn!(
                    target: "da_watcher",
                    block_hash = ?log.block_hash,
                    log_index = log.log_index,
                    "deposit publish backpressured; will retry next tick"
                );
                return Ok(published);
            }
            Err(PublishError::Closed) => return Err(MonitorError::PublisherClosed),
            Err(PublishError::Transport(detail)) => {
                // Treat as transient: log and continue with the next log,
                // analogous to PR #10's broken-deposit-doesn't-stall policy.
                warn!(
                    target: "da_watcher",
                    block_hash = ?log.block_hash,
                    log_index = log.log_index,
                    %detail,
                    "deposit publish failed"
                );
            }
        }
    }

    *cursor = Some(tip);
    Ok(published)
}

/// Build the wire-form [`Deposit`] from an [`DepositLog`]. Applies the OP
/// aliasing offset to the L1 sender and derives the canonical source hash.
fn deposit_from_log(log: &DepositLog) -> Deposit {
    Deposit {
        source_hash: source_hash(log.block_hash, log.log_index),
        from: alias_l1_address(log.from),
        // Deposits from the ETHLockbox always target an L2 recipient
        // (CREATE deposits would need a different L1 event shape and are
        // not currently produced by the lockbox).
        to: Some(log.to),
        mint: log.mint,
        // For ETH-bridge deposits `value` == `mint` (the minted ETH is
        // forwarded to the recipient). When the lockbox evolves to support
        // mint-without-forward, decouple here.
        value: U256::from(log.mint),
        gas_limit: log.gas_limit,
        is_system_transaction: false,
        input: Bytes::copy_from_slice(log.data.as_ref()),
    }
}

/// Spawn the watcher loop. Returns a [`WatcherHandle`] that owns the task
/// and a cooperative shutdown channel.
pub fn spawn<S, P>(publisher: P, source: S, config: DaWatcherConfig) -> WatcherHandle
where
    S: L1Source + 'static,
    P: DepositPublisher + 'static,
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
                            info!(target: "da_watcher", published = n, "deposits published");
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

// Helper: `Arc<T>: DepositPublisher` so the spawned task can share the
// publisher without forcing the trait to require `Clone`.
impl<P: DepositPublisher + ?Sized> DepositPublisher for Arc<P> {
    fn publish(&self, deposit: &Deposit) -> Result<kardamom_types::BPosition, PublishError> {
        (**self).publish(deposit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::fakes::InMemoryDepositPublisher;
    use crate::source::fakes::MockL1Source;
    use alloy_primitives::{Address, B256, address};

    fn lockbox() -> Address {
        address!("0000000000000000000000000000000000C0DE01")
    }

    fn dep_log(block_byte: u8, log_index: u64, mint: u128) -> DepositLog {
        DepositLog {
            block_hash: B256::repeat_byte(block_byte),
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
        let pub_ = InMemoryDepositPublisher::default();
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
    async fn deposits_are_published_and_cursor_advances() {
        let pub_ = InMemoryDepositPublisher::default();
        let src = MockL1Source::new();
        src.push_tip(Ok(200));
        src.push_logs(Ok(vec![dep_log(0xAA, 0, 100), dep_log(0xAA, 1, 200)]));
        let mut cursor = Some(150);
        let n = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(cursor, Some(200));
        let v = pub_.published.lock().unwrap();
        assert_eq!(v.len(), 2);
        // Sender aliased; source_hash derived from the L1 (block_hash, log_index).
        assert_eq!(v[0].from, alias_l1_address(Address::repeat_byte(0x11)));
        assert_eq!(v[0].source_hash, source_hash(B256::repeat_byte(0xAA), 0));
        assert_eq!(v[1].source_hash, source_hash(B256::repeat_byte(0xAA), 1));
        assert_eq!(v[0].to, Some(Address::repeat_byte(0x22)));
        assert_eq!(v[0].mint, 100);
        assert_eq!(v[0].value, U256::from(100u64));
    }

    #[tokio::test]
    async fn not_finalized_surfaces_distinct_error_no_cursor_advance() {
        let pub_ = InMemoryDepositPublisher::default();
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
        let pub_ = InMemoryDepositPublisher::default();
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
        let pub_ = InMemoryDepositPublisher::default();
        *pub_.fail_with_backpressure.lock().unwrap() = true;
        let src = MockL1Source::new();
        src.push_tip(Ok(200));
        src.push_logs(Ok(vec![dep_log(0xAA, 0, 100)]));
        let mut cursor = Some(150);
        let n = process_once(&pub_, &src, lockbox(), &mut cursor)
            .await
            .unwrap();
        assert_eq!(n, 0); // first publish backpressured; loop returned early
        assert_eq!(cursor, Some(150)); // cursor NOT advanced
    }
}
