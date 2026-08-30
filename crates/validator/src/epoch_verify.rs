//! Epoch verification against L1 — phase 1 of
//! `docs/agents/l1-origin-deposit-derivation-spec.md`.
//!
//! Deriving deposits is only half the guarantee. Without a checker, a buggy or
//! dishonest sequencer produces a chain nobody can rebuild from L1 and nothing
//! notices until someone tries. This module is that checker: for every
//! [`EpochRecord`] on the canonical stream it re-derives the epoch from L1 —
//! through the SAME [`derive_epoch`] the producer used, so a bug cannot cancel
//! itself out — and treats any disagreement as a divergence.
//!
//! Two classes of check, split by what they cost:
//!
//! - **Sequence rules (1 and 2), synchronous.** Monotonic origin, no skipped
//!   L1 block. These read only local state, so they run inline on the exec
//!   thread and reject before the epoch's deposits are applied.
//! - **Content (the epoch's hash and deposits), asynchronous.** This needs an
//!   L1 round trip. Doing it inline would put RPC latency in the execution
//!   path and let a slow L1 stall the chain, so it runs on a background task
//!   and records the verdict. The next epoch observes the recorded divergence
//!   and fail-stops.
//!
//! The deferred verdict is the honest cost of phase 1: a bad epoch is DETECTED
//! rather than PREVENTED, and one more epoch may be applied before the halt.
//! Preventing it is phase 2, where executors take their own L1 dependency and
//! reject inline — a real deployment change, hence its own phase.

use std::sync::Arc;

use alloy_primitives::{Address, B256};
use kardamom_engine::{EpochObserver, ExecutorError};
use kardamom_types::EpochRecord;
use kardamom_types::epoch::derive_epoch;

use crate::Divergence;
use crate::metrics;

/// What the verifier needs from L1. Mirrors the DA watcher's source trait so
/// both sides read L1 through one shape (and tests can drive a fake).
#[async_trait::async_trait]
pub trait L1EpochSource: Send + Sync + 'static {
    /// `(hash, parent_hash)` of L1 block `number`, from one round trip.
    async fn block_ids(&self, number: u64) -> anyhow::Result<(B256, B256)>;
    /// Both lockbox event kinds from one query — the same call the producer
    /// makes. Reading fewer kinds than the watcher writes would report every
    /// upgrade as a fabricated deposit.
    async fn lockbox_logs(
        &self,
        lockbox: Address,
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<kardamom_types::epoch::LockboxLog>>;
}

/// Blanket adapter over the DA watcher's `L1Source`, so the validator reads L1
/// through exactly the implementation the producer uses.
#[async_trait::async_trait]
impl<T> L1EpochSource for T
where
    T: kardamom_da_watcher::L1Source,
{
    async fn block_ids(&self, number: u64) -> anyhow::Result<(B256, B256)> {
        kardamom_da_watcher::L1Source::block_ids(self, number)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn lockbox_logs(
        &self,
        lockbox: Address,
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<kardamom_types::epoch::LockboxLog>> {
        kardamom_da_watcher::L1Source::lockbox_logs(self, lockbox, from_block, to_block)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }
}

/// Why an epoch failed verification. Every variant is a chain-level fault, not
/// a transient one — a transport error is reported separately and retried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochFault {
    /// Rule 1: the origin went backwards (or repeated).
    OriginRegressed { previous: u64, got: u64 },
    /// Rule 2: an L1 block was skipped, so its deposits are unaccounted for.
    OriginSkipped { previous: u64, got: u64 },
    /// The epoch names an L1 block whose hash is not what L1 reports —
    /// a different chain, or a fabricated epoch.
    HashMismatch { l1_number: u64 },
    /// The epoch's L1 block does not descend from the previous epoch's: block
    /// N's parent hash is not block N-1's hash. Consecutive origins must be
    /// consecutive BLOCKS, not merely consecutive numbers.
    ParentMismatch {
        l1_number: u64,
        expected_parent: B256,
        got_parent: B256,
    },
    /// Rule 4: the epoch names an L1 block that does not exist at or below
    /// finality, and still does not after the retry window. A chain cannot be
    /// anchored to an L1 block that has not happened.
    BlockBeyondFinality { l1_number: u64, attempts: u32 },
    /// The deposits do not match what L1 recorded for that block: dropped,
    /// added, altered, or reordered.
    DepositsMismatch {
        l1_number: u64,
        expected: usize,
        got: usize,
        detail: String,
    },
}

impl std::fmt::Display for EpochFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OriginRegressed { previous, got } => write!(
                f,
                "l1_origin regressed: {previous} -> {got} (derivation is ambiguous \
                 if two blocks claim different origins for the same L1)"
            ),
            Self::OriginSkipped { previous, got } => write!(
                f,
                "l1_origin skipped {} block(s): {previous} -> {got} — the deposits in \
                 between are unaccounted for",
                got.saturating_sub(*previous).saturating_sub(1)
            ),
            Self::HashMismatch { l1_number } => write!(
                f,
                "epoch for L1 block {l1_number} names a hash L1 does not report"
            ),
            Self::ParentMismatch {
                l1_number,
                expected_parent,
                got_parent,
            } => write!(
                f,
                "epoch for L1 block {l1_number} does not descend from the previous epoch: \
                 its parent is {got_parent}, the previous epoch's block was {expected_parent} \
                 — the origin sequence is numbered consecutively but is not one chain"
            ),
            Self::BlockBeyondFinality {
                l1_number,
                attempts,
            } => write!(
                f,
                "epoch names L1 block {l1_number}, which L1 still does not have after \
                 {attempts} attempts — the chain is anchored to a block that has not happened"
            ),
            Self::DepositsMismatch {
                l1_number,
                expected,
                got,
                detail,
            } => write!(
                f,
                "epoch for L1 block {l1_number} carries {got} deposit(s), L1 says \
                 {expected}: {detail}"
            ),
        }
    }
}

/// Compare an epoch against the truth derived from L1.
///
/// Split out so it is testable without a runtime or a chain: hand it what L1
/// said and what the stream carried.
pub fn compare_against_l1(
    epoch: &EpochRecord,
    l1_hash: alloy_primitives::B256,
    logs: &[kardamom_types::epoch::LockboxLog],
) -> Result<(), EpochFault> {
    if epoch.l1_hash != l1_hash {
        return Err(EpochFault::HashMismatch {
            l1_number: epoch.l1_number,
        });
    }
    // Derived through the producer's own rule: a verifier running a second,
    // subtly different copy would verify nothing.
    let truth = match derive_epoch(epoch.l1_number, l1_hash, logs) {
        Ok(t) => t,
        Err(e) => {
            return Err(EpochFault::DepositsMismatch {
                l1_number: epoch.l1_number,
                expected: logs.len(),
                got: epoch.deposits.len(),
                detail: format!("L1 logs do not derive: {e}"),
            });
        }
    };
    if truth.deposits != epoch.deposits {
        // Name the first divergent position: "3 vs 3 but different" is the
        // case a bare count comparison hides, and it is the reorder attack.
        let detail = truth
            .deposits
            .iter()
            .zip(epoch.deposits.iter())
            .position(|(a, b)| a != b)
            .map_or_else(
                || "differing length".to_string(),
                |i| format!("first difference at deposit index {i}"),
            );
        return Err(EpochFault::DepositsMismatch {
            l1_number: epoch.l1_number,
            expected: truth.deposits.len(),
            got: epoch.deposits.len(),
            detail,
        });
    }
    Ok(())
}

/// Rules 1 and 2 over consecutive origins. `previous` is `None` before the
/// first epoch.
///
/// The step out of "no origin yet" is exempt from rule 2: the producer seeds
/// its cursor at the finalized tip it first observes, so the chain's first
/// epoch is legitimately not L1 block 1. (That seed is also why a chain's
/// verifiable history starts at its first epoch — see the spec's
/// `l1_origin_genesis` edge case.)
pub fn check_sequence(previous: Option<u64>, got: u64) -> Result<(), EpochFault> {
    let Some(previous) = previous else {
        return Ok(());
    };
    if got <= previous {
        return Err(EpochFault::OriginRegressed { previous, got });
    }
    if got != previous + 1 {
        return Err(EpochFault::OriginSkipped { previous, got });
    }
    Ok(())
}

/// Engine-side seam: checks the sequence inline and queues the content check.
pub struct EpochVerifier {
    previous_origin: Option<u64>,
    divergence: Arc<Divergence>,
    tx: tokio::sync::mpsc::Sender<EpochRecord>,
}

impl EpochVerifier {
    /// Wire a verifier onto `rt`, reading L1 through `source`.
    ///
    /// The background task owns the L1 reads; the returned value is what the
    /// engine calls on the exec thread.
    pub fn spawn<S: L1EpochSource>(
        source: Arc<S>,
        lockbox: Address,
        divergence: Arc<Divergence>,
        rt: &tokio::runtime::Handle,
    ) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<EpochRecord>(EPOCH_QUEUE_CAP);
        let div = divergence.clone();
        rt.spawn(async move {
            // The last epoch that VERIFIED, as the anchor the next one must
            // descend from. Only a verified epoch becomes an anchor: chaining
            // from an unchecked hash would let one accepted lie legitimise
            // every block after it.
            let mut anchor: Option<(u64, B256)> = None;
            loop {
                let Some(epoch) = rx.recv().await else {
                    return; // engine gone
                };
                // Retry rather than one-shot. An unreachable L1 is not a
                // chain fault — halting on it would turn an RPC blip into a
                // false divergence — but giving up after one try silently
                // drops verification coverage for that epoch, which is
                // exactly where a forgery would hide. Retrying also absorbs
                // the honest race where this validator's L1 view still lags
                // the producer's by a block or two.
                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    match verify_one(source.as_ref(), lockbox, &epoch, anchor).await {
                        Ok(()) => {
                            anchor = Some((epoch.l1_number, epoch.l1_hash));
                            metrics::counter_epoch_verified();
                            break;
                        }
                        Err(VerifyOutcome::Fault(fault)) => {
                            metrics::counter_epoch_fault();
                            div.record(format!("epoch verification failed: {fault}"));
                            break;
                        }
                        Err(VerifyOutcome::Unavailable(e)) if attempt < VERIFY_ATTEMPTS => {
                            tracing::debug!(
                                l1_number = epoch.l1_number,
                                attempt,
                                error = %e,
                                "epoch verification retrying"
                            );
                            tokio::time::sleep(VERIFY_RETRY_DELAY).await;
                        }
                        Err(VerifyOutcome::Unavailable(e)) => {
                            // Out of retries. If L1 simply does not have this
                            // block, the epoch is anchored to something that
                            // never happened — rule 4, a fault. Any other
                            // transport failure stays a coverage gap, counted
                            // honestly rather than dressed up as a verdict.
                            if is_missing_block(&e) {
                                let fault = EpochFault::BlockBeyondFinality {
                                    l1_number: epoch.l1_number,
                                    attempts: attempt,
                                };
                                metrics::counter_epoch_fault();
                                div.record(format!("epoch verification failed: {fault}"));
                            } else {
                                tracing::warn!(
                                    l1_number = epoch.l1_number,
                                    attempts = attempt,
                                    error = %e,
                                    "epoch verification gave up: L1 unavailable"
                                );
                                metrics::counter_epoch_unverified();
                            }
                            break;
                        }
                    }
                }
            }
        });
        Self {
            previous_origin: None,
            divergence,
            tx,
        }
    }
}

/// Depth of the exec-thread -> verifier queue. Epochs arrive at L1 block
/// cadence (~1 per 12 s) and one content check takes at most
/// `VERIFY_ATTEMPTS * VERIFY_RETRY_DELAY` (16 s), so the queue drains faster
/// than it fills unless L1 is down for a long time. 64 entries is ~13 min of
/// epochs; past that, the exec thread drops the epoch (see
/// [`EpochObserver::observe`]) rather than block on the L1 outage.
const EPOCH_QUEUE_CAP: usize = 64;

/// How many times a content check is retried before a verdict. Spans a few L1
/// block times so an honest lag between this validator's L1 view and the
/// producer's resolves well inside it.
const VERIFY_ATTEMPTS: u32 = 8;
const VERIFY_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Distinguish "L1 does not have this block" from "L1 did not answer". The
/// first is a statement ABOUT THE CHAIN; the second is about the network.
fn is_missing_block(e: &anyhow::Error) -> bool {
    e.to_string().contains("not found")
}

/// Outcome of one content check, separating a chain fault from an L1 outage.
enum VerifyOutcome {
    Fault(EpochFault),
    Unavailable(anyhow::Error),
}

async fn verify_one<S: L1EpochSource + ?Sized>(
    source: &S,
    lockbox: Address,
    epoch: &EpochRecord,
    previous: Option<(u64, B256)>,
) -> Result<(), VerifyOutcome> {
    let (hash, parent) = source
        .block_ids(epoch.l1_number)
        .await
        .map_err(VerifyOutcome::Unavailable)?;
    // CHAIN the origins. Verifying each block in isolation lets an L1 endpoint
    // serve any hash it likes for any number; requiring block N to descend
    // from block N-1 forces it to fabricate a consistent chain instead. Costs
    // nothing — the parent hash came back in the same header. Only checked for
    // the immediate predecessor, which the sequence rules already require.
    if let Some((prev_number, prev_hash)) = previous
        && prev_number + 1 == epoch.l1_number
        && parent != prev_hash
    {
        return Err(VerifyOutcome::Fault(EpochFault::ParentMismatch {
            l1_number: epoch.l1_number,
            expected_parent: prev_hash,
            got_parent: parent,
        }));
    }
    let logs = source
        .lockbox_logs(lockbox, epoch.l1_number, epoch.l1_number)
        .await
        .map_err(VerifyOutcome::Unavailable)?;
    compare_against_l1(epoch, hash, &logs).map_err(VerifyOutcome::Fault)
}

impl EpochObserver for EpochVerifier {
    fn observe(&mut self, epoch: &EpochRecord) -> Result<(), ExecutorError> {
        // A verdict recorded by the background task lands here, on the next
        // epoch — the deferred half of phase 1.
        if self.divergence.is_halted() {
            return Err(ExecutorError::State(
                self.divergence
                    .reason()
                    .unwrap_or_else(|| "validator halted".to_string()),
            ));
        }
        // Rules 1 and 2 are local, so they reject INLINE, before this epoch's
        // deposits are applied.
        if let Err(fault) = check_sequence(self.previous_origin, epoch.l1_number) {
            metrics::counter_epoch_fault();
            self.divergence
                .record(format!("epoch verification failed: {fault}"));
            return Err(ExecutorError::State(fault.to_string()));
        }
        self.previous_origin = Some(epoch.l1_number);
        // Content check is queued with `try_send`: a full queue must not
        // wedge the exec thread, and a dropped item only costs coverage of
        // that epoch.
        use tokio::sync::mpsc::error::TrySendError;
        match self.tx.try_send(epoch.clone()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                metrics::counter_epoch_unverified();
                tracing::warn!(
                    l1_number = epoch.l1_number,
                    "epoch verifier queue full; epoch not content-checked"
                );
            }
            Err(TrySendError::Closed(_)) => {
                tracing::warn!("epoch verifier task is gone; content checks stopped");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, U256, address};
    use kardamom_types::DepositLog;
    use kardamom_types::epoch::{LockboxLog, UpgradeLog};

    use super::*;

    fn log(number: u64, hash: B256, index: u64, mint: u128) -> LockboxLog {
        LockboxLog::Deposit(DepositLog {
            block_number: number,
            block_hash: hash,
            log_index: index,
            from: address!("00000000000000000000000000000000000000A1"),
            to: address!("00000000000000000000000000000000000000B2"),
            mint,
            gas_limit: 200_000,
            data: alloy_primitives::Bytes::new(),
        })
    }

    fn upgrade(number: u64, hash: B256, index: u64, feature: u64) -> LockboxLog {
        LockboxLog::Upgrade(UpgradeLog {
            block_number: number,
            block_hash: hash,
            log_index: index,
            feature_id: U256::from(feature),
            activation_timestamp: 0,
        })
    }

    /// The failure this guards against is subtle and total: if the verifier
    /// read only `DepositInitiated` while the watcher wrote both kinds, every
    /// upgrade would look like a deposit the producer invented, and EVERY
    /// validator would fail-stop the moment the chain was first upgraded.
    #[test]
    fn an_epoch_carrying_an_upgrade_verifies() {
        let hash = B256::repeat_byte(0x12);
        let logs = vec![log(7, hash, 0, 100), upgrade(7, hash, 1, 1)];
        let epoch = derive_epoch(7, hash, &logs).unwrap();
        assert_eq!(epoch.deposits.len(), 2);
        assert!(epoch.deposits[1].is_system_transaction);
        assert_eq!(compare_against_l1(&epoch, hash, &logs), Ok(()));
    }

    #[test]
    fn a_forged_upgrade_in_the_stream_is_caught() {
        // The attack this closes: a sequencer inserting an upgrade L1 never
        // authorized. L1 has only the deposit, so the derived truth differs.
        let hash = B256::repeat_byte(0x13);
        let truth_logs = vec![log(7, hash, 0, 100)];
        let mut epoch = derive_epoch(7, hash, &truth_logs).unwrap();
        let forged = derive_epoch(7, hash, &[upgrade(7, hash, 1, 1)]).unwrap();
        epoch.deposits.push(forged.deposits[0].clone());

        let err = compare_against_l1(&epoch, hash, &truth_logs).unwrap_err();
        assert!(
            matches!(err, EpochFault::DepositsMismatch { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn a_dropped_upgrade_is_caught() {
        // The mirror attack: L1 authorized an upgrade, the stream omits it.
        let hash = B256::repeat_byte(0x14);
        let truth_logs = vec![upgrade(7, hash, 0, 1)];
        let mut epoch = derive_epoch(7, hash, &truth_logs).unwrap();
        epoch.deposits.clear();

        let err = compare_against_l1(&epoch, hash, &truth_logs).unwrap_err();
        assert!(
            matches!(err, EpochFault::DepositsMismatch { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn an_upgrade_with_tampered_payload_is_caught() {
        // Same position and count, different feature id — the case a count
        // comparison alone would wave through.
        let hash = B256::repeat_byte(0x15);
        let truth_logs = vec![upgrade(7, hash, 0, 1)];
        let epoch = derive_epoch(7, hash, &[upgrade(7, hash, 0, 999)]).unwrap();

        let err = compare_against_l1(&epoch, hash, &truth_logs).unwrap_err();
        assert!(
            matches!(err, EpochFault::DepositsMismatch { ref detail, .. }
                     if detail.contains("index 0")),
            "got {err:?}"
        );
    }

    #[test]
    fn an_honest_epoch_verifies() {
        let hash = B256::repeat_byte(0x11);
        let logs = vec![log(7, hash, 0, 100), log(7, hash, 1, 200)];
        let epoch = derive_epoch(7, hash, &logs).unwrap();
        assert_eq!(compare_against_l1(&epoch, hash, &logs), Ok(()));
    }

    #[test]
    fn a_dropped_deposit_is_caught() {
        // The censorship case: L1 recorded two, the chain carries one.
        let hash = B256::repeat_byte(0x22);
        let logs = vec![log(7, hash, 0, 100), log(7, hash, 1, 200)];
        let mut epoch = derive_epoch(7, hash, &logs).unwrap();
        epoch.deposits.pop();

        let err = compare_against_l1(&epoch, hash, &logs).unwrap_err();
        assert!(
            matches!(
                err,
                EpochFault::DepositsMismatch {
                    expected: 2,
                    got: 1,
                    ..
                }
            ),
            "{err}"
        );
    }

    #[test]
    fn an_injected_deposit_is_caught() {
        let hash = B256::repeat_byte(0x33);
        let logs = vec![log(7, hash, 0, 100)];
        let mut epoch = derive_epoch(7, hash, &logs).unwrap();
        epoch.deposits.push(epoch.deposits[0].clone());

        let err = compare_against_l1(&epoch, hash, &logs).unwrap_err();
        assert!(
            matches!(
                err,
                EpochFault::DepositsMismatch {
                    expected: 1,
                    got: 2,
                    ..
                }
            ),
            "{err}"
        );
    }

    #[test]
    fn a_reordered_epoch_is_caught_despite_matching_counts() {
        // The case a count comparison misses: same deposits, wrong order.
        // Order is consensus — it decides execution results.
        let hash = B256::repeat_byte(0x44);
        let logs = vec![log(7, hash, 0, 100), log(7, hash, 1, 200)];
        let mut epoch = derive_epoch(7, hash, &logs).unwrap();
        epoch.deposits.swap(0, 1);

        let err = compare_against_l1(&epoch, hash, &logs).unwrap_err();
        match err {
            EpochFault::DepositsMismatch {
                expected,
                got,
                detail,
                ..
            } => {
                assert_eq!((expected, got), (2, 2), "counts match; order does not");
                assert!(detail.contains("index 0"), "{detail}");
            }
            other => panic!("expected a deposits mismatch, got {other}"),
        }
    }

    #[test]
    fn a_tampered_amount_is_caught() {
        let hash = B256::repeat_byte(0x55);
        let logs = vec![log(7, hash, 0, 100)];
        let mut epoch = derive_epoch(7, hash, &logs).unwrap();
        epoch.deposits[0].mint += 1_000_000;

        assert!(compare_against_l1(&epoch, hash, &logs).is_err());
    }

    #[test]
    fn an_epoch_naming_the_wrong_l1_block_is_caught() {
        let hash = B256::repeat_byte(0x66);
        let logs = vec![log(7, hash, 0, 100)];
        let epoch = derive_epoch(7, hash, &logs).unwrap();

        let err = compare_against_l1(&epoch, B256::repeat_byte(0x99), &logs).unwrap_err();
        assert_eq!(err, EpochFault::HashMismatch { l1_number: 7 });
    }

    #[test]
    fn an_empty_epoch_verifies_and_an_invented_deposit_in_one_does_not() {
        // Empty epochs are normal and must pass; they are also where a
        // fabricated deposit would be easiest to hide.
        let hash = B256::repeat_byte(0x77);
        let epoch = derive_epoch(9, hash, &[]).unwrap();
        assert_eq!(compare_against_l1(&epoch, hash, &[]), Ok(()));

        let mut forged = epoch.clone();
        forged.deposits.push(kardamom_types::Deposit {
            source_hash: B256::repeat_byte(0xEE),
            mint: 1,
            ..Default::default()
        });
        assert!(compare_against_l1(&forged, hash, &[]).is_err());
    }

    #[test]
    fn sequence_rules() {
        // First epoch: anything goes — the producer seeds at the finalized tip.
        assert_eq!(check_sequence(None, 500), Ok(()));
        // Consecutive is the only accepted step.
        assert_eq!(check_sequence(Some(500), 501), Ok(()));
        assert_eq!(
            check_sequence(Some(500), 500),
            Err(EpochFault::OriginRegressed {
                previous: 500,
                got: 500
            })
        );
        assert_eq!(
            check_sequence(Some(500), 499),
            Err(EpochFault::OriginRegressed {
                previous: 500,
                got: 499
            })
        );
        // A skip means the deposits in between are unaccounted for.
        assert_eq!(
            check_sequence(Some(500), 502),
            Err(EpochFault::OriginSkipped {
                previous: 500,
                got: 502
            })
        );
    }

    #[test]
    fn skipped_origin_message_counts_the_missing_blocks() {
        let f = EpochFault::OriginSkipped {
            previous: 500,
            got: 505,
        };
        assert!(f.to_string().contains("skipped 4 block(s)"), "{f}");
    }

    #[test]
    fn a_missing_block_is_told_apart_from_an_unreachable_l1() {
        // The distinction the retry loop's verdict hinges on: "L1 does not
        // have this block" is a statement about the CHAIN (rule 4, a fault);
        // "L1 did not answer" is about the network (a coverage gap).
        assert!(is_missing_block(&anyhow::anyhow!(
            "L1 provider error: finalized L1 block 52 not found"
        )));
        assert!(!is_missing_block(&anyhow::anyhow!(
            "L1 provider error: connection refused"
        )));
        assert!(!is_missing_block(&anyhow::anyhow!("timed out")));
    }

    #[test]
    fn beyond_finality_message_names_the_block_and_the_effort() {
        let f = EpochFault::BlockBeyondFinality {
            l1_number: 52,
            attempts: 8,
        };
        let m = f.to_string();
        assert!(m.contains("52") && m.contains("8 attempts"), "{m}");
    }

    /// A fake L1 whose blocks are whatever the test says they are — the shape
    /// a lying or buggy endpoint takes.
    struct FakeL1 {
        blocks: std::collections::BTreeMap<u64, (B256, B256)>,
    }

    #[async_trait::async_trait]
    impl L1EpochSource for FakeL1 {
        async fn block_ids(&self, number: u64) -> anyhow::Result<(B256, B256)> {
            self.blocks
                .get(&number)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("finalized L1 block {number} not found"))
        }
        async fn lockbox_logs(
            &self,
            _lockbox: Address,
            _from: u64,
            _to: u64,
        ) -> anyhow::Result<Vec<LockboxLog>> {
            Ok(Vec::new())
        }
    }

    fn lockbox() -> Address {
        address!("0000000000000000000000000000000000C0DE01")
    }

    #[tokio::test]
    async fn a_properly_chained_pair_of_epochs_verifies() {
        let (h7, h8) = (B256::repeat_byte(0x77), B256::repeat_byte(0x88));
        let l1 = FakeL1 {
            blocks: [(7, (h7, B256::repeat_byte(0x66))), (8, (h8, h7))]
                .into_iter()
                .collect(),
        };
        let e7 = derive_epoch(7, h7, &[]).unwrap();
        let e8 = derive_epoch(8, h8, &[]).unwrap();

        assert!(verify_one(&l1, lockbox(), &e7, None).await.is_ok());
        assert!(verify_one(&l1, lockbox(), &e8, Some((7, h7))).await.is_ok());
    }

    #[tokio::test]
    async fn an_epoch_that_does_not_descend_from_its_predecessor_is_caught() {
        // The lie the per-block check CANNOT see: block 8 exists, its hash
        // matches what the epoch claims, and its deposits match — but it is
        // not built on block 7. Numbered consecutively, not one chain.
        let (h7, h8) = (B256::repeat_byte(0x77), B256::repeat_byte(0x88));
        let orphan_parent = B256::repeat_byte(0xEE);
        let l1 = FakeL1 {
            blocks: [(8, (h8, orphan_parent))].into_iter().collect(),
        };
        let e8 = derive_epoch(8, h8, &[]).unwrap();

        // Without an anchor the epoch passes — nothing to chain against.
        assert!(verify_one(&l1, lockbox(), &e8, None).await.is_ok());

        // With one, the break is caught.
        let err = verify_one(&l1, lockbox(), &e8, Some((7, h7)))
            .await
            .unwrap_err();
        match err {
            VerifyOutcome::Fault(EpochFault::ParentMismatch {
                l1_number,
                expected_parent,
                got_parent,
            }) => {
                assert_eq!(l1_number, 8);
                assert_eq!(expected_parent, h7);
                assert_eq!(got_parent, orphan_parent);
            }
            VerifyOutcome::Fault(other) => panic!("wrong fault: {other}"),
            VerifyOutcome::Unavailable(e) => panic!("expected a fault, got {e}"),
        }
    }

    #[tokio::test]
    async fn chaining_is_skipped_across_a_gap_in_the_anchor() {
        // After a deferred (unverified) epoch the anchor goes stale, so the
        // next epoch is not the anchor's successor. Chaining must SKIP rather
        // than report a false parent mismatch — the sequence rules already
        // reject genuine gaps, and inventing a divergence here would halt a
        // healthy validator over an L1 blip.
        let h9 = B256::repeat_byte(0x99);
        let l1 = FakeL1 {
            blocks: [(9, (h9, B256::repeat_byte(0xAB)))].into_iter().collect(),
        };
        let e9 = derive_epoch(9, h9, &[]).unwrap();

        // Anchor is block 7, this is block 9 — not adjacent, so no chain check.
        assert!(
            verify_one(&l1, lockbox(), &e9, Some((7, B256::repeat_byte(0x77))))
                .await
                .is_ok()
        );
    }

    #[test]
    fn parent_mismatch_message_names_both_hashes() {
        let f = EpochFault::ParentMismatch {
            l1_number: 8,
            expected_parent: B256::repeat_byte(0x77),
            got_parent: B256::repeat_byte(0xEE),
        };
        let m = f.to_string();
        assert!(m.contains("8") && m.contains("not one chain"), "{m}");
    }
}
