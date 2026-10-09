//! The `tx_data` source: the M lane readers, the join buffer keyed by
//! `(lane, session, position)`, the archive refetch of a join miss, and
//! the vote to void an entry that every archive refuses.

use tracing::warn;

use kardamom_types::{TxRef, VoidRecord};

use crate::error::ExecutorError;

use super::join::{JoinBuffer, JoinOutcome, JoinWait, ReaderConfig};
use super::ports::{JoinRecovery, JoinRecoveryFactory, TxDataSubscription, TxOrderingSubscription};
use super::source::{JoinAt, JoinSeed, Joined, SourceStart, TxJoin, TxSource};
use super::threads::TxDataReader;
use super::void::{ParkOutcome, VoidPark};

/// The `tx_data` source: one subscription per lane, and the optional
/// archive refetch of a join miss.
pub struct TxDataSource<D> {
    subs: Vec<D>,
    recovery: Option<JoinRecoveryFactory>,
}

impl<D> TxDataSource<D> {
    /// The source over `subs`, in any lane order: each subscription names
    /// its own lane. `recovery`, when wired, turns a join miss into an
    /// archive refetch instead of an immediate stop.
    #[must_use]
    pub fn new(subs: Vec<D>, recovery: Option<JoinRecoveryFactory>) -> Self {
        Self { subs, recovery }
    }
}

impl<D: TxDataSubscription + 'static> TxSource for TxDataSource<D> {
    type Seed = TxDataSeed;

    fn start(self) -> SourceStart<TxDataSeed> {
        let buffer = JoinBuffer::new();
        let feeds = self
            .subs
            .into_iter()
            .map(|sub| TxDataReader::new(sub, buffer.clone()).spawn())
            .collect();
        SourceStart {
            feeds,
            seed: TxDataSeed {
                buffer,
                recovery: self.recovery,
            },
        }
    }
}

/// The join buffer and the recovery factory, on their way to the reader
/// thread.
pub struct TxDataSeed {
    pub(crate) buffer: JoinBuffer,
    pub(crate) recovery: Option<JoinRecoveryFactory>,
}

impl JoinSeed for TxDataSeed {
    type Join = TxDataJoin;

    fn build(self) -> TxDataJoin {
        TxDataJoin {
            buffer: self.buffer,
            recovery: self.recovery.map(JoinRecoveryFactory::build),
            last_warn_len: 0,
        }
    }
}

/// The `tx_data` join on the reader thread.
pub struct TxDataJoin {
    buffer: JoinBuffer,
    recovery: Option<JoinRecovery>,
    last_warn_len: usize,
}

impl TxJoin for TxDataJoin {
    fn join<O: TxOrderingSubscription>(
        &mut self,
        at: JoinAt<'_, O>,
    ) -> Result<Joined, ExecutorError> {
        let wait = JoinWait::new(&self.buffer, &mut self.recovery, at.tx_ref, at.cfg)?;
        match wait.run() {
            JoinOutcome::Joined(env) => {
                self.warn_on_buffer_growth(at.cfg);
                Ok(Joined::Tx(env))
            }
            JoinOutcome::Unjoinable => Self::on_unjoinable(at),
            JoinOutcome::TimedOut => Err(Self::join_timeout(at.cfg, at.tx_ref, false)),
        }
    }
}

impl TxDataJoin {
    /// An entry whose `tx_data` every archive refused. A voter asks the
    /// sealer to void it and waits for the void record; see [`VoidPark`].
    /// The vote names the entry by its canonical index, which is the
    /// position the cluster subscription delivers. A consumer that is no
    /// voter stops, as it did before the void rule.
    pub(super) fn on_unjoinable<O: TxOrderingSubscription>(
        at: JoinAt<'_, O>,
    ) -> Result<Joined, ExecutorError> {
        let JoinAt {
            tx_ref,
            position,
            cfg,
            order,
            backlog,
        } = at;
        let Some(voter_id) = cfg.voter_id else {
            return Err(Self::join_timeout(cfg, tx_ref, true));
        };
        let void = VoidRecord {
            index: position.as_index(),
            tx_hash: tx_ref.tx_hash,
        };
        let park = VoidPark::new(order, backlog, voter_id, void, cfg.void_wait);
        match park.run() {
            Ok(ParkOutcome::Voided) => Ok(Joined::Voided),
            Ok(ParkOutcome::GaveUp) => Err(Self::join_timeout(cfg, tx_ref, true)),
            Err(ExecutorError::TxOrderingClosed) => Ok(Joined::Closed),
            Err(e) => Err(e),
        }
    }

    /// Log a join that used its whole budget, and make its error. The line
    /// says whether every archive refused the range: then the data is gone
    /// and a restart meets the same entry again, otherwise an archive was
    /// unreachable and a restart can still recover.
    fn join_timeout(
        cfg: &ReaderConfig,
        tx_ref: &TxRef,
        every_archive_refused: bool,
    ) -> ExecutorError {
        // `Duration::as_millis` already returns `u128`, so this needs no
        // fallible narrowing to a smaller integer.
        let timeout_ms = cfg.join_timeout.as_millis();
        warn!(
            target: "kardamom_executor::reader",
            sequencer_id = tx_ref.shard_id,
            session_id = tx_ref.tx_data_session_id,
            tx_data_position = ?tx_ref.tx_data_position,
            timeout_ms,
            every_archive_refused,
            "join timeout: TxRef has no envelope on tx_data (archive refetch exhausted); aborting"
        );
        ExecutorError::JoinTimeout {
            sequencer_id: tx_ref.shard_id,
            tx_data_position: tx_ref.tx_data_position,
            timeout_ms,
        }
    }

    /// Periodic warning. If the join buffer keeps growing, either the
    /// `tx_data` publisher is racing far ahead of `tx_ordering`, a
    /// back-pressure issue, or there is a leak.
    fn warn_on_buffer_growth(&mut self, cfg: &ReaderConfig) {
        let cur = self.buffer.len();
        if cur >= cfg.buffer_warn_threshold && cur > self.last_warn_len * 2 {
            warn!(
                target: "kardamom_executor::reader",
                join_buffer_len = cur,
                threshold = cfg.buffer_warn_threshold,
                "join buffer growth: tx_data publisher likely outrunning tx_ordering"
            );
            self.last_warn_len = cur;
        }
    }
}
