//! The `tx_data` source: the M lane readers, the join buffer keyed by
//! `(lane, session, position)`, the archive refetch of a join miss, the
//! peer step, and the vote to void an entry that every source refuses.

use tracing::{info, warn};

use kardamom_types::{TxEnvelope, TxRef, VoidRecord};

use crate::error::ExecutorError;

use super::join::{JoinBuffer, JoinOutcome, JoinWait, OwnTail, ReaderConfig};
use super::peer_fetch::{Expected, FetchedRecords, PeerFetch, PeerFetchInputs};
use super::ports::{
    ExecRecordReplay, ExecRecordsFrom, JoinRecovery, JoinRecoveryFactory, TxDataSubscription,
    TxOrderingSubscription,
};
use super::source::{JoinAt, JoinSeed, Joined, SourceStart, TxJoin, TxSource};
use super::threads::TxDataReader;
use super::void::{ParkOutcome, ParkPlan, VoidPark};

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

    fn build(self, cfg: &ReaderConfig) -> TxDataJoin {
        let mut recovery = self.recovery.map(JoinRecoveryFactory::build);
        let fetched = cfg
            .own_tail
            .as_ref()
            .zip(recovery.as_mut())
            .map(|(tail, replay)| TxDataJoin::preload(tail, replay))
            .unwrap_or_default();
        TxDataJoin {
            buffer: self.buffer,
            recovery,
            fetched,
            last_warn_len: 0,
        }
    }
}

/// The `tx_data` join on the reader thread.
pub struct TxDataJoin {
    buffer: JoinBuffer,
    recovery: Option<JoinRecovery>,
    /// Executor stream records that a replay delivered ahead of the
    /// reader: the tail of an earlier run of this executor, or the records
    /// after an entry that a peer served. Each one is checked at its turn.
    pub(super) fetched: FetchedRecords,
    last_warn_len: usize,
}

impl TxJoin for TxDataJoin {
    fn join<O: TxOrderingSubscription>(
        &mut self,
        at: JoinAt<'_, O>,
    ) -> Result<Joined, ExecutorError> {
        if let Some(env) = self.take_fetched(at.position.as_index(), *at.tx_ref) {
            return Ok(self.joined(env, at.cfg));
        }
        let wait = JoinWait::new(&self.buffer, &mut self.recovery, at.tx_ref, at.cfg)?;
        match wait.run() {
            JoinOutcome::Joined(env) => Ok(self.joined(env, at.cfg)),
            JoinOutcome::Unjoinable => self.on_unjoined(at, true),
            JoinOutcome::TimedOut => self.on_unjoined(at, false),
        }
    }
}

impl ReaderConfig {
    /// Send one fact to the answers state, when the executor serves one.
    pub(super) fn tell_answers(&self, tell: impl FnOnce(&kardamom_state::ExecAnswersFeed)) {
        if let Some(answers) = &self.exec_answers {
            tell(answers);
        }
    }
}

impl TxDataJoin {
    /// Replay the tail of an earlier run from this executor's own archive:
    /// the records that it joined and published above the resume index
    /// but did not commit. A failed replay costs only the preload: the
    /// joins then use the live stream, the archives and the peers.
    fn preload(tail: &OwnTail, replay: &mut impl ExecRecordReplay) -> FetchedRecords {
        let mut records = Vec::new();
        let from = ExecRecordsFrom {
            archive_id: &tail.archive_id,
            session_id: tail.session_id,
            position: tail.position,
        };
        let mut fetched = FetchedRecords::default();
        match replay.replay_exec_records(from, |record| records.push(record)) {
            Ok(delivered) => {
                fetched.keep(tail.from_index, records);
                info!(
                    target: "kardamom_executor::reader",
                    session_id = tail.session_id,
                    from_index = tail.from_index,
                    delivered,
                    kept = fetched.len(),
                    "own tail: replayed the earlier run's records from this executor's archive"
                );
            }
            Err(e) => warn!(
                target: "kardamom_executor::reader",
                session_id = tail.session_id,
                from_index = tail.from_index,
                error = %e,
                "own tail: the replay failed; the joins go on without it"
            ),
        }
        fetched
    }

    /// The envelope of the fetched record at `index`, when one is kept and
    /// it passes the check against the canonical reference.
    fn take_fetched(&mut self, index: u64, tx_ref: TxRef) -> Option<TxEnvelope> {
        let record = self.fetched.take(index)?;
        Expected { index, tx_ref }
            .check(record)
            .inspect_err(|reason| {
                warn!(
                    target: "kardamom_executor::reader",
                    index,
                    tx_hash = ?tx_ref.tx_hash,
                    reason,
                    "a fetched record does not match the reference; joining the entry anew"
                );
            })
            .ok()
    }

    /// A joined envelope, after the buffer growth check.
    fn joined(&mut self, env: TxEnvelope, cfg: &ReaderConfig) -> Joined {
        self.warn_on_buffer_growth(cfg);
        Joined::Tx(env)
    }

    /// An entry whose `tx_data` every archive failed. The reader parks: it
    /// asks its peer executors first, see [`PeerFetch`]. A peer's archive
    /// that serves the entry joins it. A voter votes to void the entry
    /// only when every archive refused the range and every peer answered
    /// `not_held`; see [`VoidPark`]. The vote names the entry by its
    /// canonical index, which is the position the cluster subscription
    /// delivers. A reader with no peer that may not vote stops, as before
    /// the peer step.
    pub(super) fn on_unjoined<O: TxOrderingSubscription>(
        &mut self,
        at: JoinAt<'_, O>,
        every_archive_refused: bool,
    ) -> Result<Joined, ExecutorError> {
        let JoinAt {
            tx_ref,
            position,
            cfg,
            order,
            backlog,
        } = at;
        let voter_id = cfg.voter_id.filter(|_| every_archive_refused);
        if voter_id.is_none() && cfg.exec_peers.is_empty() {
            return Err(Self::join_timeout(cfg, tx_ref, every_archive_refused));
        }
        let void = VoidRecord {
            index: position.as_index(),
            tx_hash: tx_ref.tx_hash,
        };
        info!(
            target: "kardamom_executor::reader",
            index = void.index,
            tx_hash = ?void.tx_hash,
            every_archive_refused,
            "every tx_data source failed the entry: parking to ask the peers"
        );
        cfg.tell_answers(|answers| answers.parked(void.index));
        let plan = ParkPlan {
            voter_id,
            void,
            wait: cfg.void_wait,
            peers: PeerFetch::new(PeerFetchInputs {
                peers: &cfg.exec_peers,
                replay: self.recovery.as_mut(),
                fetched: &mut self.fetched,
                expected: Expected {
                    index: void.index,
                    tx_ref: *tx_ref,
                },
            }),
        };
        match VoidPark::new(order, backlog, plan).run() {
            Ok(ParkOutcome::Voided) => Ok(Joined::Voided),
            Ok(ParkOutcome::Fetched(envelope)) => {
                cfg.tell_answers(|answers| answers.fetched(void.index));
                Ok(Joined::Tx(envelope))
            }
            Ok(ParkOutcome::Lost { block }) => Err(ExecutorError::PeerRecordLost {
                index: void.index,
                tx_hash: void.tx_hash,
                block,
            }),
            Ok(ParkOutcome::GaveUp) => Err(Self::join_timeout(cfg, tx_ref, every_archive_refused)),
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
