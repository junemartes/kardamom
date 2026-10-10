//! The peer step of the join path: after every `tx_data` archive failed
//! for an entry, ask the peer executors where the entry is, and replay it
//! from the archive of a peer that holds it.
//!
//! The rule:
//!
//! - A peer that joined the entry answers `located`: the archive that
//!   records its executor stream, the session, and a position at or before
//!   the record. The reader replays that archive from there and checks the
//!   record. A good record joins the entry, and the reader sends no vote.
//! - A peer that reached the entry and holds no record answers `not_held`.
//!   That answer is final for this park.
//! - A peer that executed the entry and holds no record of it answers
//!   `lost`. That answer is final for this park, and it is never "absent":
//!   the reader sends no vote while it stands.
//! - A peer that has not reached the entry, gives no answer, or names a
//!   record that the replay does not deliver or that fails the check, can
//!   still serve the entry. The reader asks it again, and sends no vote
//!   while such a peer is left. Only the peer's own answer is final: a
//!   `located` answer whose archive holds no byte of the range is no
//!   answer. The peer's node can die between the locator and the record,
//!   and its restart answers `not_held` or `lost` itself.
//!
//! A peer answer never makes a reader skip an entry. It only chooses
//! between "execute the fetched bytes", "vote" and "repair". The sealer
//! still voids an entry only on the vote of every voter, and an executor
//! that joined the entry never votes.
//!
//! The reader executes a fetched record only when it is the entry: the
//! index and the reference equal the canonical ones, the keccak of the raw
//! transaction equals the reference's hash, and the signature recovers the
//! sender. A replay delivers the records after the entry too. The reader
//! keeps them by canonical index and checks each one at its own turn.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use tracing::{info, warn};

use kardamom_state::{ExecLocatorAnswer, ExecPeer, ExecPeers};
use kardamom_types::{ExecTxRecord, TxEnvelope, TxRef};

use crate::metrics::PEER_FETCH_TOTAL;

use super::ports::{ExecRecordReplay, ExecRecordsFrom};

/// The interval between two rounds of asks to the peers that gave no
/// final answer.
const REASK_INTERVAL: Duration = Duration::from_secs(1);

/// The most records that a reader keeps ahead of its position. It equals
/// the sealer's default void window. A record past the bound comes again
/// from a later replay.
pub(super) const MAX_FETCHED: usize = 65_536;

/// The canonical entry that a fetched record must be.
#[derive(Clone, Copy, Debug)]
pub(super) struct Expected {
    pub(super) index: u64,
    pub(super) tx_ref: TxRef,
}

impl Expected {
    /// The envelope of `record`, when the record is this entry.
    ///
    /// # Errors
    ///
    /// Returns the reason when the record is another entry, or when its
    /// bytes or its sender do not match.
    pub(super) fn check(&self, record: ExecTxRecord) -> Result<TxEnvelope, String> {
        let same_entry = record.index == self.index && record.tx_ref == self.tx_ref;
        let same_hash = record.envelope.tx_hash == self.tx_ref.tx_hash;
        match (same_entry, same_hash) {
            (false, _) => Err(format!(
                "the record is index {} with hash {}",
                record.index, record.tx_ref.tx_hash
            )),
            (true, false) => Err(format!(
                "the envelope carries hash {}",
                record.envelope.tx_hash
            )),
            (true, true) => crate::stateless::verify_record_identity(&record.envelope)
                .map(|()| record.envelope)
                .map_err(|e| e.to_string()),
        }
    }
}

/// Records that a replay delivered ahead of the reader, by canonical
/// index. Nothing in it is checked yet.
#[derive(Default)]
pub(super) struct FetchedRecords(BTreeMap<u64, ExecTxRecord>);

impl FetchedRecords {
    /// Keep the records at or above `from`, up to [`MAX_FETCHED`].
    pub(super) fn keep(&mut self, from: u64, records: impl IntoIterator<Item = ExecTxRecord>) {
        let room = MAX_FETCHED.saturating_sub(self.0.len());
        records
            .into_iter()
            .filter(|r| r.index >= from)
            .take(room)
            .for_each(|r| {
                self.0.insert(r.index, r);
            });
    }

    /// The record at `index`, if one is kept. Every record below `index`
    /// goes: the reader passed it.
    pub(super) fn take(&mut self, index: u64) -> Option<ExecTxRecord> {
        self.0 = self.0.split_off(&index);
        self.0.remove(&index)
    }

    /// The count of kept records.
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }
}

/// What one peer answered, after the replay and the check.
enum Asked {
    Located(TxEnvelope),
    NotHeld,
    NotReached,
    Lost,
    Unreachable,
    Mismatch,
}

impl Asked {
    /// The `outcome` label of [`PEER_FETCH_TOTAL`].
    fn label(&self) -> &'static str {
        match self {
            Self::Located(_) => "located",
            Self::NotHeld => "not_held",
            Self::NotReached => "not_reached",
            Self::Lost => "lost",
            Self::Unreachable => "unreachable",
            Self::Mismatch => "mismatch",
        }
    }
}

/// The state of the peer step at one moment.
pub(super) enum PeerPoll {
    /// A peer's archive served a good record.
    Fetched(TxEnvelope),
    /// Every peer answered `not_held`.
    NoneHolds,
    /// Every peer gave a final answer, and at least one of them executed
    /// the entry and lost its record.
    Lost,
    /// A peer is left without a final answer.
    Waiting,
}

/// The asks for one entry. `R` replays a peer's archive.
pub(super) struct PeerFetch<'a, R> {
    peers: &'a ExecPeers,
    /// `None` for a reader with no archive replay: a `located` answer then
    /// counts as no answer.
    replay: Option<&'a mut R>,
    fetched: &'a mut FetchedRecords,
    /// The peers without a final answer.
    pending: Vec<&'a ExecPeer>,
    /// The peers that answered `lost`.
    lost: Vec<&'a ExecPeer>,
    expected: Expected,
    next_ask: Instant,
    pub(super) reask_after: Duration,
    waiting_logged: bool,
    /// The outcome label of each ask, in order, for the vote line.
    answers: Vec<&'static str>,
}

/// What [`PeerFetch::new`] takes.
pub(super) struct PeerFetchInputs<'a, R> {
    pub(super) peers: &'a ExecPeers,
    pub(super) replay: Option<&'a mut R>,
    pub(super) fetched: &'a mut FetchedRecords,
    pub(super) expected: Expected,
}

impl<'a, R: ExecRecordReplay> PeerFetch<'a, R> {
    /// The asks for one entry. With no peer configured, the step is over
    /// at once.
    pub(super) fn new(inputs: PeerFetchInputs<'a, R>) -> Self {
        Self {
            peers: inputs.peers,
            replay: inputs.replay,
            fetched: inputs.fetched,
            pending: inputs.peers.peers().iter().collect(),
            lost: Vec::new(),
            expected: inputs.expected,
            next_ask: Instant::now(),
            reask_after: REASK_INTERVAL,
            waiting_logged: false,
            answers: Vec::new(),
        }
    }

    /// Ask the peers without a final answer, when a round is due.
    pub(super) fn poll(&mut self, now: Instant) -> PeerPoll {
        if let Some(settled) = self.settled() {
            return settled;
        }
        if now < self.next_ask {
            return PeerPoll::Waiting;
        }
        // An interval too long for the clock makes every step a round.
        self.next_ask = now.checked_add(self.reask_after).unwrap_or(now);
        if let Some(envelope) = self.round() {
            return PeerPoll::Fetched(envelope);
        }
        self.settled().unwrap_or_else(|| {
            self.log_waiting();
            PeerPoll::Waiting
        })
    }

    /// The final state, when every peer gave a final answer.
    fn settled(&self) -> Option<PeerPoll> {
        if !self.pending.is_empty() {
            return None;
        }
        Some(if self.lost.is_empty() {
            PeerPoll::NoneHolds
        } else {
            PeerPoll::Lost
        })
    }

    /// The peers that gave no final answer, for the give-up line.
    pub(super) fn unanswered(&self) -> Vec<&str> {
        self.pending.iter().map(|peer| peer.url()).collect()
    }

    /// The peers that answered `lost`.
    pub(super) fn lost(&self) -> Vec<&str> {
        self.lost.iter().map(|peer| peer.url()).collect()
    }

    /// The outcome label of each ask so far, in order.
    pub(super) fn answers(&self) -> &[&'static str] {
        &self.answers
    }

    /// Ask each pending peer once, in order, until one serves the entry. A
    /// peer with a final answer leaves the pending list.
    fn round(&mut self) -> Option<TxEnvelope> {
        let mut again = Vec::new();
        let fetched = std::mem::take(&mut self.pending)
            .into_iter()
            .find_map(|peer| match self.ask(peer) {
                Asked::Located(envelope) => Some((peer, envelope)),
                Asked::NotHeld => None,
                Asked::Lost => {
                    self.lost.push(peer);
                    None
                }
                Asked::NotReached | Asked::Unreachable | Asked::Mismatch => {
                    again.push(peer);
                    None
                }
            });
        self.pending = again;
        let (peer, envelope) = fetched?;
        info!(
            target: "kardamom_executor::reader",
            peer = peer.url(),
            index = self.expected.index,
            tx_hash = ?self.expected.tx_ref.tx_hash,
            kept_ahead = self.fetched.len(),
            "peer fetch: a peer's archive holds the entry; joining it"
        );
        Some(envelope)
    }

    /// Ask `peer` once, replay and check what it names, and count the
    /// outcome.
    fn ask(&mut self, peer: &ExecPeer) -> Asked {
        let answer = peer.ask(
            self.expected.index,
            self.expected.tx_ref.tx_hash,
            self.peers.timeout(),
        );
        let asked = match answer {
            Ok(ExecLocatorAnswer::Located {
                archive_id,
                session_id,
                position,
            }) => self.fetch(
                peer,
                ExecRecordsFrom {
                    archive_id: &archive_id,
                    session_id,
                    position,
                },
            ),
            Ok(ExecLocatorAnswer::NotHeld) => Asked::NotHeld,
            Ok(ExecLocatorAnswer::NotReached) => Asked::NotReached,
            Ok(ExecLocatorAnswer::Lost) => Asked::Lost,
            Err(e) => {
                self.warn(peer, &e.to_string(), "peer fetch: the peer gave no answer");
                Asked::Unreachable
            }
        };
        metrics::counter!(PEER_FETCH_TOTAL, "outcome" => asked.label()).increment(1);
        self.answers.push(asked.label());
        asked
    }

    /// Replay the peer's archive from `from`, keep the records after the
    /// entry, and check the record of the entry. A replay that fails, also
    /// one that the archive refuses, is no answer: the reader asks the
    /// peer again.
    fn fetch(&mut self, peer: &ExecPeer, from: ExecRecordsFrom<'_>) -> Asked {
        let Some(replay) = self.replay.as_deref_mut() else {
            self.warn(
                peer,
                "no archive replay is wired",
                "peer fetch: cannot replay",
            );
            return Asked::Unreachable;
        };
        let mut records = Vec::new();
        let replayed = replay.replay_exec_records(from, |record| records.push(record));
        match replayed {
            Ok(_) => self.take_entry(peer, records),
            Err(e) => {
                self.warn(peer, &e.to_string(), "peer fetch: the replay failed");
                Asked::Unreachable
            }
        }
    }

    /// The checked record of the entry among `records`. The records after
    /// the entry stay for their own turn.
    fn take_entry(&mut self, peer: &ExecPeer, records: Vec<ExecTxRecord>) -> Asked {
        let index = self.expected.index;
        let (entry, rest): (Vec<ExecTxRecord>, Vec<ExecTxRecord>) =
            records.into_iter().partition(|r| r.index == index);
        // `u64::MAX` is no canonical index, so saturation loses no record.
        self.fetched.keep(index.saturating_add(1), rest);
        let mut reasons = Vec::new();
        let expected = self.expected;
        let good = entry.into_iter().find_map(|record| {
            expected
                .check(record)
                .map_err(|reason| reasons.push(reason))
                .ok()
        });
        if let Some(envelope) = good {
            return Asked::Located(envelope);
        }
        if reasons.is_empty() {
            self.warn(
                peer,
                "no record of the entry",
                "peer fetch: the replay missed the entry",
            );
            return Asked::Unreachable;
        }
        self.warn(
            peer,
            &reasons.join("; "),
            "peer fetch: the peer's record does not match the reference",
        );
        Asked::Mismatch
    }

    fn warn(&self, peer: &ExecPeer, reason: &str, message: &str) {
        warn!(
            target: "kardamom_executor::reader",
            peer = peer.url(),
            index = self.expected.index,
            tx_hash = ?self.expected.tx_ref.tx_hash,
            reason,
            "{message}"
        );
    }

    /// Say once that the reader waits for a peer.
    fn log_waiting(&mut self) {
        if self.waiting_logged {
            return;
        }
        self.waiting_logged = true;
        warn!(
            target: "kardamom_executor::reader",
            index = self.expected.index,
            tx_hash = ?self.expected.tx_ref.tx_hash,
            unanswered = ?self.unanswered(),
            "peer fetch: a peer gave no final answer; no vote while it is unanswered"
        );
    }
}
