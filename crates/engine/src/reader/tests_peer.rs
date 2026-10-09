//! The peer step of the join path: a reader whose archives all failed for
//! an entry asks its peer executors, and replays the entry from a peer's
//! archive, before it votes.
//!
//! Each fake peer is a real HTTP listener on loopback that answers from a
//! script, so the tests run the production client end to end. A fake
//! archive replays records from memory.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_log::error::LogError;
use kardamom_state::{ExecLocatorAnswer, ExecPeer, ExecPeers};
use kardamom_types::{
    BPosition, BlockBoundaryStart, ExecTxRecord, TxEnvelope, TxOrderingMessage, TxRef, VoidRecord,
};

use super::join::TxDataKey;
use super::peer_fetch::{Expected, FetchedRecords, PeerFetch, PeerFetchInputs, PeerPoll};
use super::ports::{ExecRecordReplay, ExecRecordsFrom, JoinRecoveryError};
use super::tests::pos;
use super::tests_void::{LOST, VotingSub, boundary, reader, slots, tx_ref, void, voter_cfg};
use super::threads::Flow;
use super::void::{ParkOutcome, ParkPlan, ReadAhead, VoidPark};
use super::*;
use crate::error::ExecutorError;

/// One scripted reply of a fake peer.
#[derive(Clone)]
enum Reply {
    Answer(ExecLocatorAnswer),
    /// Close the connection with no reply: the peer gave no answer.
    Silent,
}

/// A fake peer executor. It answers each request with the next reply of
/// its script, and repeats the last reply after the script ends.
struct FakePeer {
    url: String,
    asked: Arc<AtomicUsize>,
}

impl FakePeer {
    fn start(script: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let asked = Arc::new(AtomicUsize::new(0));
        let counter = asked.clone();
        let script = Mutex::new(VecDeque::from(script));
        std::thread::spawn(move || {
            listener
                .incoming()
                .map_while(Result::ok)
                .for_each(|stream| Self::serve(stream, &script, &counter));
        });
        Self { url, asked }
    }

    fn serve(
        mut stream: std::net::TcpStream,
        script: &Mutex<VecDeque<Reply>>,
        asked: &AtomicUsize,
    ) {
        Self::read_request(&mut stream);
        asked.fetch_add(1, Ordering::SeqCst);
        let reply = {
            let mut script = script.lock().unwrap();
            if script.len() > 1 {
                script.pop_front().unwrap()
            } else {
                script.front().cloned().unwrap()
            }
        };
        let Reply::Answer(answer) = reply else {
            return;
        };
        let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": answer}).to_string();
        let _ = write!(
            stream,
            "HTTP/1.0 200 OK\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
    }

    /// Read until the JSON body is complete: the client sends one request.
    fn read_request(stream: &mut std::net::TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut seen = Vec::new();
        let mut buf = [0u8; 1024];
        while !seen.ends_with(b"}") {
            match stream.read(&mut buf) {
                Ok(0) | Err(_) => return,
                Ok(n) => seen.extend_from_slice(&buf[..n]),
            }
        }
    }

    fn peer(&self) -> ExecPeer {
        self.url.parse().unwrap()
    }

    fn asked(&self) -> usize {
        self.asked.load(Ordering::SeqCst)
    }
}

fn peers(fakes: &[&FakePeer]) -> ExecPeers {
    ExecPeers::new(
        fakes.iter().map(|fake| fake.peer()).collect(),
        None,
        Duration::from_millis(500),
    )
}

fn not_held() -> Reply {
    Reply::Answer(ExecLocatorAnswer::NotHeld)
}

/// The answer that names session `session_id` of the archive `archive`.
fn located(archive: &str, session_id: i32) -> Reply {
    Reply::Answer(ExecLocatorAnswer::Located {
        archive_id: archive.to_owned(),
        session_id,
        position: 0,
    })
}

/// An archive of executor streams in memory, by archive and session. A
/// replay of a session it does not hold is a refusal.
#[derive(Default)]
pub(super) struct FakeArchive {
    recordings: HashMap<(String, i32), Vec<ExecTxRecord>>,
}

impl FakeArchive {
    fn record(&mut self, archive: &str, session_id: i32, records: Vec<ExecTxRecord>) {
        self.recordings
            .insert((archive.to_owned(), session_id), records);
    }
}

impl ExecRecordReplay for FakeArchive {
    fn replay_exec_records(
        &mut self,
        from: ExecRecordsFrom<'_>,
        sink: impl FnMut(ExecTxRecord),
    ) -> Result<u64, JoinRecoveryError> {
        let records = self
            .recordings
            .get(&(from.archive_id.to_owned(), from.session_id))
            .ok_or_else(|| {
                JoinRecoveryError::Archive(LogError::RangeAbsent {
                    archive: from.archive_id.to_owned(),
                    detail: "no recording of the session".to_owned(),
                })
            })?;
        records.iter().cloned().for_each(sink);
        Ok(u64::try_from(records.len()).unwrap())
    }
}

/// What one park needs beside the subscription: the peers, the archive,
/// the kept records, the voter id and the entry's reference.
pub(super) struct ParkRig {
    peers: ExecPeers,
    archive: FakeArchive,
    fetched: FetchedRecords,
    voter_id: Option<u8>,
    tx_ref: TxRef,
}

impl Default for ParkRig {
    fn default() -> Self {
        Self {
            peers: ExecPeers::default(),
            archive: FakeArchive::default(),
            fetched: FetchedRecords::default(),
            voter_id: Some(3),
            tx_ref: tx_ref(LOST, 10),
        }
    }
}

impl ParkRig {
    /// A park at `index` over `sub` that asks the peers at every message.
    pub(super) fn park<'a>(
        &'a mut self,
        sub: &'a mut VotingSub,
        backlog: &'a mut ReadAhead,
        index: u64,
    ) -> VoidPark<'a, VotingSub, FakeArchive> {
        let expected = Expected {
            index,
            tx_ref: self.tx_ref,
        };
        let plan = ParkPlan {
            voter_id: self.voter_id,
            void: VoidRecord {
                index,
                tx_hash: self.tx_ref.tx_hash,
            },
            wait: Duration::from_mins(1),
            peers: PeerFetch::new(PeerFetchInputs {
                peers: &self.peers,
                replay: Some(&mut self.archive),
                fetched: &mut self.fetched,
                expected,
            }),
        };
        let mut park = VoidPark::new(sub, backlog, plan);
        park.peers.reask_after = Duration::ZERO;
        park
    }
}

/// A signed envelope: the check recovers its sender.
fn signed(nonce: u64) -> TxEnvelope {
    let signer = PrivateKeySigner::random();
    crate::actor::test_support::legacy(&signer, Address::from([0x22u8; 20]), nonce, 1)
}

/// The reference of `env` at `data_offset` on lane 0.
fn ref_of(env: &TxEnvelope, data_offset: i32) -> TxRef {
    TxRef::new(env.tx_hash, 0, pos(data_offset), 0)
}

fn record(index: u64, tx_ref: TxRef, envelope: &TxEnvelope) -> ExecTxRecord {
    ExecTxRecord {
        index,
        tx_ref,
        envelope: envelope.clone(),
    }
}

/// A boundary that closes block `block_number` at the record count `end`.
fn closing(block_number: u64, end: u64) -> TxOrderingMessage {
    TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
        block_number,
        end_tx_idx: BPosition::from_index(end),
        l2_timestamp: 1_700_000_000,
        l1_origin: 0,
    })
}

#[test]
fn a_peer_whose_archive_holds_the_entry_joins_it_with_no_vote() {
    let env = signed(0);
    let next = signed(1);
    let empty = FakePeer::start(vec![not_held()]);
    let holder = FakePeer::start(vec![located("executor-1", 7)]);
    let mut rig = ParkRig {
        peers: peers(&[&empty, &holder]),
        tx_ref: ref_of(&env, 10),
        ..ParkRig::default()
    };
    rig.archive.record(
        "executor-1",
        7,
        vec![
            record(0, ref_of(&env, 10), &env),
            record(1, ref_of(&next, 20), &next),
        ],
    );
    let mut sub = VotingSub::new(1, vec![boundary(1)]);
    let mut backlog = ReadAhead::new();
    let outcome = rig.park(&mut sub, &mut backlog, 0).run().expect("fetched");
    let ParkOutcome::Fetched(fetched) = outcome else {
        panic!("expected the fetched entry");
    };
    assert_eq!(fetched, env);
    assert!(sub.votes.lock().unwrap().is_empty());
    assert_eq!((empty.asked(), holder.asked()), (1, 1));
    // The record after the entry stays for its own turn.
    assert_eq!(rig.fetched.take(1).map(|r| r.envelope), Some(next));
}

#[test]
fn every_peer_not_held_sends_the_vote() {
    let a = FakePeer::start(vec![not_held()]);
    let b = FakePeer::start(vec![not_held()]);
    let cfg = ReaderConfig {
        exec_peers: peers(&[&a, &b]),
        ..voter_cfg()
    };
    let sub = VotingSub::new(1, vec![boundary(1), void(0, LOST)]);
    let votes = sub.votes.clone();
    let (mut reader, rx) = reader(sub, JoinBuffer::new(), cfg);

    let flow = reader
        .on_unjoined(&tx_ref(LOST, 10), pos(0), true)
        .expect("voided");
    assert!(matches!(flow, Flow::Continue));
    assert_eq!((a.asked(), b.asked()), (1, 1));
    assert_eq!(
        *votes.lock().unwrap(),
        vec![(
            3,
            VoidRecord {
                index: 0,
                tx_hash: LOST
            }
        )]
    );
    assert_eq!(slots(&rx), vec![('V', 0)]);
}

#[test]
fn an_unanswered_peer_holds_the_vote_back_until_it_answers_not_held() {
    let a = FakePeer::start(vec![not_held()]);
    // No answer twice, then "not held".
    let b = FakePeer::start(vec![Reply::Silent, Reply::Silent, not_held()]);
    let mut rig = ParkRig {
        peers: peers(&[&a, &b]),
        ..ParkRig::default()
    };
    let queue = vec![
        boundary(1),
        boundary(2),
        boundary(3),
        boundary(4),
        void(0, LOST),
    ];
    let mut sub = VotingSub::new(1, queue);
    let mut backlog = ReadAhead::new();
    let outcome = rig.park(&mut sub, &mut backlog, 0).run().expect("voided");
    assert!(matches!(outcome, ParkOutcome::Voided));
    // Peer a answered once, for good. Peer b was asked three times, and
    // the one vote went out only after its third answer: one message was
    // read ahead by then, four were still unread.
    assert_eq!((a.asked(), b.asked()), (1, 3));
    assert_eq!(sub.votes.lock().unwrap().len(), 1);
    assert_eq!(sub.queue.len(), 0);
}

#[test]
fn a_peer_that_does_not_reach_the_entry_gets_no_vote_sent() {
    let a = FakePeer::start(vec![not_held()]);
    let b = FakePeer::start(vec![Reply::Answer(ExecLocatorAnswer::NotReached)]);
    let mut rig = ParkRig {
        peers: peers(&[&a, &b]),
        ..ParkRig::default()
    };
    let mut sub = VotingSub::new(1, (1..=20).map(boundary).collect());
    let mut backlog = ReadAhead::new();
    // One round before the read-ahead, then one before each read: twenty
    // messages, and the read that finds the order closed.
    let closed = rig.park(&mut sub, &mut backlog, 0).run();
    assert!(matches!(closed, Err(ExecutorError::TxOrderingClosed)));
    assert!(sub.votes.lock().unwrap().is_empty());
    assert_eq!((a.asked(), b.asked()), (1, 22));
}

#[test]
fn a_void_record_ends_the_wait_for_an_unanswered_peer() {
    // An earlier run's vote completed the void: the record ends the wait.
    let silent = FakePeer::start(vec![Reply::Silent]);
    let mut rig = ParkRig {
        peers: peers(&[&silent]),
        ..ParkRig::default()
    };
    let mut sub = VotingSub::new(1, vec![boundary(1), void(0, LOST)]);
    let mut backlog = ReadAhead::new();
    let outcome = rig.park(&mut sub, &mut backlog, 0).run().expect("voided");
    assert!(matches!(outcome, ParkOutcome::Voided));
    assert!(sub.votes.lock().unwrap().is_empty());
}

/// Poll a fetch for the entry of `env` at index 0 once, with one peer
/// whose archive holds `served` at index 0.
fn poll_once(env: &TxEnvelope, served: &TxEnvelope) -> (PeerPoll, Vec<&'static str>) {
    let holder = FakePeer::start(vec![located("executor-0", 1)]);
    let peers = peers(&[&holder]);
    let mut archive = FakeArchive::default();
    archive.record("executor-0", 1, vec![record(0, ref_of(env, 10), served)]);
    let mut fetched = FetchedRecords::default();
    let mut fetch = PeerFetch::new(PeerFetchInputs {
        peers: &peers,
        replay: Some(&mut archive),
        fetched: &mut fetched,
        expected: Expected {
            index: 0,
            tx_ref: ref_of(env, 10),
        },
    });
    let poll = fetch.poll(Instant::now());
    (poll, fetch.answers().to_vec())
}

#[test]
fn a_record_with_another_hash_counts_as_no_answer() {
    let env = signed(0);
    let (poll, answers) = poll_once(&env, &signed(1));
    // A failed check is never "not held": the reader asks again.
    assert!(matches!(poll, PeerPoll::Waiting));
    assert_eq!(answers, ["mismatch"]);
}

#[test]
fn a_record_with_a_forged_sender_counts_as_no_answer() {
    let env = signed(2);
    let forged = TxEnvelope {
        sender: Address::repeat_byte(0x77),
        ..env.clone()
    };
    let (poll, answers) = poll_once(&env, &forged);
    assert!(matches!(poll, PeerPoll::Waiting));
    assert_eq!(answers, ["mismatch"]);
}

#[test]
fn a_lost_record_blocks_the_vote_and_ends_the_wait_at_the_entry_block() {
    let a = FakePeer::start(vec![not_held()]);
    let b = FakePeer::start(vec![Reply::Answer(ExecLocatorAnswer::Lost)]);
    // A peer names an archive that holds no byte of the range: also lost.
    let c = FakePeer::start(vec![located("executor-9", 4)]);
    let mut rig = ParkRig {
        peers: peers(&[&a, &b, &c]),
        ..ParkRig::default()
    };
    // The first boundary closes block 6 before the entry; block 7 holds it.
    let mut sub = VotingSub::new(1, vec![closing(6, 0), closing(7, 1), void(0, LOST)]);
    let mut backlog = ReadAhead::new();
    let outcome = rig.park(&mut sub, &mut backlog, 0).run().expect("lost");
    assert!(matches!(outcome, ParkOutcome::Lost { block: 7 }));
    assert!(sub.votes.lock().unwrap().is_empty());
    // Every answer was final: one ask each.
    assert_eq!((a.asked(), b.asked(), c.asked()), (1, 1, 1));
}

#[test]
fn a_reader_stops_for_the_repair_on_a_lost_record() {
    let lost = FakePeer::start(vec![Reply::Answer(ExecLocatorAnswer::Lost)]);
    let cfg = ReaderConfig {
        exec_peers: peers(&[&lost]),
        ..voter_cfg()
    };
    let sub = VotingSub::new(1, vec![closing(7, 1)]);
    let votes = sub.votes.clone();
    let (mut reader, rx) = reader(sub, JoinBuffer::new(), cfg);
    let err = reader.on_unjoined(&tx_ref(LOST, 10), pos(0), true).err();
    assert!(matches!(
        err,
        Some(ExecutorError::PeerRecordLost {
            index: 0,
            block: 7,
            ..
        })
    ));
    assert!(votes.lock().unwrap().is_empty());
    assert!(slots(&rx).is_empty());
}

#[test]
fn a_join_that_timed_out_parks_with_no_vote() {
    let a = FakePeer::start(vec![not_held()]);
    let cfg = ReaderConfig {
        exec_peers: peers(&[&a]),
        ..voter_cfg()
    };
    let sub = VotingSub::new(1, vec![boundary(1), boundary(2)]);
    let votes = sub.votes.clone();
    let (mut reader, rx) = reader(sub, JoinBuffer::new(), cfg);
    // An archive gave no definite answer: every peer answered "not held",
    // and still no vote goes out. The order closes: a clean stop.
    let flow = reader
        .on_unjoined(&tx_ref(LOST, 10), pos(0), false)
        .expect("closed");
    assert!(matches!(flow, Flow::Stop));
    assert!(votes.lock().unwrap().is_empty());
    assert!(slots(&rx).is_empty());
}

#[test]
fn a_join_that_timed_out_with_no_peer_stops_as_before() {
    let (mut reader, _rx) = reader(
        VotingSub::new(1, Vec::new()),
        JoinBuffer::new(),
        voter_cfg(),
    );
    let err = reader.on_unjoined(&tx_ref(LOST, 10), pos(0), false).err();
    assert!(matches!(err, Some(ExecutorError::JoinTimeout { .. })));
}

#[test]
fn a_kept_record_joins_its_entry_at_its_turn_after_the_check() {
    let kept = signed(0);
    let wrong = signed(1);
    let live = signed(2);
    // Entry 1 has a good kept record. Entry 2 has a kept record with other
    // bytes, and the live envelope in the buffer.
    let buffer = JoinBuffer::new();
    buffer.insert(TxDataKey::new(0, 0, pos(30)), live.clone());
    let queue = vec![
        TxOrderingMessage::TxRef(ref_of(&kept, 20)),
        TxOrderingMessage::TxRef(ref_of(&live, 30)),
    ];
    let (mut reader, rx) = reader(VotingSub::new(1, queue), buffer, voter_cfg());
    reader.fetched.keep(
        1,
        vec![
            record(1, ref_of(&kept, 20), &kept),
            record(2, ref_of(&live, 30), &wrong),
        ],
    );
    reader.run().expect("clean close");
    let sent: Vec<(u64, B256)> = rx
        .try_iter()
        .filter_map(|msg| match msg {
            ReaderToExec::Tx {
                envelope, position, ..
            } => Some((position.as_index(), envelope.tx_hash)),
            _ => None,
        })
        .collect();
    assert_eq!(sent, vec![(1, kept.tx_hash), (2, live.tx_hash)]);
}
