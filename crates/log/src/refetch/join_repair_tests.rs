use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};
use metrics_exporter_prometheus::PrometheusBuilder;

use super::{FetchAnswer, JoinRepair, Refetched, SessionFetch};
use crate::aeron_live::{ImageJoin, PositionedTxData, TxDataItem};
use crate::error::LogError;

const STREAM: i32 = 1001;
const SESSION: i32 = 42;
/// The initial term id of the session; Aeron picks it at random.
const INITIAL_TERM: i32 = 7;
/// The term length of the session: 64 KiB.
const TERM: u64 = 1 << 16;
const MAX_GAP: u64 = 64 << 20;

/// The frame of the session at stream position `at`, tagged by `at` in
/// its correlation id.
fn frame(at: u64) -> PositionedTxData {
    let terms = i32::try_from(at / TERM).unwrap();
    PositionedTxData {
        loc: TxDataLoc::new(
            SESSION,
            BPosition {
                term_id: INITIAL_TERM + terms,
                term_offset: i32::try_from(at % TERM).unwrap(),
            },
        ),
        stream_position: at,
        session_start: BPosition {
            term_id: INITIAL_TERM,
            term_offset: 0,
        },
        envelope: TxEnvelope {
            correlation_id: at,
            ..TxEnvelope::default()
        },
    }
}

fn live(at: u64) -> TxDataItem {
    TxDataItem::Frame(frame(at))
}

/// The join marker of the session at stream position `at`.
fn joined(at: u64) -> TxDataItem {
    let f = frame(at);
    TxDataItem::Joined(ImageJoin {
        session_id: SESSION,
        join: f.loc.position,
        stream_position: at,
        session_start: f.session_start,
    })
}

/// How the archive answers a refetch.
enum Answer {
    /// The frames at these stream positions, and the raw end of the
    /// replay.
    Frames(Vec<u64>, Option<i64>),
    Fail,
    /// No answer yet: the test answers through [`Archive::later`].
    Later,
}

/// The archive copy of the session.
struct Archive {
    answer: Answer,
    calls: Vec<(i32, i32, BPosition)>,
    /// The answer channel of a refetch that waits.
    later: Option<Sender<FetchAnswer>>,
}

impl Archive {
    /// A copy that holds the frames at `frames`, recorded up to the end of
    /// the last one.
    fn holding(frames: &[u64]) -> Self {
        let end = frames.last().map(|at| i64::try_from(*at).unwrap() + 32);
        Self::answering(Answer::Frames(frames.to_vec(), end))
    }

    fn answering(answer: Answer) -> Self {
        Self {
            answer,
            calls: Vec::new(),
            later: None,
        }
    }

    /// Answer the refetch that waits.
    fn answer_now(&mut self, frames: &[u64]) {
        let refetched = Refetched {
            frames: frames
                .iter()
                .map(|at| (frame(*at).loc, frame(*at).envelope))
                .collect(),
            end: frames.last().map(|at| i64::try_from(*at).unwrap() + 32),
        };
        self.later.take().unwrap().send(Ok(refetched)).unwrap();
    }
}

impl SessionFetch for Archive {
    fn start(&mut self, stream_id: i32, session_id: i32, from: BPosition) -> Receiver<FetchAnswer> {
        self.calls.push((stream_id, session_id, from));
        let (tx, rx) = crossbeam_channel::bounded(1);
        match &self.answer {
            Answer::Frames(frames, end) => {
                let frames = frames
                    .iter()
                    .map(|at| (frame(*at).loc, frame(*at).envelope))
                    .collect();
                tx.send(Ok(Refetched { frames, end: *end })).unwrap();
            }
            Answer::Fail => tx
                .send(Err(LogError::Aeron("archive down".into())))
                .unwrap(),
            Answer::Later => self.later = Some(tx),
        }
        rx
    }
}

/// A repair whose start grace has passed.
fn armed() -> JoinRepair {
    JoinRepair::new(STREAM, Duration::ZERO, MAX_GAP)
}

/// The stream positions that `repair` gives now, and the stream position
/// that the correlation id of each one names.
fn given(repair: &mut JoinRepair, archive: &mut Archive) -> Vec<(u64, u64)> {
    std::iter::from_fn(|| repair.pop(archive))
        .map(|f| (f.stream_position, f.envelope.correlation_id))
        .collect()
}

/// The outcome counts that `test` records.
fn outcomes(test: impl FnOnce()) -> String {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, test);
    handle
        .render()
        .lines()
        .filter(|l| l.starts_with(super::JOIN_REPAIRS_TOTAL))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn a_late_join_gives_the_refetched_frames_before_the_live_frame() {
    let mut repair = armed();
    // The archive holds the session up to its tail, past the join at 300.
    let mut archive = Archive::holding(&[0, 100, 200, 300, 400]);
    let counted = outcomes(|| {
        repair.take(live(300), &mut archive);
        assert_eq!(
            given(&mut repair, &mut archive),
            [(0, 0), (100, 100), (200, 200), (300, 300)]
        );
    });
    assert_eq!(
        archive.calls,
        [(
            STREAM,
            SESSION,
            BPosition {
                term_id: INITIAL_TERM,
                term_offset: 0
            }
        )]
    );
    assert!(counted.contains("outcome=\"repaired\"} 1"), "{counted}");
}

#[test]
fn a_late_join_in_a_later_term_places_each_refetched_frame_in_its_term() {
    let mut repair = armed();
    let join = 2 * TERM + 40;
    let mut archive = Archive::holding(&[10, TERM + 20, join]);
    repair.take(live(join), &mut archive);
    assert_eq!(
        given(&mut repair, &mut archive),
        [(10, 10), (TERM + 20, TERM + 20), (join, join)]
    );
}

#[test]
fn a_join_at_the_start_of_the_session_refetches_nothing() {
    let mut repair = armed();
    let mut archive = Archive::holding(&[0]);
    repair.take(live(0), &mut archive);
    repair.take(live(100), &mut archive);
    assert!(archive.calls.is_empty());
    assert_eq!(given(&mut repair, &mut archive), [(0, 0), (100, 100)]);
}

#[test]
fn only_the_first_frame_of_a_session_can_start_a_repair() {
    let mut repair = armed();
    let mut archive = Archive::holding(&[0, 100]);
    repair.take(live(100), &mut archive);
    repair.take(live(200), &mut archive);
    assert_eq!(archive.calls.len(), 1);
    assert_eq!(
        given(&mut repair, &mut archive),
        [(0, 0), (100, 100), (200, 200)]
    );
}

#[test]
fn a_session_of_the_start_grace_is_not_repaired() {
    let mut repair = JoinRepair::new(STREAM, Duration::from_secs(60), MAX_GAP);
    let mut archive = Archive::holding(&[0, 100]);
    let counted = outcomes(|| repair.take(live(100), &mut archive));
    assert!(archive.calls.is_empty());
    assert_eq!(given(&mut repair, &mut archive), [(100, 100)]);
    assert!(counted.contains("outcome=\"start\"} 1"), "{counted}");
}

#[test]
fn a_gap_above_the_bound_is_not_repaired() {
    let mut repair = JoinRepair::new(STREAM, Duration::ZERO, 50);
    let mut archive = Archive::holding(&[0, 100]);
    let counted = outcomes(|| repair.take(live(100), &mut archive));
    assert!(archive.calls.is_empty());
    assert_eq!(given(&mut repair, &mut archive), [(100, 100)]);
    assert!(counted.contains("outcome=\"too_large\"} 1"), "{counted}");
}

#[test]
fn a_failed_refetch_still_gives_the_live_frame() {
    let mut repair = armed();
    let mut archive = Archive::answering(Answer::Fail);
    let counted = outcomes(|| {
        repair.take(live(300), &mut archive);
        assert_eq!(given(&mut repair, &mut archive), [(300, 300)]);
    });
    assert_eq!(archive.calls.len(), 1);
    assert!(counted.contains("outcome=\"failed\"} 1"), "{counted}");
}

/// The recording does not reach the join yet: the replay gives the frame
/// at 0 only, and ends at 100, before the join at 224.
#[test]
fn a_replay_that_ends_before_the_join_counts_short() {
    let mut repair = armed();
    let mut archive = Archive::answering(Answer::Frames(vec![0], Some(100)));
    let counted = outcomes(|| {
        repair.take(joined(224), &mut archive);
        assert_eq!(given(&mut repair, &mut archive), [(0, 0)]);
    });
    assert!(counted.contains("outcome=\"short\"} 1"), "{counted}");
}

#[test]
fn an_empty_replay_counts_short() {
    let mut repair = armed();
    let mut archive = Archive::answering(Answer::Frames(Vec::new(), None));
    let counted = outcomes(|| {
        repair.take(joined(224), &mut archive);
        assert!(given(&mut repair, &mut archive).is_empty());
    });
    assert!(counted.contains("outcome=\"short\"} 1"), "{counted}");
}

/// The subscriber joins after the only frame of the session: the client
/// waits for it and sends nothing more. The marker alone repairs it.
#[test]
fn a_join_marker_with_no_frame_after_it_refetches_the_frames_before_it() {
    let mut repair = armed();
    let mut archive = Archive::holding(&[0, 224]);
    repair.take(joined(224), &mut archive);
    assert_eq!(archive.calls.len(), 1);
    assert_eq!(given(&mut repair, &mut archive), [(0, 0)]);
}

#[test]
fn a_join_marker_and_the_first_frame_after_it_refetch_once() {
    let mut repair = armed();
    let mut archive = Archive::holding(&[0, 100, 200]);
    repair.take(joined(200), &mut archive);
    repair.take(live(200), &mut archive);
    assert_eq!(archive.calls.len(), 1);
    assert_eq!(
        given(&mut repair, &mut archive),
        [(0, 0), (100, 100), (200, 200)]
    );
}

/// While the refetch runs, the repair gives nothing of its lane, and the
/// reader polls its other lanes. The answer then gives the refetched
/// frames, and the held live frames after them.
#[test]
fn the_live_frames_wait_behind_the_refetch_in_flight() {
    let mut repair = armed();
    let mut archive = Archive::answering(Answer::Later);
    repair.take(joined(200), &mut archive);
    repair.take(live(200), &mut archive);
    repair.take(live(300), &mut archive);
    assert!(given(&mut repair, &mut archive).is_empty());
    assert_eq!(repair.len(), 2);
    archive.answer_now(&[0, 100, 200]);
    assert_eq!(
        given(&mut repair, &mut archive),
        [(0, 0), (100, 100), (200, 200), (300, 300)]
    );
}

/// An archive that never answers holds the lane for the refetch budget
/// only. Then the repair counts `failed` and gives the held frames.
#[test]
fn a_refetch_with_no_answer_gives_up_after_its_budget() {
    let budget = Duration::from_millis(50);
    let mut repair = armed().with_budget(budget);
    let mut archive = Archive::answering(Answer::Later);
    repair.take(live(200), &mut archive);
    assert!(given(&mut repair, &mut archive).is_empty());
    std::thread::sleep(budget + Duration::from_millis(10));
    let counted = outcomes(|| {
        assert_eq!(given(&mut repair, &mut archive), [(200, 200)]);
    });
    assert!(counted.contains("outcome=\"failed\"} 1"), "{counted}");
}
