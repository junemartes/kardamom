use std::time::Duration;

use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};

use super::{JoinRepair, SessionFetch};
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

/// The archive copy of the session: its frames at these stream positions.
struct Archive {
    frames: Vec<u64>,
    calls: Vec<(i32, i32, BPosition)>,
    fail: bool,
}

impl Archive {
    fn holding(frames: &[u64]) -> Self {
        Self {
            frames: frames.to_vec(),
            calls: Vec::new(),
            fail: false,
        }
    }

    fn down() -> Self {
        Self {
            fail: true,
            ..Self::holding(&[])
        }
    }
}

impl SessionFetch for Archive {
    fn fetch(
        &mut self,
        stream_id: i32,
        session_id: i32,
        from: BPosition,
        mut sink: impl FnMut(TxDataLoc, TxEnvelope),
    ) -> Result<u64, LogError> {
        self.calls.push((stream_id, session_id, from));
        if self.fail {
            return Err(LogError::Aeron("archive down".into()));
        }
        for at in &self.frames {
            let f = frame(*at);
            sink(f.loc, f.envelope);
        }
        Ok(u64::try_from(self.frames.len()).unwrap())
    }
}

/// A repair whose start grace has passed.
fn armed() -> JoinRepair {
    JoinRepair::new(STREAM, Duration::ZERO, MAX_GAP)
}

/// The stream positions that `repair` gives, and the stream position that
/// the correlation id of each one names.
fn given(repair: &mut JoinRepair) -> Vec<(u64, u64)> {
    std::iter::from_fn(|| repair.pop())
        .map(|f| (f.stream_position, f.envelope.correlation_id))
        .collect()
}

#[test]
fn a_late_join_gives_the_refetched_frames_before_the_live_frame() {
    let mut repair = armed();
    // The archive holds the session up to its tail, past the join at 300.
    let mut archive = Archive::holding(&[0, 100, 200, 300, 400]);
    repair.take(TxDataItem::Frame(frame(300)), &mut archive);
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
    assert_eq!(
        given(&mut repair),
        [(0, 0), (100, 100), (200, 200), (300, 300)]
    );
}

#[test]
fn a_late_join_in_a_later_term_places_each_refetched_frame_in_its_term() {
    let mut repair = armed();
    let join = 2 * TERM + 40;
    let mut archive = Archive::holding(&[10, TERM + 20, join]);
    repair.take(TxDataItem::Frame(frame(join)), &mut archive);
    assert_eq!(
        given(&mut repair),
        [(10, 10), (TERM + 20, TERM + 20), (join, join)]
    );
}

#[test]
fn a_join_at_the_start_of_the_session_refetches_nothing() {
    let mut repair = armed();
    let mut archive = Archive::holding(&[0]);
    repair.take(TxDataItem::Frame(frame(0)), &mut archive);
    repair.take(TxDataItem::Frame(frame(100)), &mut archive);
    assert!(archive.calls.is_empty());
    assert_eq!(given(&mut repair), [(0, 0), (100, 100)]);
}

#[test]
fn only_the_first_frame_of_a_session_can_start_a_repair() {
    let mut repair = armed();
    let mut archive = Archive::holding(&[0, 100]);
    repair.take(TxDataItem::Frame(frame(100)), &mut archive);
    repair.take(TxDataItem::Frame(frame(200)), &mut archive);
    assert_eq!(archive.calls.len(), 1);
    assert_eq!(given(&mut repair), [(0, 0), (100, 100), (200, 200)]);
}

#[test]
fn a_session_of_the_start_grace_is_not_repaired() {
    let mut repair = JoinRepair::new(STREAM, Duration::from_secs(60), MAX_GAP);
    let mut archive = Archive::holding(&[0, 100]);
    repair.take(TxDataItem::Frame(frame(100)), &mut archive);
    assert!(archive.calls.is_empty());
    assert_eq!(given(&mut repair), [(100, 100)]);
}

#[test]
fn a_gap_above_the_bound_is_not_repaired() {
    let mut repair = JoinRepair::new(STREAM, Duration::ZERO, 50);
    let mut archive = Archive::holding(&[0, 100]);
    repair.take(TxDataItem::Frame(frame(100)), &mut archive);
    assert!(archive.calls.is_empty());
    assert_eq!(given(&mut repair), [(100, 100)]);
}

#[test]
fn a_failed_refetch_still_gives_the_live_frame() {
    let mut repair = armed();
    let mut archive = Archive::down();
    repair.take(TxDataItem::Frame(frame(300)), &mut archive);
    assert_eq!(archive.calls.len(), 1);
    assert_eq!(given(&mut repair), [(300, 300)]);
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

/// The subscriber joins after the only frame of the session: the client
/// waits for it and sends nothing more. The marker alone repairs it.
#[test]
fn a_join_marker_with_no_frame_after_it_refetches_the_frames_before_it() {
    let mut repair = armed();
    let mut archive = Archive::holding(&[0, 224]);
    repair.take(joined(224), &mut archive);
    assert_eq!(archive.calls.len(), 1);
    assert_eq!(given(&mut repair), [(0, 0)]);
}

#[test]
fn a_join_marker_and_the_first_frame_after_it_refetch_once() {
    let mut repair = armed();
    let mut archive = Archive::holding(&[0, 100, 200]);
    repair.take(joined(200), &mut archive);
    repair.take(TxDataItem::Frame(frame(200)), &mut archive);
    assert_eq!(archive.calls.len(), 1);
    assert_eq!(given(&mut repair), [(0, 0), (100, 100), (200, 200)]);
}
