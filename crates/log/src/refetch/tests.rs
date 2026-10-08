use kardamom_types::BPosition;

use super::{FakeArchiveCatalog, FoundRecording, RecordedLimit, ReplayPlan, Wanted};
use crate::error::LogError;

/// The raw position `term_id * 65536 + term_offset` of the fake layout.
fn at(raw: i64) -> BPosition {
    BPosition {
        term_id: i32::try_from(raw >> 16).expect("term id fits"),
        term_offset: i32::try_from(raw & 0xFFFF).expect("term offset fits"),
    }
}

/// The recording id that the refetcher picks for `raw` among `recordings`
/// of session 0, as `(id, start, stop)`.
fn pick(recordings: &[(i64, i64, Option<i64>)], raw: i64) -> Option<i64> {
    let recs = recordings
        .iter()
        .map(|&(id, start, stop)| FoundRecording::fake(id, start, stop))
        .collect();
    let wanted = Wanted {
        stream_id: 0,
        session_id: 0,
        from: at(raw),
    };
    wanted.resolve(recs).ok().map(|l| l.rec.recording_id)
}

/// Two recordings of one session: recording 15 ended at 300000, and the
/// archive started recording 16 at 400000.
const TWO: [(i64, i64, Option<i64>); 2] = [(15, 100_000, Some(300_000)), (16, 400_000, None)];

fn catalog(archive: &str) -> FakeArchiveCatalog {
    FakeArchiveCatalog {
        archive: archive.to_owned(),
        recordings: vec![(100_000, Some(300_000)), (400_000, Some(600_000))],
    }
}

fn is_refusal(answer: &Result<Option<i64>, LogError>, archive: &str) -> bool {
    matches!(answer, Err(LogError::RangeAbsent { archive: a, .. }) if a == archive)
}

#[test]
fn an_ended_recording_short_of_the_range_holds_no_byte_of_it() {
    // Issue #313: the ref sat at raw position 9645920, and the newest
    // recording of the session ended at 2994688 on the one archive and at
    // 1105152 on the other.
    let ended = RecordedLimit {
        position: 2_994_688,
        active: false,
    };
    assert!(ended.ended_before(9_645_920));
    assert!(ended.ended_before(2_994_688));
    assert!(!ended.ended_before(2_994_656));
}

#[test]
fn a_live_recording_short_of_the_range_can_still_reach_it() {
    let live = RecordedLimit {
        position: 2_994_688,
        active: true,
    };
    assert!(!live.ended_before(9_645_920));
    assert_eq!(live.replay_len(9_645_920), -6_651_232);
}

#[test]
fn the_replay_runs_from_the_position_to_the_limit() {
    let live = RecordedLimit {
        position: 4096,
        active: true,
    };
    assert_eq!(live.replay_len(1024), 3072);
}

#[test]
fn a_range_in_the_older_recording_resolves_to_the_older_recording() {
    assert_eq!(pick(&TWO, 250_000), Some(15));
}

#[test]
fn a_range_in_the_newest_recording_resolves_to_the_newest_recording() {
    assert_eq!(pick(&TWO, 450_000), Some(16));
}

#[test]
fn a_range_before_every_recording_resolves_to_none() {
    assert_eq!(pick(&TWO, 50_000), None);
}

#[test]
fn a_range_in_a_gap_resolves_to_the_recording_before_the_gap() {
    assert_eq!(pick(&TWO, 350_000), Some(15));
}

#[test]
fn a_covering_recording_wins_over_a_newer_one_that_ended_before_the_range() {
    let overlap = [(15, 100_000, None), (16, 120_000, Some(150_000))];
    assert_eq!(pick(&overlap, 200_000), Some(15));
}

#[test]
fn only_the_recordings_of_the_session_count() {
    let recs = vec![FoundRecording {
        session_id: 7,
        ..FoundRecording::fake(15, 100_000, None)
    }];
    let wanted = Wanted {
        stream_id: 0,
        session_id: 0,
        from: at(250_000),
    };
    assert!(wanted.resolve(recs).is_err());
}

#[test]
fn the_archive_replays_the_range_from_the_older_recording() {
    assert_eq!(catalog("a").answer(at(250_000)).ok(), Some(Some(50_000)));
}

#[test]
fn a_range_before_the_oldest_recording_is_a_refusal() {
    let answer = catalog("a").answer(at(50_000));
    assert!(is_refusal(&answer, "a"), "{answer:?}");
    assert!(
        answer
            .err()
            .is_some_and(|e| e.to_string().contains("precedes the oldest recording 0"))
    );
}

#[test]
fn a_range_in_a_gap_between_recordings_is_a_refusal() {
    let answer = catalog("a").answer(at(350_000));
    assert!(is_refusal(&answer, "a"), "{answer:?}");
}

#[test]
fn a_range_after_the_last_ended_recording_is_a_refusal() {
    let answer = catalog("a").answer(at(700_000));
    assert!(is_refusal(&answer, "a"), "{answer:?}");
}

#[test]
fn no_recording_of_the_session_is_a_refusal() {
    let empty = FakeArchiveCatalog {
        archive: "a".to_owned(),
        recordings: Vec::new(),
    };
    let answer = empty.answer(at(50_000));
    assert!(is_refusal(&answer, "a"), "{answer:?}");
}

#[test]
fn a_live_recording_that_does_not_reach_the_range_yet_is_no_refusal() {
    let live = FakeArchiveCatalog {
        archive: "a".to_owned(),
        recordings: vec![(100_000, None)],
    };
    assert_eq!(live.answer(at(250_000)).ok(), Some(None));
}

/// A limit counter id of zero is a real counter: the archive then bounds the
/// replay by its driver's total of bytes sent. Only `-1` means "no bound".
#[test]
fn the_replay_has_no_limit_counter() {
    let plan = ReplayPlan {
        from_raw: 5_818_304,
        len: 32_480,
        endpoint: "127.0.0.1:0".to_owned(),
        limit: RecordedLimit {
            position: 5_850_784,
            active: false,
        },
    };
    let params = plan.params().expect("params");
    assert_eq!(params.bounding_limit_counter_id(), -1);
    assert_eq!(params.replay_token(), -1);
    assert_eq!(params.subscription_registration_id(), -1);
    assert_eq!(params.position(), 5_818_304);
    assert_eq!(params.length(), 32_480);
}
