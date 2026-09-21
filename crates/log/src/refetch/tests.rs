use super::{FoundRecording, RecordedLimit};

fn recording(start_position: i64) -> FoundRecording {
    FoundRecording {
        recording_id: 2,
        session_id: -280_215_725,
        start_position,
        term_layout: Err("not read by these tests".to_string()),
    }
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
    assert_eq!(
        live.replay_len(&recording(0), 9_645_920).ok(),
        Some(-6_651_232)
    );
}

#[test]
fn the_replay_runs_from_the_position_to_the_limit() {
    let live = RecordedLimit {
        position: 4096,
        active: true,
    };
    assert_eq!(live.replay_len(&recording(0), 1024).ok(), Some(3072));
}

#[test]
fn a_position_before_the_recording_start_is_refused() {
    let live = RecordedLimit {
        position: 8192,
        active: true,
    };
    let error = live.replay_len(&recording(4096), 1024).err();
    assert!(error.is_some_and(|e| e.to_string().contains("precedes recording 2 start 4096")));
}
