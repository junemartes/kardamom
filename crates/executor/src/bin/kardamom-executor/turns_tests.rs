use super::*;

#[test]
fn verdict_revolves_only_on_a_staged_checkpoint() {
    let refused = || ExecutorError::ClusterReplayUnavailable {
        from_index: 10,
        oldest_index: 500,
        oldest_block: 7,
    };
    assert!(matches!(
        Verdict::of(Some(refused()), Some("peer-checkpoint"), false),
        Verdict::Revolve
    ));
    assert!(matches!(
        Verdict::of(Some(refused()), Some("unrecoverable"), false),
        Verdict::Failed(_)
    ));
    assert!(matches!(
        Verdict::of(Some(refused()), None, false),
        Verdict::Failed(_)
    ));
    assert!(matches!(Verdict::of(None, None, false), Verdict::Done));
}

/// A divergence revolves only with a staged rollback; with none, the
/// executor stops for the operator. A restart of the stream source runs
/// again after a pause, and a divergence below the window runs again with
/// a wider window, unless no wider window can find it.
#[test]
fn verdict_rolls_back_only_with_a_staged_checkpoint() {
    let diverged = || ExecutorError::StreamDiverged {
        block: 9,
        index: 40,
        stream: alloy_primitives::B256::repeat_byte(1),
    };
    assert!(matches!(
        Verdict::of(Some(diverged()), None, true),
        Verdict::Revolve
    ));
    assert!(matches!(
        Verdict::of(Some(diverged()), None, false),
        Verdict::Failed(ExecutorError::StreamDiverged { block: 9, .. })
    ));
    let restarted = ExecutorError::SourceRestarted {
        next_index: 3,
        next_block: 2,
    };
    assert!(matches!(
        Verdict::of(Some(restarted), None, false),
        Verdict::Restart
    ));
    let below = |widen| ExecutorError::DivergedBelowWindow {
        floor_block: 5,
        widen,
    };
    assert!(matches!(
        Verdict::of(Some(below(true)), None, false),
        Verdict::Widen(_)
    ));
    assert!(matches!(
        Verdict::of(Some(below(false)), None, false),
        Verdict::Failed(_)
    ));
}

/// The window grows by its factor up to the widest window, and a
/// divergence below the widest window stops the process.
#[test]
fn the_window_widens_up_to_its_limit() {
    let below = || ExecutorError::DivergedBelowWindow {
        floor_block: 5,
        widen: true,
    };
    let mut turns = Turns::new(NonZeroU64::new(32).unwrap());
    turns.widen(below()).unwrap();
    assert_eq!(turns.window.get(), 128);
    turns.window = NonZeroU64::new(2000).unwrap();
    turns.widen(below()).unwrap();
    assert_eq!(turns.window, MAX_COMPARE_BLOCKS);
    assert!(turns.widen(below()).is_err());
}
