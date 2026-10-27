use super::*;

fn exhausted() -> anyhow::Error {
    anyhow::anyhow!("DA DNS lookup fails").context(PostExhausted {
        prev_index: 94,
        attempts: 6,
    })
}

#[test]
fn a_source_reset_cannot_turn_an_exhausted_post_into_a_process_exit() {
    let reset = ExecutorError::SourceRestarted {
        next_index: 28854,
        next_block: 312,
    };
    assert!(matches!(
        ReaderEnd::ordering_failure(&reset, &exhausted()),
        ReaderEnd::Halted(halt) if halt.cause == HaltCause::L1Unreachable
    ));
}

#[test]
fn an_exhausted_post_does_not_hide_a_fatal_ordering_failure() {
    for error in [
        ExecutorError::Divergence("BAL mismatch".into()),
        ExecutorError::LocalWrite("disk full".into()),
        ExecutorError::ClusterBehindCursor {
            next_index: 10,
            next_block: 4,
            head_index: 9,
            head_block: 3,
        },
        ExecutorError::UnjoinableReference { index: 10 },
    ] {
        assert!(matches!(
            ReaderEnd::ordering_failure(&error, &exhausted()),
            ReaderEnd::Failed(_)
        ));
    }
}

#[test]
fn replay_refusal_keeps_its_recovery_path_during_an_exhausted_post() {
    let refused = ExecutorError::ClusterReplayUnavailable {
        from_index: 10,
        oldest_index: 20,
        oldest_block: 4,
    };
    assert!(matches!(
        ReaderEnd::ordering_failure(&refused, &exhausted()),
        ReaderEnd::ReplayRefused { oldest_block: 4 }
    ));
}

#[test]
fn source_reset_recovery_needs_a_structured_exhausted_post() {
    let reset = ExecutorError::SourceRestarted {
        next_index: 28854,
        next_block: 312,
    };
    assert!(matches!(
        ReaderEnd::ordering_failure(&reset, &anyhow::anyhow!(exhausted().to_string())),
        ReaderEnd::Failed(_)
    ));
}

#[test]
fn a_post_exhaustion_in_context_enters_the_automatic_halt() {
    let error = anyhow::anyhow!("DA DNS lookup fails")
        .context(PostExhausted {
            prev_index: 94,
            attempts: 6,
        })
        .context("streaming post");
    assert!(
        matches!(ReaderEnd::exhausted_post(&error), Some(ReaderEnd::Halted(halt)) if halt.cause == HaltCause::L1Unreachable)
    );
}

#[test]
fn only_a_structured_post_exhaustion_is_an_automatic_halt() {
    let exhausted = PostExhausted {
        prev_index: 94,
        attempts: 6,
    };
    let display_only = anyhow::anyhow!(exhausted.to_string());
    assert!(ReaderEnd::exhausted_post(&display_only).is_none());
    assert!(matches!(
        ReaderEnd::exhausted_post(&anyhow::Error::new(exhausted)),
        Some(ReaderEnd::Halted(_))
    ));
}
