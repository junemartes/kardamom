use super::*;

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
