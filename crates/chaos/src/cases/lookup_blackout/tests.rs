use super::*;

const LIVE: &str = "kardamom_sequencer_last_boundary_unix_seconds 1793090000\n";

#[test]
fn answered_covers_every_v2_source_but_needs_a_live_cluster_boundary() {
    for source in ANSWERED {
        let answer = format!("{LOOKUPS}{{partition=\"0\",{source}}} 1\n");
        assert!(!Counts::read(&answer).proves(Phase::Answered));
        let counts = Counts::read(&format!("{LIVE}{answer}"));
        assert!(counts.proves(Phase::Answered));
        assert!(!counts.proves(Phase::Unavailable));
    }
    assert!(!Counts::read("").proves(Phase::Answered));
}

#[test]
fn a_failed_query_does_not_prove_blackout_if_another_source_answers() {
    let failed = format!("{LIVE}{LOOKUPS}{{outcome=\"timeout\"}} 3\n");
    assert!(Counts::read(&failed).proves(Phase::Unavailable));
    let answered = format!("{failed}{LOOKUPS}{{outcome=\"redis\"}} 1\n");
    assert!(!Counts::read(&answered).proves(Phase::Unavailable));
    assert!(!Counts::read(&failed.replace(LIVE, "")).proves(Phase::Unavailable));
}

/// An answer from the local live layer is an answer: a blackout proof
/// must not pass while the sequencer answers from it.
#[test]
fn a_local_answer_breaks_the_blackout_proof() {
    let failed = format!("{LIVE}{LOOKUPS}{{outcome=\"timeout\"}} 3\n");
    let local = format!("{failed}{LOOKUPS}{{outcome=\"local\"}} 1\n");
    assert!(!Counts::read(&local).proves(Phase::Unavailable));
}
