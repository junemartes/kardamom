//! Component faults run the production actor and wire adapter without a deployment.

#[path = "component_recovery/support.rs"]
mod support;

#[path = "component_recovery/carry.rs"]
mod carry;

#[cfg(target_os = "linux")]
#[path = "component_recovery/pressure.rs"]
mod pressure;

use kardamom_cluster_adapter::gateway::OfferOutcome;
use kardamom_sequencer::SequencerError;
use support::Actor;

impl Actor {
    fn verify_output_recovery(fault: OfferOutcome) {
        let mut actor = Self::start();
        actor.seed();
        actor.peer.set_outcome(fault);
        (0..4).for_each(|nonce| actor.send(nonce));
        Self::wait_for(|| actor.attempts() >= 8);
        assert_eq!(actor.consumed(), 1, "blocked output must hold intake");
        actor.quiet(0);
        actor.peer.set_outcome(OfferOutcome::Accepted);
        (0..4).for_each(|nonce| {
            actor.expect_offer(usize::try_from(nonce).unwrap(), nonce);
            actor.canonical(nonce);
        });
        actor.quiet(4);
        actor.stop();
    }
}

#[test]
fn output_backpressure_and_disconnect_recover_without_new_client_input() {
    [OfferOutcome::BackPressured, OfferOutcome::NotConnected]
        .into_iter()
        .for_each(Actor::verify_output_recovery);
}

#[test]
fn lost_offer_on_session_restart_replays_the_exact_prefix_before_its_suffix() {
    let mut actor = Actor::start();
    actor.seed();
    actor.send(0);
    let lost = actor.expect_offer(0, 0);
    actor.send(1);
    Actor::wait_for(|| actor.consumed() == 2);
    actor.quiet(1);
    actor.peer.set_outcome(OfferOutcome::NotConnected);
    actor.rules.on_session_start(2);
    Actor::wait_for(|| actor.attempts() >= 8);
    actor.quiet(1);
    actor.peer.set_outcome(OfferOutcome::Accepted);
    assert_eq!(actor.expect_offer(1, 0), lost);
    actor.quiet(2);
    actor.canonical(0);
    actor.expect_offer(2, 1);
    actor.canonical(1);
    actor.quiet(3);
    actor.stop();
}

#[test]
fn absent_lookup_reply_holds_the_sender_until_the_real_account_event_arrives() {
    let mut actor = Actor::start();
    actor.send(0);
    let mut requested = None;
    Actor::wait_for(|| {
        requested = actor.lookups.try_recv().ok();
        requested.is_some()
    });
    assert_eq!(requested.unwrap(), Actor::envelope(0).sender);
    actor.quiet(0);
    actor.seed();
    actor.expect_offer(0, 0);
    actor.canonical(0);
    actor.quiet(1);
    actor.stop();
}

#[test]
fn stalled_input_resumes_and_permanent_input_close_stops_the_actor() {
    let mut actor = Actor::start();
    actor.seed();
    actor.input_stalled(true);
    actor.send(0);
    actor.quiet(0);
    assert_eq!(actor.consumed(), 0);
    actor.input_stalled(false);
    actor.expect_offer(0, 0);
    actor.canonical(0);
    actor.close_input();
    actor.finish().unwrap();
}

#[test]
fn delayed_canonical_input_fills_the_outbox_then_resumes_in_order() {
    let mut actor = Actor::start();
    actor.seed();
    (0..4).for_each(|nonce| actor.send(nonce));
    actor.expect_offer(0, 0);
    Actor::wait_for(|| actor.consumed() == 3);
    actor.quiet(1);
    assert_eq!(
        actor.consumed(),
        3,
        "the bounded outbox retains the fourth input upstream"
    );
    (0..3).for_each(|nonce| {
        actor.canonical(nonce);
        actor.expect_offer(usize::try_from(nonce + 1).unwrap(), nonce + 1);
    });
    actor.canonical(3);
    actor.quiet(4);
    actor.stop();
}

#[test]
fn a_permanent_output_encoding_failure_is_reported_instead_of_retried() {
    let mut actor = Actor::start();
    actor.seed();
    actor
        .peer
        .set_outcome(OfferOutcome::TooLarge { len: 2 << 20 });
    actor.send(0);
    assert!(matches!(
        actor.finish(),
        Err(SequencerError::EncodeFailed(_))
    ));
    assert_eq!(actor.attempts(), 1);
    assert!(actor.peer.accepted().is_empty());
}

#[test]
fn shutdown_completes_while_output_is_disconnected() {
    let mut actor = Actor::start();
    actor.seed();
    actor.peer.set_outcome(OfferOutcome::NotConnected);
    actor.send(0);
    Actor::wait_for(|| actor.attempts() >= 8);
    actor.stop();
}
