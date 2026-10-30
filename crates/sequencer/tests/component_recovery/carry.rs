//! A held offer across a downgrade of the decision version.

use super::support::Actor;

/// The cluster session of the fixture.
const SESSION: i64 = 1;

/// The v2 loop holds nonce 1 for its gap while the sealer keeps version 5.
/// The sealer reports version 4, and nonce 0 arrives: the loop releases
/// both in nonce order, each in the format of version 4. Once the sealer
/// reports version 5 again, the next offer carries its bytes.
#[test]
fn a_held_offer_released_after_a_downgrade_takes_the_version_at_release() {
    let mut actor = Actor::start();
    actor.seed();
    actor.send(1);
    Actor::wait_for(|| actor.consumed() == 1);
    actor.quiet(0);
    actor.rules.on_sealer_info(SESSION, 4);
    actor.quiet(0);
    actor.send(0);
    actor.expect_frame(0, 0, false);
    actor.quiet(1);
    actor.canonical(0);
    actor.expect_frame(1, 1, false);
    actor.canonical(1);
    actor.rules.on_sealer_info(SESSION, 5);
    actor.send(2);
    actor.expect_offer(2, 2);
    actor.canonical(2);
    actor.quiet(3);
    actor.stop();
}
