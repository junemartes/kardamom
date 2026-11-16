//! The wake handshake of the verify agent in each interleaving of a
//! `loom` model: two producers push one item each and ring, and the agent
//! polls and parks until it saw both. A lost wake leaves the agent parked
//! for ever, and the model reports the deadlock.
//!
//! Run with `cargo test -p kardamom-ingress --features loom --test
//! doorbell_loom --release`.

#![cfg(feature = "loom")]

use std::time::Duration;

use kardamom_ingress::verify_agent::Doorbell;
use loom::sync::Arc;
use loom::sync::atomic::{AtomicUsize, Ordering};

/// The bound on the preemptions of one interleaving. The `loom` docs
/// advise 2 or 3: most concurrency bugs need few preemptions, and an
/// unbounded model of three threads does not end in reasonable time.
const PREEMPTIONS: usize = 3;

/// The agent: it polls the count of pushed items, and parks while it saw
/// no new one, until it saw both.
fn agent(bell: &Doorbell, pushed: &AtomicUsize) {
    bell.attach_current();
    loop {
        let seen = pushed.load(Ordering::Acquire);
        if seen == 2 {
            return;
        }
        bell.park(Duration::from_micros(50), || {
            pushed.load(Ordering::Acquire) > seen
        });
    }
}

/// A producer: one push, then the ring.
fn produce(bell: &Doorbell, pushed: &AtomicUsize) {
    pushed.fetch_add(1, Ordering::Release);
    bell.ring();
}

#[test]
fn no_push_between_the_last_poll_and_the_park_is_lost() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(PREEMPTIONS);
    model.check(|| {
        let bell = Arc::new(Doorbell::new());
        let pushed = Arc::new(AtomicUsize::new(0));
        let spawn = |role: fn(&Doorbell, &AtomicUsize)| {
            let (bell, pushed) = (bell.clone(), pushed.clone());
            loom::thread::spawn(move || role(&bell, &pushed))
        };
        let threads = [spawn(agent), spawn(produce), spawn(produce)];
        for thread in threads {
            thread.join().unwrap();
        }
    });
}
