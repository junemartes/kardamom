//! An opt-in measurement of blocked offers through the production actor.

use super::{Actor, OfferOutcome};

struct RetryProbe;

impl RetryProbe {
    fn cpu_ticks() -> u64 {
        let text = std::fs::read_to_string("/proc/self/stat").unwrap();
        let (_, fields) = text.rsplit_once(')').unwrap();
        fields
            .split_whitespace()
            .skip(11)
            .take(2)
            .map(|v| v.parse::<u64>().unwrap())
            .sum()
    }

    fn attempts(actors: &[Actor]) -> usize {
        actors.iter().map(Actor::attempts).sum()
    }
}

impl Actor {
    fn resume_offers(&self) {
        self.peer.set_outcome(OfferOutcome::Accepted);
    }

    fn verify_retry_recovery(&mut self) {
        self.expect_offer(0, 0);
        self.canonical(0);
        self.send(1);
        self.expect_offer(1, 1);
        self.canonical(1);
    }
}

#[test]
#[ignore = "local retry-pressure measurement, not a latency benchmark"]
fn measure_persistent_disconnection_retries() {
    let mut actors: Vec<_> = (0..4)
        .map(|_| {
            let actor = Actor::start();
            actor.seed();
            actor.peer.set_outcome(OfferOutcome::NotConnected);
            actor.send(0);
            actor
        })
        .collect();
    Actor::wait_for(|| actors.iter().all(|a| a.attempts() >= 8));
    std::thread::sleep(std::time::Duration::from_secs(1));
    let before = RetryProbe::attempts(&actors);
    let cpu_before = RetryProbe::cpu_ticks();
    let started = std::time::Instant::now();
    std::thread::sleep(std::time::Duration::from_secs(5));
    let seconds = started.elapsed().as_secs_f64();
    let ticks = RetryProbe::cpu_ticks().checked_sub(cpu_before).unwrap();
    let attempts = RetryProbe::attempts(&actors).checked_sub(before).unwrap();
    let recovery = std::time::Instant::now();
    actors.iter().for_each(Actor::resume_offers);
    actors.iter_mut().for_each(Actor::verify_retry_recovery);
    let recovered_ms = recovery.elapsed().as_secs_f64() * 1000.0;
    actors.iter_mut().for_each(Actor::stop);
    println!(
        "RETRY_PROBE {{\"actors\":4,\"seconds\":{seconds},\"cpu_ticks\":{ticks},\"attempts\":{attempts},\"recovery_ms\":{recovered_ms},\"exact_delivery\":true}}"
    );
}
