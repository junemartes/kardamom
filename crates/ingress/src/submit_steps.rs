//! The step times of the submit path (see `kardamom_obs::pipeline`). The
//! ingress records them once for each request. A record is a few atomic
//! adds and allocates nothing.

use std::time::Instant;

use kardamom_obs::pipeline::Step;

/// The steps of one submit. Every clone of the proxy shares the handles.
#[derive(Clone)]
pub(crate) struct SubmitSteps {
    /// The whole submit on the server, from the call to the answer. With
    /// the receipt wait, it includes the wait for the receipt.
    pub(crate) submit: Step,
    /// The shared head of the submit: the checks, the decode, and the
    /// sender recovery. It includes `recover`.
    pub(crate) validate: Step,
    /// The sender recovery: the wait for a slot in the verifier intake,
    /// the batch window, and the recovery itself.
    pub(crate) recover: Step,
    /// The `tx_data` publish: the hop to the blocking pool and the round
    /// trip to the Aeron thread up to its ack.
    pub(crate) publish_round_trip: Step,
}

impl SubmitSteps {
    pub(crate) fn new() -> Self {
        Self {
            submit: Step::new("ingress.submit"),
            validate: Step::new("ingress.validate"),
            recover: Step::new("ingress.recover"),
            publish_round_trip: Step::new("ingress.publish_round_trip"),
        }
    }
}

/// A step that runs. It records its time when it drops, so every return
/// path of the step, and a cancel of its future, records one sample.
pub(crate) struct Timing<'a> {
    step: &'a Step,
    started: Instant,
}

impl<'a> Timing<'a> {
    /// Start `step` now.
    pub(crate) fn start(step: &'a Step) -> Self {
        Self {
            step,
            started: Instant::now(),
        }
    }
}

impl Drop for Timing<'_> {
    fn drop(&mut self) {
        self.step.since(self.started);
    }
}

#[cfg(test)]
#[path = "submit_steps_tests.rs"]
mod tests;
