//! Backoff and observation of each admission-loop turn.

use std::time::Duration;

use kardamom_log::aeron_live::{IdleBackoff, IdleCap};

use super::AdmissionLoop;
use crate::error::SequencerError;
use crate::pipeline::PublishMeters;
use crate::sequencer::SequencerPorts;

impl AdmissionLoop {
    /// Run until `shutdown` or until the input closes. An idle turn waits
    /// with the same backoff as the v1 loop; a back-pressured one waits
    /// 10 µs.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Self::run_once`] other than back-pressure
    /// and a closed input.
    pub fn run<P: SequencerPorts>(
        &mut self,
        ports: &mut P,
        shutdown: &crate::sequencer::Shutdown,
        idle_cap: IdleCap,
    ) -> Result<(), SequencerError> {
        let mut backoff = IdleBackoff::new(Duration::from_micros(1), Duration::from_micros(100), 1)
            .under(idle_cap, Duration::from_micros(1));
        while !shutdown.is_signaled() && self.run_tick(ports, &mut backoff)? {}
        Ok(())
    }

    /// One turn of [`Self::run`]. Returns whether to go on. The turn, its
    /// wait included, counts in the state that its outcome names.
    fn run_tick<P: SequencerPorts>(
        &mut self,
        ports: &mut P,
        backoff: &mut IdleBackoff,
    ) -> Result<bool, SequencerError> {
        let outcome = self.run_once(ports);
        let state = PublishMeters::state_of(&outcome);
        let next = Self::after_turn(outcome, backoff);
        if self.pipeline.pass(state, ports.split().0) {
            self.outbox_depth.set(self.outbox.len());
            self.accounts_depth.set(self.accounts.len());
        }
        next
    }

    /// The dispatch of [`Self::run_tick`] on the outcome of one turn.
    fn after_turn(
        outcome: Result<bool, SequencerError>,
        backoff: &mut IdleBackoff,
    ) -> Result<bool, SequencerError> {
        match outcome {
            Ok(true) => {
                backoff.reset();
                Ok(true)
            }
            Ok(false) => {
                std::thread::sleep(backoff.idle_wait());
                Ok(true)
            }
            Err(SequencerError::Backpressure) => {
                std::thread::sleep(Duration::from_micros(10));
                Ok(true)
            }
            Err(SequencerError::IngressDisconnected) => Ok(false),
            Err(e) => Err(e),
        }
    }
}
