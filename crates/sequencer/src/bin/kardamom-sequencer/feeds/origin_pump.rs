//! The loop of one origin lane, and the `origin_gap` halt it raises.

use std::time::Duration;

use kardamom_log::aeron_live::IdleBackoff;
use kardamom_obs::halt;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::pump::OriginLane;
use kardamom_sequencer::sequencer::Shutdown;

/// How long the loop waits between two retries while its lane is
/// stalled. Short: a stalled epoch lane still reads the subscription,
/// one epoch per retry, to find the missing epoch.
const STALL_RETRY: Duration = Duration::from_millis(1);

/// One origin-advancing pump: relay through `lane` from `sub` to `publ`,
/// and idle-backoff, until `shutdown` fires or the source disconnects.
///
/// `Lane` is `kardamom_sequencer::epoch::EpochPump` or
/// `kardamom_sequencer::pump::Pump<RemoteEpochRecord>`. A lane error
/// that names a halt does not end the loop: the loop raises the halt,
/// retries, and clears the halt on the lane's next success.
pub(super) struct OriginPump<S, P, Lane> {
    shutdown: Shutdown,
    sub: S,
    publ: P,
    lane: Lane,
    /// The detail of the halt this loop raised, while it stands.
    halted: Option<String>,
}

impl<S, P, Lane> OriginPump<S, P, Lane>
where
    Lane: OriginLane<S, P>,
{
    pub(super) fn new(shutdown: Shutdown, sub: S, publ: P, lane: Lane) -> Self {
        Self {
            shutdown,
            sub,
            publ,
            lane,
            halted: None,
        }
    }

    /// Run until `shutdown` fires or the source disconnects.
    pub(super) fn run(mut self) -> Result<(), SequencerError> {
        let mut idle = IdleBackoff::new(Duration::from_micros(1), Duration::from_micros(100), 1);
        while !self.shutdown.is_signaled() && self.tick(&mut idle)? {}
        Ok(())
    }

    /// One [`Self::run`] iteration: dispatch on the lane's relay outcome.
    /// Returns whether the loop should keep going; `false` only on a
    /// clean `IngressDisconnected` exit.
    fn tick(&mut self, idle: &mut IdleBackoff) -> Result<bool, SequencerError> {
        match self.lane.relay(&mut self.sub, &mut self.publ) {
            Ok(true) => {
                self.resume();
                idle.reset();
                Ok(true)
            }
            Ok(false) => {
                self.resume();
                std::thread::sleep(idle.idle_wait());
                Ok(true)
            }
            Err(SequencerError::Backpressure) => {
                std::thread::sleep(Duration::from_micros(10));
                Ok(true)
            }
            Err(SequencerError::IngressDisconnected) => Ok(false),
            Err(e) => self.stall(e),
        }
    }

    /// Raise the halt `e` names, and retry after [`STALL_RETRY`]. An error
    /// that names no halt ends the loop. The halt is raised again only
    /// when its detail changes or an operator cleared it, so a stall that
    /// lasts does not wake the lifecycle watchers on every retry.
    fn stall(&mut self, e: SequencerError) -> Result<bool, SequencerError> {
        let Some(stalled) = e.halt() else {
            return Err(e);
        };
        if self.halted.as_deref() != Some(stalled.detail.as_str()) || halt::current().is_none() {
            self.halted = Some(stalled.detail.clone());
            halt::raise(stalled);
        }
        std::thread::sleep(STALL_RETRY);
        Ok(true)
    }

    /// The lane made progress: clear the halt this loop raised.
    fn resume(&mut self) {
        if self.halted.take().is_some() {
            halt::clear();
        }
    }
}
