//! The origin lanes' relay step, and the one-slot retry pump.
//!
//! [`OriginLane`] is the step every lane that forwards one record off a
//! subscription onto `tx_ordering` implements: [`crate::epoch`] and
//! [`crate::remote_epoch`]. [`Pump`] is the one-slot body of the
//! remote-epoch lane: it holds the record popped off the subscription but
//! not yet accepted by the cluster. The epoch lane keeps every epoch until
//! a boundary confirms it, so it has its own body,
//! [`crate::epoch::EpochPump`].

use kardamom_types::BPosition;

use crate::error::SequencerError;

/// One record popped from a subscription but not yet accepted by the
/// cluster. [`Pump`] holds it here across a `Backpressure` result and
/// retries it before it polls again.
pub struct Held<T> {
    pub pos: BPosition,
    pub record: T,
}

/// One origin lane's relay step: poll one record off `sub` and publish it
/// through `publ`, with the lane's retry state on `self`.
pub trait OriginLane<S, P> {
    /// Relay one record. Returns `Ok(true)` if a record was processed,
    /// `Ok(false)` if the subscription was idle and nothing was held.
    ///
    /// # Errors
    ///
    /// Returns the subscription's error if the poll fails, or the
    /// publisher's error (including [`SequencerError::Backpressure`]) if
    /// the publish fails.
    fn relay(&mut self, sub: &mut S, publ: &mut P) -> Result<bool, SequencerError>;
}

/// Single-step origin-advancing pump. Owns the one-slot retry state
/// described by [`Held`].
pub struct Pump<T> {
    held: Option<Held<T>>,
}

impl<T> Default for Pump<T> {
    fn default() -> Self {
        Self { held: None }
    }
}

impl<T> Pump<T> {
    /// Take the held record if there is one, else poll one via `poll`,
    /// then publish it via `publish`.
    ///
    /// On a successful publish, calls `on_relayed` once with the
    /// published record — a lane with no metric to bump passes `|_| {}`.
    ///
    /// On [`SequencerError::Backpressure`], the record goes back into the
    /// held slot, so the next call retries the SAME record before it
    /// polls again. The poll is assumed destructive, so without this slot
    /// a backpressured record would be lost.
    ///
    /// Returns `Ok(true)` if a record was processed, `Ok(false)` if the
    /// subscription was idle and nothing was held.
    ///
    /// # Errors
    ///
    /// Returns the subscription's error if `poll` fails, or the
    /// publisher's error (including [`SequencerError::Backpressure`]) if
    /// `publish` fails.
    pub fn step(
        &mut self,
        poll: impl FnOnce() -> Result<Option<(BPosition, T)>, SequencerError>,
        publish: impl FnOnce(&T) -> Result<(), SequencerError>,
        on_relayed: impl FnOnce(&T),
    ) -> Result<bool, SequencerError> {
        let Held { pos, record } = match self.held.take() {
            Some(held) => held,
            None => match poll()? {
                Some((pos, record)) => Held { pos, record },
                None => return Ok(false),
            },
        };
        if let Err(e) = publish(&record) {
            if matches!(e, SequencerError::Backpressure) {
                self.held = Some(Held { pos, record });
            }
            return Err(e);
        }
        on_relayed(&record);
        Ok(true)
    }

    /// Whether a record is currently held, mid-retry after a
    /// `Backpressure` result. Test-only: lets the shared pump contract
    /// assert on this state without a field access.
    #[cfg(test)]
    pub(crate) fn is_held(&self) -> bool {
        self.held.is_some()
    }
}
