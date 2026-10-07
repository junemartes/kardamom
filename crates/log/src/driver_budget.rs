//! How long a client waits for a quiet media-driver party before it
//! treats the party as gone.
//!
//! The Aeron C client waits up to its driver timeout for a live driver,
//! and the deploy sets that timeout to the stall tolerance of every Aeron
//! party (`AERON_DRIVER_TIMEOUT`). A wait that is shorter would end on a
//! stall that every party survives. So a wait is the driver timeout of the
//! client context plus [`DriverBudget::MARGIN`], and at least
//! [`DriverBudget::FLOOR`].

use std::time::Duration;

use crate::error::LogError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DriverBudget(Duration);

impl DriverBudget {
    /// The time past the driver timeout.
    const MARGIN: Duration = Duration::from_secs(5);

    /// The least budget, for a short driver timeout on a slow host.
    const FLOOR: Duration = Duration::from_secs(10);

    /// The budget for a client with a driver timeout of `ms`.
    ///
    /// # Errors
    ///
    /// Returns an error when the budget overflows a `Duration`.
    pub(crate) fn from_driver_timeout_ms(ms: u64) -> Result<Self, LogError> {
        Duration::from_millis(ms)
            .checked_add(Self::MARGIN)
            .map(|budget| Self(budget.max(Self::FLOOR)))
            .ok_or_else(|| {
                LogError::Aeron(format!(
                    "driver timeout {ms} ms overflows the aeron driver budget"
                ))
            })
    }

    /// The budget for the driver timeout that a client context uses: the
    /// `AERON_DRIVER_TIMEOUT` value, or a value that the caller set.
    ///
    /// # Errors
    ///
    /// Returns an error when the budget overflows a `Duration`.
    pub(crate) fn of_client(ctx: &rusteron_client::AeronContext) -> Result<Self, LogError> {
        Self::from_driver_timeout_ms(ctx.get_driver_timeout_ms())
    }

    /// [`Self::of_client`] for the context of an archive client.
    ///
    /// # Errors
    ///
    /// Returns an error when the budget overflows a `Duration`.
    pub(crate) fn of_archive_client(
        ctx: &rusteron_archive::AeronContext,
    ) -> Result<Self, LogError> {
        Self::from_driver_timeout_ms(ctx.get_driver_timeout_ms())
    }

    pub(crate) fn duration(self) -> Duration {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget_for(ms: u64) -> Duration {
        DriverBudget::from_driver_timeout_ms(ms).unwrap().duration()
    }

    #[test]
    fn the_budget_is_the_driver_timeout_plus_the_margin() {
        assert_eq!(budget_for(30_000), Duration::from_secs(35));
        assert_eq!(budget_for(10_000), Duration::from_secs(15));
        assert_eq!(budget_for(10_001), Duration::from_millis(15_001));
    }

    #[test]
    fn a_short_driver_timeout_keeps_the_floor() {
        assert_eq!(budget_for(0), Duration::from_secs(10));
        assert_eq!(budget_for(1_000), Duration::from_secs(10));
        assert_eq!(budget_for(5_000), Duration::from_secs(10));
    }

    #[test]
    fn the_largest_driver_timeout_has_a_budget() {
        assert!(DriverBudget::from_driver_timeout_ms(u64::MAX).is_ok());
    }
}
