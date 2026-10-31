//! The command wait of the Aeron thread, chosen once from the idle cap.
//!
//! - In crossbeam-channel 0.5.15, `Receiver::recv_timeout` spins with
//!   `Backoff::snooze` before it parks (`flavors/array.rs` and
//!   `flavors/list.rs`).
//! - `Select::select_timeout` tries the receiver once and parks at once.
//!
//! [`Spin`] keeps `recv_timeout` for every wait. [`ParkIdle`] uses
//! `select_timeout` for an empty wait, so an empty long-cap wait does not
//! spin. After useful work it keeps `recv_timeout`, so the thread sees a
//! command that follows the work before it parks. The measurement is in
//! `docs/benches/2026-10-28-empty-poll-parking.md`.

use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Select};

use super::RuntimeCmd;

#[cfg(test)]
mod tests;

pub(super) trait CommandWait {
    fn receive(
        &mut self,
        commands: &Receiver<RuntimeCmd>,
        wait: Duration,
        worked: bool,
    ) -> Result<RuntimeCmd, RecvTimeoutError>;
}

pub(super) struct Spin;

impl CommandWait for Spin {
    fn receive(
        &mut self,
        commands: &Receiver<RuntimeCmd>,
        wait: Duration,
        _worked: bool,
    ) -> Result<RuntimeCmd, RecvTimeoutError> {
        commands.recv_timeout(wait)
    }
}

pub(super) struct ParkIdle<'a> {
    commands: &'a Receiver<RuntimeCmd>,
    selected: Select<'a>,
}

impl<'a> ParkIdle<'a> {
    pub(super) fn new(commands: &'a Receiver<RuntimeCmd>) -> Self {
        let mut selected = Select::new();
        selected.recv(commands);
        Self { commands, selected }
    }
}

impl CommandWait for ParkIdle<'_> {
    fn receive(
        &mut self,
        commands: &Receiver<RuntimeCmd>,
        wait: Duration,
        worked: bool,
    ) -> Result<RuntimeCmd, RecvTimeoutError> {
        if worked {
            return commands.recv_timeout(wait);
        }
        self.selected
            .select_timeout(wait)
            .map_err(|_| RecvTimeoutError::Timeout)?
            .recv(self.commands)
            .map_err(|_| RecvTimeoutError::Disconnected)
    }
}
