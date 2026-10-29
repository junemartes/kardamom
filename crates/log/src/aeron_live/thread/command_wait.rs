//! Static wait strategies preserve the short-cap loop and park empty long-cap polls.

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
