//! The test seam of the `floor-seams` feature on the v2 loop: a row sets
//! no first state of a sender, so the first state comes only from a
//! lookup. Only the stack suite turns it on.

use alloy_primitives::Address;

use super::AdmissionLoop;
use crate::admission::AccountState;

impl AdmissionLoop {
    /// The test seam of the stack suite: a row sets no first state of a
    /// sender, so the first state comes only from a lookup. A sender with
    /// parked work asks a lookup instead. Only the `floor-seams` feature
    /// compiles this method.
    pub fn enable_first_floor_from_lookup(&mut self, on: bool) {
        self.first_floor_from_lookup = on;
    }

    /// The test seam: with it, only the rows of the senders that the
    /// filter holds a state of. The other senders wait for a lookup.
    pub(super) fn rows_of_known_senders(
        &mut self,
        rows: Vec<(Address, AccountState)>,
    ) -> Vec<(Address, AccountState)> {
        if !self.first_floor_from_lookup {
            return rows;
        }
        rows.into_iter()
            .filter(|(address, _)| {
                !self
                    .admission
                    .waits_for_lookup(*address, &mut self.verdicts)
            })
            .collect()
    }
}
