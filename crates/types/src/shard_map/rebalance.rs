use core::num::NonZeroU8;

use super::{LANE_COUNT, ShardMap, ShardMapError, VSLOT_COUNT};

impl ShardMap {
    /// Balance the next map with the fewest slot moves. Ties favor the lowest lane.
    ///
    /// # Errors
    ///
    /// Returns an error for a lane outside the physical plane, a target outside
    /// 1..=8, or an exhausted map version.
    pub fn rebalance(&self, target: u32) -> Result<Self, ShardMapError> {
        let target = u8::try_from(target)
            .ok()
            .and_then(NonZeroU8::new)
            .filter(|n| n.get() <= LANE_COUNT)
            .ok_or(ShardMapError::AboveLanePlane(target))?;
        self.validate()?;
        let version = self
            .version
            .checked_add(1)
            .ok_or(ShardMapError::VersionExhausted)?;
        let mut balance = Balance::new(self.table, target);
        if target.get() != self.active_lanes() {
            (0..VSLOT_COUNT).for_each(|slot| balance.move_slot(slot));
        }
        Ok(Self::from_table(version, balance.table))
    }
}

struct Balance {
    table: [u8; VSLOT_COUNT],
    load: [usize; LANE_COUNT as usize],
    want: [usize; LANE_COUNT as usize],
}

impl Balance {
    fn new(table: [u8; VSLOT_COUNT], target: NonZeroU8) -> Self {
        let count = usize::from(target.get());
        let mut load = [0; LANE_COUNT as usize];
        for lane in &table {
            load[usize::from(*lane)] += 1;
        }
        let want = core::array::from_fn(|lane| {
            if lane < count {
                VSLOT_COUNT / count + usize::from(lane < VSLOT_COUNT % count)
            } else {
                0
            }
        });
        Self { table, load, want }
    }

    fn move_slot(&mut self, slot: usize) {
        let lane = usize::from(self.table[slot]);
        if self.load[lane] <= self.want[lane] {
            return;
        }
        // Reverse iteration makes max_by_key choose the lowest lane on a tie.
        let dest = (0..LANE_COUNT)
            .rev()
            .max_by_key(|lane| {
                let lane = usize::from(*lane);
                self.want[lane].saturating_sub(self.load[lane])
            })
            .expect("the physical lane plane is nonempty");
        self.table[slot] = dest;
        // Each step moves one of 256 slots from an overfull lane to a deficit.
        self.load[lane] -= 1;
        self.load[usize::from(dest)] += 1;
    }
}

#[cfg(test)]
mod tests;
