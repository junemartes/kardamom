//! How a frozen consumer heals once the window of the sealer passed its
//! cursor: the repair from a peer checkpoint, or the read of the egress.

use std::time::Duration;

use super::{Repair, Victim};
use crate::harness::Harness;
use crate::poll::{self, Budget};

/// The line of a consumer whose replay the sealer refused.
const REFUSED: &str = "cluster replay unavailable";
/// The line of a staged peer checkpoint.
const PREPARED: &str = "resync prepared: peer checkpoint staged";
/// The line of a carry executor that leaves its input source. Its first
/// source is the member feed of its node, so after a start below the
/// window of the member, the switch goes to the egress.
const SOURCE_SWITCH: &str = "switching the input source";

/// How the frozen consumer heals once the window of the sealer passed
/// its cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Heal {
    /// The sealer refuses the replay. The consumer fetches a peer
    /// checkpoint, parks its state, and restores or adopts the checkpoint
    /// in-process.
    Repair,
    /// The consumer reads the range from the egress, which keeps far more
    /// blocks than the exact window of the sealer. It logs no refused
    /// replay and restores no checkpoint. A carry executor also logs its
    /// switch from the member feed to the egress. A carry-born cluster
    /// heals this way: the executors read the member feed and then the
    /// egress, and the validator reads the egress.
    Egress,
}

impl Heal {
    pub(super) fn of(genesis_carry: bool) -> Self {
        if genesis_carry {
            Self::Egress
        } else {
            Self::Repair
        }
    }

    /// The shortest freeze of an overrun. Every path outlasts the
    /// evicting freeze of the Aeron stall tolerance. The repair path also
    /// crosses the 90 s cluster session timeout, so its resume goes
    /// through a fresh session. The egress path holds no cluster session.
    pub(super) fn shortest_freeze(self, h: &Harness) -> Duration {
        let evicting = h.knobs.aeron_stall.evicting_freeze();
        match self {
            Self::Repair => evicting.max(Duration::from_mins(2)),
            Self::Egress => evicting,
        }
    }

    /// The log lines that tell the heal of `victim`.
    pub(super) fn needles(self, victim: Victim) -> [&'static str; 3] {
        match self {
            Self::Repair => [REFUSED, PREPARED, victim.restored_needle()],
            Self::Egress => [SOURCE_SWITCH, REFUSED, victim.restored_needle()],
        }
    }
}

/// The frozen executor of a carry-born cluster switches from the member
/// feed to the egress after the thaw. The validator reads the egress from
/// its own cursor and logs no switch: its verifying gives the proof.
pub(super) async fn await_egress_read(
    h: &Harness,
    victim: Victim,
    ctx: &str,
    read: &Repair,
) -> anyhow::Result<()> {
    if victim == Victim::Validator {
        return Ok(());
    }
    let outcome = poll::until(Budget::secs(300, 6), |_| async move {
        Ok(read.seen(h).await?.0.then_some(()))
    })
    .await?;
    let ((), elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the executor logged no '{SOURCE_SWITCH}' within {}s after the thaw: it did not leave the member feed for the egress",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: the executor left the member feed for the egress ({}s after thaw)",
        elapsed.as_secs()
    ));
    Ok(())
}

/// The egress kept the range, so no consumer met a refused replay or
/// restored a peer checkpoint.
pub(super) async fn assert_no_repair(h: &Harness, ctx: &str, read: &Repair) -> anyhow::Result<()> {
    let (_, refused, restored) = read.seen(h).await?;
    anyhow::ensure!(
        !refused && !restored,
        "{}: {ctx}: the consumer met a refused replay ({refused}) or restored a checkpoint ({restored}), but the egress keeps the range",
        crate::FAIL_PREFIX
    );
    crate::log(format!(
        "{ctx}: the consumer healed from the egress with no refused replay and no checkpoint"
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_carry_born_consumer_heals_from_the_egress() {
        assert_eq!(Heal::of(false), Heal::Repair);
        assert_eq!(Heal::of(true), Heal::Egress);
        assert_eq!(
            Heal::Repair.needles(Victim::Validator),
            [REFUSED, PREPARED, "adopted state from checkpoint"]
        );
        // The egress heal counts the refusal and the restore only to
        // require that neither happens.
        assert_eq!(
            Heal::Egress.needles(Victim::Executor),
            [SOURCE_SWITCH, REFUSED, "restored state from checkpoint"]
        );
    }
}
