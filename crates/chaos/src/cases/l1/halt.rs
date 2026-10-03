//! The one place a case judges "the service halted". Today the judgment
//! reads what each service exports: the followers' tick-outcome
//! counters, the batcher's refusal line. The halt record
//! (`kardamom_halt{service, cause, recovery}` and the `/halt` route)
//! replaces those reads here, and nowhere else.

use std::time::Duration;

use super::batcher::{REFUSED_LINE, count};
use super::followers::Followers;
use crate::harness::Harness;
use crate::poll::{self, Budget};

/// How long a follower halt may take to show. The da-watcher ticks
/// every second and the indexer every two, so three ticks are six
/// seconds; the lie reaches them with the next block, and a wrong hash
/// shows one block later still. The log names the time it took.
const FOLLOWER_HALT_BUDGET: Duration = Duration::from_secs(60);

/// Both followers halted on a chain break since `base`.
pub(super) async fn await_followers_halted(
    h: &Harness,
    base: Followers,
    ctx: &str,
) -> anyhow::Result<()> {
    let last = std::cell::Cell::new(base);
    let last_ref = &last;
    let outcome = poll::until(
        Budget::new(FOLLOWER_HALT_BUDGET, Duration::from_secs(2)),
        |_| async move {
            let now = Followers::read(h).await;
            last_ref.set(now);
            Ok::<_, anyhow::Error>(now.chain_broke_since(base).then_some(now))
        },
    )
    .await?;
    let (now, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the followers did not halt on the lie within {}s (before: {}; last: {})",
            t.as_secs(),
            base.show(),
            last.get().show()
        )
    })?;
    crate::log(format!(
        "{ctx}: both followers halted after {}s ({})",
        elapsed.as_secs(),
        now.show()
    ));
    Ok(())
}

/// The batcher halted on a replay the sealers refused: its cursor is
/// past the retention floor. `refused0` is the refusal count before.
pub(super) async fn await_batcher_halted_on_replay(
    h: &Harness,
    refused0: usize,
    budget: Duration,
    ctx: &str,
) -> anyhow::Result<()> {
    let outcome = poll::until(
        Budget::new(budget, Duration::from_secs(5)),
        |_| async move {
            Ok::<_, anyhow::Error>((count(h, REFUSED_LINE).await? > refused0).then_some(()))
        },
    )
    .await?;
    let ((), elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the batcher never halted on a refused replay within {}s",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: the batcher halted on the refused replay after {}s",
        elapsed.as_secs()
    ));
    Ok(())
}
