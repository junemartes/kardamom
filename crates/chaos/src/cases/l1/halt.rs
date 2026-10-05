//! The one place a case judges "the service halted". Today the judgment
//! reads what each service exports: the followers' tick-outcome
//! counters. The halt record
//! (`kardamom_halt{service, cause, recovery}` and the `/halt` route)
//! replaces those reads here, and nowhere else.

use std::time::Duration;

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
