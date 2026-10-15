//! The one place a case judges "the follower halted": the L1 follower's
//! failed ticks, and the da-watcher's pause with the follower as its
//! root.

use std::time::Duration;

use super::followers::Followers;
use crate::harness::Harness;
use crate::poll::{self, Budget};

/// How long a follower halt may take to show. The follower reads every
/// two seconds, and the lie reaches it with the next block; the
/// da-watcher pauses once the board shows every follower instance
/// halted, within a heartbeat (5 s), or once the stream is silent for its
/// silence window. The log names the time it took.
const FOLLOWER_HALT_BUDGET: Duration = Duration::from_secs(60);

/// The follower halted and the da-watcher paused on it since `base`.
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
            Ok::<_, anyhow::Error>(now.halted_since(base).then_some(now))
        },
    )
    .await?;
    let (now, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the follower did not halt on the lie, or the da-watcher did not pause on it, within {}s (before: {}; last: {})",
            t.as_secs(),
            base.show(),
            last.get().show()
        )
    })?;
    crate::log(format!(
        "{ctx}: the follower halted and the da-watcher paused on it after {}s ({})",
        elapsed.as_secs(),
        now.show()
    ));
    Ok(())
}
