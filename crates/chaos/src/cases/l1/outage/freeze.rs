//! The freeze of the batcher with an unposted group in its spool.

use std::time::Duration;

use crate::harness::Harness;
use crate::l1::L1;
use crate::poll::{self, Budget};
use crate::probes::BATCHER_PORT;

/// How many times the freeze is retried to land on an unposted group.
const FREEZE_ATTEMPTS: u32 = 10;
/// How long a freeze attempt holds the batcher before it reads the
/// covered block. A post in flight at the freeze lands on L1 in this
/// time. The attempt then stays under the Aeron client's service
/// interval (10 s), so a thaw for a retry does not restart the batcher.
const SETTLE: Duration = Duration::from_secs(5);

/// The spool root of the batcher on the aux node: its `--spool-dir`.
const SPOOL_ROOT: &str = "/opt/kardamom/batcher/spool";

/// The format registry of this build. The batcher checks its
/// `SPOOL_VERSION` against the `batcher-spool` entry, so the directory of
/// its spool follows from this entry.
const FORMATS: &str = include_str!("../../../../../../formats.toml");

/// One freeze attempt on the aux node: find the running batcher
/// container, then in one exec SIGSTOP it, read its process state from
/// `/proc`, list the block files of its spool, and SIGCONT it again
/// unless it is stopped with a non-empty spool. The exec takes about one
/// second. A stopped batcher then waits `SETTLE` for the covered block.
/// A scrape of the frozen exporter waits for its timeout, and does not
/// fit. Each attempt finds the container again: a restart between two
/// attempts can replace it.
pub(super) struct FreezeAttempt<'a> {
    aux: &'a str,
    /// The directory of the spool that the batcher of this build writes.
    spool: String,
}

impl<'a> FreezeAttempt<'a> {
    /// The attempt on the aux node `aux`, against the spool directory of
    /// this build.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry does not load or lists no
    /// `batcher-spool`.
    pub(super) fn new(aux: &'a str) -> anyhow::Result<Self> {
        let versions = kardamom_formats::Registry::parse(FORMATS)?
            .versions("batcher-spool")
            .ok_or_else(|| anyhow::anyhow!("the format registry lists no batcher-spool"))?;
        Ok(Self {
            aux,
            spool: format!("{SPOOL_ROOT}/v{}", versions.writes),
        })
    }

    /// The shell command that prints the count of block files in the
    /// spool directory and the number of the highest block, and `0 0`
    /// when the directory does not exist. The file names are zero-padded
    /// block numbers, so `ls` lists them in block order.
    fn count_command(&self) -> String {
        format!(
            "blocks=$(ls {} 2>/dev/null | grep '\\.block$'); last=$(printf '%s\\n' \"$blocks\" | tail -n 1); last=${{last%.block}}; echo \"$(printf '%s' \"$blocks\" | grep -c .) ${{last:-0}}\"",
            self.spool
        )
    }

    /// The shell script of the attempt on the container `inner`. It
    /// prints the process state, the count of spooled blocks and the
    /// highest spooled block, or `gone 0 0` when the container is gone
    /// before the signal.
    fn script(&self, inner: &str) -> String {
        let count = self.count_command();
        format!(
            "docker kill -s STOP {inner} >/dev/null 2>&1 || {{ echo 'gone 0 0'; exit 0; }}
pid=$(docker inspect -f '{{{{.State.Pid}}}}' {inner})
state=?
for _ in $(seq 1 40); do
  state=$(sed 's/.*) //' /proc/$pid/stat | cut -d' ' -f1)
  [ \"$state\" = T ] && break
  sleep 0.05
done
spool=$({count})
if [ \"$state\" != T ] || [ \"${{spool%% *}}\" = 0 ]; then docker kill -s CONT {inner} >/dev/null; fi
echo \"$state $spool\""
        )
    }

    /// Run the attempt. `Some` when the batcher stays frozen with a
    /// non-empty spool; `None` when no batcher container runs, or the
    /// spool was empty and the batcher runs again.
    ///
    /// # Errors
    ///
    /// Returns an error if the exec fails, or if the process is not
    /// stopped after the signal.
    async fn run(&self, h: &Harness, ctx: &str) -> anyhow::Result<Option<Frozen>> {
        let Some(inner) = h.nodes.inner_container(self.aux, "batcher").await else {
            crate::log(format!(
                "{ctx}: no batcher container runs on {}; the freeze waits for it",
                self.aux
            ));
            return Ok(None);
        };
        let out = h.nodes.exec(self.aux, &self.script(&inner)).await?;
        Frozen::parse(&out, inner, ctx)
    }

    /// Hold the stopped batcher for `SETTLE`, so a post in flight at the
    /// freeze lands, then read the block L1 covers. Keep the freeze when
    /// the spool holds a block past it. Else thaw the batcher for a
    /// retry: the restart drops a spool that L1 covers, so the thaw has
    /// no group to recover. A failed L1 read thaws the batcher too.
    ///
    /// # Errors
    ///
    /// Returns an error if the L1 read fails.
    async fn settle(
        &self,
        h: &Harness,
        l1: &L1,
        frozen: Frozen,
        ctx: &str,
    ) -> anyhow::Result<Option<Held>> {
        tokio::time::sleep(SETTLE).await;
        let covered = l1.covered_through().await;
        match &covered {
            Ok(covered) if frozen.holds_unposted(*covered) => {
                return Ok(Some(Held {
                    frozen,
                    covered: *covered,
                }));
            }
            Ok(covered) => crate::log(format!(
                "{ctx}: the spool of {} ends at block {}, and L1 covers through {covered}; thawing to retry",
                frozen.inner, frozen.last_block
            )),
            Err(error) => crate::log(format!(
                "{ctx}: the L1 read of the covered block fails ({error:#}); thawing {}",
                frozen.inner
            )),
        }
        if h.thaw(self.aux, &frozen.inner).await.is_err() {
            crate::log(format!(
                "{ctx}: SIGCONT failed (the task was replaced mid-freeze); the next attempt finds the new container"
            ));
        }
        covered.map(|_| None)
    }

    /// One attempt to the end: freeze, then settle. `None` when the
    /// batcher runs again.
    ///
    /// # Errors
    ///
    /// Returns an error if the exec or the L1 read fails, or if the
    /// process is not stopped after the signal.
    async fn hold(&self, h: &Harness, l1: &L1, ctx: &str) -> anyhow::Result<Option<Held>> {
        match self.run(h, ctx).await? {
            Some(frozen) => self.settle(h, l1, frozen, ctx).await,
            None => Ok(None),
        }
    }
}

/// A batcher stopped with a non-empty spool: its container, the count
/// of spooled blocks, and the highest spooled block.
pub(super) struct Frozen {
    pub(super) inner: String,
    pub(super) blocks: usize,
    pub(super) last_block: u64,
}

impl Frozen {
    /// Parse the attempt's output, `<state> <blocks> <last block>`.
    /// `None` when the container was gone or the spool was empty.
    fn parse(out: &str, inner: String, ctx: &str) -> anyhow::Result<Option<Self>> {
        let mut words = out.split_whitespace();
        let (Some(state), Some(blocks), Some(last), None) =
            (words.next(), words.next(), words.next(), words.next())
        else {
            return Err(crate::chaos_fail!(
                "{ctx}: the freeze attempt printed no state, count and last block: {out:?}"
            ));
        };
        let blocks: usize = blocks
            .parse()
            .map_err(|e| crate::chaos_fail!("{ctx}: spool listing is not a count: {e}"))?;
        let last_block: u64 = last.parse().map_err(|e| {
            crate::chaos_fail!("{ctx}: the last spooled block is not a number: {e}")
        })?;
        match state {
            "gone" => Ok(None),
            "T" => Ok((blocks > 0).then_some(Self {
                inner,
                blocks,
                last_block,
            })),
            _ => Err(crate::chaos_fail!(
                "{ctx}: freeze did NOT take effect (process state {state:?} after SIGSTOP, not T)"
            )),
        }
    }

    /// True when the spool holds a block past `covered`: a group that L1
    /// does not hold. A spool of only the batch in flight at the freeze
    /// holds none once L1 confirms that batch.
    fn holds_unposted(&self, covered: u64) -> bool {
        self.last_block > covered
    }
}

/// A frozen batcher whose spool holds an unposted group, and the block
/// L1 covered while it was frozen.
pub(super) struct Held {
    pub(super) frozen: Frozen,
    pub(super) covered: u64,
}

/// Freeze the batcher with an unposted group in its spool, so the thaw
/// has a group to recover. The spool empties for an instant after every
/// post, and the batch in flight at the freeze can land during it. An
/// attempt that holds no unposted group thaws the batcher and tries
/// again.
pub(super) async fn freeze_with_spool(
    h: &Harness,
    l1: &L1,
    aux: &str,
    ctx: &str,
) -> anyhow::Result<Held> {
    let attempt = FreezeAttempt::new(aux)?;
    let attempt = &attempt;
    let target = h.probes.aux_target(BATCHER_PORT);
    let target = &target;
    let outcome = poll::until(
        Budget::new(
            (Duration::from_secs(4) + SETTLE + h.knobs.restart_slo) * FREEZE_ATTEMPTS,
            Duration::from_secs(1),
        ),
        |_| async move {
            let held = attempt.hold(h, l1, ctx).await?;
            if held.is_none() {
                await_answering(h, target, ctx).await?;
            }
            Ok::<_, anyhow::Error>(held)
        },
    )
    .await?;
    let (held, _) = outcome.or_fail(|_| {
        crate::chaos_fail!(
            "{ctx}: the spool held no block past the covered block, or no batcher ran, on every freeze"
        )
    })?;
    crate::log(format!(
        "{ctx}: freeze verified (process state T, {} spooled blocks through {} in {})",
        held.frozen.blocks, held.frozen.last_block, held.frozen.inner
    ));
    Ok(held)
}

/// Wait until the batcher's metrics endpoint answers: after a thaw, the
/// task can restart, and its container is gone until the new one runs.
async fn await_answering(
    h: &Harness,
    target: &crate::metrics::Target,
    ctx: &str,
) -> anyhow::Result<()> {
    let budget = Budget::new(h.knobs.restart_slo, Duration::from_secs(1));
    poll::until(budget, |_| async move {
        Ok::<_, anyhow::Error>(h.probes.scrape().answers(target).await.then_some(()))
    })
    .await?
    .or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the batcher did not answer within {}s after a thaw",
            t.as_secs()
        )
    })
    .map(|_| ())
}

#[cfg(test)]
mod tests;
