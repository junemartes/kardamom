//! A cold client must not read an established sender from a stale, live projection.

use std::fmt::Write;
use std::num::NonZeroU64;
use std::time::Duration;

use anyhow::Context;

use super::{MIRROR_HEAD_KEYS, aux, live_primary, redis_cli, wait_readers_recovered};
use crate::harness::Harness;
use crate::metrics;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::rpc::{AccountState, Rpc};

const CTX: &str = "mirror-backlog-client-restart";
/// The fixture uses the service default of 8192 canonical positions.
const STALE_GAP: i64 = 8_192;

struct Baseline {
    primary: String,
    heads: Heads,
    account: AccountState,
}

#[derive(Clone, Copy)]
struct Heads([NonZeroU64; 3]);

impl Heads {
    fn parse(raw: &str) -> anyhow::Result<Self> {
        let heads = raw
            .lines()
            .map(|line| {
                anyhow::ensure!(line.len() == 20, "{CTX}: invalid mirror head {line:?}");
                Ok(line.parse::<NonZeroU64>()?)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self(heads.try_into().map_err(|_| {
            anyhow::anyhow!("{CTX}: expected all three live mirror heads")
        })?))
    }

    fn keep_alive(&self) -> String {
        self.0
            .iter()
            .enumerate()
            .fold(String::new(), |mut script, (id, head)| {
                write!(
                    script,
                    "redis.call('SET','head:{id}','{head:020}','EX',600);"
                )
                .expect("writing to a String is infallible");
                script
            })
    }

    fn advanced_over(self, before: Self) -> bool {
        self.0.into_iter().zip(before.0).all(|(now, old)| now > old)
    }
}

impl Baseline {
    fn report_cleanup(result: Result<(), &anyhow::Error>) {
        if let Err(error) = result {
            crate::log(format!("{CTX}: fault cleanup fails: {error:#}"));
        }
    }

    async fn capture(h: &Harness) -> anyhow::Result<Self> {
        let account = AccountState::after_transfer(h, h.knobs.gate_account).await?;
        let primary = live_primary(h, CTX).await?;
        let raw = h
            .nodes
            .exec(
                &aux(h),
                &redis_cli(&format!("--raw -h {primary} MGET {MIRROR_HEAD_KEYS}")),
            )
            .await?;
        let baseline = Self {
            primary,
            heads: Heads::parse(raw.trim_end())?,
            account,
        };
        baseline.await_row(h, &baseline.account).await?;
        Ok(baseline)
    }

    async fn row(&self, h: &Harness) -> anyhow::Result<String> {
        h.nodes
            .exec(
                &aux(h),
                &redis_cli(&format!(
                    "--raw -h {} HMGET acct:{} nonce balance tx_idx",
                    self.primary, self.account.address
                )),
            )
            .await
    }

    async fn await_row(&self, h: &Harness, account: &AccountState) -> anyhow::Result<()> {
        let last = std::cell::RefCell::new(String::from("no row response"));
        let last_ref = &last;
        poll::until(Budget::secs(120, 2), |_| async move {
            let observed = self.row(h).await?;
            let complete = account.matches_projection(&observed);
            last_ref.replace(observed);
            Ok::<_, anyhow::Error>(complete.then_some(()))
        })
        .await?
        .or_fail(|elapsed| crate::chaos_fail!(
            "{CTX}: exact account row is not projected after {}s: expected {account:?}, observed {:?}, primary {}",
            elapsed.as_secs(), last.borrow(), self.primary
        ))?;
        Ok(())
    }

    /// A writer can resume from a projection another writer rebuilt.
    /// Every writer must publish two later coverage heads, and the shared
    /// row must match the committed account within the same budget.
    async fn await_recovery(&self, h: &Harness, account: &AccountState) -> anyhow::Result<()> {
        let first = std::cell::Cell::new(None::<Heads>);
        let last = std::cell::RefCell::new(String::from("no projection response"));
        poll::until(Budget::secs(300, 5), |_| async {
            let raw = h.nodes.exec(&aux(h), &redis_cli(&format!(
                "--raw -h {} MGET {MIRROR_HEAD_KEYS}", self.primary
            ))).await?;
            last.replace(raw.clone());
            let Ok(heads) = Heads::parse(raw.trim_end()) else {
                return Ok(None);
            };
            if !heads.advanced_over(self.heads) {
                return Ok(None);
            }
            let advancing = first.get().is_some_and(|floor| heads.advanced_over(floor));
            if first.get().is_none() {
                first.set(Some(heads));
            }
            Ok((advancing && account.matches_projection(&self.row(h).await?)).then_some(()))
        }).await?.or_fail(|elapsed| crate::chaos_fail!(
            "{CTX}: restored writers do not advance every coverage head twice with exact account state within {}s (heads {:?})",
            elapsed.as_secs(), last.borrow()
        ))?;
        crate::log(format!(
            "{CTX}: every restored writer advances its coverage head twice and the projected account matches committed state"
        ));
        Ok(())
    }

    async fn exercise(&self, h: &mut Harness, mirrors: &SavedJob) -> anyhow::Result<AccountState> {
        mirrors.stop().await?;
        // The held heartbeat models live writers whose completed coverage is behind.
        // Its value never advances; no account row is changed by the fault.
        h.nodes
            .exec(
                &aux(h),
                &redis_cli(&format!(
                    "-h {} EVAL \"{} return 1\" 0",
                    self.primary,
                    self.heads.keep_alive()
                )),
            )
            .await?;
        Rpc::new(&h.rpc_url, h.knobs.chain_id)?
            .transfer_hash(
                h.knobs.gate_account,
                self.account.nonce,
                Duration::from_secs(60),
            )
            .await?;
        let floor = self
            .account
            .nonce
            .checked_add(1)
            .context("established sender nonce overflows")?;
        let expected = AccountState::replicated(h, h.knobs.gate_account, floor).await?;
        anyhow::ensure!(
            expected.nonce > self.account.nonce && expected.balance < self.account.balance,
            "{CTX}: the executor did not commit the established sender's transfer"
        );
        let ingress = h.probes.ingresses[1].clone();
        h.inject_hard(&[&ingress.container], "ingress").await?;
        h.assert_count("ingress", 2, h.knobs.restart_slo).await?;
        h.assert_ingress_pair_live(CTX).await?;
        Self::await_stale(h, &ingress).await?;
        self.assert_client(h, &ingress, &expected).await?;
        Ok(expected)
    }

    async fn await_stale(h: &Harness, ingress: &crate::probes::Probed) -> anyhow::Result<()> {
        poll::until(Budget::secs(180, 2), |_| async move {
            let body = h
                .probes
                .scrape()
                .fetch(&h.probes.ingress_target(ingress))
                .await;
            Ok::<_, anyhow::Error>(
                body.and_then(|body| metrics::first(&body, "kardamom_cache_head_lag_txs"))
                    .filter(|lag| *lag > STALE_GAP),
            )
        })
        .await?
        .or_fail(|_| crate::chaos_fail!("{CTX}: live projection lag never exceeds {STALE_GAP}"))?;
        Ok(())
    }

    async fn assert_client(
        &self,
        h: &Harness,
        ingress: &crate::probes::Probed,
        expected: &AccountState,
    ) -> anyhow::Result<()> {
        let before = Reader::read(h, ingress).await?;
        let observed = Rpc::new(&ingress.rpc_url(), h.knobs.chain_id)?
            .account_state(h.knobs.gate_account)
            .await?;
        let after = Reader::read(h, ingress).await?;
        anyhow::ensure!(
            observed == *expected,
            "{CTX}: restarted client reads {observed:?}; executor holds {expected:?}; stale Redis holds {:?}",
            self.account
        );
        anyhow::ensure!(
            after.proves_bypass(&before),
            "{CTX}: nonce and balance did not both bypass stale Redis on a local miss: {before:?} -> {after:?}"
        );
        anyhow::ensure!(
            self.account.matches_projection(&self.row(h).await?),
            "{CTX}: the stale row changed while its mirror job was stopped"
        );
        let raw = h
            .nodes
            .exec(
                &aux(h),
                &redis_cli(&format!(
                    "--raw -h {} MGET {MIRROR_HEAD_KEYS}",
                    self.primary
                )),
            )
            .await?;
        anyhow::ensure!(
            Heads::parse(raw.trim_end())?.0 == self.heads.0,
            "{CTX}: held progress markers changed"
        );
        crate::log(format!(
            "{CTX}: restarted client reads exact nonce and balance from executor fallback while Redis stays stale and reachable"
        ));
        Ok(())
    }
}

#[derive(Debug)]
struct Reader {
    stale: i64,
    live_hits: i64,
    live_misses: i64,
    redis_hits: i64,
}

impl Reader {
    async fn read(h: &Harness, ingress: &crate::probes::Probed) -> anyhow::Result<Self> {
        let body = h
            .probes
            .scrape()
            .fetch(&h.probes.ingress_target(ingress))
            .await
            .context("restart client exporter is unavailable")?;
        Ok(Self::parse(&body))
    }

    fn parse(body: &str) -> Self {
        Self {
            stale: metrics::sum_where(body, "kardamom_cache_degraded_total", "reason=\"stale\"")
                .unwrap_or(0),
            live_hits: Self::lookups(body, "live", "hit"),
            live_misses: Self::lookups(body, "live", "miss"),
            redis_hits: Self::lookups(body, "redis", "hit"),
        }
    }

    fn lookups(body: &str, layer: &str, outcome: &str) -> i64 {
        let matching = body
            .lines()
            .filter(|line| line.contains(&format!("outcome=\"{outcome}\"")))
            .collect::<Vec<_>>()
            .join("\n");
        metrics::sum_where(
            &matching,
            "kardamom_cache_lookups_total",
            &format!("layer=\"{layer}\""),
        )
        .unwrap_or(0)
    }

    fn proves_bypass(&self, before: &Self) -> bool {
        self.stale
            .checked_sub(before.stale)
            .is_some_and(|delta| delta >= 2)
            && self
                .live_misses
                .checked_sub(before.live_misses)
                .is_some_and(|delta| delta >= 2)
            && self.live_hits == before.live_hits
            && self.redis_hits == before.redis_hits
    }
}

pub(crate) async fn client_restart(h: &mut Harness) -> anyhow::Result<()> {
    let baseline = Baseline::capture(h).await?;
    let mirrors = SavedJob::capture(&h.nomad, "state-mirror").await?;
    let verdict = baseline.exercise(h, &mirrors).await;
    let cleared = h
        .nodes
        .exec(
            &aux(h),
            &redis_cli(&format!("-h {} DEL {MIRROR_HEAD_KEYS}", baseline.primary)),
        )
        .await;
    let restored = mirrors.restore().await;
    for result in [cleared.as_ref().map(|_| ()), restored.as_ref().copied()] {
        Baseline::report_cleanup(result);
    }
    let report = serde_json::json!({
        "case": CTX,
        "fault": "mirror job stopped; fixed old heads held live for at most 600 seconds",
        "stale_account": format!("{:?}", baseline.account),
        "held_heads": baseline.heads.0.map(NonZeroU64::get),
        "expected": verdict.as_ref().ok().map(|account| format!("{account:?}")),
        "failure": verdict.as_ref().err().map(|error| format!("{error:#}")),
        "clear_failure": cleared.as_ref().err().map(|error| format!("{error:#}")),
        "restore_failure": restored.as_ref().err().map(|error| format!("{error:#}")),
    });
    let saved = std::fs::write(
        "/tmp/chaos-mirror-backlog-client-restart-fault.json",
        serde_json::to_vec_pretty(&report)?,
    );
    let expected = verdict?;
    cleared?;
    restored?;
    saved?;
    baseline.await_recovery(h, &expected).await?;
    wait_readers_recovered(h, CTX).await?;
    h.assert_progress().await
}

#[cfg(test)]
mod tests {
    use super::{Heads, Reader};

    #[test]
    fn projection_recovery_requires_a_later_head_from_every_writer() {
        let parse =
            |values: [u64; 3]| Heads(values.map(|value| std::num::NonZeroU64::new(value).unwrap()));
        let before = parse([10, 20, 30]);
        assert!(parse([11, 21, 31]).advanced_over(before));
        assert!(!parse([100, 200, 30]).advanced_over(before));
        assert!(!parse([100, 19, 300]).advanced_over(before));
        assert!(!before.advanced_over(before));
    }

    #[test]
    fn held_heads_require_three_complete_positive_position_tags() {
        assert_eq!(
            Heads::parse("00000000000000000001\n00000000000000000002\n00000000000000000003")
                .unwrap()
                .0
                .map(std::num::NonZeroU64::get),
            [1, 2, 3]
        );
        for raw in [
            "",
            "1\n2\n3",
            "00000000000000000000\n00000000000000000002\n00000000000000000003",
            "00000000000000000001\n00000000000000000002",
        ] {
            assert!(Heads::parse(raw).is_err());
        }
    }

    #[test]
    fn regressed_or_single_step_counters_cannot_prove_both_fallback_reads() {
        let before = Reader {
            stale: 10,
            live_hits: 3,
            live_misses: 20,
            redis_hits: 5,
        };
        let after = |stale, live_misses| Reader {
            stale,
            live_misses,
            live_hits: 3,
            redis_hits: 5,
        };
        assert!(after(12, 22).proves_bypass(&before));
        for (stale, misses) in [(11, 22), (12, 21), (9, 22), (12, 19)] {
            assert!(!after(stale, misses).proves_bypass(&before));
        }
        let near_limit = Reader {
            stale: i64::MAX - 1,
            live_misses: i64::MAX - 1,
            ..before
        };
        assert!(!after(i64::MAX, i64::MAX).proves_bypass(&near_limit));
    }

    #[test]
    fn a_nonce_and_balance_fallback_needs_two_misses_and_no_cache_hit() {
        let before = Reader::parse("# exporter is live\n");
        let body = concat!(
            "kardamom_cache_degraded_total{reason=\"stale\"} 2\n",
            "kardamom_cache_lookups_total{outcome=\"miss\",layer=\"live\"} 2\n",
        );
        assert!(Reader::parse(body).proves_bypass(&before));
        for extra in [
            "kardamom_cache_lookups_total{layer=\"live\",outcome=\"hit\"} 1\n",
            "kardamom_cache_lookups_total{layer=\"redis\",outcome=\"hit\"} 1\n",
        ] {
            assert!(!Reader::parse(&format!("{body}{extra}")).proves_bypass(&before));
        }
        assert!(!Reader::parse(&body.replace("stale\"} 2", "stale\"} 1")).proves_bypass(&before));
        assert!(!Reader::parse(&body.replace("live\"} 2", "live\"} 1")).proves_bypass(&before));
        assert!(!Reader::parse("").proves_bypass(&before));
    }
}
