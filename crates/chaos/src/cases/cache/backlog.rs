//! A cold client must not read an established sender from a stale, live projection.

use std::fmt::Write;
use std::num::NonZeroU64;
use std::time::Duration;

use anyhow::Context;

use super::rebuilds::MirrorRebuilds;
use super::{aux, live_primary, redis_cli, wait_readers_recovered};
use crate::harness::Harness;
use crate::metrics;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::rpc::{AccountState, Rpc};

const CTX: &str = "mirror-backlog-client-restart";
/// The fixture uses the service default of 8192 canonical positions.
const STALE_GAP: i64 = 8_192;
const HEAD_KEYS: &str = "head:0 head:1 head:2";

struct Baseline {
    primary: String,
    heads: Heads,
    account: AccountState,
    rebuilds: MirrorRebuilds,
}

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
}

impl Baseline {
    fn report_cleanup(result: Result<(), &anyhow::Error>) {
        if let Err(error) = result {
            crate::log(format!("{CTX}: fault cleanup fails: {error:#}"));
        }
    }

    async fn capture(h: &Harness) -> anyhow::Result<Self> {
        let account = Self::transfer_committed(h).await?;
        let primary = live_primary(h, CTX).await?;
        let raw = h
            .nodes
            .exec(
                &aux(h),
                &redis_cli(&format!("--raw -h {primary} MGET {HEAD_KEYS}")),
            )
            .await?;
        let baseline = Self {
            primary,
            heads: Heads::parse(raw.trim_end())?,
            account,
            rebuilds: MirrorRebuilds::read(h).await?,
        };
        baseline.await_row(h, &baseline.account).await?;
        Ok(baseline)
    }

    async fn transfer_committed(h: &Harness) -> anyhow::Result<AccountState> {
        let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
        let account = h.knobs.gate_account;
        let nonce = rpc.nonce_of(account).await?;
        let committed = nonce
            .checked_add(1)
            .context("the fixture nonce overflows")?;
        rpc.transfer_hash(account, nonce, Duration::from_secs(60))
            .await?;
        Self::executor(h)?
            .await_account_nonce(account, committed, Budget::secs(120, 1))
            .await
    }

    fn executor(h: &Harness) -> anyhow::Result<Rpc> {
        Rpc::new(
            &format!("http://{}:9024", h.probes.executors[0].ip),
            h.knobs.chain_id,
        )
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
        let last = std::cell::RefCell::new(String::new());
        let last_ref = &last;
        poll::until(Budget::secs(120, 2), |_| async move {
            let observed = self.row(h).await?;
            let matches = account.matches_projection(&observed);
            last_ref.replace(observed);
            Ok::<_, anyhow::Error>(matches.then_some(()))
        })
        .await?
        .or_fail(|_| crate::chaos_fail!(
            "{CTX}: exact account row is not projected on {}: expected {account:?}, observed {:?}",
            self.primary, last.borrow()
        ))?;
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
        let expected = Self::transfer_committed(h).await?;
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
            after.stale >= before.stale.saturating_add(2) && after.live == before.live,
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
                &redis_cli(&format!("--raw -h {} MGET {HEAD_KEYS}", self.primary)),
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
    live: i64,
}

impl Reader {
    async fn read(h: &Harness, ingress: &crate::probes::Probed) -> anyhow::Result<Self> {
        let body = h
            .probes
            .scrape()
            .fetch(&h.probes.ingress_target(ingress))
            .await
            .context("restart client exporter is unavailable")?;
        Ok(Self {
            stale: metrics::sum_where(&body, "kardamom_cache_degraded_total", "reason=\"stale\"")
                .unwrap_or(0),
            live: metrics::sum_where(&body, "kardamom_cache_lookups_total", "layer=\"live\"")
                .unwrap_or(0),
        })
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
            &redis_cli(&format!("-h {} DEL {HEAD_KEYS}", baseline.primary)),
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
    baseline.rebuilds.await_all(h, CTX).await?;
    baseline.await_row(h, &expected).await?;
    wait_readers_recovered(h, CTX).await?;
    h.assert_progress().await
}

#[cfg(test)]
mod tests {
    use super::Heads;

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
}
