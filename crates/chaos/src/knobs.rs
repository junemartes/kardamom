//! The tunable settings of the suite. Every knob reads the same
//! environment variable the bash suite read, and defaults to the value
//! the CI workflow sets for every chaos shard, so a local run without
//! settings behaves like a CI shard.

use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use anyhow::Context;

/// The CPU-squeeze drill settings.
#[derive(Debug, Clone)]
pub struct Squeeze {
    /// One squeeze window.
    pub window: Duration,
    /// The `docker update --cpus` value of every node during a squeeze.
    pub cpus_per_node: String,
    /// How long the validator gets to verify live again after the last
    /// release.
    pub recover: Duration,
    pub cycles: NonZeroU32,
    /// The pause between two cycles.
    pub release: Duration,
}

/// The suite settings. See the module doc for the defaults.
#[derive(Debug, Clone)]
pub struct Knobs {
    /// The L2 chain id. The ingress `eth_chainId` returns a default that
    /// differs from the cluster chain, so the load never probes it.
    pub chain_id: u64,
    /// The steady load rate of every case.
    pub tps: NonZeroU32,
    /// The per-case load window. A case with a longer floor widens it.
    pub case_window: Duration,
    /// The keep-pace gap bound of the load verdict.
    pub load_max_gap: u64,
    /// The per-submit retry attempts of the load, outside the resize case.
    pub load_retry: u32,
    /// The same-node restart SLO. A restart re-pulls the image.
    pub restart_slo: Duration,
    /// The node-loss recovery SLO.
    pub reschedule_slo: Duration,
    /// The new-leader election SLO of the Raft cluster.
    pub leader_slo: Duration,
    /// The blank-member full-log-replay SLO.
    pub rejoin_slo: Duration,
    /// The minimum load time before an injection.
    pub inject_delay: Duration,
    /// How long past `inject_delay` the load may take to reach the
    /// ingress before the case refuses to inject into an idle pipeline.
    pub load_flow_timeout: Duration,
    /// The first funded account a case may use.
    pub account_base: u32,
    /// Which ingress replica the hard kill targets, 0 or 1.
    pub ingress_victim: u32,
    /// The fleet convergence budget after every case.
    pub converge_slo: Duration,
    /// The per-replica lag every executor must be within at case end.
    pub converge_lag: u64,
    /// The sequencer-lapse freeze window.
    pub seq_lapse: Duration,
    /// The validator-lapse freeze window.
    pub validator_lapse: Duration,
    /// The cluster egress retention the cluster was deployed with, in
    /// frames. The retention cases need it; other cases ignore it.
    pub cluster_retention: Option<NonZeroU64>,
    /// The hard cap of the adaptive retention freeze.
    pub retention_freeze_cap: Duration,
    pub squeeze: Squeeze,
    /// Whether the load stage ran on this cluster, which decides whether
    /// the resize case may take a load-reserve account.
    pub run_load: bool,
}

/// The setting source: the process environment first, then the shard's
/// workflow values, then the built-in CI default.
struct Source<'a> {
    shard: &'a [(&'a str, &'a str)],
}

impl Source<'_> {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok().or_else(|| {
            self.shard
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| (*v).to_string())
        })
    }

    fn or(&self, name: &str, default: &str) -> String {
        self.get(name).unwrap_or_else(|| default.to_string())
    }

    fn u64(&self, name: &str, default: u64) -> anyhow::Result<u64> {
        self.or(name, &default.to_string())
            .parse()
            .with_context(|| format!("{name} is not a number"))
    }

    fn secs(&self, name: &str, default: u64) -> anyhow::Result<Duration> {
        Ok(Duration::from_secs(self.u64(name, default)?))
    }

    fn nonzero_u32(&self, name: &str, default: u32) -> anyhow::Result<NonZeroU32> {
        let value = u32::try_from(self.u64(name, u64::from(default))?)
            .with_context(|| format!("{name} does not fit a u32"))?;
        NonZeroU32::new(value).ok_or_else(|| anyhow::anyhow!("{name} must not be zero"))
    }

    fn u32(&self, name: &str, default: u32) -> anyhow::Result<u32> {
        u32::try_from(self.u64(name, u64::from(default))?)
            .with_context(|| format!("{name} does not fit a u32"))
    }
}

impl Knobs {
    /// Read every knob from the environment, with the CI shard defaults.
    ///
    /// # Errors
    ///
    /// Returns an error if a set variable does not parse.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::read(&[])
    }

    /// Read every knob from the environment, falling back to the shard's
    /// workflow values, then to the CI defaults.
    ///
    /// # Errors
    ///
    /// Returns an error if a set variable does not parse.
    pub fn read(shard: &[(&str, &str)]) -> anyhow::Result<Self> {
        let env = Source { shard };
        let retention = match env.get("KARDAMOM_CLUSTER_RETENTION") {
            Some(raw) if !raw.is_empty() => Some(
                raw.parse::<NonZeroU64>()
                    .context("KARDAMOM_CLUSTER_RETENTION is not a positive number")?,
            ),
            _ => None,
        };
        Ok(Self {
            chain_id: env.u64("CHAIN_ID", 412_346)?,
            tps: env.nonzero_u32("CHAOS_TPS", 200)?,
            case_window: env.secs("CHAOS_CASE_S", 120)?,
            load_max_gap: env.u64("LOAD_MAX_GAP", 5)?,
            load_retry: env.u32("LOAD_RETRY", 2)?,
            restart_slo: env.secs("CHAOS_RESTART_SLO_S", 60)?,
            reschedule_slo: env.secs("CHAOS_RESCHEDULE_SLO_S", 200)?,
            leader_slo: env.secs("CHAOS_LEADER_SLO_S", 45)?,
            rejoin_slo: env.secs("CLUSTER_REJOIN_SLO_S", 360)?,
            inject_delay: env.secs("INJECT_DELAY", 10)?,
            load_flow_timeout: env.secs("LOAD_FLOW_TIMEOUT_S", 60)?,
            account_base: env.u32("CHAOS_ACCT_BASE", 7)?,
            ingress_victim: Self::ingress_victim(&env)?,
            converge_slo: env.secs("EXEC_CONVERGE_SLO_S", 150)?,
            converge_lag: env.u64("EXEC_CONVERGE_LAG", 50)?,
            seq_lapse: env.secs("SEQ_LAPSE_S", 30)?,
            validator_lapse: env.secs("LAPSE_S", 30)?,
            cluster_retention: retention,
            retention_freeze_cap: env.secs("RETENTION_FREEZE_CAP_S", 600)?,
            squeeze: Squeeze {
                window: env.secs("SQUEEZE_S", 120)?,
                cpus_per_node: env.or("SQUEEZE_CPUS_PER_NODE", "0.75"),
                recover: env.secs("SQUEEZE_RECOVER_S", 180)?,
                cycles: env.nonzero_u32("SQUEEZE_CYCLES", 1)?,
                release: env.secs("SQUEEZE_RELEASE_S", 30)?,
            },
            run_load: env.or("RUN_LOAD", "1") == "1",
        })
    }

    /// The hard-kill ingress victim rotates with the CI run id, so the
    /// blast radius is not pinned to one replica forever.
    fn ingress_victim(env: &Source<'_>) -> anyhow::Result<u32> {
        if let Some(explicit) = env.get("INGRESS_VICTIM") {
            return explicit.parse().context("INGRESS_VICTIM is not 0 or 1");
        }
        let run_id = env.u64("GITHUB_RUN_ID", 0)?;
        u32::try_from(run_id % 2).context("run id parity")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shard_values_fill_in_below_the_environment() {
        let knobs = Knobs::read(&[
            ("SQUEEZE_CYCLES", "3"),
            ("KARDAMOM_CLUSTER_RETENTION", "6144"),
        ])
        .unwrap();
        assert_eq!(knobs.squeeze.cycles.get(), 3);
        assert_eq!(knobs.cluster_retention.map(NonZeroU64::get), Some(6144));
        assert_eq!(knobs.reschedule_slo, Duration::from_secs(200));
        assert!(Knobs::read(&[("CHAOS_TPS", "0")]).is_err());
    }
}
