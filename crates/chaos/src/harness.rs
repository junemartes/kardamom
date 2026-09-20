//! The harness: the cluster handles one case runs against, and the
//! per-case spine every case shares: the account, the load, the
//! injection gate, the case body, the progress probe, the fleet
//! convergence check, and the load verdict.

use std::path::PathBuf;
use std::time::Duration;

use crate::accounts::{Accounts, Pin};
use crate::cases::Case;
use crate::contract::NodeContract;
use crate::evidence::Evidence;
use crate::inject::Killed;
use crate::knobs::Knobs;
use crate::lifecycle::{Lifecycle, NOMAD_HTTP_PORT};
use crate::load::{LoadRun, LoadSpec, Verdict};
use crate::rpc::Rpc;
use kardamom_bench::load::Completeness;

/// The recovery probe's window.
const PROBE_WINDOW: Duration = Duration::from_secs(30);
use crate::nodes::Nodes;
use crate::nomad::Nomad;
use crate::poll::{self, Budget};
use crate::probes::{IngressCounts, Probes};

/// The port of the ingress JSON-RPC.
/// The eth JSON-RPC port of every ingress node.
pub const INGRESS_RPC_PORT: u16 = 8545;

pub struct Harness {
    pub contract: NodeContract,
    pub nomad: Nomad,
    pub nodes: Nodes,
    pub probes: Probes,
    pub evidence: Evidence,
    pub knobs: Knobs,
    pub lifecycle: Lifecycle,
    /// The ingress-0 JSON-RPC URL the loads submit to.
    pub rpc_url: String,
    pub(crate) accounts: Accounts,
    pub(crate) killed: Option<Killed>,
}

impl Harness {
    /// The harness of the cluster `contract` describes.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract lacks a node the probes need or
    /// the HTTP clients fail to build.
    pub fn new(contract: NodeContract, knobs: Knobs, lifecycle: Lifecycle) -> anyhow::Result<Self> {
        let nomad = Nomad::new(&contract.nomad_addr(NOMAD_HTTP_PORT)?)?;
        let probes = Probes::new(&contract)?;
        let ingress0 = contract.node("ingress-0")?;
        let rpc_url = format!("http://{}:{INGRESS_RPC_PORT}", ingress0.ip);
        Ok(Self {
            evidence: Evidence::new(nomad.clone()),
            nomad,
            nodes: Nodes,
            probes,
            accounts: Accounts::new(knobs.account_base, knobs.run_load),
            killed: None,
            knobs,
            lifecycle,
            rpc_url,
            contract,
        })
    }

    /// Follow a new node contract: the probes and the RPC URL read the
    /// addresses again. The Nomad client keeps its address, since the
    /// control node is never replaced.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract lacks a node the probes need.
    pub fn follow(&mut self, contract: NodeContract) -> anyhow::Result<()> {
        self.probes = Probes::new(&contract)?;
        let ingress0 = contract.node("ingress-0")?;
        self.rpc_url = format!("http://{}:{INGRESS_RPC_PORT}", ingress0.ip);
        self.contract = contract;
        Ok(())
    }

    /// The host container name of a node, `kardamom-<name>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract has no such node.
    pub fn container(&self, name: &str) -> anyhow::Result<String> {
        Ok(self.contract.node(name)?.container.clone())
    }

    /// The report path of a case's load.
    fn report_path(case: &str) -> PathBuf {
        std::env::temp_dir().join(format!("chaos-{case}.json"))
    }

    /// Run one case: start the load, gate the injection on flowing
    /// traffic, run the body, then the common tail.
    ///
    /// # Errors
    ///
    /// Returns the first failure, with the load aborted.
    pub async fn run_case(&mut self, name: &str) -> anyhow::Result<()> {
        let case = Case::parse(name)?;
        crate::log(format!(
            "================= CHAOS CASE: {name} ================="
        ));
        let account = self.pick_account(case)?;
        let window = case.window(&self.knobs);
        let rx0 = self.probes.ingress_counts().await;
        let load = LoadRun::start(&self.load_spec(case, account, window))?;
        let outcome = self.run_body(case, &load, &rx0).await;
        if let Err(e) = outcome {
            load.abort();
            return Err(e);
        }
        let verdict = load.finish().await?;
        case.judge_load(&verdict)?;
        self.assert_executors_converged(case.name()).await?;
        self.probe_recovery(case, account).await?;
        self.validator_verdict().await?;
        crate::log(format!("CHAOS CASE {name}: PASS"));
        Ok(())
    }

    /// The recovery probe: a short load at the case rate, after the
    /// case load ended and the executors converged, on the case's
    /// account from its next nonce. Every offered transaction must get
    /// a receipt, and the pipeline must accept at a fraction of the
    /// rate. The case load cannot prove this: a submit refused during
    /// the outage leaves a nonce hole, every later submit of the sender
    /// then parks and fails, and the chaos verdict does not count a
    /// failed submit. The executor block gauge cannot prove it either:
    /// it advances on empty blocks.
    async fn probe_recovery(&self, case: Case, account: u32) -> anyhow::Result<()> {
        let rpc = Rpc::new(&self.rpc_url, self.knobs.chain_id)?;
        let nonce = rpc.nonce_of(account).await?;
        crate::log(format!(
            "{}: recovery probe: {}s at {} tps on account #{account} from nonce {nonce}",
            case.name(),
            PROBE_WINDOW.as_secs(),
            self.knobs.tps
        ));
        let verdict = LoadRun::start(&self.probe_spec(case, account, nonce))?
            .finish()
            .await?;
        case.judge_probe(&verdict, self.probe_floor())?;
        crate::log(format!(
            "{}: recovery probe PASS: {} offered, {} accepted, all receipted",
            case.name(),
            verdict.offered,
            verdict.accepted
        ));
        Ok(())
    }

    /// The least accepted submits a healthy pipeline lands in the probe
    /// window: a quarter of the rate.
    fn probe_floor(&self) -> u64 {
        u64::from(self.knobs.tps.get()) * PROBE_WINDOW.as_secs() / 4
    }

    fn probe_spec(&self, case: Case, account: u32, nonce: u64) -> LoadSpec {
        LoadSpec {
            nonce_start: nonce,
            completeness: Completeness::Offered,
            fixed_rate: true,
            duration: PROBE_WINDOW,
            retry_submit: self.knobs.load_retry,
            report_path: Self::report_path(&format!("{}-probe", case.name())),
            ..self.load_spec(case, account, PROBE_WINDOW)
        }
    }

    async fn run_body(
        &mut self,
        case: Case,
        load: &LoadRun,
        rx0: &IngressCounts,
    ) -> anyhow::Result<()> {
        self.inject_gate(case, load, rx0).await?;
        case.run(self).await?;
        case.assert_recovered_progress(self).await?;
        self.assert_executors_converged(case.name()).await
    }

    fn pick_account(&mut self, case: Case) -> anyhow::Result<u32> {
        let moves = match case.pin() {
            Pin::MovesOnScaleOut => {
                crate::cases::resize::moved_accounts(self.lifecycle.cluster_dir())?
            }
            Pin::Any | Pin::Shard0 => Vec::new(),
        };
        self.accounts
            .take(case.pin(), case.name(), |a| moves.contains(&a))
    }

    /// The RPC URLs of the other ingresses. Every replica consumes the
    /// same receipt fan-in, so a drain asks them for a receipt the submit
    /// ingress no longer holds, for example after its restart.
    pub(crate) fn receipt_rpcs(&self) -> Vec<String> {
        self.probes
            .ingresses
            .iter()
            .map(|n| format!("http://{}:{INGRESS_RPC_PORT}", n.ip))
            .filter(|url| *url != self.rpc_url)
            .collect()
    }

    fn load_spec(&self, case: Case, account: u32, window: Duration) -> LoadSpec {
        LoadSpec {
            rpc_url: self.rpc_url.clone(),
            receipt_rpcs: self.receipt_rpcs(),
            chain_id: self.knobs.chain_id,
            account,
            nonce_start: 0,
            completeness: Completeness::Accepted,
            fixed_rate: false,
            duration: window,
            tps: self.knobs.tps,
            retry_submit: case.load_retry(&self.knobs),
            max_gap: self.knobs.load_max_gap,
            drain_timeout: self
                .knobs
                .reschedule_slo
                .saturating_add(Duration::from_secs(60)),
            report_path: Self::report_path(case.name()),
            executor_nodes: self
                .probes
                .executors
                .iter()
                .map(|n| n.container.clone())
                .collect(),
            ingress_node: self.probes.ingresses[0].container.clone(),
            sequencer_nodes: self
                .probes
                .sequencers
                .iter()
                .map(|n| n.container.clone())
                .collect(),
        }
    }

    /// The injection gate: the ingress received counter must move past
    /// its pre-load baseline within the flow timeout, or the case
    /// refuses to inject into an idle pipeline.
    /// Wait for the load to sign its queues. The load signs the whole
    /// case window before its first submit: about 200,000 transactions
    /// for the longest case, which took over 120 s on a CI runner. The
    /// flow check below starts after this, so slow signing never reads
    /// as an idle pipeline.
    async fn wait_load_ready(&self, case: Case, load: &LoadRun) -> anyhow::Result<()> {
        let budget = Budget::new(self.knobs.load_ready_timeout, Duration::from_secs(3));
        let outcome = poll::until(budget, |_| async move {
            anyhow::ensure!(
                !load.is_finished(),
                "{}: {}: the load exited before it signed its queues",
                crate::FAIL_PREFIX,
                case.name()
            );
            Ok(load.is_ready().then_some(()))
        })
        .await?;
        let ((), elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{}: the load is still signing its queues after {}s",
                case.name(),
                t.as_secs()
            )
        })?;
        crate::log(format!("load ready after {}s", elapsed.as_secs()));
        Ok(())
    }

    async fn inject_gate(
        &self,
        case: Case,
        load: &LoadRun,
        rx0: &IngressCounts,
    ) -> anyhow::Result<()> {
        tokio::time::sleep(self.knobs.inject_delay).await;
        self.wait_load_ready(case, load).await?;
        let budget = Budget::new(self.knobs.load_flow_timeout, Duration::from_secs(3));
        let outcome = poll::until(budget, |_| async move {
            anyhow::ensure!(
                !load.is_finished(),
                "{}: {}: the load exited before any tx reached ingress",
                crate::FAIL_PREFIX,
                case.name()
            );
            Ok(self.probes.ingress_counts().await.rose_over(rx0))
        })
        .await?;
        let (rx1, _) = match outcome {
            poll::Outcome::Ready { value, elapsed } => (value, elapsed),
            poll::Outcome::TimedOut { elapsed } => {
                let now = self.probes.ingress_counts().await;
                return Err(crate::chaos_fail!(
                    "{}: load not flowing after {}s (ingress received [{}] -> [{}]); refusing to inject into an idle pipeline",
                    case.name(),
                    elapsed.as_secs(),
                    rx0.describe(),
                    now.describe()
                ));
            }
        };
        crate::log(format!(
            "load flowing (ingress received [{}] -> {rx1}); injecting",
            rx0.describe()
        ));
        Ok(())
    }
}

impl Case {
    /// The progress probe after the body, by case family: the cluster
    /// cases and a node failure use the executor gauge with a wide
    /// window, since a returning node thrashes the runner.
    async fn assert_recovered_progress(self, h: &Harness) -> anyhow::Result<()> {
        match self.name() {
            n if n.starts_with("cluster-") => {
                h.assert_executor_progress(Duration::from_secs(60)).await
            }
            n if n.starts_with("node-failure-") => {
                h.assert_executor_progress(Duration::from_secs(180)).await
            }
            _ => h.assert_progress().await,
        }
    }

    /// The recovery probe must land every offered transaction and
    /// accept at least `floor` of them: a recovered pipeline that
    /// refuses or parks submits is not recovered.
    fn judge_probe(self, verdict: &Verdict, floor: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            verdict.pass && verdict.missing == 0,
            "{}: recovery probe not PASS for case {}: {:?} (missing={} of {} offered)",
            crate::FAIL_PREFIX,
            self.name(),
            verdict.failures,
            verdict.missing,
            verdict.offered
        );
        anyhow::ensure!(
            verdict.accepted >= floor,
            "{}: recovery probe for case {} accepted {} submits in {}s, below the floor of {} — the case's sender cannot get new transactions through after the recovery (a stuck sender or a dead pipeline; the probe reuses the case's account, so it cannot tell which)",
            crate::FAIL_PREFIX,
            self.name(),
            verdict.accepted,
            PROBE_WINDOW.as_secs(),
            floor
        );
        Ok(())
    }

    /// Chaos mode already tolerates duplicate-submit drops. Every other
    /// load failure remains fatal, including bad receipts and replica lag.
    fn judge_load(self, verdict: &Verdict) -> anyhow::Result<()> {
        anyhow::ensure!(
            verdict.pass && verdict.missing == 0,
            "{}: load verdict not PASS for case {}: {:?} (missing={})",
            crate::FAIL_PREFIX,
            self.name(),
            verdict.failures,
            verdict.missing
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;
