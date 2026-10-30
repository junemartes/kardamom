//! The recovery cases of the chaos suite, on the local stack. Each case
//! runs the real services as processes: the ingress pair, the
//! sequencers, the sealer members, the executor and the validator. The
//! case stops and starts them as the deployment's scheduler does, and
//! then judges the pipeline with the checks that end every chaos case:
//!
//! 1. The case load: one sender submits through ingress 0 for the whole
//!    case. Every offered transfer must land exactly once, in nonce
//!    order, with a receipt from every ingress ([`load`]).
//! 2. The load lands a transfer after the fault, within the restart
//!    bound.
//! 3. The executor block advances.
//! 4. The recovery probe: the case sender sends again through ingress
//!    0, and a fresh sender sends through ingress 1, at the same time.
//!    Both must land everything they offer, above a floor.
//! 5. The validator verifies the chain with no divergence.

pub mod cases;
mod epochs;
pub mod load;
pub mod sealer_logs;

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use anyhow::{Context, Result};

use crate::harness::l2::{self, L2Client};
use crate::harness::metrics;
use crate::harness::{LocalStack, StackConfig};
pub use cases::Case;
use load::{LoadPlan, LoadReport, RunningLoad};

/// The dev-mnemonic index of the case sender.
const CASE_SENDER: usize = 3;
/// The dev-mnemonic index of the fresh sender of the probe.
const FRESH_SENDER: usize = 4;
/// The bound for a returned service: from its start until the load lands
/// a transfer again, and until both ingress listeners answer. The chaos
/// suite allows 60 s for a Nomad restart; a local start has no
/// scheduler delay.
pub const RESTART_BOUND: Duration = Duration::from_secs(30);
/// How long each probe leg sends.
const PROBE_WINDOW: Duration = Duration::from_secs(4);
/// The least transfers each probe leg lands in its window.
const PROBE_FLOOR: usize = 4;
/// The client bound of one RPC call: above the 30 s park bound of the
/// ingress, so a server timeout is not a client abort.
const CALL_BOUND: Duration = Duration::from_secs(40);

/// One case run on one stack.
pub struct Run<'a> {
    pub stack: &'a mut LocalStack,
    pub name: &'static str,
    chain_id: u64,
    started: Instant,
}

impl<'a> Run<'a> {
    /// A run of `name` on `stack`. The stack gets its second ingress
    /// replica here: every case runs against the deployed pair.
    ///
    /// # Errors
    /// Returns an error when the second replica does not start.
    pub fn new(stack: &'a mut LocalStack, name: &'static str) -> Result<Self> {
        if stack.ingress_count() < 2 {
            stack.add_ingress()?;
        }
        let chain_id = stack.cfg.chain_id.get();
        Ok(Self {
            stack,
            name,
            chain_id,
            started: Instant::now(),
        })
    }

    /// The log line prefix: the case and the seconds since its start.
    #[must_use]
    pub fn at(&self) -> String {
        format!(
            "{} +{:.1}s",
            self.name,
            self.started.elapsed().as_secs_f64()
        )
    }

    /// A client of ingress replica `i`.
    ///
    /// # Errors
    /// Returns an error when the stack has no replica `i`.
    pub fn client(&self, i: usize) -> Result<L2Client> {
        L2Client::new(&self.stack.ingress_url(i)?, CALL_BOUND)
    }

    /// A client of every ingress replica.
    ///
    /// # Errors
    /// Returns an error when a client fails to build.
    pub fn clients(&self) -> Result<Vec<L2Client>> {
        (0..self.stack.ingress_count())
            .map(|i| self.client(i))
            .collect()
    }

    /// The plan of a load of dev account `sender` through ingress
    /// `submit`, from the sender's live nonce.
    async fn plan(&self, sender: usize, submit: usize, payee: Address) -> Result<LoadPlan> {
        let signer = l2::dev_signers_through(sender)?
            .get(sender)
            .cloned()
            .context("dev signer")?;
        let client = self.client(submit)?;
        let nonce = client
            .get_transaction_count(signer.address)
            .await
            .result
            .map_err(|e| anyhow::anyhow!("read the sender nonce: {e}"))?;
        Ok(LoadPlan {
            sender: signer,
            payee,
            chain_id: self.chain_id,
            submit: client,
            receipts: self.clients()?,
            first_nonce: u64::from_str_radix(nonce.trim_start_matches("0x"), 16)?,
            window: NonZeroUsize::new(4).context("window")?,
            pace: Duration::from_millis(100),
            retry: Duration::from_millis(250),
            give_up: Duration::from_secs(55),
        })
    }

    /// Run `case`: start the load, wait for it to flow, inject the fault
    /// and its recovery, and judge the pipeline after it.
    ///
    /// # Errors
    /// Returns the first failed check.
    pub async fn run(mut self, case: Case) -> Result<()> {
        case.prepare(&mut self)?;
        let plan = self
            .plan(CASE_SENDER, 0, Address::repeat_byte(0xc1))
            .await?;
        let load = RunningLoad::start(plan);
        load.await_landed_above(2, Duration::from_secs(30))
            .await
            .context("the case load does not flow before the fault")?;
        eprintln!("{}: the load flows; inject the fault", self.at());
        case.inject(&mut self, &load).await?;
        let returned = load.landed();
        let resumed = load.await_landed_above(returned, RESTART_BOUND).await?;
        eprintln!(
            "{}: the case load lands again {:.1}s after the return",
            self.at(),
            resumed.as_secs_f64()
        );
        let report = load.finish().await?;
        eprintln!("{}: case load: {}", self.at(), report.summary());
        report.assert_all_landed(self.name)?;
        report.assert_on_chain(self.name, &self.clients()?).await?;
        self.assert_executor_progress().await?;
        self.probe(report.end_nonce()).await?;
        self.validator_verdict().await
    }

    /// The executor block rises twice within ten seconds: the pipeline
    /// executes again.
    ///
    /// # Errors
    /// Returns an error when the block does not rise.
    pub async fn assert_executor_progress(&self) -> Result<()> {
        let addr = self.stack.executor_metrics();
        let first = metrics::poll_until(
            "an executor block reading",
            Duration::from_secs(20),
            Duration::from_millis(100),
            async || {
                Ok(metrics::scrape(addr)
                    .await
                    .ok()
                    .and_then(|s| s.value(crate::scenarios::EXEC_BLOCK_NUMBER)))
            },
        )
        .await?;
        metrics::poll_until(
            &format!("the executor block rises over {first}"),
            Duration::from_secs(10),
            Duration::from_millis(100),
            async || {
                let now = metrics::scrape(addr)
                    .await
                    .ok()
                    .and_then(|s| s.value(crate::scenarios::EXEC_BLOCK_NUMBER));
                Ok(now.filter(|b| *b > first))
            },
        )
        .await?;
        Ok(())
    }

    /// Both ingress replicas answer on their RPC and metrics ports.
    ///
    /// # Errors
    /// Returns an error when a replica stays dark past `bound`.
    pub async fn assert_ingress_pair_live(&self, bound: Duration) -> Result<()> {
        for i in 0..self.stack.ingress_count() {
            self.assert_ingress_live(i, bound).await?;
        }
        Ok(())
    }

    async fn assert_ingress_live(&self, i: usize, bound: Duration) -> Result<()> {
        let took = self.stack.await_ingress_live(i, bound).await?;
        eprintln!(
            "{}: ingress {i} live after {:.1}s",
            self.at(),
            took.as_secs_f64()
        );
        Ok(())
    }

    /// The recovery probe: the case sender through ingress 0 from
    /// `case_nonce`, the nonce after its case load, and a fresh sender
    /// through ingress 1, at the same time.
    async fn probe(&self, case_nonce: u64) -> Result<()> {
        let legs = [
            LoadPlan {
                first_nonce: case_nonce,
                ..self
                    .plan(CASE_SENDER, 0, Address::repeat_byte(0xc2))
                    .await?
            },
            self.plan(FRESH_SENDER, 1, Address::repeat_byte(0xc3))
                .await?,
        ];
        let runs = legs.map(RunningLoad::start);
        tokio::time::sleep(PROBE_WINDOW).await;
        let [case_leg, fresh_leg] = runs;
        let reports = [case_leg.finish().await?, fresh_leg.finish().await?];
        let readers = self.clients()?;
        for (report, who) in reports.iter().zip(["case sender", "fresh sender"]) {
            self.judge_leg(report, who, &readers).await?;
        }
        Ok(())
    }

    /// One probe leg landed everything it offered, above the floor.
    async fn judge_leg(&self, report: &LoadReport, who: &str, readers: &[L2Client]) -> Result<()> {
        let what = format!("{}: probe of the {who}", self.name);
        report.assert_all_landed(&what)?;
        anyhow::ensure!(
            report.landed() >= PROBE_FLOOR,
            "{what}: landed {} in {PROBE_WINDOW:?}, below the floor {PROBE_FLOOR}",
            report.landed()
        );
        report.assert_on_chain(&what, readers).await?;
        eprintln!("{}: {what}: {}", self.at(), report.summary());
        Ok(())
    }

    /// The validator verdict, when the stack runs a validator.
    async fn validator_verdict(&self) -> Result<()> {
        if !self.stack.cfg.validator {
            return Ok(());
        }
        self.stack
            .target(CALL_BOUND)?
            .assert_validator_verdict(self.name)
            .await
    }
}

/// The stack of a case: a validator, whose verdict ends the case.
#[must_use]
pub fn validated() -> StackConfig {
    StackConfig {
        validator: true,
        ..StackConfig::default()
    }
}

/// The stack of a case whose checks read the L1 epochs: Anvil, the
/// bridge, the L1 follower and the da-watcher.
#[must_use]
pub fn with_l1() -> StackConfig {
    StackConfig {
        l1: true,
        ..validated()
    }
}

/// The stack of a case that takes the sealer down: three members, so the
/// return closes a real election of a majority.
#[must_use]
pub fn with_quorum(cfg: StackConfig) -> StackConfig {
    StackConfig {
        sealer_members: NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN),
        ..cfg
    }
}

/// Launch the stack of `cfg`, run `case` on it, and print the runtime. `launch_opt` also brings up the L1 stack; a missing Anvil
/// fails the case instead of skipping it. A failure dumps the process
/// logs.
///
/// # Panics
/// Panics when the stack does not start or the case fails.
pub async fn run_case(name: &'static str, case: Case, cfg: StackConfig) {
    let started = Instant::now();
    let mut stack = LocalStack::launch_opt(cfg)
        .await
        .expect("stack")
        .expect("the L1 cases need Anvil");
    eprintln!(
        "{name}: stack up in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    let outcome = match Run::new(&mut stack, name) {
        Ok(run) => run.run(case).await,
        Err(e) => Err(e),
    };
    if let Err(e) = outcome {
        stack.dump_tails();
        panic!("{name}: {e:#}");
    }
    eprintln!(
        "PIPELINE_RECOVERY {name} seconds={:.1}",
        started.elapsed().as_secs_f64()
    );
}
