//! The faults of the recovery cases, and the checks that each case makes
//! while its services are down and after they return. Each variant names
//! the chaos case whose fault it injects at the same boundary.

use std::time::Duration;

use anyhow::{Context, Result};

use super::load::RunningLoad;
use super::sealer_logs::SealerLogs;
use super::{RESTART_BOUND, Run};
use crate::harness::faults::Stop;
use crate::harness::metrics;
use crate::scenarios::{EXEC_BLOCK_NUMBER, EXEC_TX_APPLIED, metric_u64};

/// The gauge of buffered refs below their sender's floor. Only the v1
/// loop exports it; the v2 loop cannot strand a ref.
const REF_BELOW_FLOOR: &str = "kardamom_sequencer_ref_below_floor";
/// The window between the two readings that judge the pipeline while a
/// class is down: twenty boundary ticks of the local sealer.
const DOWN_WINDOW: Duration = Duration::from_secs(5);
/// How long a live ingress gets to refuse a submit while no sealer
/// answers: the 10 s sealer silence budget, then its next status poll.
const REFUSAL_BOUND: Duration = Duration::from_secs(30);
/// The cause that an ingress names while no sealer answers.
const NO_QUORUM: &str = "sealer_no_quorum";
/// The sealer's line of a void decision, one for each member and entry.
const VOID_DECIDED: &str = "result=DECIDED";
/// How long a returned state mirror gets to serve: it waits for live
/// receipts and then for an executor checkpoint at or past them.
const MIRROR_BOUND: Duration = Duration::from_secs(40);
/// The dev account whose row only a rebuild writes: funded at genesis,
/// and no case sends from it or to it.
const UNTOUCHED: usize = 10;
/// The sealed head of the sealer status, as ingress 0 exports it.
const SEALED_HEAD: &str = "kardamom_ingress_cluster_sealed_head";
/// How long the sealed head gets to rise, and the executor then gets to
/// reach it. The new leader answers a replay request at once, so this is
/// far above the time that a recovery needs.
const HEAD_BOUND: Duration = Duration::from_secs(15);
/// How many blocks the new leader seals before the executor must reach
/// its head: about two seconds of boundary ticks.
const HEAD_RISE: u64 = 8;
/// How long the returned members get to close an election.
const ELECTION_BOUND: Duration = Duration::from_secs(30);

/// A class of services that a combined case takes down, in the order the
/// data flows: the sealers order, the sequencers offer to the sealers,
/// and the ingresses publish to the sequencers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    Sealer,
    Sequencer,
    Ingress,
}

/// What the pipeline does while the classes are down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// No block is committed: the executor block stays flat, and every
    /// live ingress refuses a submit with `sealer_no_quorum`.
    Stall,
    /// The sealer still seals empty blocks: the executor block rises,
    /// and no transaction is applied.
    SealOnly,
}

/// The order and the pace of the return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Return {
    /// Every class returns at once.
    AllAtOnce,
    /// The classes return in data-flow order, the stagger apart.
    Ordered(Duration),
    /// The classes return against data-flow order, the stagger apart: a
    /// class returns before the class it needs, and must wait for it.
    Reverse(Duration),
}

/// The L1 epoch checks after a combined outage. Both need the L1 stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Epochs {
    /// The sealer's L1 origin reaches the da-watcher's last publish.
    pub no_origin_gap: bool,
    /// The da-watcher publishes a lost epoch again.
    pub refill: bool,
}

/// One combined outage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Combined {
    /// The classes, in the order the case takes them down.
    pub down: &'static [Class],
    pub expect: Expect,
    pub back: Return,
    pub epochs: Epochs,
}

/// The origin gap check alone.
const ORIGIN: Epochs = Epochs {
    no_origin_gap: true,
    refill: false,
};

/// The stagger of the ordered and reverse returns. A returned process
/// must stay up through it while the class it needs is still down: its
/// cluster client connects with retries, and a failed start exits at
/// once.
pub(super) const STAGGER: Duration = Duration::from_secs(8);

/// `ingress-sequencer-loss-recover`.
pub const INGRESS_SEQUENCER: Combined = Combined {
    down: &[Class::Ingress, Class::Sequencer],
    expect: Expect::SealOnly,
    back: Return::Ordered(STAGGER),
    epochs: ORIGIN,
};
/// `ingress-sealer-loss-recover`.
pub const INGRESS_SEALER: Combined = Combined {
    down: &[Class::Ingress, Class::Sealer],
    expect: Expect::Stall,
    back: Return::Reverse(STAGGER),
    epochs: Epochs {
        no_origin_gap: false,
        refill: false,
    },
};
/// `sequencer-sealer-loss-recover`.
pub const SEQUENCER_SEALER: Combined = Combined {
    down: &[Class::Sequencer, Class::Sealer],
    expect: Expect::Stall,
    back: Return::AllAtOnce,
    epochs: Epochs {
        no_origin_gap: true,
        refill: true,
    },
};
/// `ingress-sequencer-sealer-loss-recover`.
pub const ALL_THREE: Combined = Combined {
    down: &[Class::Ingress, Class::Sequencer, Class::Sealer],
    expect: Expect::Stall,
    back: Return::Ordered(STAGGER),
    epochs: ORIGIN,
};
/// `ingress-sequencer-sealer-reverse`.
pub const ALL_THREE_REVERSE: Combined = Combined {
    down: &[Class::Ingress, Class::Sequencer, Class::Sealer],
    expect: Expect::Stall,
    back: Return::Reverse(STAGGER),
    epochs: ORIGIN,
};

/// The fault of one case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Case {
    /// `ingress-pair-loss-recover`: SIGKILL to both ingresses.
    IngressPairLoss,
    /// `graceful-sequencer`: SIGTERM to one of the two replicas of the case
    /// sender's lane, then its cold start.
    GracefulSequencer,
    /// `sequencer-lane-loss-recover`: SIGKILL to both replicas of the case
    /// sender's lane at once; no twin keeps the lane. Both return cold.
    SequencerLaneLoss,
    /// `pipeline-blackout-recover`: every pipeline process dies at once,
    /// with the media driver, and every one starts again.
    PipelineBlackout,
    /// `sequencer-executor-redis-loss`: SIGKILL to every sequencer, the
    /// executor and Redis. They return in reverse order of need: the
    /// sequencers, Redis (empty), then the executor.
    SequencerExecutorRedis,
    /// A combined outage of the ingress, sequencer and sealer classes.
    Combined(Combined),
    /// `sealer-leader-loss-recover`: a stop of the sealer leader alone,
    /// while the case load flows. The other two members elect a new
    /// leader, and every cluster session moves to it.
    SealerLeaderLoss(Stop),
}

/// One reading of the pipeline while classes are down.
#[derive(Debug, Clone, Copy)]
pub(super) struct Reading {
    pub(super) block: u64,
    applied: u64,
}

impl Run<'_> {
    pub(super) async fn reading(&self) -> Result<Reading> {
        let scrape = metrics::scrape(self.stack.executor_metrics()).await?;
        Ok(Reading {
            block: metric_u64(scrape.value(EXEC_BLOCK_NUMBER).context("executor block")?)?,
            applied: metric_u64(scrape.value(EXEC_TX_APPLIED).unwrap_or(0.0))?,
        })
    }

    /// The lane of the case sender.
    pub(super) fn case_lane(&self) -> Result<u32> {
        let sender = crate::harness::l2::dev_signers_through(super::CASE_SENDER)?
            .get(super::CASE_SENDER)
            .map(|s| s.address)
            .context("case sender")?;
        Ok(self.stack.sequencer_for(sender))
    }

    /// The sequencer processes of `lane`.
    pub(super) fn lane_replicas(&self, lane: u32) -> Result<Vec<usize>> {
        (0..self.stack.sequencer_count())
            .filter_map(|i| match self.stack.sequencer_lane(i) {
                Ok(l) if l == lane => Some(Ok(i)),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    /// One reading of a gauge at `addr`, `None` while it is absent.
    async fn gauge(addr: std::net::SocketAddr, name: &str) -> Result<Option<u64>> {
        metrics::scrape(addr)
            .await?
            .value(name)
            .map(metric_u64)
            .transpose()
    }

    /// The new leader seals [`HEAD_RISE`] blocks past the sealed head
    /// that ingress 0 reads now, and the executor block reaches that
    /// target, within [`HEAD_BOUND`]. The rise proves that the target
    /// was sealed after every client moved to the new leader.
    async fn assert_executor_at_sealed_head(&self) -> Result<()> {
        let ingress = self.stack.ingress_metrics(0)?;
        let executor = self.stack.executor_metrics();
        let start = Self::gauge(ingress, SEALED_HEAD)
            .await?
            .context("the sealed head of ingress 0")?;
        let target = start + HEAD_RISE;
        metrics::poll_until(
            &format!("the sealed head rises from {start} to {target}"),
            HEAD_BOUND,
            Duration::from_millis(200),
            async || {
                Ok(Self::gauge(ingress, SEALED_HEAD)
                    .await?
                    .filter(|h| *h >= target))
            },
        )
        .await?;
        let mut last = None;
        metrics::poll_until(
            &format!("the executor block reaches the sealed head {target}"),
            HEAD_BOUND,
            Duration::from_millis(200),
            async || {
                last = Self::gauge(executor, EXEC_BLOCK_NUMBER).await?;
                Ok(last.filter(|b| *b >= target))
            },
        )
        .await
        .with_context(|| format!("last executor block: {last:?}"))?;
        eprintln!(
            "{}: the executor block reached the sealed head {target}",
            self.at()
        );
        Ok(())
    }

    /// The case load lands nothing for one second: the outage is real.
    pub(super) async fn assert_load_stopped(&self, load: &RunningLoad) -> Result<()> {
        let before = load.landed();
        tokio::time::sleep(Duration::from_secs(1)).await;
        // A transfer in flight at the kill can still land once.
        let after = load.landed();
        tokio::time::sleep(Duration::from_secs(1)).await;
        anyhow::ensure!(
            load.landed() == after,
            "{}: the case load still lands ({before} -> {after} -> {}): the fault is not active",
            self.name,
            load.landed()
        );
        Ok(())
    }

    /// No replica holds a buffered ref below its sender's floor.
    pub(super) async fn assert_no_ref_below_floor(&self) -> Result<()> {
        let target = self.stack.target(Duration::from_secs(5))?;
        for addr in &target.sequencer_metrics {
            self.assert_no_stranded_ref(*addr).await?;
        }
        Ok(())
    }

    async fn assert_no_stranded_ref(&self, addr: std::net::SocketAddr) -> Result<()> {
        let stranded = metrics::scrape(addr).await?.value(REF_BELOW_FLOOR);
        anyhow::ensure!(
            stranded.unwrap_or(0.0) <= 0.0,
            "{}: the sequencer at {addr} holds {stranded:?} refs below a floor",
            self.name
        );
        Ok(())
    }

    /// The state mirror serves again, and Redis, which returned empty,
    /// holds the row of a funded account that no case traffic touches:
    /// only a rebuild from the executor's checkpoint writes it.
    async fn assert_read_path_back(&self) -> Result<()> {
        if !self.stack.cfg.read_path {
            return Ok(());
        }
        let untouched = crate::harness::l2::dev_signers_through(UNTOUCHED)?
            .get(UNTOUCHED)
            .map(|s| s.address)
            .context("untouched dev account")?;
        let keys = self.stack.await_read_path(MIRROR_BOUND, untouched).await?;
        eprintln!(
            "{}: the state mirror serves again; Redis holds {keys} keys, with the row of {untouched}",
            self.at()
        );
        Ok(())
    }

    /// The returned members closed an election, and no term had two
    /// leaders.
    async fn assert_sealer_elected(&self) -> Result<()> {
        let root = self.stack.root();
        let members = self.stack.sealer_members();
        let elected = SealerLogs::await_elected(&root, members, ELECTION_BOUND).await?;
        SealerLogs::read(&root, members)?.assert_one_leader_per_term(self.name)?;
        eprintln!(
            "{}: members elected member {} in term {}",
            self.at(),
            elected.leader,
            elected.term
        );
        Ok(())
    }
}

impl Case {
    /// Inject the fault, observe it, and bring the services back.
    ///
    /// # Errors
    /// Returns the first failed step or check.
    pub async fn inject(self, run: &mut Run<'_>, load: &RunningLoad) -> Result<()> {
        match self {
            Self::IngressPairLoss => Self::ingress_pair(run, load).await,
            Self::GracefulSequencer => Self::graceful_sequencer(run, load).await,
            Self::SequencerLaneLoss => Self::sequencer_lane(run, load).await,
            Self::PipelineBlackout => Self::blackout(run).await,
            Self::SequencerExecutorRedis => run.sequencer_executor_redis(load).await,
            Self::Combined(c) => c.run(run, load).await,
            Self::SealerLeaderLoss(stop) => Self::sealer_leader(run, stop).await,
        }
    }

    /// Stop the sealer leader. The two other members elect a new leader.
    /// The executor then reaches the sealed head that the ingress reads
    /// from the sealer status: its reader must close the frames that the
    /// old leader never sent.
    async fn sealer_leader(run: &mut Run<'_>, stop: Stop) -> Result<()> {
        let root = run.stack.root();
        let members = run.stack.sealer_members();
        let old = SealerLogs::await_elected(&root, members, ELECTION_BOUND).await?;
        run.stack
            .stop_sealer_member(usize::try_from(old.leader)?, stop)?;
        eprintln!(
            "{}: sealer leader {} of term {} stopped ({stop:?})",
            run.at(),
            old.leader,
            old.term
        );
        let new = metrics::poll_until(
            "the two other sealer members elect a new leader",
            ELECTION_BOUND,
            Duration::from_millis(200),
            async || {
                Ok(SealerLogs::read(&root, members)?
                    .elected()
                    .filter(|e| e.term > old.term))
            },
        )
        .await?;
        eprintln!(
            "{}: members elected member {} in term {}",
            run.at(),
            new.leader,
            new.term
        );
        run.assert_executor_at_sealed_head().await
    }

    /// Stop ingress 0, the ingress of the case load, then start it.
    /// Kill both ingresses. No submit lands and no receipt is served;
    /// both return.
    async fn ingress_pair(run: &mut Run<'_>, load: &RunningLoad) -> Result<()> {
        (0..run.stack.ingress_count()).try_for_each(|i| run.stack.stop_ingress(i, Stop::Hard))?;
        run.assert_load_stopped(load).await?;
        let refused = run.client(1)?.chain_id().await.result.is_err();
        anyhow::ensure!(
            refused,
            "{}: ingress 1 still answers after its kill",
            run.name
        );
        (0..run.stack.ingress_count()).try_for_each(|i| run.stack.start_ingress(i))?;
        run.assert_ingress_pair_live(RESTART_BOUND).await?;
        run.assert_executor_progress().await
    }

    /// Kill the sequencer of the case sender's lane. Its senders wait
    /// until a cold replica returns and learns their floors from the
    /// executor.
    /// Kill every replica of the case sender's lane, then start them all
    /// cold. Their senders wait until a replica learns their floors from
    /// the executor. Both replicas learn the same floors and offer the
    /// same references, and the sealer keeps the first copy.
    async fn sequencer_lane(run: &mut Run<'_>, load: &RunningLoad) -> Result<()> {
        let stop = Stop::Hard;
        let lane = run.case_lane()?;
        let replicas = run.lane_replicas(lane)?;
        replicas
            .iter()
            .try_for_each(|&i| run.stack.stop_sequencer(i, stop))?;
        eprintln!(
            "{}: lane {lane} replicas {replicas:?} stopped ({stop:?})",
            run.at()
        );
        run.assert_load_stopped(load).await?;
        replicas
            .iter()
            .try_for_each(|&i| run.stack.start_sequencer(i))?;
        replicas
            .iter()
            .try_for_each(|&i| run.stack.await_sequencer_session(i, RESTART_BOUND))?;
        run.assert_executor_progress().await?;
        run.assert_no_ref_below_floor().await
    }

    /// SIGTERM to the first replica of the case sender's lane. It must exit
    /// within its kill bound, and its twin keeps the lane ordering: the
    /// case load lands during the stop. The replica then starts cold.
    async fn graceful_sequencer(run: &mut Run<'_>, load: &RunningLoad) -> Result<()> {
        let lane = run.case_lane()?;
        let first = usize::try_from(lane)?;
        run.stack.stop_sequencer(first, Stop::Graceful)?;
        eprintln!(
            "{}: sequencer {first} of lane {lane} exited on SIGTERM",
            run.at()
        );
        let landed = load.landed();
        let took = load.await_landed_above(landed, RESTART_BOUND).await?;
        eprintln!(
            "{}: the twin orders the lane while the replica is down ({:.1}s)",
            run.at(),
            took.as_secs_f64()
        );
        run.stack.start_sequencer(first)?;
        run.stack.await_sequencer_session(first, RESTART_BOUND)?;
        run.assert_executor_progress().await?;
        run.assert_no_ref_below_floor().await
    }

    /// Set the stack up before the case load: the sequencer cases run two
    /// replicas of the case sender's lane, as the deployment does. So does
    /// the sequencer, executor and Redis loss: every replica of the lane
    /// must look its floors up.
    ///
    /// # Errors
    /// Returns an error when the twin replica does not start.
    pub fn prepare(self, run: &mut Run<'_>) -> Result<()> {
        if matches!(
            self,
            Self::SequencerLaneLoss | Self::GracefulSequencer | Self::SequencerExecutorRedis
        ) {
            let lane = run.case_lane()?;
            run.stack.add_sequencer_twin(lane)?;
        }
        Ok(())
    }

    /// Kill every pipeline process and the media driver at once, then
    /// start them all, as an operator does after a power loss.
    async fn blackout(run: &mut Run<'_>) -> Result<()> {
        let stack = &mut *run.stack;
        (0..stack.ingress_count()).try_for_each(|i| stack.stop_ingress(i, Stop::Hard))?;
        (0..stack.sequencer_count()).try_for_each(|i| stack.stop_sequencer(i, Stop::Hard))?;
        stack.kill_executor();
        stack.kill_validator();
        stack.kill_l1_watchers();
        stack.kill_read_path()?;
        stack.kill_sealer();
        stack.kill_driver();
        let dark = metrics::scrape(stack.executor_metrics()).await.is_err();
        anyhow::ensure!(dark, "{}: the executor still answers", run.name);
        eprintln!("{}: every pipeline process is dead", run.at());
        // Every process starts at once, as after a power loss: the clients
        // of the sealer start before it and connect when it is up.
        run.stack.restart_driver()?;
        run.stack.restart_executor()?;
        if run.stack.cfg.validator {
            run.stack.restart_validator()?;
        }
        run.stack.restart_l1_watchers()?;
        run.stack.restart_read_path()?;
        (0..run.stack.sequencer_count()).try_for_each(|i| run.stack.start_sequencer(i))?;
        (0..run.stack.ingress_count()).try_for_each(|i| run.stack.start_ingress(i))?;
        run.stack.start_sealer()?;
        eprintln!("{}: every process started", run.at());
        let run: &Run<'_> = run;
        tokio::try_join!(
            run.assert_sealer_elected(),
            run.assert_ingress_pair_live(RESTART_BOUND),
            run.assert_executor_progress(),
            run.assert_read_path_back(),
        )?;
        let voided =
            SealerLogs::read(&run.stack.root(), run.stack.sealer_members())?.count(VOID_DECIDED);
        eprintln!("{}: void decisions in the sealer logs: {voided}", run.at());
        if run.stack.cfg.l1 {
            run.assert_no_origin_gap().await?;
        }
        Ok(())
    }
}

impl Combined {
    async fn run(self, run: &mut Run<'_>, load: &RunningLoad) -> Result<()> {
        for class in self.down {
            class.take_down(run)?;
        }
        eprintln!("{}: {:?} down", run.at(), self.down);
        let down_head = self.judge_down(run).await?;
        let baseline = if self.epochs.refill {
            Some(run.epoch_baseline().await?)
        } else {
            None
        };
        let waves = self.waves();
        for (i, wave) in waves.iter().enumerate() {
            Self::bring_back(run, wave, i.checked_sub(1).map(|j| waves[j].as_slice())).await?;
        }
        self.assert_back(run, load).await?;
        if self.expect == Expect::Stall {
            Self::assert_stalled(run, down_head)?;
        }
        if self.epochs.no_origin_gap {
            run.assert_no_origin_gap().await?;
        }
        match baseline {
            Some(base) => run.assert_epoch_refill(base).await,
            None => Ok(()),
        }
    }

    /// The pipeline while the classes are down, over [`DOWN_WINDOW`].
    /// Returns the highest executor block of the window. With no sealer,
    /// the executor still commits blocks that the sealer sealed before its
    /// loss, so a stall is judged after the return: no block of the window
    /// may pass the block at which the returned sealer resumes.
    async fn judge_down(self, run: &mut Run<'_>) -> Result<u64> {
        let before = run.reading().await?;
        tokio::time::sleep(DOWN_WINDOW).await;
        let after = run.reading().await?;
        if self.expect == Expect::SealOnly {
            anyhow::ensure!(
                after.block > before.block && after.applied == before.applied,
                "{}: expected sealed empty blocks and no applied transaction, read {before:?} -> {after:?}",
                run.name
            );
        }
        eprintln!("{}: while down: {before:?} -> {after:?}", run.at());
        if self.expect == Expect::Stall {
            anyhow::ensure!(
                !run.stack.sealer_alive(),
                "{}: a sealer member still runs",
                run.name
            );
        }
        if self.expect == Expect::Stall && !self.down.contains(&Class::Ingress) {
            Self::assert_ingresses_refuse(run).await?;
        }
        Ok(after.block)
    }

    /// No block was sealed while the sealer was down: the returned
    /// members resume at a block at or above every block that the
    /// executor committed during the outage.
    fn assert_stalled(run: &Run<'_>, down_head: u64) -> Result<()> {
        let resumed = SealerLogs::read(&run.stack.root(), run.stack.sealer_members())?
            .resume_block()
            .context("the returned sealer logged no term")?;
        anyhow::ensure!(
            down_head <= resumed,
            "{}: the executor reached block {down_head} while the sealer was down, past its resume block {resumed}",
            run.name
        );
        eprintln!(
            "{}: stalled: the executor head {down_head} of the outage is not past the resume block {resumed}",
            run.at()
        );
        Ok(())
    }

    /// Every live ingress refuses a submit with `sealer_no_quorum`.
    async fn assert_ingresses_refuse(run: &Run<'_>) -> Result<()> {
        let signer = crate::harness::l2::dev_signers_through(5)?
            .get(5)
            .cloned()
            .context("probe signer")?;
        let tx = crate::harness::l2::sign_transfer(
            &signer,
            run.stack.cfg.chain_id.get(),
            0,
            alloy_primitives::Address::repeat_byte(0xc4),
            1,
        )?;
        // A submit before the halt parks at its ingress, so each try has a
        // short client bound and the next try follows.
        let clients = (0..run.stack.ingress_count())
            .map(|i| {
                crate::harness::l2::L2Client::new(
                    &run.stack.ingress_url(i)?,
                    Duration::from_secs(3),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        for client in &clients {
            Self::await_refusal(run, client, &tx).await?;
        }
        Ok(())
    }

    /// `client` refuses `tx` with `sealer_no_quorum` within the bound.
    async fn await_refusal(
        run: &Run<'_>,
        client: &crate::harness::l2::L2Client,
        tx: &crate::harness::l2::SignedTransfer,
    ) -> Result<()> {
        let refusal = metrics::poll_until(
            &format!("{} refuses with {NO_QUORUM}", client.url),
            REFUSAL_BOUND,
            Duration::from_millis(500),
            async || {
                let out = client.send_raw(&tx.raw).await;
                Ok(out
                    .result
                    .err()
                    .map(|e| e.to_string())
                    .filter(|e| e.contains(NO_QUORUM)))
            },
        )
        .await?;
        eprintln!("{}: {} refuses: {refusal}", run.at(), client.url);
        Ok(())
    }

    /// The waves of the return: the classes that start together.
    fn waves(self) -> Vec<Vec<Class>> {
        let mut order = self.down.to_vec();
        match self.back {
            Return::AllAtOnce => return vec![order],
            Return::Ordered(_) => order.sort_unstable(),
            Return::Reverse(_) => order.sort_unstable_by(|a, b| b.cmp(a)),
        }
        order.into_iter().map(|c| vec![c]).collect()
    }

    /// Pause the stagger, prove that the wave before stayed up, then
    /// start every class of `wave`.
    async fn bring_back(run: &mut Run<'_>, wave: &[Class], before: Option<&[Class]>) -> Result<()> {
        if let Some(before) = before {
            tokio::time::sleep(STAGGER).await;
            before.iter().try_for_each(|c| c.assert_up(run))?;
        }
        wave.iter().try_for_each(|c| c.start(run))?;
        eprintln!("{}: {wave:?} started", run.at());
        Ok(())
    }

    /// Every returned process runs, the members elected a leader, both
    /// ingresses answer, the executor advances, and no ref sits below a
    /// floor.
    async fn assert_back(self, run: &mut Run<'_>, load: &RunningLoad) -> Result<()> {
        if self.down.contains(&Class::Sealer) {
            run.assert_sealer_elected().await?;
        }
        run.assert_ingress_pair_live(RESTART_BOUND).await?;
        if self.down.contains(&Class::Sequencer) {
            (0..run.stack.sequencer_count())
                .try_for_each(|i| run.stack.await_sequencer_session(i, RESTART_BOUND))?;
        }
        load.await_landed_above(load.landed(), RESTART_BOUND)
            .await?;
        self.down.iter().try_for_each(|c| c.assert_up(run))?;
        run.assert_executor_progress().await?;
        run.assert_no_ref_below_floor().await
    }
}

impl Class {
    fn take_down(self, run: &mut Run<'_>) -> Result<()> {
        let stack = &mut *run.stack;
        match self {
            Self::Sealer => {
                stack.kill_sealer();
                Ok(())
            }
            Self::Sequencer => {
                (0..stack.sequencer_count()).try_for_each(|i| stack.stop_sequencer(i, Stop::Hard))
            }
            Self::Ingress => {
                (0..stack.ingress_count()).try_for_each(|i| stack.stop_ingress(i, Stop::Hard))
            }
        }
    }

    pub(super) fn start(self, run: &mut Run<'_>) -> Result<()> {
        let stack = &mut *run.stack;
        match self {
            Self::Sealer => stack.start_sealer(),
            Self::Sequencer => {
                (0..stack.sequencer_count()).try_for_each(|i| stack.start_sequencer(i))
            }
            Self::Ingress => (0..stack.ingress_count()).try_for_each(|i| stack.start_ingress(i)),
        }
    }

    /// Every process of the class still runs: it waited for what it
    /// needs, with no exit.
    pub(super) fn assert_up(self, run: &mut Run<'_>) -> Result<()> {
        let stack = &mut *run.stack;
        let down: Vec<usize> = match self {
            Self::Sealer => Vec::new(),
            Self::Sequencer => (0..stack.sequencer_count())
                .filter(|&i| !stack.sequencer_alive(i).unwrap_or(false))
                .collect(),
            Self::Ingress => (0..stack.ingress_count())
                .filter(|&i| !stack.ingress_alive(i).unwrap_or(false))
                .collect(),
        };
        anyhow::ensure!(
            down.is_empty(),
            "{}: {self:?} processes {down:?} exited while they waited: they did not wait for the class they need",
            run.name
        );
        Ok(())
    }
}
