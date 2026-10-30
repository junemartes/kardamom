//! Process faults on a running stack: a graceful or a hard stop of a
//! service, and its return on the state that it persisted. The recovery
//! suite drives these steps as the deployment's scheduler stops and
//! starts a task: a graceful stop is SIGTERM with the job's kill timeout,
//! and a hard stop is SIGKILL with no shutdown hook.
//!
//! A start only spawns the process. A service that waits for a
//! dependency (the cluster session of an ingress or a sequencer) must not
//! block the caller, so the readiness waits are separate calls.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use super::LocalStack;
use super::ingress::{IngressSlot, SpawnedIngress};
use super::proc::Proc;
use super::services::{self, SequencerOptions};

/// How a service stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// SIGTERM. The service must exit on its own within the grace bound,
    /// as a task does within its kill timeout.
    Graceful,
    /// SIGKILL. No shutdown hook runs, and no final flush happens.
    Hard,
}

impl Stop {
    /// Stop `proc`. A graceful stop that overruns `grace` is an error: the
    /// scheduler would kill the task, so the stop was not graceful.
    fn apply(self, proc: &mut Proc, grace: Duration) -> Result<()> {
        match self {
            Self::Graceful => {
                anyhow::ensure!(
                    proc.terminate(grace),
                    "{} did not exit within {grace:?} after SIGTERM",
                    proc.name
                );
                Ok(())
            }
            Self::Hard => {
                proc.kill();
                Ok(())
            }
        }
    }
}

impl LocalStack {
    /// The first log name `<base>-r<k>` with no file in the stack root. A
    /// returned process logs to its own file, so the log of each earlier
    /// process survives for the failure dump.
    fn fresh_log_name(&self, base: &str) -> String {
        (1u32..=u32::MAX)
            .map(|k| format!("{base}-r{k}"))
            .find(|name| !self.root.path().join(format!("{name}.log")).exists())
            .unwrap_or_else(|| base.to_string())
    }

    /// The ingress replicas, in replica order.
    fn ingresses(&self) -> impl Iterator<Item = &SpawnedIngress> {
        std::iter::once(&self.ingress).chain(&self.extra_ingresses)
    }

    /// Ingress replica `i`.
    fn ingress_at(&self, i: usize) -> Result<&SpawnedIngress> {
        self.ingresses()
            .nth(i)
            .with_context(|| format!("no ingress replica {i}"))
    }

    /// Ingress replica `i`, for a stop or a start.
    fn ingress_at_mut(&mut self, i: usize) -> Result<&mut SpawnedIngress> {
        match i.checked_sub(1) {
            None => Ok(&mut self.ingress),
            Some(j) => self
                .extra_ingresses
                .get_mut(j)
                .with_context(|| format!("no ingress replica {i}")),
        }
    }

    /// The number of ingress replicas.
    #[must_use]
    pub fn ingress_count(&self) -> usize {
        self.ingresses().count()
    }

    /// The JSON-RPC URL of ingress replica `i`.
    ///
    /// # Errors
    /// Returns an error when the stack has no replica `i`.
    pub fn ingress_url(&self, i: usize) -> Result<String> {
        Ok(self.ingress_at(i)?.rpc_url.clone())
    }

    /// The metrics address of ingress replica `i`.
    ///
    /// # Errors
    /// Returns an error when the stack has no replica `i`.
    pub fn ingress_metrics(&self, i: usize) -> Result<SocketAddr> {
        Ok(self.ingress_at(i)?.metrics_addr)
    }

    /// Start one more ingress replica with the next replica id, as the
    /// active/active pair of the deployment runs it. Every replica reads
    /// the same receipt stream and publishes on the same lanes. Returns
    /// the index of the new replica once it listens.
    ///
    /// # Errors
    /// Returns an error when the replica fails to spawn or to listen
    /// within a minute.
    pub fn add_ingress(&mut self) -> Result<usize> {
        let index = self.ingress_count();
        let mut spawned = services::spawn_ingress_at(
            &self.service_spec(),
            &self.cfg.ingress,
            &IngressSlot {
                id: u16::try_from(index).context("ingress replica id")?,
                rpc_port: None,
                metrics_port: None,
                log_name: format!("ingress-{index}"),
            },
            None,
        )?;
        spawned
            .proc
            .wait_for_log_line("JSON-RPC listening", Duration::from_mins(1))?;
        self.extra_ingresses.push(spawned);
        Ok(index)
    }

    /// Stop ingress replica `i`. A graceful stop gets the park bound plus
    /// ten seconds: the drain waits for the parked submits up to the park
    /// bound.
    ///
    /// # Errors
    /// Returns an error when the stack has no replica `i`, or when a
    /// graceful stop overruns its grace bound.
    pub fn stop_ingress(&mut self, i: usize, stop: Stop) -> Result<()> {
        let grace = self
            .cfg
            .ingress
            .pending_receipt_timeout
            .as_duration()
            .saturating_add(Duration::from_secs(10));
        stop.apply(&mut self.ingress_at_mut(i)?.proc, grace)
    }

    /// Start ingress replica `i` again, with its replica id and on its
    /// RPC and metrics ports, so clients and probes keep their URLs. The
    /// ingress keeps no state of its own: it reads the chain from the
    /// receipt stream and the executor. The call returns once the process
    /// runs; [`Self::await_ingress_live`] waits for its listener.
    ///
    /// # Errors
    /// Returns an error when the stack has no replica `i`, or when the
    /// process fails to spawn.
    pub fn start_ingress(&mut self, i: usize) -> Result<()> {
        let old = self.ingress_at(i)?;
        let slot = IngressSlot {
            id: u16::try_from(i).context("ingress replica id")?,
            rpc_port: Some(old.rpc_port()?),
            metrics_port: Some(old.metrics_addr.port()),
            log_name: self.fresh_log_name(&format!("ingress-{i}")),
        };
        let spawned =
            services::spawn_ingress_at(&self.service_spec(), &self.cfg.ingress, &slot, None)?;
        *self.ingress_at_mut(i)? = spawned;
        Ok(())
    }

    /// Whether the process of ingress replica `i` runs.
    ///
    /// # Errors
    /// Returns an error when the stack has no replica `i`.
    pub fn ingress_alive(&mut self, i: usize) -> Result<bool> {
        Ok(self.ingress_at_mut(i)?.proc.is_alive())
    }

    /// Wait until ingress replica `i` answers `eth_chainId` on its RPC
    /// port and serves its metrics. Returns how long that took.
    ///
    /// # Errors
    /// Returns an error when the replica does not answer within `bound`.
    pub async fn await_ingress_live(&self, i: usize, bound: Duration) -> Result<Duration> {
        let replica = self.ingress_at(i)?;
        let probe = super::l2::L2Client::new(&replica.rpc_url, Duration::from_secs(2))?;
        let metrics = replica.metrics_addr;
        let started = Instant::now();
        super::metrics::poll_until(
            &format!("ingress replica {i} RPC and metrics"),
            bound,
            Duration::from_millis(100),
            async || {
                let rpc = probe.chain_id().await.result.is_ok();
                let exporter = super::metrics::scrape(metrics).await.is_ok();
                Ok((rpc && exporter).then_some(()))
            },
        )
        .await?;
        Ok(started.elapsed())
    }

    /// Stop sequencer `i`, the replica of lane `i`.
    ///
    /// # Errors
    /// Returns an error when the stack has no sequencer `i`, or when a
    /// graceful stop overruns ten seconds.
    pub fn stop_sequencer(&mut self, i: usize, stop: Stop) -> Result<()> {
        let seq = self
            .sequencers
            .get_mut(i)
            .with_context(|| format!("no sequencer {i}"))?;
        stop.apply(&mut seq.proc, Duration::from_secs(10))
    }

    /// Start sequencer `i` again. The new process is cold: it holds no
    /// nonce state, and it learns each sender's floor from the executor.
    /// The call returns once the process runs;
    /// [`Self::await_sequencer_session`] waits for its cluster session.
    ///
    /// # Errors
    /// Returns an error when the stack has no sequencer `i`, or when the
    /// process fails to spawn.
    pub fn start_sequencer(&mut self, i: usize) -> Result<()> {
        let index = u32::try_from(i).context("sequencer index")?;
        let opts = SequencerOptions {
            log_tag: self
                .fresh_log_name(&format!("sequencer-{i}"))
                .trim_start_matches(&format!("sequencer-{i}"))
                .to_string(),
            ..self.sequencer_options(i)?
        };
        let spawned = services::spawn_sequencer_with(&self.service_spec(), index, &opts)?;
        *self
            .sequencers
            .get_mut(i)
            .with_context(|| format!("no sequencer {i}"))? = spawned;
        Ok(())
    }

    /// The options of sequencer `i`: its lane, when it is a second
    /// replica of the lane.
    fn sequencer_options(&self, i: usize) -> Result<SequencerOptions> {
        let shards = usize::try_from(self.cfg.shards.get())?;
        let partition = i
            .checked_sub(shards)
            .map(|k| {
                self.sequencer_twins
                    .get(k)
                    .copied()
                    .with_context(|| format!("no sequencer {i}"))
            })
            .transpose()?;
        Ok(SequencerOptions {
            partition,
            ..SequencerOptions::default()
        })
    }

    /// Start a second replica of `lane`, as the deployment runs two
    /// racing replicas of each lane. Both read the lane's `tx_data` and
    /// offer the same references; the sealer keeps the first copy.
    /// Returns the index of the new process once its cluster session is
    /// up.
    ///
    /// # Errors
    /// Returns an error when the replica fails to spawn or to connect.
    pub fn add_sequencer_twin(&mut self, lane: u32) -> Result<usize> {
        let i = self.sequencers.len();
        self.sequencer_twins.push(lane);
        let opts = self.sequencer_options(i)?;
        let mut spawned = services::spawn_sequencer_with(
            &self.service_spec(),
            u32::try_from(i).context("sequencer index")?,
            &opts,
        )?;
        spawned
            .proc
            .wait_for_log_line("tx_ordering via Aeron Cluster", Duration::from_mins(1))?;
        self.sequencers.push(spawned);
        Ok(i)
    }

    /// The lane of sequencer `i`.
    ///
    /// # Errors
    /// Returns an error when the stack has no sequencer `i`.
    pub fn sequencer_lane(&self, i: usize) -> Result<u32> {
        Ok(self
            .sequencer_options(i)?
            .partition
            .unwrap_or(u32::try_from(i)?))
    }

    /// Whether the process of sequencer `i` runs.
    ///
    /// # Errors
    /// Returns an error when the stack has no sequencer `i`.
    pub fn sequencer_alive(&mut self, i: usize) -> Result<bool> {
        Ok(self
            .sequencers
            .get_mut(i)
            .with_context(|| format!("no sequencer {i}"))?
            .proc
            .is_alive())
    }

    /// Wait until sequencer `i` logs its connected cluster session.
    ///
    /// # Errors
    /// Returns an error when the process exits first, or when the session
    /// does not connect within `bound`.
    pub fn await_sequencer_session(&mut self, i: usize, bound: Duration) -> Result<()> {
        self.sequencers
            .get_mut(i)
            .with_context(|| format!("no sequencer {i}"))?
            .proc
            .wait_for_log_line("tx_ordering via Aeron Cluster", bound)
    }

    /// The number of sequencer processes.
    #[must_use]
    pub fn sequencer_count(&self) -> usize {
        self.sequencers.len()
    }

    /// Kill every sealer member at once.
    pub fn kill_sealer(&mut self) {
        self.sealer.kill_all();
    }

    /// Start every sealer member again on its own snapshot and log. The
    /// call returns once every member is up; the election follows.
    ///
    /// # Errors
    /// Returns an error when a member fails to come up within a minute.
    pub fn start_sealer(&mut self) -> Result<()> {
        let tag = self
            .fresh_log_name("sealer-0")
            .trim_start_matches("sealer-0")
            .to_string();
        // See `restart_driver` for the thread that spawns the members.
        tokio::task::block_in_place(|| self.sealer.start_all(&tag))
    }

    /// Whether any sealer member process runs.
    pub fn sealer_alive(&mut self) -> bool {
        self.sealer.procs.iter_mut().any(Proc::is_alive)
    }

    /// The number of sealer members.
    #[must_use]
    pub fn sealer_members(&self) -> usize {
        self.sealer.procs.len()
    }

    /// Kill the executor with no shutdown hook, as a node loss does.
    pub fn kill_executor(&mut self) {
        self.executor.proc.kill();
    }

    /// Kill the shared media driver and start it again on the same
    /// directories and ports. Every client of the killed driver must exit
    /// or reconnect; the archive keeps its catalog and recordings.
    ///
    /// # Errors
    /// Returns an error when the driver does not come up again.
    pub fn restart_driver(&mut self) -> Result<()> {
        // The driver must spawn on a thread that lives as long as the
        // stack: its child dies with the spawning thread. `block_in_place`
        // keeps this runtime thread, and frees the runtime meanwhile.
        tokio::task::block_in_place(|| self.driver.restart())
    }

    /// Kill the shared media driver.
    pub fn kill_driver(&mut self) {
        self.driver.proc.kill();
    }

    /// Kill the validator and start it again on its state directory and
    /// metrics port. It resumes from its persisted cursor.
    ///
    /// # Errors
    /// Returns an error when the stack runs no validator, or when the
    /// process fails to spawn.
    pub fn restart_validator(&mut self) -> Result<()> {
        let port = self
            .validator
            .as_mut()
            .map(|v| {
                v.proc.kill();
                v.metrics_addr.port()
            })
            .context("the stack runs no validator")?;
        let spawned = services::spawn_validator(
            &self.service_spec(),
            &services::ValidatorOptions {
                trie_shadow_check: self.cfg.trie_shadow_check,
                attester: self.validator_l1.as_ref(),
                parallel: self.cfg.validator_parallel,
                serve_feed_addr_file: None,
                egress: None,
                metrics_port: Some(port),
            },
        )?;
        self.validator = Some(spawned);
        Ok(())
    }

    /// Kill the validator with no shutdown hook.
    pub fn kill_validator(&mut self) {
        if let Some(v) = self.validator.as_mut() {
            v.proc.kill();
        }
    }

    /// The metrics address of the da-watcher, when the stack runs L1.
    #[must_use]
    pub fn da_watcher_metrics(&self) -> Option<SocketAddr> {
        self.da_watcher.as_ref().map(|w| w.metrics_addr)
    }

    /// Kill the L1 follower and the da-watcher, as a loss of their node
    /// does.
    pub fn kill_l1_watchers(&mut self) {
        if let Some(f) = self.l1_follower.as_mut() {
            f.service.proc.kill();
        }
        if let Some(w) = self.da_watcher.as_mut() {
            w.proc.kill();
        }
    }

    /// Start the L1 follower and the da-watcher again, when the stack
    /// runs L1. The da-watcher resumes after the sealer's L1 origin.
    ///
    /// # Errors
    /// Returns an error when a process fails to spawn.
    pub fn restart_l1_watchers(&mut self) -> Result<()> {
        let Some(wiring) = self.follower_l1.clone() else {
            return Ok(());
        };
        self.kill_l1_watchers();
        let spec = self.service_spec();
        let follower = services::spawn_l1_follower(&spec, &wiring)?;
        let watcher = services::spawn_da_watcher(&spec, &follower.api_url)?;
        self.l1_follower = Some(follower);
        self.da_watcher = Some(watcher);
        Ok(())
    }

    /// The executor's metrics address.
    #[must_use]
    pub fn executor_metrics(&self) -> SocketAddr {
        self.executor.metrics_addr
    }
}
