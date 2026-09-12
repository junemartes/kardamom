//! Process control on a running stack: freeze, crash, suspend, resume,
//! restart, and liveness probes.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_types::shard_map::ShardMap;

use super::LocalStack;
use super::services::{self, SequencerOptions, ServiceSpec, Spawned};

impl LocalStack {
    /// SIGSTOP the sealer, so it stops stamping block boundaries. The
    /// chain settles on one final head and stays there. Scenarios that
    /// must reason about "the current block" with no race (the
    /// bridge-withdrawal test matches a withdrawal to the attested output
    /// for its block) freeze the clock first. This suspends the sealer
    /// instead of killing it. Killing it would make the consumers'
    /// cluster clients retry forever and wedge shutdown.
    pub fn freeze_block_clock(&self) {
        for p in &self.sealer.procs {
            p.suspend();
        }
    }

    /// SIGKILL the executor: an unclean crash, with no shutdown hooks and
    /// no final flush. What survives is exactly what mdbx committed.
    pub fn crash_executor(&mut self) {
        self.executor.proc.kill();
    }

    /// Liveness probe for the validator process, for failure diagnostics.
    /// The bridge-withdrawal-test mdbx-read-only failure family depends on
    /// whether the validator was already dead at probe time (an
    /// Aeron driver-timeout abort leaves the environment unsteadily
    /// closed). `None` when the stack runs no validator.
    pub fn validator_alive(&mut self) -> Option<bool> {
        self.validator.as_mut().map(|v| v.proc.is_alive())
    }

    /// SIGSTOP the DA watcher, so no honest epoch competes with an
    /// injected one. This is the same determinism trick that
    /// `suspend_executor` gives the BAL drill. Returns false when the
    /// stack has no watcher (no L1).
    #[must_use]
    pub fn suspend_da_watcher(&self) -> bool {
        match &self.da_watcher {
            Some(w) => {
                w.proc.suspend();
                true
            }
            None => false,
        }
    }

    /// Freeze the executor (SIGSTOP). Its genuine BAL and receipt
    /// publications stop, so an injected corrupt frame faces no
    /// competition (the divergence-detection test).
    pub fn suspend_executor(&self) {
        self.executor.proc.suspend();
    }

    pub fn resume_executor(&self) {
        self.executor.proc.resume();
    }

    /// Restart the executor against the same state directory and metrics
    /// port. This makes it resume from its persisted cursor, instead of
    /// re-syncing from genesis, and lets scenarios keep the address they
    /// already hold.
    ///
    /// # Errors
    /// Returns an error when the executor fails to spawn.
    pub fn restart_executor(&mut self) -> Result<()> {
        let port = self.executor.metrics_addr.port();
        let respawned = services::spawn_executor_at(&self.service_spec(), Some(port))?;
        self.executor = respawned;
        Ok(())
    }

    /// The index of the sequencer (and the lane) that serves `sender`.
    #[must_use]
    pub fn sequencer_for(&self, sender: alloy_primitives::Address) -> u32 {
        kardamom_types::shard_map::partition_for(sender, self.cfg.shards)
    }

    /// Kill sequencer `index` and start a fresh process in its place. The
    /// new process is cold: it holds no nonce state. The call returns once
    /// the process has connected its cluster session. Scenarios that hold a
    /// [`crate::scenarios::Target`] rebuild it: the metrics port changed.
    ///
    /// # Errors
    /// Returns an error when the sequencer fails to spawn or connect.
    pub fn restart_sequencer(&mut self, index: u32) -> Result<()> {
        let opts = SequencerOptions {
            log_tag: "-restarted".into(),
            ..SequencerOptions::default()
        };
        self.restart_sequencer_with(index, &opts)
    }

    /// [`Self::restart_sequencer`] with explicit lane-plane options: the
    /// resize runbook restarts a shard with a new vslot set.
    ///
    /// # Errors
    /// Returns an error when the sequencer fails to spawn or connect.
    pub fn restart_sequencer_with(&mut self, index: u32, opts: &SequencerOptions) -> Result<()> {
        let i = kardamom_types::num::u32_to_usize(index);
        self.sequencers[i].proc.kill();
        let respawned = Self::spawn_sequencer_ready(&self.service_spec(), index, opts)?;
        self.sequencers[i] = respawned;
        Ok(())
    }

    /// Start one more sequencer process, at the next index. The resize
    /// runbook starts the new shard this way. The call returns once the
    /// process has connected its cluster session.
    ///
    /// # Errors
    /// Returns an error when the sequencer fails to spawn or connect, or
    /// when the stack already runs `u32::MAX` sequencers.
    pub fn add_sequencer(&mut self, opts: &SequencerOptions) -> Result<u32> {
        let index = u32::try_from(self.sequencers.len()).context("sequencer index fits u32")?;
        let spawned = Self::spawn_sequencer_ready(&self.service_spec(), index, opts)?;
        self.sequencers.push(spawned);
        Ok(index)
    }

    /// Spawn one sequencer and wait for its cluster session.
    fn spawn_sequencer_ready(
        spec: &ServiceSpec<'_>,
        index: u32,
        opts: &SequencerOptions,
    ) -> Result<Spawned> {
        let mut spawned = services::spawn_sequencer_with(spec, index, opts)?;
        spawned
            .proc
            .wait_for_log_line("tx_ordering via Aeron Cluster", Duration::from_secs(60))?;
        Ok(spawned)
    }

    /// The metrics address of sequencer `index`.
    #[must_use]
    pub fn sequencer_metrics(&self, index: u32) -> std::net::SocketAddr {
        self.sequencers[kardamom_types::num::u32_to_usize(index)].metrics_addr
    }

    /// Kill the ingress and start a fresh one on the same RPC port, with
    /// `shard_map` as its `--shard-map`. This is the resize runbook's
    /// "switch the ingress" step. Parked submits die with the old process;
    /// clients see a transport error and resubmit. The call returns once
    /// the new process listens.
    ///
    /// # Errors
    /// Returns an error when the RPC port cannot be read back from the
    /// old URL, or when the ingress fails to spawn or listen.
    pub fn restart_ingress(&mut self, shard_map: Option<&Path>) -> Result<()> {
        let port: u16 = self
            .ingress
            .rpc_url
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .context("ingress rpc port")?;
        self.ingress.proc.kill();
        let mut respawned = services::spawn_ingress_at(
            &self.service_spec(),
            &self.cfg.ingress,
            Some(port),
            shard_map,
        )?;
        respawned
            .proc
            .wait_for_log_line("JSON-RPC listening", Duration::from_secs(60))?;
        self.ingress = respawned;
        Ok(())
    }

    /// Write `map` as a `--shard-map` TOML file in the stack root.
    ///
    /// # Errors
    /// Returns an error when the file cannot be written.
    pub fn write_shard_map(&self, map: &ShardMap) -> Result<PathBuf> {
        let path = self
            .root
            .path()
            .join(format!("shard-map-v{}.toml", map.version()));
        let table: Vec<String> = map.table().iter().map(u8::to_string).collect();
        std::fs::write(
            &path,
            format!(
                "version = {}\ntable = [{}]\n",
                map.version(),
                table.join(", ")
            ),
        )?;
        Ok(path)
    }

    /// Wait for the validator process to exit on its own. Returns its
    /// exit code (exit code 2 is the divergence fail-stop).
    pub fn wait_validator_exit(&mut self, timeout: Duration) -> Option<Option<i32>> {
        self.validator.as_mut()?.proc.wait_exit(timeout)
    }

    #[must_use]
    pub fn validator_log(&self) -> Option<String> {
        let v = self.validator.as_ref()?;
        std::fs::read_to_string(&v.proc.log_path).ok()
    }
}
