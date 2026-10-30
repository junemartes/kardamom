//! Spawners for the services that keep a state directory: the executor and
//! the validator. They share one command: the config file, the media
//! driver, the chain and the state directory.

use std::path::Path;
use std::process::Command;

use anyhow::Result;
use kardamom_obs::testkit::{free_port, free_udp_port};

use super::services::{
    L1Wiring, ServiceSpec, SpawnPlan, Spawned, bin, common_service_env, with_log_config,
};

/// The parts of a stateful service's (executor, validator) bring-up that
/// vary per spawn: the binary name (also the config file's name and the
/// registry's `kardamom-<bin_name>` lookup), the state directory, the
/// reserved ports, and the `--host-id`.
struct StateService<'a> {
    bin_name: &'a str,
    state_dir: &'a Path,
    metrics_port: u16,
    egress_port: u16,
    host_id: &'a str,
}

impl ServiceSpec<'_> {
    /// Build the `Command` shared by every stateful service (executor,
    /// validator): the config file (`<root>/<svc.bin_name>.toml`, no
    /// prefix), aeron dir, shard/chain wiring, state dir, durability
    /// mode, cluster egress endpoint, and metrics address. The caller
    /// adds its own flags (e.g. `--parallel-validation`) after this
    /// returns.
    ///
    /// # Errors
    /// Returns an error when the binary is not built, or when writing
    /// the config file fails.
    fn state_service_cmd(&self, svc: &StateService<'_>) -> Result<Command> {
        let cfg_path = self.write_cluster_config(svc.bin_name, "")?;
        let mut cmd = Command::new(bin(&format!("kardamom-{}", svc.bin_name))?);
        cmd.arg("--config")
            .arg(&cfg_path)
            .arg("--aeron-dir")
            .arg(self.aeron_dir)
            .args(["--chain-id", &self.chain_id.to_string()])
            .arg("--chain")
            .arg(self.genesis)
            .arg("--state-dir")
            .arg(svc.state_dir)
            // Ephemeral test DBs: skip the per-block fsync.
            .args(["--state-durability", "safe-no-sync"])
            .args([
                "--cluster-egress-endpoint",
                &format!("127.0.0.1:{}", svc.egress_port),
            ])
            .args(["--metrics-addr", &format!("127.0.0.1:{}", svc.metrics_port)])
            .args(["--host-id", svc.host_id]);
        Ok(cmd)
    }
}

/// # Errors
/// Returns an error under the same conditions as [`spawn_executor_at`].
pub fn spawn_executor(spec: &ServiceSpec<'_>) -> Result<Spawned> {
    spawn_executor_at(spec, &ExecutorRun::default())
}

/// The parts of an executor spawn that a respawn changes.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutorRun<'a> {
    /// The metrics port of a previous instance, so a restarted executor
    /// keeps the address that scenarios already hold.
    pub metrics_port: Option<u16>,
    /// `--carry-bytes` with `--egress` at this base URL: the executor reads
    /// the member feed of member 0 on the shared media driver, then the
    /// egress.
    pub carry_egress: Option<&'a str>,
}

impl ExecutorRun<'_> {
    /// The log file of this run. A respawn logs to its own file, so the
    /// log of the first process survives for later inspection
    /// (`Proc::spawn` truncates its log file).
    fn log_name(&self) -> &'static str {
        match (self.carry_egress, self.metrics_port) {
            (Some(_), _) => "executor-carry.log",
            (None, Some(_)) => "executor-restarted.log",
            (None, None) => "executor.log",
        }
    }

    /// The flags of the carry mode and of the frames for the egress.
    fn add_args(&self, cmd: &mut Command, spec: &ServiceSpec<'_>) {
        if spec.exec_frames {
            cmd.arg("--exec-frames");
        }
        if let Some(url) = self.carry_egress {
            cmd.args(["--carry-bytes", "--egress", url]);
        }
    }
}

/// Join-miss refetch only matters alongside a log config that lists
/// durability-archive endpoints (the archive-durability variant). Without
/// it, a restarted executor or validator cannot get envelopes for canonical records
/// replayed from before the crash, and aborts by design.
fn add_archive_endpoints(cmd: &mut Command, spec: &ServiceSpec<'_>) {
    if spec.log_config.is_some() {
        cmd.args([
            "--replay-destination-endpoint",
            &format!("127.0.0.1:{}", free_udp_port().port()),
        ])
        .args([
            "--archive-control-response-endpoint",
            &format!("127.0.0.1:{}", free_udp_port().port()),
        ]);
    }
}

/// Spawn the executor, or respawn it with the settings of `run`. The
/// crash-recovery scenario restarts it against the same state directory,
/// which drives the resume-from-cursor path.
///
/// # Errors
/// Returns an error when the binary is not built, when the state
/// directory or config file cannot be created, or when the process fails
/// to spawn.
pub fn spawn_executor_at(spec: &ServiceSpec<'_>, run: &ExecutorRun<'_>) -> Result<Spawned> {
    let state_dir = spec.root.join("executor-state");
    std::fs::create_dir_all(&state_dir)?;
    let metrics_port = run.metrics_port.unwrap_or_else(|| free_port().port());
    let egress_port = free_udp_port().port();
    let mut cmd = spec.state_service_cmd(&StateService {
        bin_name: "executor",
        state_dir: &state_dir,
        metrics_port,
        egress_port,
        host_id: "e2e-exec",
    })?;
    cmd.args(["--nonce-query-addr", &spec.executor_query.to_string()]);
    // The footprint shadow is on for every e2e executor. It only
    // measures: execution stays sequential, and the handoff never blocks.
    // Running it here gives the shadow thread real multi-process pipeline
    // coverage in CI. Its per-block summary lines land in executor.log.
    cmd.env("KARDAMOM_FOOTPRINT_SHADOW", "1");
    run.add_args(&mut cmd, spec);
    with_log_config(&mut cmd, spec);
    add_archive_endpoints(&mut cmd, spec);
    common_service_env(&mut cmd);
    SpawnPlan {
        name: "executor".to_string(),
        cmd,
        log: spec.root.join(run.log_name()),
        metrics_port,
        state_dir: Some(state_dir),
    }
    .spawn()
}

/// Validator spawn knobs beyond the shared [`ServiceSpec`].
pub struct ValidatorOptions<'a> {
    /// Trie shadow-check cadence. `Some(1)` checks every block, the
    /// semantics-suite default. The production cluster runs 8.
    pub trie_shadow_check: Option<std::num::NonZeroU64>,
    /// L1 output attestation + epoch verification wiring.
    pub attester: Option<&'a L1Wiring>,
    /// `--parallel-validation` (the deployed cluster's mode).
    pub parallel: bool,
    /// `Some(path)` enables the egress-E1 serving role: `--serve-feed
    /// 127.0.0.1:0` with the bound address written to `path`.
    pub serve_feed_addr_file: Option<&'a Path>,
    /// `--egress` at this base URL: the validator reads the canonical
    /// stream from the egress.
    pub egress: Option<&'a str>,
    /// The metrics port of a previous instance, so a restarted validator
    /// keeps the address that scenarios already hold.
    pub metrics_port: Option<u16>,
}

/// The egress-E1 serving role: port 0 (OS-assigned, so concurrent stacks
/// never collide), real address discovered through the addr file.
fn add_serve_feed_args(cmd: &mut Command, opts: &ValidatorOptions<'_>) {
    if let Some(addr_file) = opts.serve_feed_addr_file {
        cmd.args(["--serve-feed", "127.0.0.1:0"])
            .arg("--serve-feed-addr-file")
            .arg(addr_file);
    }
}

/// L1 output attestation needs all three flags together, or none of them
/// (the binary rejects a partial set). `--attester-post-interval 1` posts
/// an output per block, so a withdrawal becomes finalizable promptly.
fn add_attester_args(cmd: &mut Command, opts: &ValidatorOptions<'_>) {
    if let Some(l1) = opts.attester {
        cmd.args(["--l1-rpc-url", &l1.rpc_url])
            .args(["--output-oracle", &l1.oracle])
            .args(["--attester-key", &l1.attester_key])
            .args(["--attester-post-interval", "1"])
            // With the RPC URL, this turns on epoch verification. Every
            // epoch is re-derived from L1, and a mismatch counts as a
            // divergence. So the whole L1-backed suite runs with
            // verification on. An honest producer that quietly stopped
            // matching L1 would then surface as a failure in every bridge
            // scenario, not only the forged-epoch one.
            .args(["--lockbox", &l1.lockbox]);
    }
}

/// Spawn `kardamom-validator` with its own state directory.
///
/// # Errors
/// Returns an error when the binary is not built, when the state
/// directory or config file cannot be created, or when the process fails
/// to spawn.
pub fn spawn_validator(spec: &ServiceSpec<'_>, opts: &ValidatorOptions<'_>) -> Result<Spawned> {
    let state_dir = spec.root.join("validator-state");
    std::fs::create_dir_all(&state_dir)?;
    let metrics_port = opts.metrics_port.unwrap_or_else(|| free_port().port());
    let egress_port = free_udp_port().port();
    let mut cmd = spec.state_service_cmd(&StateService {
        bin_name: "validator",
        state_dir: &state_dir,
        metrics_port,
        egress_port,
        host_id: "e2e-validator",
    })?;
    with_log_config(&mut cmd, spec);
    // As in the deployed job: a restarted validator refetches the
    // envelopes of replayed canonical records from the archive.
    add_archive_endpoints(&mut cmd, spec);
    if let Some(n) = opts.trie_shadow_check {
        cmd.args(["--trie-shadow-check", &n.to_string()]);
    }
    if opts.parallel {
        cmd.arg("--parallel-validation");
    }
    add_serve_feed_args(&mut cmd, opts);
    add_attester_args(&mut cmd, opts);
    if let Some(url) = opts.egress {
        cmd.args(["--egress", url]);
    }
    common_service_env(&mut cmd);
    // A respawn logs to its own file, so the first log survives.
    let log = if opts.metrics_port.is_some() {
        "validator-restarted.log"
    } else {
        "validator.log"
    };
    SpawnPlan {
        name: "validator".to_string(),
        cmd,
        log: spec.root.join(log),
        metrics_port,
        state_dir: Some(state_dir),
    }
    .spawn()
}
