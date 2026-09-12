//! Ansible owns the perf cluster lifecycle, including reset and provisioning.
//! Runtime CPU sampling and profiling helpers remain here.

use std::process::Command;

use anyhow::{Context, bail};

/// The node containers of the standard deploy/cluster topology.
pub const NODES: &[&str] = &[
    "kardamom-control-0",
    "kardamom-sequencer-0",
    "kardamom-sequencer-1",
    "kardamom-ingress-0",
    "kardamom-ingress-1",
    "kardamom-executor-0",
    "kardamom-executor-1",
    "kardamom-executor-2",
    "kardamom-sealer-0",
    "kardamom-sealer-1",
    "kardamom-sealer-2",
    "kardamom-aux-0",
];

const SEALER_NODES: &[&str] = &[
    "kardamom-sealer-0",
    "kardamom-sealer-1",
    "kardamom-sealer-2",
];

/// Run a command, and capture stdout. Errors with context on a
/// non-zero exit code.
pub(crate) fn sh(program: &str, args: &[&str]) -> anyhow::Result<String> {
    let out = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("spawn {program}"))?;
    if !out.status.success() {
        bail!(
            "{program} {args:?} failed ({}):\n{}\n{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// `docker exec <container> bash -c <script>`.
pub(crate) fn docker_exec(container: &str, script: &str) -> anyhow::Result<String> {
    sh("docker", &["exec", container, "bash", "-c", script])
}

/// Build and deploy a fresh chain through the same Ansible lifecycle as CI.
///
/// # Errors
///
/// Returns an error if the `ansible-playbook` run fails.
pub fn up(repo_root: &std::path::Path, skip_build: bool) -> anyhow::Result<()> {
    let playbook = repo_root.join("deploy/cluster/ansible/local.yml");
    let vars = serde_json::json!({
        "local_runner_operation": "reset",
        "local_runner_build": !skip_build,
        "local_runner_keep": true,
    });
    let out = Command::new("ansible-playbook")
        .args(["-i", "localhost,"])
        .arg(&playbook)
        .args(["--extra-vars", &vars.to_string()])
        .env("RUN_LOAD", "0")
        .env("RUN_CHAOS", "0")
        .output()
        .context("run Ansible cluster lifecycle")?;
    if !out.status.success() {
        bail!(
            "cluster lifecycle failed: {}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    println!("{}", String::from_utf8_lossy(&out.stdout));
    println!("==> cluster up; smoke + ingress-churn gates passed");
    Ok(())
}

/// Take one `docker stats` sample of a set of containers. Returns
/// `(name, cpu%)` for each.
///
/// # Errors
///
/// Returns an error if the `docker stats` command fails, or if its
/// output cannot be parsed.
pub fn cpu_sample(containers: &[&str]) -> anyhow::Result<Vec<(String, f64)>> {
    let mut args = vec!["stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}}"];
    args.extend_from_slice(containers);
    let out = sh("docker", &args)?;
    Ok(out
        .lines()
        .filter_map(|l| {
            let (name, pct) = l.trim().split_once(' ')?;
            Some((name.to_string(), pct.trim_end_matches('%').parse().ok()?))
        })
        .collect())
}

/// Sample sealer CPU once, folding each container's percentage into
/// `totals`.
fn accumulate_cpu_sample(
    totals: &mut std::collections::HashMap<String, f64>,
) -> anyhow::Result<()> {
    for (name, pct) in cpu_sample(SEALER_NODES)? {
        *totals.entry(name).or_default() += pct;
    }
    Ok(())
}

/// The sealer node currently doing leader work: the busiest sealer
/// container, sampled twice to avoid a transient spike. This result is
/// meaningful only while load is flowing.
///
/// # Errors
///
/// Returns an error if the `docker stats` sampling fails, or if no
/// sealer container is running.
pub fn detect_sealer_leader() -> anyhow::Result<String> {
    let mut totals: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
    for _ in 0..2 {
        accumulate_cpu_sample(&mut totals)?;
    }
    totals
        .into_iter()
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(name, _)| name)
        .context("no sealer containers responding to docker stats")
}
