//! Cluster orchestration for the perf pipeline: purge the previous
//! deployment, wipe node state, and re-run `ci-cluster.sh` (KEEP=1, load and
//! chaos stages skipped) from the orchestrator container — the same
//! bring-up path CI and `local-cluster.sh` use, so a perf run always measures
//! a fresh chain with the current build.

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

pub const SEALER_NODES: &[&str] = &[
    "kardamom-sealer-0",
    "kardamom-sealer-1",
    "kardamom-sealer-2",
];

const NOMAD_ADDR: &str = "http://192.168.56.10:4646";

/// Run a command, capture stdout, error with context on non-zero exit.
pub fn sh(program: &str, args: &[&str]) -> anyhow::Result<String> {
    let out = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("spawn {program}"))?;
    if !out.status.success() {
        bail!(
            "{program} {args:?} failed ({}):\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// `docker exec <container> bash -c <script>`.
pub fn docker_exec(container: &str, script: &str) -> anyhow::Result<String> {
    sh("docker", &["exec", container, "bash", "-c", script])
}

/// Stop + purge every Nomad job, then wipe per-node state so the next deploy
/// starts a fresh chain with new-build allocs. `nomad job status` is parsed
/// from its plain table output (the `-t` template flag silently emits nothing
/// for the list form). Periodic-batch children (`batcher/periodic-*`) are
/// purged with their parent.
pub fn purge() -> anyhow::Result<()> {
    println!("==> purging nomad jobs");
    docker_exec(
        "kardamom-control-0",
        &format!(
            r#"export NOMAD_ADDR={NOMAD_ADDR}
for j in $(nomad job status 2>/dev/null | awk 'NR>1 && $1 !~ /\// {{print $1}}'); do
  echo "   stop -purge $j"
  nomad job stop -purge "$j" >/dev/null 2>&1 || true
done"#
        ),
    )?;
    std::thread::sleep(std::time::Duration::from_secs(10));

    println!("==> wiping node state");
    for node in NODES {
        docker_exec(
            node,
            "rm -rf /opt/kardamom/state /opt/kardamom/cluster /opt/kardamom/archive \
             /opt/kardamom/checkpoints /opt/kardamom/aeron-mount/* 2>/dev/null; \
             mkdir -p /opt/kardamom/state /opt/kardamom/archive",
        )
        .with_context(|| format!("wipe {node}"))?;
    }
    Ok(())
}

/// Bring the stack up fresh: `local-cluster.sh build` (reproducible builder +
/// orchestrator image), then `ci-cluster.sh` from a fresh orchestrator with
/// KEEP=1 and the load/chaos stages skipped. Blocks until the deploy's smoke
/// gates pass; the cluster is left running.
pub fn up(repo_root: &std::path::Path, skip_build: bool) -> anyhow::Result<()> {
    let root = repo_root.to_str().context("repo root not utf-8")?;
    if !skip_build {
        println!("==> building service binaries + orchestrator image");
        sh(
            "bash",
            &[
                &format!("{root}/deploy/cluster/scripts/local-cluster.sh"),
                "build",
            ],
        )?;
    }
    purge()?;

    println!("==> deploying fresh cluster (ci-cluster.sh, KEEP=1, no load/chaos stages)");
    let _ = sh("docker", &["rm", "-f", "kardamom-orch"]);
    sh(
        "docker",
        &[
            "run",
            "-d",
            "--name",
            "kardamom-orch",
            "--privileged",
            "--network=host",
            "--pid=host",
            "-v",
            "/var/run/docker.sock:/var/run/docker.sock",
            "-v",
            &format!("{root}:/work"),
            "kardamom-orchestrator:latest",
        ],
    )?;
    let out = sh(
        "docker",
        &[
            "exec",
            "-e",
            "KEEP=1",
            "-e",
            "RUN_LOAD=0",
            "-e",
            "RUN_CHAOS=0",
            "-e",
            "REGISTRY_PUSH_NODE=control-0",
            "kardamom-orch",
            "bash",
            "-lc",
            "cd /work && deploy/cluster/scripts/ci-cluster.sh",
        ],
    )?;
    let passes = out.matches("RESULT: PASS").count();
    if passes < 2 {
        bail!("ci-cluster.sh finished but smoke gates did not both pass (saw {passes})");
    }
    println!("==> cluster up; smoke + ingress-churn gates passed");
    Ok(())
}

/// One `docker stats` sample of a set of containers → `(name, cpu%)`.
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

/// The sealer node currently doing leader work = the busiest sealer
/// container, sampled twice to dodge a transient spike. Only meaningful
/// while load is flowing.
pub fn detect_sealer_leader() -> anyhow::Result<String> {
    let mut totals: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
    for _ in 0..2 {
        for (name, pct) in cpu_sample(SEALER_NODES)? {
            *totals.entry(name).or_default() += pct;
        }
    }
    totals
        .into_iter()
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(name, _)| name)
        .context("no sealer containers responding to docker stats")
}
