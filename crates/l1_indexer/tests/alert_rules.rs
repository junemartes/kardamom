//! The L1 alert rules of `deploy/alerts.yml` under promtool, in the
//! Prometheus image that the monitoring job deploys. The series are the
//! ones the batcher, the follower and the da-watcher export under the
//! faults of the L1 chaos cases: the stale-post alert fires after its
//! hold on a growing post age and resolves when the posts show again,
//! and each follower halt pages with its cause.

use std::path::Path;
use std::process::Command;

/// The monitoring job, which names the Prometheus image it runs.
const MONITORING: &str = include_str!("../../../deploy/cluster/nomad/monitoring.nomad.hcl");

/// The deployed Prometheus image, read from the monitoring job.
fn deployed_image() -> &'static str {
    MONITORING
        .lines()
        .find_map(|line| {
            let (_, rest) = line.split_once("\"prom/prometheus:")?;
            let end = rest.find('"')?;
            let start = line.find("prom/prometheus:")?;
            Some(&line[start..start + "prom/prometheus:".len() + end])
        })
        .expect("the monitoring job names its Prometheus image")
}

#[test]
#[ignore = "requires Docker and the deployed Prometheus image"]
fn the_l1_alert_rules_fire_and_resolve_on_the_case_series() {
    let deploy = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy");
    let deploy = deploy.canonicalize().unwrap();
    let output = Command::new("timeout")
        .args([
            "50",
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
        ])
        .args(["--tmpfs", "/tmp", "-v"])
        .arg(format!("{}:/rules:ro", deploy.display()))
        .args(["--entrypoint", "promtool", deployed_image()])
        .args(["test", "rules", "/rules/alerts_tests/l1.yml"])
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "promtool failed:\n{text}");
    assert!(text.contains("SUCCESS"), "{text}");
}
