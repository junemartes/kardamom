//! The alert rules and the inhibit rules of the deployment, in the real
//! Prometheus rule engine and the real Alertmanager.
//!
//! - `promtool test rules` evaluates `deploy/alerts.yml` on the series of a
//!   DA-lag halt: the ingress exports the halt of the sealer, and each
//!   canary transfer gets the code -32010. The halt pages at once, the
//!   canary pages after its own windows, and the halt page clears after
//!   the batcher posts again. The rule engine runs on its own virtual
//!   clock, so the windows of minutes take no wall time.
//! - Alertmanager runs with the configuration that the deploy writes: the
//!   routes of the environment, then `deploy/alertmanager-inhibit.yml`.
//!   While the halt pages, each canary page and the pause of the ingress
//!   on the same cause are suppressed, and they name the fingerprint of
//!   the halt as their inhibitor. When the halt resolves, the canary page
//!   is active again.
//!
//! Each check runs one container with no network, from the image digest
//! that the monitoring job pins. Alertmanager is reached through `amtool`
//! in its own container.

#![cfg(feature = "docker-e2e")]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use serde_json::Value;

/// The images of `deploy/cluster/nomad/monitoring.nomad.hcl`.
const PROMETHEUS: &str = "prom/prometheus:v3.5.1@sha256:38c3b05c3bc744ff1b0b7b4eb82196026442845e62a1e2073795565da506d7a2";
const ALERTMANAGER: &str = "prom/alertmanager:v0.34.1@sha256:e9733bafb1bdef9b00e25a21f8f99dc26a22224bf16641ad754d1649f4c3357a";
/// The routing of an environment with no receiver of its own, as the
/// monitoring job writes it.
const ROUTES: &str = "route:\n  receiver: nobody\nreceivers:\n  - name: nobody";
const AMTOOL_URL: &str = "--alertmanager.url=http://127.0.0.1:9093";

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn docker(args: &[&str]) -> Output {
    Command::new("docker").args(args).output().expect("docker runs")
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
#[ignore = "needs Docker"]
fn promtool_pages_the_da_lag_halt_and_the_canary_pages_that_it_explains() {
    let root = repo();
    let tests = root.join("crates/obs/tests/alert_rules/da_lag.test.yml");
    let rules = root.join("deploy/alerts.yml");
    let output = docker(&[
        "run",
        "--rm",
        "--network",
        "none",
        "-v",
        &format!("{}:/rules/da_lag.test.yml:ro", tests.display()),
        "-v",
        &format!("{}:/rules/alerts.yml:ro", rules.display()),
        "-w",
        "/rules",
        "--entrypoint",
        "promtool",
        PROMETHEUS,
        "test",
        "rules",
        "da_lag.test.yml",
    ]);
    assert!(output.status.success(), "{}", text(&output));
    assert!(text(&output).contains("SUCCESS"), "{}", text(&output));
}

/// One Alertmanager container. It is removed when the value ends.
struct Alertmanager {
    id: String,
    _config: tempfile::TempDir,
}

impl Alertmanager {
    /// Start Alertmanager with the configuration of the deploy: `routes`,
    /// then the inhibit rules of the repository.
    fn start(routes: &str) -> Self {
        let inhibit =
            std::fs::read_to_string(repo().join("deploy/alertmanager-inhibit.yml")).unwrap();
        let config = tempfile::tempdir().unwrap();
        let file = config.path().join("alertmanager.yml");
        std::fs::write(&file, [routes, inhibit.as_str(), ""].join("\n")).unwrap();
        let output = docker(&[
            "run",
            "-d",
            "--rm",
            "--network",
            "none",
            "-v",
            &format!("{}:/etc/alertmanager/alertmanager.yml:ro", file.display()),
            ALERTMANAGER,
            "--config.file=/etc/alertmanager/alertmanager.yml",
            "--storage.path=/tmp/alertmanager",
            "--cluster.listen-address=",
        ]);
        assert!(output.status.success(), "{}", text(&output));
        let manager = Self {
            id: String::from_utf8(output.stdout).unwrap().trim().to_string(),
            _config: config,
        };
        manager.await_ready();
        manager
    }

    fn amtool(&self, args: &[&str]) -> Output {
        let mut all = vec!["exec", self.id.as_str(), "amtool", AMTOOL_URL];
        all.extend_from_slice(args);
        docker(&all)
    }

    fn await_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        let ready = std::iter::repeat_with(|| {
            std::thread::sleep(Duration::from_millis(100));
            self.amtool(&["config", "show"]).status.success()
        })
        .take_while(|_| Instant::now() < deadline)
        .any(|ready| ready);
        assert!(ready, "Alertmanager does not answer");
    }

    /// Send the alert `name` with `labels`, as Prometheus sends a firing
    /// alert. `end` ends it, as Prometheus sends a resolved alert.
    fn fire(&self, name: &str, labels: &[&str], end: Option<&str>) {
        let mut args = vec!["alert", "add", name];
        args.extend_from_slice(labels);
        let end_arg = end.map(|end| format!("--end={end}"));
        args.extend(end_arg.as_deref());
        let output = self.amtool(&args);
        assert!(output.status.success(), "{}", text(&output));
    }

    /// The time in the container, in RFC 3339.
    fn now(&self) -> String {
        let output = docker(&["exec", self.id.as_str(), "date", "-u", "+%Y-%m-%dT%H:%M:%SZ"]);
        assert!(output.status.success(), "{}", text(&output));
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    /// Every alert, with the suppressed ones.
    fn alerts(&self) -> Vec<Value> {
        let output = self.amtool(&["alert", "query", "--active", "--inhibited", "-o", "json"]);
        assert!(output.status.success(), "{}", text(&output));
        serde_json::from_slice(&output.stdout).unwrap()
    }

    /// Wait until `done` holds for the alerts, and return them.
    fn await_alerts(&self, what: &str, done: impl Fn(&[Value]) -> bool) -> Vec<Value> {
        let deadline = Instant::now() + Duration::from_secs(20);
        std::iter::repeat_with(|| {
            std::thread::sleep(Duration::from_millis(100));
            self.alerts()
        })
        .take_while(|_| Instant::now() < deadline)
        .find(|alerts| done(alerts))
        .unwrap_or_else(|| panic!("{what}: {:?}", self.alerts()))
    }
}

impl Drop for Alertmanager {
    fn drop(&mut self) {
        docker(&["rm", "-f", self.id.as_str()]);
    }
}

/// The alert named `name` in `alerts`.
fn named<'a>(alerts: &'a [Value], name: &str) -> Option<&'a Value> {
    alerts.iter().find(|alert| alert["labels"]["alertname"] == name)
}

/// Whether `alert` is suppressed by the alert with `fingerprint`.
fn inhibited_by(alert: Option<&Value>, fingerprint: &Value) -> bool {
    alert.is_some_and(|alert| {
        alert["status"]["state"] == "suppressed"
            && alert["status"]["inhibitedBy"]
                .as_array()
                .is_some_and(|by| by.contains(fingerprint))
    })
}

const HALT: [&str; 5] = [
    "severity=critical",
    "cause=da_lag",
    "service=ingress",
    "host_id=h",
    "instance=ingress-0:9001",
];

#[test]
#[ignore = "needs Docker"]
fn the_deployed_inhibit_rules_mute_the_canary_page_only_while_the_da_lag_halt_pages() {
    let manager = Alertmanager::start(ROUTES);
    manager.fire("KardamomHaltDaLag", &HALT, None);
    manager.fire("KardamomCanaryStalled", &["severity=critical", "host_id=h"], None);
    manager.fire("KardamomCanaryFailing", &["severity=critical", "probe=transfer"], None);
    manager.fire(
        "KardamomServicePaused",
        &["severity=info", "cause=da_lag", "service=ingress", "host_id=h"],
        None,
    );
    let alerts = manager.await_alerts("every page arrives", |alerts| alerts.len() == 4);
    let halt = named(&alerts, "KardamomHaltDaLag").expect("the halt pages");
    assert_eq!(halt["status"]["state"], "active", "the halt pages");
    let fingerprint = &halt["fingerprint"];
    ["KardamomCanaryStalled", "KardamomCanaryFailing", "KardamomServicePaused"]
        .iter()
        .for_each(|name| {
            assert!(
                inhibited_by(named(&alerts, name), fingerprint),
                "{name} waits behind the halt: {alerts:?}"
            );
        });

    // The batcher posts again: the halt resolves, and a canary page that
    // stays has no halt to explain it.
    manager.fire("KardamomHaltDaLag", &HALT, Some(&manager.now()));
    let alerts = manager.await_alerts("the canary page is active again", |alerts| {
        named(alerts, "KardamomHaltDaLag").is_none()
            && named(alerts, "KardamomCanaryStalled")
                .is_some_and(|alert| alert["status"]["state"] == "active")
    });
    assert_eq!(
        named(&alerts, "KardamomCanaryFailing").map(|alert| &alert["status"]["state"]),
        Some(&Value::from("active"))
    );
}
