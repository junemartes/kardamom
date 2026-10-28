use std::time::Duration;

use super::SCRIPT;

struct Fixture {
    node: String,
    root: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let node = format!(
            "chaos-volume-{}",
            root.path().file_name().unwrap().to_str().unwrap()
        );
        std::fs::write(root.path().join("volume.py"), SCRIPT).unwrap();
        std::fs::write(
            root.path().join("probe.py"),
            include_str!("kernel_probe.py"),
        )
        .unwrap();
        Self { node, root }
    }

    async fn run(&self) -> serde_json::Value {
        let mount = format!(
            "type=bind,source={},target=/fixture,readonly",
            self.root.path().display()
        );
        let output = tokio::process::Command::new("docker")
            .kill_on_drop(true)
            .args([
                "run",
                "--rm",
                "--name",
                &self.node,
                "--network=none",
                "--read-only",
                "--shm-size=512m",
                "--env=KARDAMOM_VOLUME_PROBE=1",
                "--mount",
                &mount,
                "python:3.13-alpine",
                "python",
                "-B",
                "/fixture/probe.py",
            ])
            .output();
        let output = tokio::time::timeout(Duration::from_secs(120), output)
            .await
            .expect("the volume probe must finish within 120 seconds")
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::process::Command::new("timeout")
            .args(["10s", "docker", "rm", "-f", &self.node])
            .output();
    }
}

#[tokio::test]
#[ignore = "requires one disposable Python container; no chain services"]
async fn large_images_survive_the_exact_controller_enospc_and_release() {
    let fixture = Fixture::new();
    let report = fixture.run().await;
    assert_eq!(report["armed"]["errno"], 28);
    assert_eq!(report["armed"]["available"], 0);
    assert_eq!(report["recovered"]["image_bytes"], 160 * 1024 * 1024);
    assert_eq!(report["baseline_preserved"], true);
    println!("{report}");
}
