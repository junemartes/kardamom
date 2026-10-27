use super::*;
use std::os::unix::fs::PermissionsExt;

struct LeaseShell {
    root: tempfile::TempDir,
    node: DataNode,
}

impl LeaseShell {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("local")).unwrap();
        for name in ["docker", "redis-cli"] {
            Self::executable(root.path(), name, include_str!("lease_docker.py"));
        }
        Self::executable(root.path(), "sleep", "#!/bin/sh\nexit 0\n");
        std::fs::write(
            root.path().join("state.json"),
            r#"{"allowed":true,"changes":[],"workers":[]}"#,
        )
        .unwrap();
        Self {
            root,
            node: DataNode {
                allocation: "primary".into(),
                node: "node".into(),
                ip: Ipv4Addr::LOCALHOST,
                cid: "cid".into(),
                started_at: "start".into(),
            },
        }
    }

    fn executable(root: &std::path::Path, name: &str, content: &str) {
        let file = root.join(name);
        std::fs::write(&file, content).unwrap();
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn run(&self, script: &str) -> bool {
        std::process::Command::new("sh")
            .args(["-c", script])
            .env("LEASE_FIXTURE", self.root.path())
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.root.path().display()),
            )
            .output()
            .unwrap()
            .status
            .success()
    }

    fn state(&self) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(self.root.path().join("state.json")).unwrap())
            .unwrap()
    }

    fn change(&self, lease: &str, access: ScriptAccess) {
        assert!(self.run(&self.node.access_script(lease, access)));
    }

    fn expire(&self, worker: usize) {
        assert!(self.run(self.state()["workers"][worker].as_str().unwrap()));
    }

    fn marker(&self, lease: &str) -> bool {
        self.root
            .path()
            .join(format!("local/kardamom-chaos-lua-{lease}"))
            .exists()
    }
}

#[test]
fn a_lease_on_the_task_mount_expires_and_restores_lua() {
    let shell = LeaseShell::new();
    shell.change("first", ScriptAccess::Blocked);
    assert!(shell.marker("first"));
    assert_eq!(shell.state()["allowed"], false);
    shell.expire(0);
    assert!(!shell.marker("first"));
    assert_eq!(shell.state()["changes"], serde_json::json!([false, true]));
}

#[test]
fn explicit_cleanup_disarms_the_old_worker_before_a_new_lease() {
    let shell = LeaseShell::new();
    shell.change("first", ScriptAccess::Blocked);
    shell.change("first", ScriptAccess::Allowed);
    assert!(!shell.marker("first"));
    shell.change("second", ScriptAccess::Blocked);
    shell.expire(0);
    assert_eq!(shell.state()["allowed"], false);
    assert!(shell.marker("second"));
    shell.expire(1);
    assert_eq!(
        shell.state()["changes"],
        serde_json::json!([false, true, false, true])
    );
}

#[test]
fn an_image_root_marker_cannot_arm_the_fault() {
    let shell = LeaseShell::new();
    let script = shell
        .node
        .access_script("first", ScriptAccess::Blocked)
        .replace("/local/", "/tmp/");
    assert!(!shell.run(&script));
    assert_eq!(shell.state()["allowed"], true);
    assert_eq!(shell.state()["workers"], serde_json::json!([]));
}

#[test]
fn both_data_tasks_use_the_nomad_task_directory_with_a_read_only_root() {
    let job = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/cluster/nomad/redis.nomad.hcl"
    ));
    assert!(["primary", "replica"].into_iter().all(|group| {
        let start = format!("group \"{group}\"");
        let task = job
            .split(&start)
            .nth(1)
            .unwrap()
            .split("group ")
            .next()
            .unwrap();
        task.contains("readonly_rootfs = true") && task.contains("/local")
    }));
}
