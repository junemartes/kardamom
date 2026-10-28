use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Output};
use std::time::{Duration, Instant};

use kardamom_state::{StateEnvBuilder, StateWriter, WriteBatch};
use kardamom_types::{BlockBoundary, BlockDelta, GenesisAlloc};

use super::CheckpointCopy;
use crate::nodes::Nodes;

struct Fixture {
    node: String,
    name: String,
    root: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let node = format!(
            "chaos-copy-{}",
            root.path().file_name().unwrap().to_str().unwrap()
        );
        let mut fixture = Self {
            node,
            name: String::new(),
            root,
        };
        fixture.checkpoint();
        Nodes
            .docker_ok(&[
                "run",
                "-d",
                "--rm",
                "--name",
                &fixture.node,
                "--shm-size=128m",
                "--entrypoint",
                "sh",
                "redis:7-alpine",
                "-c",
                "exec sleep 120",
            ])
            .await
            .unwrap();
        let source = fixture.root.path().join("checkpoints");
        Nodes
            .docker_ok_long(&[
                "cp",
                source.to_str().unwrap(),
                &format!("{}:/tmp/checkpoints", fixture.node),
            ])
            .await
            .unwrap();
        Nodes
            .docker_ok(&[
                "exec",
                &fixture.node,
                "cp",
                "-a",
                "/tmp/checkpoints",
                "/dev/shm/checkpoints",
            ])
            .await
            .unwrap();
        fixture
    }

    fn checkpoint(&mut self) {
        let env = StateEnvBuilder::new(self.root.path().join("state"))
            .open()
            .unwrap();
        kardamom_state::seed_genesis(&env, &GenesisAlloc::default()).unwrap();
        let mut writer = StateWriter::spawn(env.clone()).unwrap();
        writer
            .delta_tx
            .send(WriteBatch::new(
                BlockBoundary {
                    block_number: 1,
                    ..BlockBoundary::default()
                },
                BlockDelta {
                    block_number: 1,
                    ..BlockDelta::default()
                },
            ))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while writer
            .snapshot_rx
            .recv_deadline(deadline)
            .unwrap()
            .block_number()
            < 1
        {}
        writer.shutdown().unwrap();
        let checkpoint = kardamom_state::checkpoint::create_checkpoint(
            &env,
            &self.root.path().join("checkpoints"),
        )
        .unwrap();
        self.name = checkpoint
            .path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .into();
    }

    fn copy(&self) -> CheckpointCopy<'_> {
        CheckpointCopy {
            node: &self.node,
            root: "/dev/shm/checkpoints",
            name: &self.name,
            with_log: false,
        }
    }

    fn output(code: u8, error: &str) -> Output {
        Output {
            status: ExitStatus::from_raw(i32::from(code) << 8),
            stdout: Vec::new(),
            stderr: error.as_bytes().to_vec(),
        }
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
#[ignore = "requires one disposable Docker container; no chain services"]
async fn a_tmpfs_checkpoint_is_copied_and_verified_in_the_container_mount_namespace() {
    let fixture = Fixture::new().await;
    let copy = fixture.copy();
    let destination = tempfile::tempdir().unwrap();
    let old = Nodes
        .docker(&[
            "cp",
            &format!("{}:{}/{}", copy.node, copy.root, copy.name),
            destination.path().to_str().unwrap(),
        ])
        .await
        .unwrap();
    assert!(
        !old.status.success(),
        "the fixture must reproduce docker cp's tmpfs limitation"
    );
    assert_eq!(copy.verify().await.unwrap(), 1);
    Nodes
        .docker_ok(&[
            "exec",
            &fixture.node,
            "rm",
            &format!("{}/{}/MANIFEST", copy.root, copy.name),
        ])
        .await
        .unwrap();
    let error = copy.verify().await.unwrap_err();
    assert!(
        format!("{error:#}").contains("no verified restore point"),
        "the production verifier must reject the unmanifested copy: {error:#}"
    );
}

#[test]
fn a_live_log_growth_warning_is_the_only_tolerated_archive_read_failure() {
    let growth = Fixture::output(1, "tar: log/segment: file changed as we read it\n");
    assert!(CheckpointCopy::copied(&growth, "log").is_ok());
    assert!(CheckpointCopy::copied(&growth, "checkpoint-1").is_err());
}

#[test]
fn missing_files_and_mixed_archive_errors_fail_the_copy() {
    let absent = Fixture::output(1, "tar: log: Cannot stat: No such file or directory\n");
    assert!(CheckpointCopy::copied(&absent, "log").is_err());
    let mixed = Fixture::output(
        1,
        "tar: log: file changed as we read it\ntar: log: Cannot stat\n",
    );
    assert!(CheckpointCopy::copied(&mixed, "log").is_err());
    assert!(CheckpointCopy::copied(&Fixture::output(1, ""), "log").is_err());
    assert!(
        CheckpointCopy::copied(
            &Fixture::output(2, "tar: log: file changed as we read it\n"),
            "log"
        )
        .is_err()
    );
}
