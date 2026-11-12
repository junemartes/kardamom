use super::*;
use kardamom_state::{BlockRange, StateEnvBuilder, restore_best_checkpoint};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct Fixture {
    root: tempfile::TempDir,
    checkpoint: PathBuf,
    peer: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let peer = tempfile::tempdir().unwrap();
        let checkpoint = {
            let env = StateEnvBuilder::new(root.path().join("state"))
                .open()
                .unwrap();
            kardamom_state::seed_genesis(&env, &kardamom_types::GenesisAlloc::default()).unwrap();
            let mut writer = kardamom_state::StateWriter::spawn_with_trie(
                env.clone(),
                kardamom_state::TrieMode::Incremental,
            )
            .unwrap();
            writer
                .delta_tx
                .send(kardamom_state::WriteBatch::new(
                    kardamom_types::BlockBoundary {
                        block_number: 1,
                        ..Default::default()
                    },
                    kardamom_types::BlockDelta {
                        block_number: 1,
                        ..Default::default()
                    },
                ))
                .unwrap();
            assert!(
                std::iter::from_fn(|| writer.snapshot_rx.recv())
                    .any(|snapshot| snapshot.block_number() == 1)
            );
            writer.shutdown().unwrap();
            let info = kardamom_state::checkpoint::create_checkpoint(
                &env,
                &root.path().join("checkpoints"),
            )
            .unwrap();
            info.path
        };
        for file in ["MANIFEST", "mdbx.dat"] {
            std::fs::copy(checkpoint.join(file), peer.path().join(file)).unwrap();
        }
        Self {
            root,
            checkpoint,
            peer,
        }
    }

    fn script(&self, operation: &str) -> Output {
        let mut command = Command::new("python3");
        command
            .arg("-c")
            .arg(format!(
                "import hashlib\nhashlib.__dict__.pop('file_digest', None)\n{FAULT}"
            ))
            .arg(self.root.path())
            .arg(operation);
        if operation == "arm" {
            let inspected = self.script("inspect");
            assert!(inspected.status.success());
            let evidence: Value = serde_json::from_slice(&inspected.stdout).unwrap();
            let image: alloy_primitives::B256 = evidence["original_sha256"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            let manifest: alloy_primitives::B256 = evidence["manifest_sha256"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            command
                .arg(format!("{image:x}"))
                .arg(format!("{manifest:x}"));
        }
        command.output().unwrap()
    }

    fn restore(&self, destination: &Path) -> Option<(u64, PathBuf)> {
        restore_best_checkpoint(
            &self.root.path().join("checkpoints"),
            destination,
            BlockRange::ANY,
            None,
        )
        .unwrap()
    }

    fn rejected(&self) -> PathBuf {
        self.checkpoint.with_file_name(format!(
            ".rejected-{}",
            self.checkpoint.file_name().unwrap().to_str().unwrap()
        ))
    }
}

#[test]
fn the_exact_fault_script_forces_real_image_rejection_then_a_verified_restore() {
    let f = Fixture::new();
    let baseline = tempfile::tempdir().unwrap();
    assert!(f.restore(baseline.path()).is_some());
    let armed = f.script("arm");
    assert!(
        armed.status.success(),
        "{}",
        String::from_utf8_lossy(&armed.stderr)
    );
    let evidence: Value = serde_json::from_slice(&armed.stdout).unwrap();
    assert_ne!(evidence["original_sha256"], evidence["corrupt_sha256"]);
    assert!(
        !f.script("verify").status.success(),
        "a faulted image is not yet quarantined"
    );
    assert!(f.restore(&f.root.path().join("state")).is_none());
    assert!(!f.root.path().join("state/mdbx.dat").exists());
    assert!(f.script("verify").status.success());
    std::fs::create_dir(&f.checkpoint).unwrap();
    for file in ["MANIFEST", "mdbx.dat"] {
        std::fs::copy(f.peer.path().join(file), f.checkpoint.join(file)).unwrap();
    }
    assert!(f.restore(&f.root.path().join("state")).is_some());
    assert!(f.script("verify").status.success());
    std::fs::write(f.rejected().join("MANIFEST"), "another manifest").unwrap();
    assert!(!f.script("verify").status.success());
}

#[test]
fn the_fault_removes_older_local_bases_but_preserves_the_block_log() {
    let f = Fixture::new();
    let newer = f.checkpoint.with_file_name("checkpoint-000000000000000010");
    std::fs::create_dir(&newer).unwrap();
    for file in ["MANIFEST", "mdbx.dat"] {
        std::fs::copy(f.checkpoint.join(file), newer.join(file)).unwrap();
    }
    let log = f.root.path().join("checkpoints/block-log");
    std::fs::create_dir(&log).unwrap();
    std::fs::write(log.join("keep"), "retained log").unwrap();
    assert!(f.script("arm").status.success());
    assert!(!f.checkpoint.exists());
    assert!(newer.exists());
    assert_eq!(
        std::fs::read_to_string(log.join("keep")).unwrap(),
        "retained log"
    );
}

#[test]
fn the_fault_preserves_the_executor_owned_state_directory() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let f = Fixture::new();
    let state = f.root.path().join("state");
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o750)).unwrap();
    let before = std::fs::metadata(&state).unwrap();
    let armed = f.script("arm");
    assert!(
        armed.status.success(),
        "{}",
        String::from_utf8_lossy(&armed.stderr)
    );
    let after = std::fs::metadata(&state).unwrap();
    assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
    assert_eq!(
        (after.uid(), after.gid(), after.mode()),
        (before.uid(), before.gid(), before.mode())
    );
    assert_eq!(std::fs::read_dir(&state).unwrap().count(), 0);
}

#[test]
fn only_the_pinned_victim_group_stops_and_the_saved_definition_stays_intact() {
    let saved = serde_json::json!({"TaskGroups":[
        {"Name":"executor-0","Count":1,"Tasks":[{"Name":"executor","Config":{"image":"pinned"}}]},
        {"Name":"executor-1","Count":1}, {"Name":"executor-2","Count":1}
    ]});
    let mut expected = saved.clone();
    expected["TaskGroups"][0]["Count"] = 0.into();
    assert_eq!(VictimStop::paused(&saved).unwrap(), expected);
    assert_eq!(saved["TaskGroups"][0]["Count"], 1);
    assert!(VictimStop::paused(&expected).is_err());
    assert!(
        VictimStop::paused(&serde_json::json!({"TaskGroups":[{"Name":"executor","Count":3}]}))
            .is_err()
    );
}

#[test]
fn only_the_replacement_can_reject_this_image_then_fetch_and_restore() {
    let fault = CorruptCheckpoint {
        victim: "victim".into(),
        previous: RunningProcess {
            cid: "old".into(),
            started: "before".into(),
        },
    };
    let current = RunningProcess {
        cid: "new".into(),
        started: "after".into(),
    };
    let reject = "checkpoint-12 is CORRUPT; checkpoint failed verification; quarantining";
    let logs = format!(
        "{reject}\n{}\n{}",
        super::super::FETCHED,
        super::super::RESTORED
    );
    assert!(fault.proves_recovery(&current, &logs, "checkpoint-12"));
    assert!(!fault.proves_recovery(&fault.previous, &logs, "checkpoint-12"));
    assert!(!fault.proves_recovery(&current, &logs, "checkpoint-13"));
    assert!(!fault.proves_recovery(
        &current,
        &logs.replace("CORRUPT", "missing"),
        "checkpoint-12"
    ));
    assert!(!fault.proves_recovery(
        &current,
        &format!(
            "{}\n{}\n{reject}",
            super::super::FETCHED,
            super::super::RESTORED
        ),
        "checkpoint-12"
    ));
    assert!(!fault.proves_recovery(
        &current,
        &format!("{logs}\nfresh start will replay from genesis"),
        "checkpoint-12"
    ));
}

#[test]
fn a_changed_baseline_refuses_the_fault_before_mutating_state_or_checkpoint() {
    let f = Fixture::new();
    let inspected: Value = serde_json::from_slice(&f.script("inspect").stdout).unwrap();
    let stale = Command::new("python3")
        .arg("-c")
        .arg(FAULT)
        .arg(f.root.path())
        .arg("arm")
        .arg("00".repeat(32))
        .arg(inspected["manifest_sha256"].as_str().unwrap())
        .output()
        .unwrap();
    assert!(!stale.status.success());
    assert!(String::from_utf8_lossy(&stale.stderr).contains("differs from the verified baseline"));
    assert!(f.root.path().join("state/mdbx.dat").exists());
    assert_eq!(
        serde_json::from_slice::<Value>(&f.script("inspect").stdout).unwrap(),
        inspected
    );
    let destination = tempfile::tempdir().unwrap();
    assert!(f.restore(destination.path()).is_some());
}

#[test]
fn streaming_evidence_hashes_every_chunk_without_the_new_python_api() {
    let f = Fixture::new();
    let image = f.checkpoint.join("mdbx.dat");
    let mut bytes = vec![0xAB; 2 * 1024 * 1024 + 13];
    bytes[1024 * 1024] = 0xCD;
    bytes[2 * 1024 * 1024] = 0xEF;
    std::fs::write(&image, bytes).unwrap();
    let reference = Command::new("sha256sum").arg(&image).output().unwrap();
    assert!(reference.status.success());
    let expected = String::from_utf8(reference.stdout).unwrap();
    let inspected = f.script("inspect");
    assert!(
        inspected.status.success(),
        "{}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let observed: Value = serde_json::from_slice(&inspected.stdout).unwrap();
    assert_eq!(
        observed["original_sha256"],
        expected.split_whitespace().next().unwrap()
    );
}
