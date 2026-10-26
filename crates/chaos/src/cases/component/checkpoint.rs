//! One bounded checkpoint install and restore, tied to the restarted victim.

use std::cell::{Cell, RefCell};

use anyhow::Context;

use super::{Harness, RESTORED};
use crate::poll::{self, Budget};
use kardamom_state::checkpoint::replay::LOG_DIR;

pub(super) struct CheckpointRestore {
    victim: String,
    donor: String,
    previous_inner: String,
    copied: Cell<bool>,
    phase: RefCell<String>,
}

impl CheckpointRestore {
    pub(super) async fn observe(h: &Harness, victim: &str, donor: &str) -> anyhow::Result<Self> {
        let previous_inner = h
            .nodes
            .inner_container(victim, "executor")
            .await
            .context("state-checkpoint-restore: no live victim before the fault")?;
        Ok(Self {
            victim: victim.into(),
            donor: donor.into(),
            previous_inner,
            copied: Cell::new(false),
            phase: RefCell::new("waiting for a replacement executor".into()),
        })
    }

    /// Copying and the victim's verified restore share one two-minute
    /// restore budget. A healthy peer restore also satisfies the proof.
    pub(super) async fn await_restored(&self, h: &Harness) -> anyhow::Result<()> {
        poll::until(Budget::secs(120, 2), |_| self.step(h))
            .await?
            .or_fail(|_| {
                crate::chaos_fail!(
                    "state-checkpoint-restore: {} does not restore in 120s; last phase: {}",
                    self.victim,
                    self.phase.borrow(),
                )
            })?;
        Ok(())
    }

    async fn step(&self, h: &Harness) -> anyhow::Result<Option<()>> {
        if let Some(inner) = h.nodes.inner_container(&self.victim, "executor").await {
            let logs = h
                .nodes
                .inner_logs(&self.victim, &inner, 100_000)
                .await
                .unwrap_or_default();
            *self.phase.borrow_mut() = format!("waiting for restore in executor container {inner}");
            return Ok(self.proves_restore(&inner, &logs).then_some(()));
        }
        if !self.copied.get() {
            match self.copy(h).await {
                Ok(copied) => self.copied.set(copied),
                Err(error) => *self.phase.borrow_mut() = format!("checkpoint copy: {error:#}"),
            }
        }
        Ok(None)
    }

    fn proves_restore(&self, inner: &str, logs: &str) -> bool {
        inner != self.previous_inner && logs.contains(RESTORED)
    }

    async fn copy(&self, h: &Harness) -> anyhow::Result<bool> {
        *self.phase.borrow_mut() = "selecting a complete donor checkpoint".into();
        let name = h.nodes.exec(&self.donor,
            "ls -d /opt/kardamom/checkpoints/checkpoint-* 2>/dev/null | sort | tail -1 | xargs -rn1 basename")
            .await?;
        if name.is_empty() {
            return Ok(false);
        }
        let copy = CheckpointCopy {
            name,
            log: h.knobs.checkpoint_log,
        };
        *self.phase.borrow_mut() = format!("reading {} from {}", copy.name, self.donor);
        let tar = h
            .nodes
            .exec_bytes(
                &self.donor,
                &format!(
                    "tar -C /opt/kardamom --warning=no-file-changed -cf - {}",
                    copy.sources()
                ),
                1,
            )
            .await?;
        *self.phase.borrow_mut() = format!("staging {} on {}", copy.name, self.victim);
        h.nodes
            .exec_with_stdin(
                &self.victim,
                &format!("rm -rf {STAGING} && mkdir -p {STAGING} && tar -C {STAGING} -xf -"),
                tar,
            )
            .await?;
        h.nodes.exec(&self.victim, &copy.install()).await?;
        let complete = h.nodes.exec_status(&self.victim,
            &format!("test -s /opt/kardamom/checkpoints/{name}/MANIFEST && test -s /opt/kardamom/checkpoints/{name}/mdbx.dat", name=copy.name)).await?;
        *self.phase.borrow_mut() = format!(
            "checkpoint {} installed; waiting for the replacement to restore",
            copy.name
        );
        Ok(complete)
    }
}

/// The staging directory of a checkpoint copy on the victim.
const STAGING: &str = "/opt/kardamom/checkpoints/.harness-staging";

/// One checkpoint copy from the donor to the victim: the newest base
/// `name`, and with the block log on, the log next to it. With the log
/// on, a base is rare, so the newest base can be many blocks old. The
/// log carries the blocks after the base. Without it, the victim
/// restores at the old base and executes the full tail.
struct CheckpointCopy {
    name: String,
    log: bool,
}

impl CheckpointCopy {
    /// The paths under `/opt/kardamom` that the donor sends.
    fn sources(&self) -> String {
        let base = format!("checkpoints/{}", self.name);
        if self.log {
            format!("{base} checkpoints/{LOG_DIR}")
        } else {
            base
        }
    }

    /// The script that moves the staged copy into place on the victim.
    /// The log moves first. A restore reads the log when the base
    /// becomes visible, so the base must not come before the log. A
    /// reader of the log stops at a torn last record, so a log copied
    /// during an append is a valid log.
    fn install(&self) -> String {
        let base = format!(
            "mv -T {STAGING}/checkpoints/{name} /opt/kardamom/checkpoints/{name}",
            name = self.name
        );
        let log = format!(
            "mv -T {STAGING}/checkpoints/{LOG_DIR} /opt/kardamom/checkpoints/{LOG_DIR} && "
        );
        let first = if self.log { log.as_str() } else { "" };
        format!("{first}{base} && rm -rf {STAGING}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_restarted_victim_can_prove_a_restore() {
        let proof = CheckpointRestore {
            victim: "victim".into(),
            donor: "donor".into(),
            previous_inner: "old".into(),
            copied: Cell::new(false),
            phase: RefCell::new(String::new()),
        };
        assert!(!proof.proves_restore("old", RESTORED));
        assert!(!proof.proves_restore("new", "fetched checkpoint from peer"));
        assert!(!proof.proves_restore("new", "seeding genesis account"));
        assert!(proof.proves_restore("new", RESTORED));
    }

    #[test]
    fn an_existing_peer_checkpoint_cannot_receive_a_nested_harness_copy() {
        let root = tempfile::tempdir().unwrap();
        let copy = CheckpointCopy {
            name: "checkpoint-89".into(),
            log: false,
        };
        let base = root.path().join(&copy.name);
        let staging = root
            .path()
            .join(".harness-staging/checkpoints")
            .join(&copy.name);
        std::fs::create_dir_all(&base).unwrap();
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(base.join("mdbx.dat"), "verified peer state").unwrap();
        std::fs::write(staging.join("mdbx.dat"), "harness state").unwrap();
        let script = copy
            .install()
            .replace("/opt/kardamom/checkpoints", root.path().to_str().unwrap());
        let status = std::process::Command::new("sh")
            .args(["-c", &script])
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success());
        assert_eq!(
            std::fs::read_to_string(base.join("mdbx.dat")).unwrap(),
            "verified peer state"
        );
        assert!(!base.join(&copy.name).exists());
    }

    #[test]
    fn a_copy_with_the_log_sends_the_log_and_installs_it_before_the_base() {
        let copy = CheckpointCopy {
            name: "checkpoint-000000000000000089".to_string(),
            log: true,
        };
        assert_eq!(
            copy.sources(),
            "checkpoints/checkpoint-000000000000000089 checkpoints/log"
        );
        let install = copy.install();
        let log = install.find("checkpoints/log /opt").unwrap();
        let base = install
            .find("checkpoints/checkpoint-000000000000000089 /opt")
            .unwrap();
        assert!(
            log < base,
            "the log must be in place before the base: {install}"
        );
    }

    #[test]
    fn a_copy_without_the_log_sends_only_the_base() {
        let copy = CheckpointCopy {
            name: "checkpoint-000000000000000089".to_string(),
            log: false,
        };
        assert_eq!(copy.sources(), "checkpoints/checkpoint-000000000000000089");
        assert!(!copy.install().contains("/log"), "{}", copy.install());
    }
}
