//! One checkpoint install on a stopped victim, and its restore by the
//! next process of the victim.

use anyhow::Context;
use serde_json::Value;

use super::{FETCHED, Harness, LOG_APPLIED, LOG_FETCHED, RESTORED};
use crate::nodes::RunningProcess;
use crate::nomad::{Alloc, SavedJob};
use crate::poll::{self, Budget};
use kardamom_state::checkpoint::replay::LOG_DIR;

const CTX: &str = "state-checkpoint-restore";

pub(super) struct CheckpointRestore {
    victim: String,
    donor: String,
    previous: RunningProcess,
}

impl CheckpointRestore {
    pub(super) async fn observe(h: &Harness, victim: &str, donor: &str) -> anyhow::Result<Self> {
        let previous = h
            .nodes
            .running_process(victim, "executor")
            .await
            .context("state-checkpoint-restore: no live victim before the fault")?;
        Ok(Self {
            victim: victim.into(),
            donor: donor.into(),
            previous,
        })
    }

    /// Stop the victim's group, wipe its state and checkpoints, and
    /// install the newest checkpoint of the donor. No victim process
    /// runs during the copy, so its next process finds only the copy.
    /// The saved job comes back on every path.
    pub(super) async fn install(&self, h: &Harness) -> anyhow::Result<CheckpointCopy> {
        let saved = SavedJob::capture(&h.nomad, "executor").await?;
        let copied = match (VictimStop {
            victim: &self.victim,
        })
        .stop(h, &saved)
        .await
        {
            Ok(()) => self.wipe_and_copy(h).await,
            Err(error) => Err(error),
        };
        let restored = saved.restore().await;
        let copy = copied?;
        restored?;
        Ok(copy)
    }

    async fn wipe_and_copy(&self, h: &Harness) -> anyhow::Result<CheckpointCopy> {
        super::wipe_state(h, &self.victim, CTX).await?;
        self.copy(h).await
    }

    /// The next process of the victim restores the installed copy within
    /// two minutes.
    pub(super) async fn await_restored(
        &self,
        h: &Harness,
        copy: &CheckpointCopy,
    ) -> anyhow::Result<()> {
        poll::until(Budget::secs(120, 2), |_| self.step(h, copy))
            .await?
            .or_fail(|_| {
                crate::chaos_fail!(
                    "{CTX}: {} does not restore the installed {} in 120s (a peer fetch, a fresh start, or no restore line)",
                    self.victim,
                    copy.name,
                )
            })?;
        Ok(())
    }

    async fn step(&self, h: &Harness, copy: &CheckpointCopy) -> anyhow::Result<Option<()>> {
        let Some(process) = h.nodes.running_process(&self.victim, "executor").await else {
            return Ok(None);
        };
        let logs = h
            .nodes
            .process_logs(&self.victim, &process, 100_000)
            .await
            .unwrap_or_default();
        Ok(self.proves_restore(&process, &logs, copy).then_some(()))
    }

    fn proves_restore(&self, process: &RunningProcess, logs: &str, copy: &CheckpointCopy) -> bool {
        *process != self.previous && copy.restored_by(logs)
    }

    async fn copy(&self, h: &Harness) -> anyhow::Result<CheckpointCopy> {
        let name = h.nodes.exec(&self.donor,
            "ls -d /opt/kardamom/checkpoints/checkpoint-* 2>/dev/null | sort | tail -1 | xargs -rn1 basename")
            .await?;
        anyhow::ensure!(
            !name.is_empty(),
            "{CTX}: {} has no checkpoint to copy",
            self.donor
        );
        let copy = CheckpointCopy {
            name,
            log: h.knobs.checkpoint_log,
        };
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
        anyhow::ensure!(
            complete,
            "{CTX}: the copy of {} on {} is incomplete",
            copy.name,
            self.victim
        );
        crate::log(format!(
            "{CTX}: installed {} from {} on the stopped {}",
            copy.name, self.donor, self.victim
        ));
        Ok(copy)
    }
}

/// The stop of one pinned executor group. The carry deployment pins one
/// executor per group, so only the victim stops.
pub(super) struct VictimStop<'a> {
    pub(super) victim: &'a str,
}

impl VictimStop<'_> {
    /// Register `saved` with the `executor-0` group at count 0, and wait
    /// until no executor process runs on the victim.
    ///
    /// # Errors
    ///
    /// Returns an error if the register fails or the victim still runs
    /// after two minutes.
    pub(super) async fn stop(&self, h: &Harness, saved: &SavedJob) -> anyhow::Result<()> {
        saved.register(&Self::paused(saved.definition())?).await?;
        poll::until(Budget::secs(120, 2), |_| async {
            let allocs = h.nomad.allocations("executor").await?;
            let stopped = allocs
                .iter()
                .filter(|a| a.task_group == "executor-0")
                .all(Alloc::is_terminal);
            Ok((stopped
                && h.nodes
                    .running_process(self.victim, "executor")
                    .await
                    .is_none())
            .then_some(()))
        })
        .await?
        .or_fail(|_| {
            anyhow::anyhow!(
                "{} still runs an executor; refusing to change its files",
                self.victim
            )
        })?;
        Ok(())
    }

    /// `saved` with the pinned `executor-0` group at count 0.
    pub(super) fn paused(saved: &Value) -> anyhow::Result<Value> {
        let mut paused = saved.clone();
        let group = paused["TaskGroups"]
            .as_array_mut()
            .and_then(|groups| {
                groups
                    .iter_mut()
                    .find(|group| group["Name"] == "executor-0")
            })
            .context("checkpoint fault needs the pinned executor-0 group")?;
        anyhow::ensure!(
            group["Count"] == 1,
            "checkpoint victim must have one allocation"
        );
        group["Count"] = 0.into();
        Ok(paused)
    }
}

/// The staging directory of a checkpoint copy on the victim.
const STAGING: &str = "/opt/kardamom/checkpoints/.harness-staging";

/// One checkpoint copy from the donor to the victim: the newest base
/// `name`, and with the block log on, the log next to it. With the log
/// on, a base is rare, so the newest base can be many blocks old. The
/// log carries the blocks after the base. Without it, the victim
/// restores at the old base and executes the full tail.
pub(super) struct CheckpointCopy {
    name: String,
    log: bool,
}

impl CheckpointCopy {
    /// Whether `logs` of one process show the restore of this copy: the
    /// restore line names the copy, the process fetched nothing from a
    /// peer, and with the block log on, it applied the log after the
    /// base.
    fn restored_by(&self, logs: &str) -> bool {
        let restored = logs
            .lines()
            .any(|line| line.contains(RESTORED) && line.contains(&self.name));
        let fetched = logs.contains(FETCHED) || logs.contains(LOG_FETCHED);
        let applied = !self.log || logs.contains(LOG_APPLIED);
        restored && applied && !fetched
    }

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
    fn only_the_restarted_victim_can_prove_a_restore_of_the_copy() {
        let proof = CheckpointRestore {
            victim: "victim".into(),
            donor: "donor".into(),
            previous: RunningProcess {
                cid: "old-cid".into(),
                started: "first-start".into(),
            },
        };
        let copy = CheckpointCopy {
            name: "checkpoint-000000000000000089".into(),
            log: false,
        };
        let replacement = RunningProcess {
            cid: "replacement-cid".into(),
            started: "second-start".into(),
        };
        let restored = format!(
            "{RESTORED}; will replay tail from here restored_block=89 checkpoint=/opt/kardamom/checkpoints/{}",
            copy.name
        );
        assert!(!proof.proves_restore(&proof.previous, &restored, &copy));
        assert!(!proof.proves_restore(&replacement, "seeding genesis account", &copy));
        assert!(proof.proves_restore(&replacement, &restored, &copy));
        let restarted = RunningProcess {
            cid: "old-cid".into(),
            started: "second-start".into(),
        };
        assert!(proof.proves_restore(&restarted, &restored, &copy));
        let other = format!(
            "{RESTORED} checkpoint=/opt/kardamom/checkpoints/checkpoint-000000000000000120"
        );
        assert!(!proof.proves_restore(&replacement, &other, &copy));
        let healed = format!("{FETCHED} (verified) block=89\n{restored}");
        assert!(!proof.proves_restore(&replacement, &healed, &copy));
    }

    /// With the block log on, the restore must also apply the copied log,
    /// and a log fetched from a peer is no proof.
    #[test]
    fn a_copy_with_the_log_needs_the_applied_log() {
        let copy = CheckpointCopy {
            name: "checkpoint-000000000000000089".into(),
            log: true,
        };
        let restored = format!(
            "{RESTORED} checkpoint=/opt/kardamom/checkpoints/{}",
            copy.name
        );
        assert!(!copy.restored_by(&restored));
        let applied = format!("{restored}\n{LOG_APPLIED} base=89 block=140");
        assert!(copy.restored_by(&applied));
        assert!(!copy.restored_by(&format!("{LOG_FETCHED}\n{applied}")));
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
