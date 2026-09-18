//! Offline parity at a drained canonical head, across every executor and validator.

use std::path::{Path, PathBuf};

use anyhow::Context;
use kardamom_state::{StateEnvBuilder, deep_compare, sweep};

use crate::harness::Harness;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::probes::EXECUTOR_BLOCK_METRIC;

impl Harness {
    /// Stop ordering, drain the consumers to one head, stop their writers,
    /// and compare copies of all persisted state. Restore the jobs on both
    /// success and failure. Copies remain in the reported directory on failure.
    ///
    /// # Errors
    ///
    /// Returns an error on missing state, lag, corruption, replica differences,
    /// or failure to stop or restore a job.
    pub async fn assert_persisted_state(&self) -> anyhow::Result<()> {
        let audit = StateAudit::new(self).await?;
        let outcome = audit.run().await;
        let restored = audit.restore().await;
        match (outcome, restored) {
            (Err(error), Err(restore)) => {
                Err(error.context(format!("job restoration also failed: {restore:#}")))
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

struct StateAudit<'a> {
    harness: &'a Harness,
    cluster: SavedJob,
    executor: SavedJob,
    validator: SavedJob,
}

impl<'a> StateAudit<'a> {
    async fn new(harness: &'a Harness) -> anyhow::Result<Self> {
        Ok(Self {
            cluster: SavedJob::capture(&harness.nomad, "cluster").await?,
            executor: SavedJob::capture(&harness.nomad, "executor").await?,
            validator: SavedJob::capture(&harness.nomad, "validator").await?,
            harness,
        })
    }

    async fn run(&self) -> anyhow::Result<()> {
        self.cluster.stop().await?;
        let heads = self.drain().await?;
        self.executor.stop().await?;
        self.validator.stop().await?;
        let directory = tempfile::Builder::new()
            .prefix("chaos-state-")
            .tempdir()?
            .keep();
        crate::log(format!("persisted-state evidence: {}", directory.display()));
        let validator = directory.join("validator");
        self.require_stopped(&self.harness.probes.validator.container, "validator")
            .await?;
        self.copy(
            &self.harness.probes.validator.container,
            "/opt/kardamom/state/validator",
            &validator,
        )
        .await?;
        let mut executors = Vec::new();
        for node in heads.settled(&self.harness.probes.executors) {
            executors.push(self.copy_executor(&node.container, &directory).await?);
        }
        tokio::task::spawn_blocking(move || {
            StateCopies {
                validator,
                executors,
            }
            .verify()
        })
        .await??;
        crate::log(
            "persisted-state PASS: all executors match validator accounts, storage, code, headers, receipts and indexes; validator root rebuild matches",
        );
        Ok(())
    }

    async fn copy_executor(&self, node: &str, directory: &Path) -> anyhow::Result<PathBuf> {
        self.require_stopped(node, "executor").await?;
        let path = directory.join(node);
        self.copy(node, "/opt/kardamom/state", &path).await?;
        Ok(path)
    }

    async fn require_stopped(&self, node: &str, task: &str) -> anyhow::Result<()> {
        let running = self
            .harness
            .nodes
            .exec(node, &format!("docker ps -q --filter name={task}"))
            .await?;
        anyhow::ensure!(
            running.trim().is_empty(),
            "{task} writer still runs on {node}"
        );
        Ok(())
    }

    async fn copy(&self, node: &str, source: &str, destination: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(destination)?;
        self.harness
            .nodes
            .docker_ok(&[
                "cp",
                &format!("{node}:{source}/."),
                destination.to_str().context("state copy path")?,
            ])
            .await?;
        anyhow::ensure!(
            destination.join("mdbx.dat").is_file(),
            "missing mdbx.dat from {node}:{source}"
        );
        Ok(())
    }

    async fn drain(&self) -> anyhow::Result<Heads> {
        let last = std::cell::RefCell::new(Heads::default());
        let last_ref = &last;
        let outcome = poll::until(
            Budget::new(
                self.harness.knobs.converge_slo,
                std::time::Duration::from_secs(2),
            ),
            |_| async move {
                let heads = self.heads().await;
                let aligned = heads.aligned();
                *last_ref.borrow_mut() = heads;
                Ok(aligned)
            },
        )
        .await?;
        let heads = last.into_inner();
        let (head, elapsed) = outcome.or_fail(|t| {
            anyhow::anyhow!(
                "consumers did not drain to one nonzero head within {}s after ordering stopped ({heads})",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "persisted-state: consumers drained to head {head} after {}s ({})",
            elapsed.as_secs(),
            heads.tail_note()
        ));
        Ok(heads)
    }

    /// One sample of every consumer's own head gauge. A failed scrape
    /// stays `None`: it is evidence, not zero.
    async fn heads(&self) -> Heads {
        let mut executors = Vec::new();
        for i in 0..self.harness.probes.executors.len() {
            executors.push(
                self.harness
                    .probes
                    .exec_metric(i, EXECUTOR_BLOCK_METRIC)
                    .await,
            );
        }
        let validator = self
            .harness
            .probes
            .val_metric("validator_committed_block")
            .await;
        Heads {
            executors,
            validator,
        }
    }

    async fn restore(&self) -> anyhow::Result<()> {
        // All restoration attempts run even if one job cannot be restored.
        let cluster = self.cluster.restore().await;
        let executor = self.executor.restore().await;
        let validator = self.validator.restore().await;
        cluster.and(executor).and(validator)
    }
}

/// The consumers' head gauges at one sample: the executors' settled
/// block and the validator's committed block.
#[derive(Debug, Default, PartialEq, Eq)]
struct Heads {
    executors: Vec<Option<i64>>,
    validator: Option<i64>,
}

impl Heads {
    /// The settled head: the validator's committed block, nonzero, with
    /// every executor at that block or one behind it, and at least one
    /// executor at it. A stopped sealer job freezes the durability
    /// watermark, so an executor that had the last block in flight can
    /// never settle it; that replica is one behind, not divergent, and
    /// [`Self::settled`] leaves it out of the compare.
    fn aligned(&self) -> Option<i64> {
        self.validator.filter(|v| {
            *v > 0
                && self.executors.contains(&Some(*v))
                && self
                    .executors
                    .iter()
                    .all(|e| e.is_some_and(|e| e == *v || e + 1 == *v))
        })
    }

    /// The executors at the settled head, in probe order.
    fn settled<'a, T>(&self, nodes: &'a [T]) -> impl Iterator<Item = &'a T> {
        let head = self.validator;
        nodes
            .iter()
            .zip(&self.executors)
            .filter(move |(_, e)| **e == head)
            .map(|(node, _)| node)
    }

    /// How many executors sit one block behind the settled head.
    fn tail_note(&self) -> String {
        let behind = self
            .executors
            .iter()
            .filter(|e| **e != self.validator)
            .count();
        if behind == 0 {
            "every executor at the head".to_string()
        } else {
            format!("{behind} executor(s) one block behind, left out of the compare")
        }
    }
}

impl std::fmt::Display for Heads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let show = |h: &Option<i64>| h.map_or("unreachable".to_string(), |v| v.to_string());
        let executors: Vec<String> = self.executors.iter().map(show).collect();
        write!(
            f,
            "executors=[{}] validator={}",
            executors.join(", "),
            show(&self.validator)
        )
    }
}

struct StateCopies {
    validator: PathBuf,
    executors: Vec<PathBuf>,
}

impl StateCopies {
    fn verify(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.executors.is_empty(), "no executor state to compare");
        let validator = Self::open(&self.validator)?;
        let report = sweep(&validator)?;
        anyhow::ensure!(
            report.is_clean(),
            "validator integrity: {:?}",
            report.problems
        );
        anyhow::ensure!(
            report.last_committed_block > 0 && report.receipts > 0,
            "state audit has no executed workload"
        );
        anyhow::ensure!(
            report.state_root.is_some() && report.state_root == report.rebuilt_root,
            "validator has no reproducible state root"
        );
        self.executors
            .iter()
            .try_for_each(|path| Self::compare(&validator, path))
    }

    fn open(path: &Path) -> anyhow::Result<kardamom_state::StateEnv> {
        anyhow::ensure!(
            path.join("mdbx.dat").is_file(),
            "missing database at {}",
            path.display()
        );
        StateEnvBuilder::new(path)
            .open()
            .with_context(|| format!("open {}", path.display()))
    }

    fn compare(validator: &kardamom_state::StateEnv, path: &Path) -> anyhow::Result<()> {
        let executor = Self::open(path)?;
        let report = sweep(&executor)?;
        anyhow::ensure!(
            report.is_clean(),
            "{} integrity: {:?}",
            path.display(),
            report.problems
        );
        let differences = deep_compare(&executor, validator)?;
        anyhow::ensure!(
            differences.is_empty(),
            "{} differs from validator: {}",
            path.display(),
            differences.join("; ")
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;
