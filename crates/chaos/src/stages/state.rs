//! Offline parity at a drained canonical head, across every executor and validator.

use std::path::{Path, PathBuf};

use anyhow::Context;
use kardamom_state::{StateEnvBuilder, deep_compare_to, sweep};

use super::rebuild::{Rebuild, Target};

use crate::harness::Harness;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::probes::EXECUTOR_BLOCK_METRIC;

impl Harness {
    /// Stop ordering, drain the consumers to one head, stop their writers,
    /// and compare copies of all persisted state. Restore the jobs on both
    /// success and failure. Copies remain in the reported directory on failure.
    /// Then rebuild the state at the validator's head from L1 alone, with
    /// the jobs back up so the batcher posts through that head, and
    /// require the validator's root.
    ///
    /// # Errors
    ///
    /// Returns an error on missing state, lag, corruption, replica differences,
    /// a rebuild that misses the root, or failure to stop or restore a job.
    pub async fn assert_persisted_state(&self) -> anyhow::Result<()> {
        let audit = StateAudit::new(self).await?;
        let outcome = audit.run().await;
        let restored = audit.restore().await;
        let rebuild = match (outcome, restored) {
            (Err(error), Err(restore)) => {
                return Err(error.context(format!("job restoration also failed: {restore:#}")));
            }
            (Err(error), _) | (_, Err(error)) => return Err(error),
            (Ok(rebuild), Ok(())) => rebuild,
        };
        rebuild.assert_parity().await
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

    async fn run(&self) -> anyhow::Result<Rebuild<'a>> {
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
        for node in &self.harness.probes.executors {
            executors.push(self.copy_executor(&node.container, &directory).await?);
        }
        let head = u64::try_from(heads.aligned().unwrap_or(0)).unwrap_or(0);
        let target = tokio::task::spawn_blocking(move || {
            StateCopies {
                validator,
                executors,
                head,
            }
            .verify()
        })
        .await??;
        crate::log(
            "persisted-state PASS: all executors match validator accounts, storage, code, headers, receipts and indexes; validator root rebuild matches",
        );
        Ok(Rebuild {
            harness: self.harness,
            evidence: directory,
            target,
            executor_image: false,
        })
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
    /// The common head: the lowest committed block, nonzero, when every
    /// consumer answers and none is more than one block past it. A
    /// stopped sealer job freezes the durability watermark, so an
    /// executor that had the last block in flight cannot settle it, and
    /// the validator commits a boundary it received before the stop; a
    /// consumer one block ahead holds one more empty block, which the
    /// compare bounded at the common head tolerates.
    fn aligned(&self) -> Option<i64> {
        let all: Option<Vec<i64>> = self
            .executors
            .iter()
            .copied()
            .chain(std::iter::once(self.validator))
            .collect();
        let all = all?;
        let low = all.iter().copied().min()?;
        let high = all.iter().copied().max()?;
        (low > 0 && high - low <= 1).then_some(low)
    }

    /// How many consumers sit one block past the common head.
    fn tail_note(&self) -> String {
        let Some(head) = self.aligned() else {
            return "no common head".to_string();
        };
        let ahead = self
            .executors
            .iter()
            .chain(std::iter::once(&self.validator))
            .filter(|h| **h != Some(head))
            .count();
        if ahead == 0 {
            "every consumer at the head".to_string()
        } else {
            format!("{ahead} consumer(s) one empty block past it")
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
    /// The common head the compare is bounded at.
    head: u64,
}

impl StateCopies {
    /// Compare every executor with the validator, and return the
    /// validator's committed root at its head.
    fn verify(&self) -> anyhow::Result<Target> {
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
        let root = report
            .state_root
            .filter(|root| Some(*root) == report.rebuilt_root)
            .context("validator has no reproducible state root")?;
        self.executors
            .iter()
            .try_for_each(|path| self.compare(&validator, path))?;
        let cursor = kardamom_state::read_recovery_point(&validator)?;
        Ok(Target {
            block: report.last_committed_block,
            root: Some(root),
            end_tx_idx: Some(cursor.last_fsynced_reader_position.as_index()),
        })
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

    fn compare(&self, validator: &kardamom_state::StateEnv, path: &Path) -> anyhow::Result<()> {
        let executor = Self::open(path)?;
        let report = sweep(&executor)?;
        anyhow::ensure!(
            report.is_clean(),
            "{} integrity: {:?}",
            path.display(),
            report.problems
        );
        let differences = deep_compare_to(&executor, validator, self.head)?;
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
