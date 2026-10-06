//! Every sealer member loses its cluster and archive directories, so no
//! member holds a log or a snapshot. The chain restarts after the posted
//! head H from a state rebuilt from L1 and the DA store: the members
//! start from a seed, the executors and the validator resume on the
//! rebuilt state at the seed's cursor, and the da-watcher starts after
//! the seed's L1 origin. Blocks after H are reverted. The case lets them
//! hold only epochs, so the revert takes no receipt from the load.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::json;

use super::seed_evidence::{ResumeCounts, SealerCounts};
use super::seeding::{JobDefinition, SEED_DIR, SeedHead};
use super::{
    FULL_RESTART_ELECTION, RESUMED, StateOwner, evidence_dir, install_image, install_rebuilt_image,
    sealers, unchecked_target, wipe_node,
};
use crate::cases::cache::freeze_and_flush;
use crate::cases::component::{executor_containers, wipe_dirs};
use crate::evidence::CountWait;
use crate::harness::Harness;
use crate::l1::L1;
use crate::nomad::{SavedJob, Streams};
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;
use crate::stages::rebuild::{Output, Rebuild, Rebuilt};

pub(super) const CTX: &str = "sealer-fleet-total-wipe-recover";

/// The Nomad variable that opens the bootstrap of a new sealer cluster.
/// While it exists, the cluster job renders `true` into each member's
/// bootstrap file, and a blank member starts at log position 0 instead of
/// waiting for a peer's snapshot. No peer holds one after a fleet wipe.
const BOOTSTRAP_VARIABLE: &str = "nomad/jobs/cluster";

/// The validator's state directory on the aux node, and its gauge of
/// the last committed block.
const VALIDATOR_STATE: &str = "/opt/kardamom/state/validator";
const VALIDATOR_COMMITTED: &str = "validator_committed_block";

/// The da-watcher's metrics port on the aux node, and its gauges: the
/// last L1 block whose epoch it published, the finalized tip it read, and
/// the count of epochs this process published.
const DA_WATCHER_PORT: u16 = 9005;
const EPOCH_ORIGIN: &str = "kardamom_da_watcher_epoch_origin_block_number";
const L1_FINALIZED: &str = "kardamom_da_watcher_l1_finalized_block_number";
const EPOCHS_PUBLISHED: &str = "kardamom_da_watcher_epochs_published_total";

/// The script that empties a member's cluster and archive directories,
/// and makes the seed directory. It keeps `archive/dir`: that is the
/// node's shared Aeron archive, not the member's.
const SEALER_WIPE: &str = "find /opt/kardamom/cluster -mindepth 1 -delete && find /opt/kardamom/archive -mindepth 1 -maxdepth 1 ! -name dir -exec rm -rf {} + && mkdir -p /opt/kardamom/seed";

/// The batcher's spool and cursor file. A spool that holds blocks after
/// H continues the confirmed cursor, so the batcher would post reverted
/// blocks. Without a cursor file, the batcher reads `(E_H, H + 1)` from
/// the last posted batch.
const BATCHER_WIPE: &str = "rm -rf /opt/kardamom/batcher/spool /opt/kardamom/batcher/cursor.json";

/// Wipe every sealer member and restart the chain after the posted head
/// from a state rebuilt from L1. One executor keeps the state of the old
/// chain and must get `REPLAY_AHEAD`; it then gets the rebuilt image too.
pub(crate) async fn sealer_fleet_total_wipe_recover(h: &mut Harness) -> anyhow::Result<()> {
    let wipe = FleetWipe::new(h).await?;
    let outcome = wipe.run().await;
    if outcome.is_err() {
        wipe.bring_back().await;
    }
    outcome
}

/// The jobs the case stops, as Nomad held them before the case.
struct Jobs {
    ingress: SavedJob,
    sequencer: SavedJob,
    batcher: SavedJob,
    da_watcher: SavedJob,
    mirror: SavedJob,
    executor: SavedJob,
    validator: SavedJob,
    cluster: SavedJob,
}

impl Jobs {
    async fn capture(h: &Harness) -> anyhow::Result<Self> {
        let n = &h.nomad;
        Ok(Self {
            ingress: SavedJob::capture(n, "ingress").await?,
            sequencer: SavedJob::capture(n, "sequencer").await?,
            batcher: SavedJob::capture(n, "batcher").await?,
            da_watcher: SavedJob::capture(n, "da-watcher").await?,
            mirror: SavedJob::capture(n, "state-mirror").await?,
            executor: SavedJob::capture(n, "executor").await?,
            validator: SavedJob::capture(n, "validator").await?,
            cluster: SavedJob::capture(n, CLUSTER_TASK).await?,
        })
    }

    /// The jobs that stop after the batcher, in the order they stop: the
    /// producers first, then the cluster, then its consumers.
    fn after_the_batcher(&self) -> [&SavedJob; 6] {
        [
            &self.sequencer,
            &self.da_watcher,
            &self.mirror,
            &self.cluster,
            &self.executor,
            &self.validator,
        ]
    }

    /// Every job, in the order a failed case starts them again.
    fn all(&self) -> [&SavedJob; 8] {
        [
            &self.cluster,
            &self.executor,
            &self.validator,
            &self.mirror,
            &self.sequencer,
            &self.da_watcher,
            &self.ingress,
            &self.batcher,
        ]
    }
}

/// The rebuild at H: the seed's head, the executor image, and the state
/// that keeps the trie for the validator.
struct Seeded {
    head: SeedHead,
    image: Rebuilt,
    validator: Rebuilt,
    seed: PathBuf,
}

struct FleetWipe<'a> {
    h: &'a Harness,
    jobs: Jobs,
    l1: L1,
    executors: Vec<String>,
    sealers: Vec<String>,
    aux: String,
}

impl<'a> FleetWipe<'a> {
    async fn new(h: &'a Harness) -> anyhow::Result<Self> {
        Ok(Self {
            jobs: Jobs::capture(h).await?,
            l1: L1::new(h).await?,
            executors: executor_containers(h),
            sealers: sealers(h)?,
            aux: h.probes.validator.container.clone(),
            h,
        })
    }

    async fn run(&self) -> anyhow::Result<()> {
        let old_head = self.quiesce().await?;
        let seeded = self.rebuild(old_head).await?;
        self.wipe(&seeded).await?;
        self.seed_members(&seeded).await?;
        self.resume_executors(&seeded).await?;
        self.resume_the_rest(&seeded).await?;
        self.assert_the_record_continues(seeded.head.block).await
    }

    /// The executor that keeps the state of the old chain.
    fn stale(&self) -> anyhow::Result<&str> {
        self.executors
            .last()
            .map(String::as_str)
            .ok_or_else(|| crate::chaos_fail!("{CTX}: no executor node"))
    }

    /// The executors that resume on the rebuilt image at once.
    fn fresh(&self) -> &[String] {
        self.executors
            .split_last()
            .map_or(&[][..], |(_, fresh)| fresh)
    }

    /// Stop the ingress and let the blocks in flight land and post. Then
    /// stop the batcher, so H stays where it is, and seal a few more
    /// blocks with an epoch in them: a consumer of the old chain then
    /// holds a cursor past the new head. Stop the rest. Returns the head
    /// of the old chain.
    async fn quiesce(&self) -> anyhow::Result<u64> {
        self.jobs.ingress.stop().await?;
        let drained = self.advance(5).await?;
        self.await_posted_through(drained).await?;
        self.jobs.batcher.stop().await?;
        crate::log(format!(
            "{CTX}: ingress and batcher stopped; L1 covers block {drained} or more"
        ));
        self.seal_an_epoch().await?;
        let old_head = self.advance(5).await?;
        for job in self.jobs.after_the_batcher() {
            job.stop().await?;
        }
        crate::log(format!(
            "{CTX}: every pipeline job stopped at head {old_head}"
        ));
        Ok(old_head)
    }

    /// Wait until the executors' head moves `blocks` past its current
    /// value, and return it.
    async fn advance(&self, blocks: i64) -> anyhow::Result<u64> {
        let start = self
            .h
            .probes
            .executor_progress()
            .await
            .ok_or_else(|| crate::chaos_fail!("{CTX}: no executor head to wait from"))?;
        let target = start
            .checked_add(blocks)
            .ok_or_else(|| crate::chaos_fail!("{CTX}: executor head {start} overflows"))?;
        let probes = &self.h.probes;
        let outcome = poll::until(Budget::secs(120, 2), |_| async move {
            Ok(probes
                .executor_progress()
                .await
                .filter(|head| *head >= target))
        })
        .await?;
        let (head, _) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{CTX}: the executor head did not move {blocks} blocks past {start} in {}s",
                t.as_secs()
            )
        })?;
        Ok(u64::try_from(head)?)
    }

    /// Wait until L1 covers `block`.
    async fn await_posted_through(&self, block: u64) -> anyhow::Result<u64> {
        let l1 = &self.l1;
        let outcome = poll::until(Budget::secs(300, 3), |_| async move {
            Ok(Some(l1.covered_through().await?).filter(|end| *end >= block))
        })
        .await?;
        let (end, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{CTX}: L1 does not cover block {block} after {}s",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "{CTX}: L1 covers block {end} after {}s",
            elapsed.as_secs()
        ));
        Ok(end)
    }

    /// Mine L1 blocks until the da-watcher publishes a new epoch. Anvil
    /// mines only for a transaction, and the stopped batcher sends none.
    /// The epoch takes a canonical slot after H, so the old chain's index
    /// ends past the seed's.
    async fn seal_an_epoch(&self) -> anyhow::Result<()> {
        let probes = &self.h.probes;
        let before = probes
            .aux_metric(DA_WATCHER_PORT, EPOCH_ORIGIN)
            .await
            .ok_or_else(|| crate::chaos_fail!("{CTX}: the da-watcher reports no epoch origin"))?;
        let l1 = &self.l1;
        let outcome = poll::until(Budget::secs(120, 3), |_| async move {
            l1.mine(1).await?;
            Ok(probes
                .aux_metric(DA_WATCHER_PORT, EPOCH_ORIGIN)
                .await
                .filter(|origin| *origin > before))
        })
        .await?;
        let (origin, _) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{CTX}: the da-watcher published no epoch past L1 block {before} in {}s",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "{CTX}: the da-watcher published epochs past L1 block {before}, through {origin}"
        ));
        Ok(())
    }

    /// Rebuild the state at H, the posted head, from L1: the executor
    /// image and the seed in one run, the validator's state in another.
    async fn rebuild(&self, old_head: u64) -> anyhow::Result<Seeded> {
        let head = self.l1.covered_through().await?;
        anyhow::ensure!(
            head < old_head,
            "{}: {CTX}: L1 covers block {head}, the old chain ends at {old_head}: nothing lies past the posted head",
            crate::FAIL_PREFIX
        );
        let evidence = evidence_dir(CTX, "chaos-sealer-wipe-")?;
        let seed = evidence.join("seed.bin");
        let validator = Rebuild {
            harness: self.h,
            evidence,
            target: unchecked_target(head),
            output: Output::ValidatorState,
            sealer_seed: None,
        };
        let (image, validator) = tokio::join!(
            install_rebuilt_image(self.h, CTX, self.fresh(), head, Some(seed.clone())),
            validator.run()
        );
        let seeded = Seeded {
            head: SeedHead::parse(&std::fs::read(&seed)?)?,
            image: image?,
            validator: validator?,
            seed,
        };
        seeded.check(head)?;
        crate::log(format!(
            "{CTX}: rebuilt block {head} (old head {old_head}): E_H {}, L1 origin {}",
            seeded.head.end_tx_idx, seeded.head.l1_origin
        ));
        Ok(seeded)
    }

    /// Wipe every copy of the old chain except the stale executor's state:
    /// the members' directories, every checkpoint, the validator's state,
    /// the batcher's spool and cursor, and the account cache. Then give
    /// each member the seed and the validator its rebuilt state.
    async fn wipe(&self, seeded: &Seeded) -> anyhow::Result<()> {
        for node in &self.sealers {
            self.seed_node(node, seeded).await?;
        }
        let checkpoints = format!("find {} -mindepth 1 -delete", StateOwner::CHECKPOINTS);
        wipe_dirs(self.h, self.stale()?, CTX, &checkpoints).await?;
        let owner = StateOwner::read(self.h, CTX, &self.aux, VALIDATOR_STATE).await?;
        wipe_dirs(self.h, &self.aux, CTX, &owner.wipe_script()).await?;
        install_image(self.h, CTX, &self.aux, &owner, &seeded.validator).await?;
        wipe_dirs(self.h, &self.aux, CTX, BATCHER_WIPE).await?;
        let primary = freeze_and_flush(self.h, CTX, &[]).await?;
        crate::log(format!(
            "{CTX}: old state wiped; account cache {primary} flushed"
        ));
        Ok(())
    }

    /// Wipe one member's directories and copy the seed to its node.
    async fn seed_node(&self, node: &str, seeded: &Seeded) -> anyhow::Result<()> {
        wipe_dirs(self.h, node, CTX, SEALER_WIPE).await?;
        let seed = seeded.seed.display().to_string();
        self.h
            .nodes
            .docker_ok(&["cp", &seed, &format!("{node}:{}", seed_path())])
            .await
            .map_err(|e| crate::chaos_fail!("{CTX}: could not copy the seed to {node}: {e}"))?;
        wipe_dirs(self.h, node, CTX, &format!("chmod 0644 {}", seed_path())).await
    }

    /// Start the members from the seed, inside the bootstrap. Every member
    /// must log `SEEDED` at H and none `FRESH`. After the first snapshot,
    /// start them again without the seed: each one restores the snapshot.
    async fn seed_members(&self, seeded: &Seeded) -> anyhow::Result<()> {
        let at_head = format!(
            "block={} endTx={} senders=",
            seeded.head.block, seeded.head.end_tx_idx
        );
        let base = SealerCounts::read(self.h, &at_head).await?;
        let job = JobDefinition(self.jobs.cluster.definition()).seeded(&seed_path())?;
        self.h
            .nomad
            .put_variable(BOOTSTRAP_VARIABLE, &json!({ "bootstrap": "true" }))
            .await?;
        let started = self.start_seeded(&job).await;
        let closed = self.h.nomad.delete_variable(BOOTSTRAP_VARIABLE).await;
        started?;
        closed?;
        let members = self.sealers.len();
        base.await_seeded(self.h, &at_head, members).await?;
        crate::log(format!(
            "{CTX}: every member seeded at block {} and took the first snapshot",
            seeded.head.block
        ));
        let seeded_lines = self.count(&at_head, CLUSTER_TASK).await?;
        self.jobs.cluster.restore().await?;
        base.await_restored(self.h, members).await?;
        anyhow::ensure!(
            self.count(&at_head, CLUSTER_TASK).await? == seeded_lines,
            "{}: {CTX}: a member started from the seed after the seed property was cleared",
            crate::FAIL_PREFIX
        );
        let leader = self
            .h
            .evidence
            .cluster_leader(FULL_RESTART_ELECTION)
            .await?;
        crate::log(format!(
            "{CTX}: members without the seed elected memberId={leader}"
        ));
        Ok(())
    }

    /// Register the seeded cluster job and wait for a leader.
    async fn start_seeded(&self, job: &serde_json::Value) -> anyhow::Result<()> {
        self.jobs.cluster.register(job).await?;
        let leader = self
            .h
            .evidence
            .cluster_leader(FULL_RESTART_ELECTION)
            .await?;
        crate::log(format!("{CTX}: seeded members elected memberId={leader}"));
        Ok(())
    }

    /// Start the executors. The fresh ones resume at `(E_H, H + 1)`; the
    /// stale one asks for a cursor past the head and gets `REPLAY_AHEAD`.
    /// Then the stale one gets the image too, and all of them resume.
    async fn resume_executors(&self, seeded: &Seeded) -> anyhow::Result<()> {
        let base = ResumeCounts::read(self.h, &seeded.head).await?;
        self.jobs.executor.restore().await?;
        base.await_resumed(self.h, &seeded.head, self.fresh().len())
            .await?;
        base.await_ahead(self.h).await?;
        base.assert_no_checkpoint(self.h).await?;
        self.jobs.executor.stop().await?;
        let stale = self.stale()?;
        let owner = wipe_node(self.h, CTX, stale).await?;
        install_image(self.h, CTX, stale, &owner, &seeded.image).await?;
        let resumed = self.count(RESUMED, "executor").await?;
        self.jobs.executor.restore().await?;
        self.wait_count("executor", RESUMED, resumed, self.executors.len())
            .await?;
        crate::log(format!(
            "{CTX}: all {} executors resume on the new chain",
            self.executors.len()
        ));
        Ok(())
    }

    /// Start the validator on its rebuilt state, the state mirrors, the
    /// sequencers, the da-watcher after the seed's L1 origin, the
    /// ingresses and the batcher. The sequencers come before the
    /// da-watcher: they read its epochs live, with no replay, so an epoch
    /// published before they subscribe never reaches the sealer.
    async fn resume_the_rest(&self, seeded: &Seeded) -> anyhow::Result<()> {
        self.jobs.validator.restore().await?;
        self.await_validator_past(seeded.head.block).await?;
        self.jobs.mirror.restore().await?;
        self.jobs.sequencer.restore().await?;
        self.start_da_watcher(seeded.head.l1_origin).await?;
        self.jobs.ingress.restore().await?;
        self.jobs.batcher.restore().await
    }

    /// Wait until the validator commits a block past `head`: it resumed
    /// on its rebuilt state, which ends at `head`.
    async fn await_validator_past(&self, head: u64) -> anyhow::Result<()> {
        let probes = &self.h.probes;
        let outcome = poll::until(Budget::secs(180, 3), |_| async move {
            Ok(probes
                .val_metric(VALIDATOR_COMMITTED)
                .await
                .filter(|block| u64::try_from(*block).is_ok_and(|block| block > head)))
        })
        .await?;
        let (block, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{CTX}: the validator committed no block past {head} on its rebuilt state within {}s",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "{CTX}: the validator resumed on its rebuilt state: block {block} after {}s",
            elapsed.as_secs()
        ));
        Ok(())
    }

    /// Start the da-watcher after L1 block `origin`, and require that its
    /// first ticks publish every finalized block after it. A watcher that
    /// started at the tip would have published only the blocks finalized
    /// since its start.
    async fn start_da_watcher(&self, origin: u64) -> anyhow::Result<()> {
        let job = JobDefinition(self.jobs.da_watcher.definition()).resumed_after(origin)?;
        self.jobs.da_watcher.register(&job).await?;
        let probes = &self.h.probes;
        let outcome = poll::until(Budget::secs(120, 3), |_| async move {
            let finalized = probes.aux_metric(DA_WATCHER_PORT, L1_FINALIZED).await;
            let published = probes.aux_metric(DA_WATCHER_PORT, EPOCHS_PUBLISHED).await;
            Ok(finalized.zip(published).filter(|(finalized, published)| {
                finalized
                    .checked_sub(*published)
                    .and_then(|start| u64::try_from(start).ok())
                    .is_some_and(|start| start <= origin)
            }))
        })
        .await?;
        let ((finalized, published), _) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{CTX}: the da-watcher did not publish every L1 block after {origin} within {}s",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "{CTX}: the da-watcher resumed after L1 block {origin}: {published} epochs through {finalized}"
        ));
        Ok(())
    }

    /// The batcher posts after H again, and L1's record has no gap and no
    /// overlap: the first new batch starts at H + 1.
    async fn assert_the_record_continues(&self, head: u64) -> anyhow::Result<()> {
        let next = head
            .checked_add(1)
            .ok_or_else(|| crate::chaos_fail!("{CTX}: block {head} overflows"))?;
        self.await_posted_through(next).await?;
        self.l1.assert_contiguous(CTX).await
    }

    async fn count(&self, needle: &str, job: &str) -> anyhow::Result<usize> {
        self.h
            .evidence
            .count_lines(job, needle, Streams::Both)
            .await
    }

    /// Wait until `job` logs `needle` `more` times past `baseline`.
    async fn wait_count(
        &self,
        job: &str,
        needle: &str,
        baseline: usize,
        more: usize,
    ) -> anyhow::Result<()> {
        let fail_msg = format!("{CTX}: {job} did not log '{needle}' {more} more time(s)");
        self.h
            .evidence
            .wait_count_reaches(
                &CountWait {
                    job,
                    needle,
                    baseline,
                    timeout: Duration::from_mins(3),
                    interval: Duration::from_secs(5),
                    streams: Streams::Both,
                    fail_msg: &fail_msg,
                },
                baseline + more,
            )
            .await
    }

    /// Start every job again from its saved definition, and close the
    /// bootstrap, after a failure. Best effort: the case already failed,
    /// and the next stage needs the jobs back to dump its diagnostics.
    async fn bring_back(&self) {
        if let Err(e) = self.h.nomad.delete_variable(BOOTSTRAP_VARIABLE).await {
            crate::log(format!("{CTX}: could not close the bootstrap: {e:#}"));
        }
        for job in self.jobs.all() {
            Self::restore_logged(job).await;
        }
    }

    async fn restore_logged(job: &SavedJob) {
        if let Err(e) = job.restore().await {
            crate::log(format!("{CTX}: could not start a job again: {e:#}"));
        }
    }
}

/// The seed's path on a sealer node.
fn seed_path() -> String {
    format!("{SEED_DIR}/seed.bin")
}

impl Seeded {
    /// The seed names H and an L1 origin, and both rebuilds end at the
    /// seed's cursor with one state root. The chain holds epochs long
    /// before this case, so its origin is never 0.
    fn check(&self, head: u64) -> anyhow::Result<()> {
        let end = Some(self.head.end_tx_idx);
        anyhow::ensure!(
            self.head.block == head
                && self.head.l1_origin > 0
                && self.image.end_tx_idx() == end
                && self.validator.end_tx_idx() == end
                && self.image.state_root().is_some()
                && self.image.state_root() == self.validator.state_root(),
            "{}: {CTX}: the seed ({:?}) and the rebuilds disagree at block {head}: image '{}', validator state '{}'",
            crate::FAIL_PREFIX,
            self.head,
            self.image.report,
            self.validator.report
        );
        Ok(())
    }
}
