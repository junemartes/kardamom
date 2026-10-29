//! The reads of the egress and of its readers: R, the highest stored
//! reference of each egress instance, and the blocks and the cursors of
//! the batcher and the validator.

use std::cell::Cell;
use std::time::Duration;

use crate::cases::validator::COMMITTED;
use crate::harness::Harness;
use crate::metrics::{self, Target};
use crate::poll::{self, Budget};
use crate::probes::BATCHER_PORT;

/// The metrics port of an egress instance (`ports.egress_metrics` in
/// `group_vars/all.yml`).
const EGRESS_METRICS_PORT: u16 = 9013;
const REF_MAX_INDEX: &str = "kardamom_egress_ref_frame_max_index";
const REF_MAX_BLOCK: &str = "kardamom_egress_ref_frame_max_block";

/// How long a reader may take to pass a mark.
const WAIT: Budget = Budget {
    timeout: Duration::from_mins(3),
    interval: Duration::from_secs(2),
};

/// R: the canonical index and the block of the highest stored reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Reference {
    pub(super) index: i64,
    pub(super) block: i64,
}

/// One running egress instance.
pub(super) struct Instance {
    pub(super) node: String,
    target: Target,
}

impl Instance {
    /// R of this instance, or `None` when it does not answer.
    pub(super) async fn reference(&self, h: &Harness) -> Option<Reference> {
        let body = h.probes.scrape().fetch(&self.target).await?;
        Some(Reference {
            index: metrics::first(&body, REF_MAX_INDEX)?,
            block: metrics::first(&body, REF_MAX_BLOCK)?,
        })
    }

    /// Wait until this instance answers R again, after its restart.
    ///
    /// # Errors
    ///
    /// Returns an error when the wait ends first.
    pub(super) async fn await_reference(&self, h: &Harness) -> anyhow::Result<Reference> {
        let outcome = poll::until(WAIT, |_| async move { Ok(self.reference(h).await) }).await?;
        outcome
            .or_fail(|t| {
                crate::chaos_fail!(
                    "the egress on {} reports no reference mark within {}s",
                    self.node,
                    t.as_secs()
                )
            })
            .map(|(r, _)| r)
    }
}

/// The running egress instances.
pub(super) struct Egress {
    pub(super) instances: Vec<Instance>,
}

impl Egress {
    /// The instances that Nomad runs now.
    ///
    /// # Errors
    ///
    /// Returns an error when Nomad does not answer, or when an instance
    /// runs on a node that the contract does not name.
    pub(super) async fn of(h: &Harness) -> anyhow::Result<Self> {
        let instances = h
            .nomad
            .running("egress")
            .await?
            .iter()
            .map(|a| {
                let node = h.contract.node(&a.node_name)?;
                Ok(Instance {
                    node: node.container.clone(),
                    target: Target::bridged(node.ip, &node.container, EGRESS_METRICS_PORT),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(
            !instances.is_empty(),
            "{}: no egress instance runs",
            crate::FAIL_PREFIX
        );
        Ok(Self { instances })
    }

    /// The highest R of all instances.
    ///
    /// # Errors
    ///
    /// Returns an error when an instance does not answer: a missing
    /// instance could hold the highest R.
    pub(super) async fn reference(&self, h: &Harness) -> anyhow::Result<Reference> {
        let mut marks = Vec::with_capacity(self.instances.len());
        for instance in &self.instances {
            marks.push(instance.reference(h).await);
        }
        marks
            .iter()
            .copied()
            .collect::<Option<Vec<Reference>>>()
            .and_then(|marks| marks.into_iter().max())
            .ok_or_else(|| {
                crate::chaos_fail!("an egress instance reports no reference mark: {marks:?}")
            })
    }
}

/// One gauge of the batcher and one of the validator, on the aux node.
#[derive(Debug, Clone, Copy)]
pub(super) struct Readers {
    batcher: &'static str,
    validator: &'static str,
}

/// The posted block of the batcher and the committed block of the
/// validator.
pub(super) const BLOCKS: Readers = Readers {
    batcher: "kardamom_batcher_last_posted_block",
    validator: COMMITTED,
};

/// The canonical index of the newest frame that each reader took from the
/// egress.
pub(super) const CURSORS: Readers = Readers {
    batcher: "kardamom_batcher_egress_cursor_index",
    validator: "kardamom_validator_egress_cursor_index",
};

impl Readers {
    /// The two values, or `None` for a reader that does not answer.
    pub(super) async fn read(self, h: &Harness) -> [Option<i64>; 2] {
        [
            h.probes.aux_metric(BATCHER_PORT, self.batcher).await,
            h.probes.val_metric(self.validator).await,
        ]
    }

    /// Wait until the batcher value is above `floors[0]` and the
    /// validator value is above `floors[1]`.
    ///
    /// # Errors
    ///
    /// Returns an error when the wait ends first.
    pub(super) async fn await_past(self, h: &Harness, floors: [i64; 2]) -> anyhow::Result<()> {
        let last = Cell::new([None; 2]);
        let last_ref = &last;
        let outcome = poll::until(WAIT, |_| async move {
            let values = self.read(h).await;
            last_ref.set(values);
            let past = values
                .iter()
                .zip(floors)
                .all(|(v, floor)| v.is_some_and(|v| v > floor));
            Ok(past.then_some(()))
        })
        .await?;
        outcome
            .or_fail(|t| {
                crate::chaos_fail!(
                    "{} and {} are not past {floors:?} within {}s: {:?}",
                    self.batcher,
                    self.validator,
                    t.as_secs(),
                    last.get()
                )
            })
            .map(|_| ())
    }
}
