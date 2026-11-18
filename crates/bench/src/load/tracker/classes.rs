//! The tracker keeps one tally for each [`TxClass`]: the receipts, the
//! failed receipts, and the client latency. A mixed workload then shows
//! the latency of each class apart, and a failed swap stays visible next
//! to a clean transfer count.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use hdrhistogram::Histogram;

use super::{lock, quantiles};
use crate::load::config::ClassReport;
use crate::load::plan::TxClass;

/// The receipts, failed receipts, and latency of one class.
struct ClassTally {
    receipted: AtomicU64,
    /// The receipts with a status other than `0x1`.
    failed: AtomicU64,
    lat_us: Mutex<Histogram<u64>>,
}

impl ClassTally {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            receipted: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            lat_us: Mutex::new(crate::config::new_latency_hist()?),
        })
    }

    /// The report of this class, or `None` when no receipt of this class
    /// arrived.
    fn report(&self) -> Option<ClassReport> {
        let receipted = self.receipted.load(Ordering::Relaxed);
        if receipted == 0 {
            return None;
        }
        let h = lock(&self.lat_us);
        let q = quantiles(&h);
        Some(ClassReport {
            receipted,
            failed: self.failed.load(Ordering::Relaxed),
            lat_p50_us: q.p50,
            lat_p95_us: q.p95,
            lat_p99_us: q.p99,
            lat_max_us: h.max(),
        })
    }
}

/// One [`ClassTally`] for each [`TxClass`].
pub(super) struct ClassTallies {
    transfer: ClassTally,
    swap: ClassTally,
    call: ClassTally,
}

impl ClassTallies {
    pub(super) fn new() -> anyhow::Result<Self> {
        Ok(Self {
            transfer: ClassTally::new()?,
            swap: ClassTally::new()?,
            call: ClassTally::new()?,
        })
    }

    fn of(&self, class: TxClass) -> &ClassTally {
        match class {
            TxClass::Transfer => &self.transfer,
            TxClass::Swap => &self.swap,
            TxClass::Call => &self.call,
        }
    }

    /// Count one receipt of `class`, and record its latency `us`. The
    /// caller clamps `us` into the histogram bounds.
    pub(super) fn record(&self, class: TxClass, status: u64, us: u64) {
        let tally = self.of(class);
        tally.receipted.fetch_add(1, Ordering::Relaxed);
        if status != 1 {
            tally.failed.fetch_add(1, Ordering::Relaxed);
        }
        let _ = lock(&tally.lat_us).record(us);
    }

    /// The report of each class that got a receipt.
    pub(super) fn report(&self) -> BTreeMap<TxClass, ClassReport> {
        [TxClass::Transfer, TxClass::Swap, TxClass::Call]
            .into_iter()
            .filter_map(|class| Some((class, self.of(class).report()?)))
            .collect()
    }
}
