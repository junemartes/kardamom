//! The recorded-cursor check of `hard-executor`: while one executor is
//! down, the sealer's best recorded cursor keeps moving. The sealer takes
//! the best cursor over the executors, so one dead executor does not stop
//! the floor of the record-lag guard.

use crate::harness::Harness;
use crate::metrics;
use crate::poll::{self, Budget};

/// The sealer's best recorded cursor, as the first ingress mirrors it.
/// -1 while no executor sent a cursor.
const RECORDED_HEAD: &str = "kardamom_ingress_cluster_recorded_head";

/// The canonical records past the best recorded cursor.
const RECORD_LAG: &str = "kardamom_ingress_cluster_record_lag";

/// The wait for a move of the best cursor, from the kill. Under the case
/// load the live executors send a cursor every 100 ms, so a move shows in
/// a few seconds. Nomad restarts the killed task only after 5 s, and the
/// restarted executor then waits for its recording and catches up.
const WINDOW_S: u64 = 20;

/// The recorded-cursor gauges of the first ingress.
#[derive(Debug, Clone, Copy)]
struct Cursor {
    head: i64,
    lag: i64,
}

impl Cursor {
    async fn read(h: &Harness) -> Option<Self> {
        let target = h.probes.ingress_target(&h.probes.ingresses[0]);
        let body = h.probes.scrape().fetch(&target).await?;
        Some(Self {
            head: metrics::first(&body, RECORDED_HEAD)?,
            lag: metrics::first(&body, RECORD_LAG)?,
        })
    }

    /// The canonical count at the sealer: the records at or below the
    /// cursor, plus the lag.
    fn count(self) -> i64 {
        // Gauge values stay far below i64::MAX; the saturation only keeps
        // the sum defined for a corrupt scrape.
        self.head.saturating_add(1).saturating_add(self.lag)
    }
}

/// The best recorded cursor before the injection. `None` when the
/// deployed cluster sends no cursor.
pub(crate) struct BestRecorded {
    before: Option<Cursor>,
}

impl BestRecorded {
    /// Read the cursor before the injection. With `KARDAMOM_EXEC_CURSOR`
    /// off, the executors send no cursor, and the check does not apply.
    ///
    /// # Errors
    ///
    /// Returns an error when the cursor is on, but the ingress shows no
    /// recorded cursor.
    pub(crate) async fn read(h: &Harness, ctx: &str) -> anyhow::Result<Self> {
        if !h.knobs.exec_cursor {
            crate::log(format!(
                "{ctx}: KARDAMOM_EXEC_CURSOR is off in the deployed cluster; the recorded-cursor check does not apply"
            ));
            return Ok(Self { before: None });
        }
        let before = Cursor::read(h).await.filter(|c| c.head >= 0).ok_or_else(|| {
            crate::chaos_fail!(
                "{ctx}: KARDAMOM_EXEC_CURSOR is on, but the ingress shows no recorded cursor ({RECORDED_HEAD})"
            )
        })?;
        Ok(Self {
            before: Some(before),
        })
    }

    /// After the kill, wait for the best cursor to pass its value before
    /// the injection. The check passes with a log line when the sealer
    /// ordered no record in the window.
    ///
    /// # Errors
    ///
    /// Returns an error when the sealer orders records while the best
    /// cursor stays flat.
    pub(crate) async fn assert_moves(&self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let Some(before) = self.before else {
            return Ok(());
        };
        let outcome = poll::until(Budget::secs(WINDOW_S, 1), |_| async move {
            Ok(Cursor::read(h).await.filter(|now| now.head > before.head))
        })
        .await?;
        if let poll::Outcome::Ready { value, elapsed } = outcome {
            crate::log(format!(
                "{ctx}: the best recorded cursor moves {} -> {} {}s after the kill, with one executor down",
                before.head,
                value.head,
                elapsed.as_secs()
            ));
            return Ok(());
        }
        let after = Cursor::read(h).await.unwrap_or(before);
        if after.count() > before.count() {
            return Err(crate::chaos_fail!(
                "{ctx}: the best recorded cursor stays at {} for {WINDOW_S}s while the sealer orders records ({} -> {})",
                before.head,
                before.count(),
                after.count()
            ));
        }
        crate::log(format!(
            "{ctx}: the sealer ordered no record in {WINDOW_S}s; the recorded-cursor check does not apply"
        ));
        Ok(())
    }
}
