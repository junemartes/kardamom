//! Egress recovery preserves the reference mark and advances both readers.

use super::Run;
use super::egress::{CURSORS, Egress};
use std::time::Duration;

const EGRESS_TASK: &str = "egress";

impl Run<'_> {
    pub(super) async fn egress_restart(&mut self) -> anyhow::Result<()> {
        let egress = Egress::of(self.h).await?;
        let victim = egress
            .instances
            .first()
            .ok_or_else(|| crate::chaos_fail!("{}: no egress instance", self.ctx))?;
        let before = victim.await_reference(self.h).await?;
        let cursors = CURSORS.read(self.h).await;
        let floors = cursors.map(|c| c.unwrap_or(0));
        self.h.inject_hard(&[&victim.node], EGRESS_TASK).await?;
        self.h
            .assert_count(
                EGRESS_TASK,
                egress.instances.len(),
                self.h.knobs.restart_slo,
            )
            .await?;
        let after = Egress::of(self.h)
            .await?
            .instances
            .iter()
            .find(|i| i.node == victim.node)
            .ok_or_else(|| {
                crate::chaos_fail!("{}: the egress did not return on {}", self.ctx, victim.node)
            })?
            .await_reference(self.h)
            .await?;
        anyhow::ensure!(
            after == before,
            "{}: {}: R of the egress on {} changed across its restart: {before:?} -> {after:?}",
            crate::FAIL_PREFIX,
            self.ctx,
            victim.node
        );
        CURSORS.await_past(self.h, floors).await?;
        self.h
            .assert_executor_progress(Duration::from_mins(1))
            .await
    }
}
