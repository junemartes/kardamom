//! The faults around the switch: feature 5 off and on, a leader that dies
//! right after the switch, and the restart of an egress instance.

use std::time::Duration;

use kardamom_types::upgrades::FEATURE_CARRY_WINDOW;

use super::Run;
use super::egress::{CURSORS, Egress};
use super::members::{Members, Window};
use super::upgrade::Upgrade;
use crate::cases::cluster::sealer;
use crate::inject::Killed;
use crate::probes::CLUSTER_TASK;

/// The task of an egress instance.
const EGRESS_TASK: &str = "egress";

/// The latest kill of the leader after its switch.
const KILL_AFTER_SWITCH: Duration = Duration::from_secs(2);

impl Run<'_> {
    /// Feature 5 off, then on again, under load.
    pub(super) async fn window_off_on(&mut self) -> anyhow::Result<()> {
        let on = self.off_then_check_on().await?;
        self.h
            .assert_executor_progress(Duration::from_mins(1))
            .await?;
        on.send(self.h, self.ctx).await?;
        Members::new(self.h)?
            .await_window(Window::On, self.retention()?)
            .await
    }

    /// Feature 5 off, then on, and the leader dies within 2 s after its
    /// switch. The container of the leader is known before the
    /// transaction, so the kill is one call after the switch.
    pub(super) async fn leader_kill_at_switch(&mut self) -> anyhow::Result<()> {
        let on = self.off_then_check_on().await?;
        let members = Members::new(self.h)?;
        let leader = members.leader().await?;
        let node = sealer(self.h, leader)?;
        let cid = self
            .h
            .nodes
            .inner_cid(&node, CLUSTER_TASK)
            .await
            .ok_or_else(|| crate::chaos_fail!("{}: no cluster container on {node}", self.ctx))?;
        on.send(self.h, self.ctx).await?;
        let switched = members.await_switch(leader).await?;
        self.h.nodes.inner_kill(&node, &cid).await?;
        let late = switched.elapsed();
        crate::log(format!(
            "{}: leader {leader} on {node} killed {}ms after its switch",
            self.ctx,
            late.as_millis()
        ));
        anyhow::ensure!(
            late <= KILL_AFTER_SWITCH,
            "{}: {}: the kill came {}ms after the switch, past {}ms",
            crate::FAIL_PREFIX,
            self.ctx,
            late.as_millis(),
            KILL_AFTER_SWITCH.as_millis()
        );
        self.h.killed = Some(Killed::Inner {
            node,
            task: CLUSTER_TASK.to_string(),
            cid,
        });
        self.h
            .assert_executor_progress(Duration::from_mins(1))
            .await?;
        self.h
            .assert_count(CLUSTER_TASK, 3, self.h.knobs.restart_slo)
            .await?;
        Members::new(self.h)?
            .await_window(Window::On, self.retention()?)
            .await
    }

    /// One egress instance restarts. It reads R from its mark, so
    /// R is the same after the restart, and the readers keep reading.
    pub(super) async fn egress_restart(&mut self) -> anyhow::Result<()> {
        let egress = Egress::of(self.h).await?;
        let victim = egress
            .instances
            .first()
            .ok_or_else(|| crate::chaos_fail!("{}: no egress instance", self.ctx))?;
        let before = victim.await_reference(self.h).await?;
        let cursors = CURSORS.read(self.h).await;
        let [Some(batcher), Some(validator)] = cursors else {
            return Err(crate::chaos_fail!(
                "{}: a reader cursor does not answer before the restart: {cursors:?}",
                self.ctx
            ));
        };
        let floors = [batcher, validator];
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

    /// Check and send feature 5 off, and check the transaction that
    /// turns it on again while the members switch off: the switch does
    /// not change the release that the check reads. Returns the checked
    /// transaction.
    async fn off_then_check_on(&self) -> anyhow::Result<Upgrade> {
        let on = Upgrade::on(FEATURE_CARRY_WINDOW);
        Upgrade::off(FEATURE_CARRY_WINDOW)
            .apply(self.h, self.ctx)
            .await?;
        let members = Members::new(self.h)?;
        let (off, checked) = tokio::join!(
            members.await_window(Window::Off, self.retention()?),
            on.check(self.h, self.ctx)
        );
        off?;
        checked?;
        Ok(on)
    }
}
