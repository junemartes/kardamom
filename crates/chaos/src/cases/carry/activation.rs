//! The activation steps: the versions, the provoked full reference below
//! version 5, the readers on the egress, the switch with a member down,
//! and the moves of the executors to carry mode.

use std::time::Duration;

use kardamom_types::upgrades::{FEATURE_CARRY_WINDOW, FEATURE_SEALER_V4, FEATURE_SEALER_V5};

use super::Run;
use super::egress::{BLOCKS, CURSORS, Egress};
use super::members::{
    ALL, DECISION_VERSION, FEED_LIVE, FEED_STATE, FULL_REF_BELOW_V5, Lanes, Members, REF_REFUSED,
    Window,
};
use super::upgrade::Upgrade;
use crate::accounts::ACCT_SHARD;
use crate::cases::cluster::a_follower;
use crate::cases::deploy::MANIFEST;
use crate::l1::L1_OWNER;
use crate::lifecycle::{CarrySwitches, EgressStage, LastAttempt, NOMAD_HTTP_PORT};
use crate::nomad::{SavedJob, Streams};
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;
use crate::rpc::Rpc;

/// The sender of the provoked full reference: a load-reserve account. A
/// chaos shard runs no load stage, so no other sender spends it.
const PROBE_ACCOUNT: u32 = 1;

/// How long the probe transfers may take to get their receipts.
const RECEIPT_WAIT: Duration = Duration::from_mins(1);

/// The line of an executor that reads the member feed of its node.
const FEED_LINE: &str = "canonical entries via the member feed";

/// How long a moved executor may take to read the member feed.
const FEED_WAIT: Budget = Budget {
    timeout: Duration::from_mins(3),
    interval: Duration::from_secs(5),
};

impl Run<'_> {
    /// Step 1: a lockbox, a deploy that names it, and features 3 and 4.
    pub(super) async fn versions_on(&mut self) -> anyhow::Result<()> {
        let lockbox = self.lockbox().await?;
        self.redeploy(|c| c.lockbox = Some(lockbox)).await?;
        self.version(Upgrade::on(FEATURE_SEALER_V4), 4).await?;
        self.version(Upgrade::on(FEATURE_SEALER_V5), 5).await?;
        Lanes::new(self.h).await_version(5).await
    }

    /// Step 2: a full reference that the sealer orders below version 5.
    pub(super) async fn full_ref_below_v5(&mut self) -> anyhow::Result<()> {
        let rpc = Rpc::new(&self.h.rpc_url, self.h.knobs.chain_id)?;
        let nonce = rpc.nonce_of(PROBE_ACCOUNT).await?;
        let next = nonce
            .checked_add(1)
            .ok_or_else(|| crate::chaos_fail!("{}: the probe nonce overflows", self.ctx))?;
        let held = rpc.sign_transfer(PROBE_ACCOUNT, next)?;
        let before = Members::new(self.h)?.counts(FULL_REF_BELOW_V5).await?;
        let off = Upgrade::off(FEATURE_SEALER_V5);
        off.check(self.h, self.ctx).await?;
        let parked = {
            let (rpc, raw) = (rpc.clone(), held.raw.clone());
            tokio::spawn(async move { rpc.send_raw(&raw).await })
        };
        let lane = ACCT_SHARD[PROBE_ACCOUNT as usize];
        Lanes::new(self.h).await_held(lane).await?;
        off.send(self.h, self.ctx).await?;
        Members::new(self.h)?
            .await_all(&ALL, DECISION_VERSION, |v| v == 4)
            .await?;
        rpc.transfer_hash(PROBE_ACCOUNT, nonce, RECEIPT_WAIT)
            .await?;
        rpc.await_receipt(&held.hash, RECEIPT_WAIT).await?;
        parked.abort();
        let after = Members::new(self.h)?.counts(FULL_REF_BELOW_V5).await?;
        anyhow::ensure!(
            after.iter().zip(&before).all(|(a, b)| a > b),
            "{}: {}: not every member ordered the held transfer as a full reference below version 5: {before:?} -> {after:?}",
            crate::FAIL_PREFIX,
            self.ctx
        );
        crate::log(format!(
            "{}: full references below version 5 {before:?} -> {after:?}",
            self.ctx
        ));
        self.version(Upgrade::on(FEATURE_SEALER_V5), 5).await?;
        Lanes::new(self.h).await_version(5).await
    }

    /// Step 3: the batcher and the validator move to the egress past R,
    /// with the version floor 5 on the sequencers. Under version 5 the
    /// v2 loop offers no reference without bytes, so R does not move.
    pub(super) async fn readers_on_egress(&mut self) -> anyhow::Result<()> {
        let r = Egress::of(self.h).await?.reference(self.h).await?;
        anyhow::ensure!(
            r.index > 0,
            "{}: {}: the egress stores no reference, so the move past R proves nothing",
            crate::FAIL_PREFIX,
            self.ctx
        );
        BLOCKS.await_past(self.h, [r.block, r.block]).await?;
        self.redeploy(|c| {
            c.egress = EgressStage::Readers;
            c.version_floor = Some(5);
        })
        .await?;
        CURSORS.await_past(self.h, [r.index, r.index]).await?;
        let now = Egress::of(self.h).await?.reference(self.h).await?;
        anyhow::ensure!(
            now == r,
            "{}: {}: R moved under version 5: {r:?} -> {now:?}",
            crate::FAIL_PREFIX,
            self.ctx
        );
        Ok(())
    }

    /// Step 4: a follower is down at the switch of feature 5, and starts
    /// after it. The check runs first: it refuses a member that is down.
    pub(super) async fn window_on(&mut self) -> anyhow::Result<()> {
        let on = Upgrade::on(FEATURE_CARRY_WINDOW);
        on.check(self.h, self.ctx).await?;
        let follower = a_follower(Members::new(self.h)?.leader().await?);
        let saved = SavedJob::capture(&self.h.nomad, CLUSTER_TASK).await?;
        let mut down = saved.definition().clone();
        Self::stop_group(&mut down, &format!("cluster-{follower}"))?;
        crate::log(format!(
            "{}: member {follower} stops before the switch",
            self.ctx
        ));
        saved.register(&down).await?;
        on.send(self.h, self.ctx).await?;
        let running: Vec<u32> = ALL.into_iter().filter(|m| *m != follower).collect();
        Members::new(self.h)?
            .await_carry_window(&running, true)
            .await?;
        crate::log(format!(
            "{}: member {follower} starts after the switch",
            self.ctx
        ));
        saved.restore().await?;
        let members = Members::new(self.h)?;
        members.await_window(Window::On, self.retention()?).await?;
        let refused = members.counts(REF_REFUSED).await?;
        anyhow::ensure!(
            refused.iter().all(|n| *n == 0),
            "{}: {}: a member refused a reference of this release: {refused:?}",
            crate::FAIL_PREFIX,
            self.ctx
        );
        Ok(())
    }

    /// Step 5: executor 0 moves to carry mode.
    pub(super) async fn executor_first(&mut self) -> anyhow::Result<()> {
        self.move_to_carry_mode(0).await
    }

    /// Step 6: executors 1 and 2 move, one at a time, then the smoke gate.
    pub(super) async fn executors_rest(&mut self) -> anyhow::Result<()> {
        self.move_to_carry_mode(1).await?;
        self.move_to_carry_mode(2).await?;
        self.h.smoke_gate(self.h.knobs.gate_account).await
    }

    /// Move executors `0..=id` to carry mode: the deploy rolls the group
    /// of `id`. Then executor `id` reads the member feed of its node, the
    /// feed serves it live, and the executors advance.
    async fn move_to_carry_mode(&mut self, id: u32) -> anyhow::Result<()> {
        self.redeploy(|c| c.carry_executors = (0..=id).collect())
            .await?;
        self.await_feed_line(id).await?;
        Members::new(self.h)?
            .await_all(&[id], FEED_STATE, |v| v == FEED_LIVE)
            .await?;
        self.h
            .assert_executor_progress(Duration::from_mins(1))
            .await
    }

    async fn await_feed_line(&self, id: u32) -> anyhow::Result<()> {
        let group = format!("executor-{id}");
        let (h, group_ref) = (&*self.h, &group);
        let outcome = poll::until(FEED_WAIT, |_| async move {
            let mut logs = String::new();
            for alloc in h.nomad.running_in_group("executor", group_ref).await? {
                logs.push_str(&h.nomad.alloc_logs(&alloc, Streams::Both).await?);
            }
            Ok(logs.contains(FEED_LINE).then_some(()))
        })
        .await?;
        outcome
            .or_fail(|t| {
                crate::chaos_fail!(
                    "{}: the group {group} logs no '{FEED_LINE}' within {}s",
                    self.ctx,
                    t.as_secs()
                )
            })
            .map(|_| ())
    }

    /// The lockbox of the in-cluster L1. The deploy CLI deploys it when
    /// the registry has none. The suite sends no deposit, so the owner is
    /// the minter and the oracle is the zero address.
    async fn lockbox(&self) -> anyhow::Result<String> {
        let chain = self.h.knobs.chain_id.to_string();
        let find = [
            "addresses",
            "--l2-chain-id",
            &chain,
            "--contract",
            "ETHLockbox",
            "--json",
        ];
        if let Some(lockbox) = Self::proxy_in(&self.h.deploy_cli(&find).await?)? {
            return Ok(lockbox);
        }
        self.h
            .deploy_cli(&[
                "deploy",
                "--private-key",
                "env:KARDAMOM_DEPLOY_OWNER_KEY",
                "ETHLockbox",
                "--l2-chain-id",
                &chain,
                "--l2-minter",
                L1_OWNER,
            ])
            .await?;
        Self::proxy_in(&self.h.deploy_cli(&find).await?)?.ok_or_else(|| {
            crate::chaos_fail!("{}: the deployed lockbox is not in the registry", self.ctx)
        })
    }

    /// The proxy of the first entry of an `addresses --json` listing.
    pub(super) fn proxy_in(listing: &str) -> anyhow::Result<Option<String>> {
        let entries: Vec<serde_json::Value> = serde_json::from_str(listing.trim())?;
        Ok(entries
            .first()
            .and_then(|e| e["proxy"].as_str())
            .map(str::to_string))
    }

    /// Set the count of the group `name` of the job `definition` to 0.
    pub(super) fn stop_group(definition: &mut serde_json::Value, name: &str) -> anyhow::Result<()> {
        let group = definition["TaskGroups"]
            .as_array_mut()
            .and_then(|groups| groups.iter_mut().find(|g| g["Name"] == name))
            .ok_or_else(|| crate::chaos_fail!("the job has no group {name}"))?;
        group["Count"] = 0.into();
        Ok(())
    }

    /// Send `upgrade` and wait until every member runs `version`.
    pub(super) async fn version(&self, upgrade: Upgrade, version: i64) -> anyhow::Result<()> {
        upgrade.apply(self.h, self.ctx).await?;
        Members::new(self.h)?
            .await_all(&ALL, DECISION_VERSION, |v| v == version)
            .await
    }

    /// Deploy again with the carry switches that `change` sets. Later
    /// deploys and the upgrade check keep them.
    async fn redeploy(&mut self, change: impl FnOnce(&mut CarrySwitches)) -> anyhow::Result<()> {
        let mut vars = self.h.lifecycle.vars().clone();
        change(&mut vars.carry);
        crate::log(format!("{}: deploy with {:?}", self.ctx, vars.carry));
        self.h.lifecycle = self.h.lifecycle.clone().with_vars(vars);
        let nomad_addr = self.h.contract.nomad_addr(NOMAD_HTTP_PORT)?;
        let deployed = self
            .h
            .lifecycle
            .deploy(&nomad_addr, MANIFEST, LastAttempt::Settled)
            .await?;
        anyhow::ensure!(
            deployed,
            "{}: {}: the deploy failed",
            crate::FAIL_PREFIX,
            self.ctx
        );
        Ok(())
    }

    /// The sealer retention of the shard, in frames.
    pub(super) fn retention(&self) -> anyhow::Result<i64> {
        let frames = self.h.knobs.cluster_retention.ok_or_else(|| {
            crate::chaos_fail!(
                "{}: the shard deploys no KARDAMOM_CLUSTER_RETENTION",
                self.ctx
            )
        })?;
        Ok(i64::try_from(frames.get())?)
    }
}
