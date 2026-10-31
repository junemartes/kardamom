//! The activation steps: the lockbox, the versions, a parked offer across
//! a downgrade, the readers on the egress, the switch with a member down,
//! and the moves of the executors to carry mode.

use std::time::Duration;

use kardamom_types::upgrades::{FEATURE_CARRY_WINDOW, FEATURE_SEALER_V4, FEATURE_SEALER_V5};

use super::Run;
use super::egress::{BLOCKS, CURSORS, Egress};
use super::members::{
    ALL, DECISION_VERSION, FEED_LIVE, FEED_STATE, Lanes, Members, REF_REFUSED, Window,
};
use super::patch::{Edit, JobPatch};
use super::upgrade::Upgrade;
use crate::accounts::ACCT_SHARD;
use crate::cases::cluster::a_follower;
use crate::l1::L1_OWNER;
use crate::lifecycle::{CarrySwitches, EgressStage};
use crate::nomad::{SavedJob, Streams};
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;
use crate::rpc::{Rpc, Signed};

/// The sender of the downgrade probe: a load-reserve account. A
/// chaos shard runs no load stage, so no other sender spends it.
const PROBE_ACCOUNT: u32 = 1;

/// The lane of [`PROBE_ACCOUNT`] at shard map version 0.
const PROBE_LANE: u8 = ACCT_SHARD[PROBE_ACCOUNT as usize];

/// How long the probe transfers may take to get their receipts.
const RECEIPT_WAIT: Duration = Duration::from_mins(1);

/// The line of an executor that reads the member feed of its node.
const FEED_LINE: &str = "canonical entries via the member feed";

/// How long a moved executor may take to read the member feed.
const FEED_WAIT: Budget = Budget {
    timeout: Duration::from_mins(2),
    interval: Duration::from_secs(5),
};

/// An arg that the task of each job passes in every deploy, next to
/// which a switch adds its args.
const BATCHER_MARKER: &str = "--blocks-per-batch";
const VALIDATOR_MARKER: &str = "--trie-shadow-check";
const SEQUENCER_MARKER: &str = "--tx-ttl-ms";
const EXECUTOR_MARKER: &str = "--metrics-addr";

/// The job meta of a reader of the egress.
const READS_EGRESS: &str = "reads_egress";

impl Run<'_> {
    /// A lockbox on L1, and the L1 follower and the validator read it.
    /// The two jobs do not depend on each other, so they roll at once.
    async fn lockbox_on(&mut self) -> anyhow::Result<()> {
        let lockbox = self.lockbox().await?;
        let [indexer, validator] = ["l1-indexer", "validator"].map(|job| {
            JobPatch::new(job).edit(Edit::Value {
                flag: "--lockbox",
                value: lockbox.clone(),
            })
        });
        crate::log(format!(
            "{}: the follower and the validator read the lockbox {lockbox}",
            self.ctx
        ));
        let (follower_read, validator_read) = tokio::join!(
            indexer.register(self.h, self.ctx),
            validator.register(self.h, self.ctx)
        );
        follower_read?;
        validator_read?;
        self.switch(|c| c.lockbox = Some(lockbox.clone()), &[])
            .await?;
        self.h
            .assert_executor_progress(Duration::from_mins(1))
            .await
    }

    /// The lockbox, then features 3 and 4. Every member runs decision
    /// version 5, and every sequencer keeps it. The check of feature 4
    /// reads the release of each member and service, which the switch to
    /// version 4 does not change, so it runs while the members switch.
    pub(super) async fn versions_on(&mut self) -> anyhow::Result<()> {
        self.lockbox_on().await?;
        let five = Upgrade::on(FEATURE_SEALER_V5);
        Upgrade::on(FEATURE_SEALER_V4)
            .apply(self.h, self.ctx)
            .await?;
        let members = Members::new(self.h)?;
        let (switched, checked) = tokio::join!(
            members.await_all(&ALL, DECISION_VERSION, |v| v == 4),
            five.check(self.h, self.ctx)
        );
        switched?;
        checked?;
        self.sent(five, 5).await?;
        Lanes::new(self.h).await_version(5).await
    }

    /// A parked offer uses the version at release after a
    /// downgrade. The v2 loop holds the transfer for its nonce gap for at
    /// most the transaction lifetime (30 s), and less under load. So the
    /// transfer parks after the check, while the lane keeps version 5,
    /// and the transaction that turns feature 4 off follows at once. The
    /// hold lasts until every member runs version 4. The check of the
    /// transaction that turns feature 4 on again runs during the hold:
    /// the downgrade does not change the release that it reads.
    pub(super) async fn held_offer_after_downgrade(&mut self) -> anyhow::Result<()> {
        let rpc = Rpc::new(&self.h.rpc_url, self.h.knobs.chain_id)?;
        let nonce = rpc.nonce_of(PROBE_ACCOUNT).await?;
        let next = nonce
            .checked_add(1)
            .ok_or_else(|| crate::chaos_fail!("{}: the probe nonce overflows", self.ctx))?;
        let held = rpc.sign_transfer(PROBE_ACCOUNT, next)?;
        let off = Upgrade::off(FEATURE_SEALER_V5);
        let on = Upgrade::on(FEATURE_SEALER_V5);
        off.check(self.h, self.ctx).await?;
        let parked = {
            let (rpc, raw) = (rpc.clone(), held.raw.clone());
            tokio::spawn(async move { rpc.send_raw(&raw).await })
        };
        let (released, checked) = tokio::join!(
            self.release_after_downgrade(off, &rpc, nonce, &held),
            on.check(self.h, self.ctx)
        );
        parked.abort();
        released?;
        checked?;
        let expected = next
            .checked_add(1)
            .ok_or_else(|| crate::chaos_fail!("{}: the probe nonce overflows", self.ctx))?;
        let after = rpc.nonce_of(PROBE_ACCOUNT).await?;
        anyhow::ensure!(
            after == expected,
            "{}: {}: the downgrade stranded the probe sender: nonce {after}, expected {expected}",
            crate::FAIL_PREFIX,
            self.ctx
        );
        crate::log(format!(
            "{}: both probe transactions executed; sender nonce {after}",
            self.ctx
        ));
        self.sent(on, 5).await?;
        Lanes::new(self.h).await_version(5).await
    }

    /// The lane holds the parked transfer under version 5, feature 4
    /// turns off, and every member and lane reaches version 4 while the
    /// lane still holds it. Then nonce `n` releases the held offer, and
    /// both transfers get their receipts.
    async fn release_after_downgrade(
        &self,
        off: Upgrade,
        rpc: &Rpc,
        nonce: u64,
        held: &Signed,
    ) -> anyhow::Result<()> {
        self.hold_under_version_five().await?;
        off.send(self.h, self.ctx).await?;
        Members::new(self.h)?
            .await_all(&ALL, DECISION_VERSION, |v| v == 4)
            .await?;
        Lanes::new(self.h).await_version(4).await?;
        anyhow::ensure!(
            Lanes::new(self.h).holds(PROBE_LANE).await,
            "{}: {}: the park expired: lane {PROBE_LANE} holds no transfer when every member runs version 4",
            crate::FAIL_PREFIX,
            self.ctx
        );
        rpc.transfer_hash(PROBE_ACCOUNT, nonce, RECEIPT_WAIT)
            .await?;
        rpc.await_receipt(&held.hash, RECEIPT_WAIT).await
    }

    /// Wait until the lane of the probe holds its transfer, and require
    /// that the lane still keeps version 5: the v2 loop then took the
    /// bytes of the transfer.
    async fn hold_under_version_five(&self) -> anyhow::Result<()> {
        let lanes = Lanes::new(self.h);
        lanes.await_held(PROBE_LANE).await?;
        anyhow::ensure!(
            lanes.keeps(PROBE_LANE, 5).await,
            "{}: {}: lane {PROBE_LANE} left version 5 before it held the transfer, so the transfer carries no bytes",
            crate::FAIL_PREFIX,
            self.ctx
        );
        Ok(())
    }

    /// The batcher and the validator move to the egress past R,
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
        let url = JobPatch::egress_url(self.h).await?;
        let egress = |marker| Edit::Append {
            marker,
            args: vec!["--egress".into(), url.clone()],
        };
        let patches = [
            JobPatch::new("validator")
                .meta(READS_EGRESS)
                .edit(egress(VALIDATOR_MARKER)),
            JobPatch::new("batcher")
                .meta(READS_EGRESS)
                .edit(egress(BATCHER_MARKER)),
            JobPatch::new("sequencer").edit(Edit::Append {
                marker: SEQUENCER_MARKER,
                args: vec!["--sealer-version-floor".into(), "5".into()],
            }),
        ];
        self.switch(
            |c| {
                c.egress = EgressStage::Readers;
                c.version_floor = Some(5);
            },
            &patches,
        )
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

    /// A follower is down at the switch of feature 5, and starts
    /// after it. The check runs first: it refuses a member that is down.
    /// The member starts again on every path, so a failure does not leave
    /// it stopped.
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
        let switched = match saved.register(&down).await {
            Ok(()) => self.switch_with_member_down(on, follower).await,
            Err(error) => Err(error),
        };
        crate::log(format!(
            "{}: member {follower} starts after the switch",
            self.ctx
        ));
        let restored = saved.restore().await;
        switched?;
        restored?;
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

    /// Send `on` while member `follower` is down, and wait until every
    /// running member switches.
    async fn switch_with_member_down(&self, on: Upgrade, follower: u32) -> anyhow::Result<()> {
        on.send(self.h, self.ctx).await?;
        let running: Vec<u32> = ALL.into_iter().filter(|m| *m != follower).collect();
        Members::new(self.h)?
            .await_carry_window(&running, true)
            .await
    }

    /// Executor 0 moves to carry mode.
    pub(super) async fn executor_first(&mut self) -> anyhow::Result<()> {
        self.move_to_carry_mode(0).await
    }

    /// Executors 1 and 2 move to carry mode, one at a time, and the
    /// smoke gate runs with every executor in carry mode.
    pub(super) async fn executors_rest(&mut self) -> anyhow::Result<()> {
        self.move_to_carry_mode(1).await?;
        self.move_to_carry_mode(2).await?;
        self.h.smoke_gate(self.h.knobs.gate_account).await
    }

    /// Move executors `0..=id` to carry mode: the group of `id` rolls
    /// with `--carry-bytes` and the egress. Then executor `id` reads the
    /// member feed of its node, the feed serves it live, and the
    /// executors advance.
    async fn move_to_carry_mode(&mut self, id: u32) -> anyhow::Result<()> {
        let url = JobPatch::egress_url(self.h).await?;
        let patch = JobPatch::new("executor")
            .in_group(format!("executor-{id}"))
            .edit(Edit::Append {
                marker: EXECUTOR_MARKER,
                args: vec!["--carry-bytes".into(), "--egress".into(), url],
            });
        self.switch(|c| c.carry_executors = (0..=id).collect(), &[patch])
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
    /// the registry has none. The owner mints the test deposits, and the
    /// oracle is the zero address.
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

    /// Send `upgrade`, which passed its check, and wait until every member
    /// runs `version`.
    async fn sent(&self, upgrade: Upgrade, version: i64) -> anyhow::Result<()> {
        upgrade.send(self.h, self.ctx).await?;
        Members::new(self.h)?
            .await_all(&ALL, DECISION_VERSION, |v| v == version)
            .await
    }

    /// Turn on the carry switches that `change` sets: register each of
    /// `patches`, the targeted change of a job that the switches render,
    /// in order. Later patches and the upgrade check keep the switches.
    async fn switch(
        &mut self,
        change: impl FnOnce(&mut CarrySwitches),
        patches: &[JobPatch],
    ) -> anyhow::Result<()> {
        let mut vars = self.h.lifecycle.vars().clone();
        change(&mut vars.carry);
        crate::log(format!("{}: switch to {:?}", self.ctx, vars.carry));
        for patch in patches {
            patch.register(self.h, self.ctx).await?;
        }
        self.h.lifecycle = self.h.lifecycle.clone().with_vars(vars);
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
