//! The carry mode from genesis on a new local chain
//! ([`crate::harness::CarryOptions::genesis`]).
//!
//! 1. Every member starts with the features 3, 4 and 5 at genesis. Each one
//!    reports decision version 5, the carry window and the genesis
//!    features, and each feature on. No member logs a switch of the
//!    decision version.
//! 2. A follower restarts under load before the first snapshot of the
//!    chain. It replays the log from genesis, and keeps the rules.
//! 3. Under load, the executor reads the member feed from its first block,
//!    and the validator agrees.
//! 4. Every member takes a periodic snapshot with the switch section. A
//!    follower restarts from its snapshot under load, and keeps the rules
//!    and the exact window.
//! 5. The L2 state holds the `KardamomChainState` code and the activation
//!    word of each feature, as the upgrade transactions write them on a
//!    live chain.
//! 6. Feature 5 turns off with a later activation, and on again: the live
//!    upgrade path works on a chain that started in carry mode. The words of
//!    features 3 and 4 stay, and the word of feature 5 is the new one.
//! 7. No member and no sequencer refuses a reference, no member logs a
//!    switch of the decision version, and the batcher posts the head.
//!
//! The test then stops the stack, and rebuilds the chain from L1 and the DA
//! proxy alone: the rebuilt root equals the root of the executor.

use std::time::Duration;

use alloy_primitives::{B256, U256};
use anyhow::{Context, Result};
use kardamom_types::StateDatabase;
use kardamom_types::upgrades::{
    CHAIN_STATE, CHAIN_STATE_CODE_HASH, GENESIS_ACTIVATION_MS, SEALER_FEATURES, activation_slot,
};

use super::reads::{self, FEATURE_ON, Gauge};
use super::{Params, Rollout};
use crate::harness::LocalStack;
use crate::harness::metrics::poll_until;
use crate::scenarios::metric_u64;

/// The `genesisFeatures` value of `/status` on every member.
const GENESIS_FEATURES: &str = "3,4,5";

/// `kardamom_sealer_snapshot_version` of a snapshot that holds the switch
/// section: the held activations of the genesis features.
const SWITCH_SNAPSHOT: u64 = 18;

/// The log line of a member that switches its decision version.
const VERSION_SWITCH_LINE: &str = "cluster DECISION-VERSION";

/// How long the first periodic snapshot of every member may take. It comes
/// after [`crate::harness::GENESIS_SNAPSHOT_INTERVAL_S`] of blocks.
const FIRST_SNAPSHOT_WAIT: Duration = Duration::from_mins(3);

const WAIT: Duration = Duration::from_mins(1);
const POLL: Duration = Duration::from_millis(250);

/// The `KardamomChainState` account in the state of the executor: its code
/// hash, and the activation word of each carry feature.
#[derive(Debug, PartialEq, Eq)]
struct ChainStateWords {
    code_hash: Option<B256>,
    words: Vec<U256>,
}

/// # Errors
/// Returns an error when a step fails. See the module doc.
pub async fn from_genesis(stack: &mut LocalStack, p: Params) -> Result<()> {
    anyhow::ensure!(
        stack.cfg.carry_genesis(),
        "the scenario needs CarryOptions::genesis"
    );
    let mut r = Rollout::new(stack, p)?;
    r.await_genesis_rules().await?;
    r.assert_no_version_switch()?;
    r.restart_before_the_first_snapshot().await?;
    r.under_load(async |r| {
        r.await_member_feed().await?;
        r.await_progress().await?;
        r.t.assert_validator_verdict("the carry mode from genesis")
            .await
    })
    .await?;
    r.restart_from_the_snapshot().await?;
    r.assert_genesis_words()?;
    r.window_off_and_on().await?;
    r.await_upgraded_window_word().await?;
    r.finish_from_genesis().await
}

impl Rollout<'_> {
    /// Wait until every member runs the genesis rules: decision version 5,
    /// the carry window, the genesis features, and each carry feature on.
    async fn await_genesis_rules(&self) -> Result<()> {
        poll_until("the genesis rules on every member", WAIT, POLL, || async {
            let statuses = self.members.statuses().await.ok();
            Ok(statuses
                .filter(|all| all.iter().all(Self::runs_genesis_rules))
                .map(|_| ()))
        })
        .await?;
        futures::future::try_join_all(SEALER_FEATURES.iter().map(|id| async move {
            let label = format!("feature=\"{id}\"");
            self.members
                .await_all(reads::FEATURE_STATE, &label, |v| v == FEATURE_ON)
                .await
        }))
        .await
        .map(|_| ())
    }

    /// Whether the `/status` of a member shows the genesis rules.
    fn runs_genesis_rules(status: &serde_json::Value) -> bool {
        status["decisionVersion"].as_u64() == Some(5)
            && status["carryWindow"].as_bool() == Some(true)
            && status["genesisFeatures"].as_str() == Some(GENESIS_FEATURES)
    }

    /// No sealer process of the stack logged a switch of the decision
    /// version: every member runs version 5 from genesis.
    fn assert_no_version_switch(&self) -> Result<()> {
        let switched: Vec<String> = self
            .stack
            .every_sealer_log()?
            .into_iter()
            .filter(|(_, text)| text.contains(VERSION_SWITCH_LINE))
            .map(|(name, _)| name)
            .collect();
        anyhow::ensure!(
            switched.is_empty(),
            "a member switched its decision version on a chain that starts at version 5: \
             {switched:?}"
        );
        Ok(())
    }

    /// Step 2: a follower restarts before the first snapshot of the chain.
    async fn restart_before_the_first_snapshot(&mut self) -> Result<()> {
        let follower = self.members.follower().await?;
        let snapshot = Gauge::of(self.members.admin(follower), reads::SNAPSHOT_VERSION)
            .read()
            .await?;
        anyhow::ensure!(
            snapshot == 0,
            "member {follower} took a snapshot before the restart (version {snapshot}): \
             raise GENESIS_SNAPSHOT_INTERVAL_S"
        );
        self.under_load(async |r| {
            r.stack.restart_sealer_member(follower)?;
            r.await_genesis_rules().await?;
            r.await_progress().await
        })
        .await
        .context("a restart before the first snapshot")
    }

    /// Step 4: every member takes a snapshot with the switch section, and a
    /// follower restarts from it.
    async fn restart_from_the_snapshot(&mut self) -> Result<()> {
        self.under_load(async |r| r.await_switch_snapshots().await)
            .await?;
        let follower = self.members.follower().await?;
        let retention = self.retention;
        self.under_load(async |r| {
            r.stack.restart_sealer_member(follower)?;
            r.await_genesis_rules().await?;
            r.members.await_exact_window(retention).await?;
            r.await_switch_snapshots().await?;
            r.await_progress().await
        })
        .await
        .context("a restart from the snapshot")
    }

    /// Wait until the newest snapshot of every member holds the switch
    /// section. A restarted member shows the version of the snapshot that
    /// it restored.
    async fn await_switch_snapshots(&self) -> Result<()> {
        poll_until(
            "a snapshot with the switch section on every member",
            FIRST_SNAPSHOT_WAIT,
            POLL,
            || async {
                let versions = self.members.values(reads::SNAPSHOT_VERSION, "").await.ok();
                Ok(versions
                    .filter(|all| all.iter().all(|v| *v == SWITCH_SNAPSHOT))
                    .map(|_| ()))
            },
        )
        .await
    }

    /// The `KardamomChainState` account in the live state of the executor.
    fn chain_state_words(&self) -> Result<ChainStateWords> {
        let dir = self
            .stack
            .executor_state_dir()
            .context("executor state dir")?;
        let env = crate::scenarios::open_state_ro(&dir)?;
        let snap = kardamom_state::StateSnapshot::open(&env).context("snapshot state")?;
        let code_hash = snap
            .basic(CHAIN_STATE)
            .context("read the chain-state account")?
            .map(|(_, _, hash)| hash);
        let words = SEALER_FEATURES
            .iter()
            .map(|id| {
                snap.storage(CHAIN_STATE, activation_slot(*id))
                    .context("read an activation word")
            })
            .collect::<Result<_>>()?;
        Ok(ChainStateWords { code_hash, words })
    }

    /// Step 5: the genesis words, before any upgrade transaction.
    fn assert_genesis_words(&self) -> Result<()> {
        let want = ChainStateWords {
            code_hash: Some(CHAIN_STATE_CODE_HASH),
            words: vec![U256::from(GENESIS_ACTIVATION_MS); SEALER_FEATURES.len()],
        };
        let got = self.chain_state_words()?;
        anyhow::ensure!(got == want, "the genesis words: {got:?}, want {want:?}");
        Ok(())
    }

    /// Step 6: after feature 5 turns on again, the words of features 3 and
    /// 4 keep their genesis value, and the word of feature 5 holds the time
    /// of the block of the last upgrade transaction. The executor commits
    /// that block after it publishes the receipt, so the read waits.
    async fn await_upgraded_window_word(&self) -> Result<()> {
        let genesis = U256::from(GENESIS_ACTIVATION_MS);
        let upgraded =
            |got: &ChainStateWords| got.words[..2] == [genesis, genesis] && got.words[2] > genesis;
        poll_until(
            "the word of feature 5 after the upgrade transactions",
            WAIT,
            POLL,
            || async { Ok(Some(self.chain_state_words()?).filter(upgraded).map(|_| ())) },
        )
        .await
    }

    /// Step 7: no refusal anywhere, no version switch, the exact window,
    /// the validator agrees, and the batcher posts the head of the
    /// executor.
    async fn finish_from_genesis(&mut self) -> Result<()> {
        self.rounds(3).await?;
        self.members.await_exact_window(self.retention).await?;
        self.assert_no_refusal().await?;
        let refused = self
            .t
            .sequencer_metric_values_where(reads::SEQ_REF_REFUSED, "")
            .await?
            .into_iter()
            .map(metric_u64)
            .collect::<Result<Vec<_>>>()?;
        anyhow::ensure!(
            refused.iter().all(|v| *v == 0),
            "a sequencer got a refusal on a chain at version 5 from genesis: {refused:?}"
        );
        self.assert_no_version_switch()?;
        self.t
            .assert_validator_verdict("the carry mode from genesis")
            .await?;
        let head = self.block().await?;
        Gauge::of(self.batcher, reads::BATCHER_POSTED)
            .await_above(head)
            .await
            .context("the batcher posts the head of the executor")?;
        let load = self.load.as_ref().context("the load runs")?;
        eprintln!(
            "carry from genesis: {} transfers landed, {} resends",
            load.landed, load.resends
        );
        Ok(())
    }
}
