use std::time::Duration;

use alloy_primitives::B256;
use kardamom_state::{StateEnvBuilder, StateWriter, TrieMode, WriteBatch, seed_genesis};
use kardamom_types::upgrades::{CHAIN_STATE, CHAIN_STATE_CODE_HASH};
use kardamom_types::{AccountChange, BPosition, BlockBoundary, BlockDelta, GenesisAlloc};
use tempfile::TempDir;

use super::assert_chain_state_code;

struct StoredState {
    dir: TempDir,
}

impl StoredState {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(dir.path()).open().unwrap();
        seed_genesis(&env, &GenesisAlloc::default()).unwrap();
        Self { dir }
    }

    fn commit_upgrade_block(&self, code_hash: Option<B256>) {
        let env = StateEnvBuilder::new(self.dir.path()).open().unwrap();
        let mut writer = StateWriter::spawn_with_trie(env, TrieMode::Off).unwrap();
        let delta = BlockDelta {
            block_number: 1,
            accounts: code_hash
                .into_iter()
                .map(|code_hash| AccountChange {
                    address: CHAIN_STATE,
                    code_hash,
                    ..AccountChange::default()
                })
                .collect(),
            ..BlockDelta::default()
        };
        let boundary = BlockBoundary {
            block_number: 1,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1,
            l1_origin: 0,
            base_fee: 0,
            gas_used: 0,
        };
        writer
            .delta_tx
            .send(WriteBatch::new(boundary, delta))
            .unwrap();
        writer.shutdown().unwrap();
    }
}

struct StoredPair {
    executor: StoredState,
    validator: StoredState,
}

impl StoredPair {
    fn new() -> Self {
        Self {
            executor: StoredState::new(),
            validator: StoredState::new(),
        }
    }

    async fn assert_code(&self) -> anyhow::Result<()> {
        assert_chain_state_code(self.executor.dir.path(), self.validator.dir.path(), 1).await
    }

    async fn rejects_code(code_hash: Option<B256>) {
        Self::rejects_role(code_hash, Some(CHAIN_STATE_CODE_HASH), "executor").await;
        Self::rejects_role(Some(CHAIN_STATE_CODE_HASH), code_hash, "validator").await;
    }

    async fn rejects_role(executor: Option<B256>, validator: Option<B256>, role: &str) {
        let pair = Self::new();
        pair.executor.commit_upgrade_block(executor);
        pair.validator.commit_upgrade_block(validator);
        let error = tokio::time::timeout(Duration::from_secs(1), pair.assert_code())
            .await
            .expect("committed bad code must fail without waiting for another block")
            .unwrap_err();
        assert!(error.to_string().contains(role), "{error:#}");
        assert!(
            error.to_string().contains("not the canonical code"),
            "{error:#}"
        );
    }
}

#[tokio::test]
async fn the_executor_snapshot_waits_for_the_upgrade_commit() {
    let pair = StoredPair::new();
    pair.validator
        .commit_upgrade_block(Some(CHAIN_STATE_CODE_HASH));
    let assertion = pair.assert_code();
    tokio::pin!(assertion);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), assertion.as_mut())
            .await
            .is_err()
    );
    pair.executor
        .commit_upgrade_block(Some(CHAIN_STATE_CODE_HASH));
    tokio::time::timeout(Duration::from_secs(2), assertion)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn the_validator_snapshot_waits_for_the_upgrade_commit() {
    let pair = StoredPair::new();
    pair.executor
        .commit_upgrade_block(Some(CHAIN_STATE_CODE_HASH));
    let assertion = pair.assert_code();
    tokio::pin!(assertion);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), assertion.as_mut())
            .await
            .is_err()
    );
    pair.validator
        .commit_upgrade_block(Some(CHAIN_STATE_CODE_HASH));
    tokio::time::timeout(Duration::from_secs(2), assertion)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_committed_upgrade_with_missing_or_wrong_code_still_fails() {
    for code_hash in [None, Some(B256::ZERO)] {
        StoredPair::rejects_code(code_hash).await;
    }
}
