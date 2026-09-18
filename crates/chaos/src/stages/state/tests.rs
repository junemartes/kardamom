use super::*;
use alloy_primitives::{Address, B256, U256};
use kardamom_state::{StateWriter, TrieMode, WriteBatch};
use kardamom_types::{AccountChange, BPosition, BlockBoundary, BlockDelta, Receipt};

struct Fixture {
    directory: tempfile::TempDir,
    copies: StateCopies,
}

impl Fixture {
    fn new(balance: u64) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let validator = directory.path().join("validator");
        let executors = vec![
            directory.path().join("executor-0"),
            directory.path().join("executor-1"),
        ];
        Self::database(&validator, 10);
        for path in &executors {
            Self::database(path, balance);
        }
        Self {
            directory,
            copies: StateCopies {
                validator,
                executors,
            },
        }
    }

    fn database(path: &Path, balance: u64) {
        let env = StateEnvBuilder::new(path).open().unwrap();
        kardamom_state::genesis::seed_genesis(&env, &[], &[]).unwrap();
        let mut writer = StateWriter::spawn_with_trie(env, TrieMode::Incremental).unwrap();
        let boundary = BlockBoundary {
            block_number: 1,
            end_tx_idx: BPosition::from_index(1),
            l2_timestamp: 1,
            l1_origin: 0,
        };
        let delta = BlockDelta {
            block_number: 1,
            accounts: vec![AccountChange {
                address: Address::repeat_byte(1),
                nonce: 1,
                balance: U256::from(balance),
                code_hash: B256::ZERO,
            }],
            receipts: vec![Receipt {
                tx_idx: BPosition::from_index(1),
                tx_hash: B256::repeat_byte(2),
                status: true,
                gas_used: 21_000,
                ..Default::default()
            }],
            storage: vec![],
            code: vec![],
        };
        writer
            .delta_tx
            .send(WriteBatch::new(boundary, delta))
            .unwrap();
        writer.shutdown().unwrap();
    }
}

#[test]
fn identical_executed_state_passes() {
    Fixture::new(10).copies.verify().unwrap();
}

#[test]
fn equal_height_and_receipts_do_not_hide_wrong_balances() {
    let fixture = Fixture::new(9);
    let error = fixture.copies.verify().unwrap_err().to_string();
    assert!(error.contains("differs from validator"), "{error}");
}

#[test]
fn every_executor_is_checked_and_missing_state_fails() {
    let mut fixture = Fixture::new(10);
    fixture.copies.executors[1] = fixture.directory.path().join("missing");
    assert!(
        fixture
            .copies
            .verify()
            .unwrap_err()
            .to_string()
            .contains("missing database")
    );
    fixture.copies.executors.clear();
    assert!(fixture.copies.verify().is_err());
}

#[test]
fn heads_align_only_on_one_nonzero_head_from_every_consumer() {
    let aligned = Heads {
        executors: vec![Some(870), Some(870), Some(870)],
        validator: Some(870),
    };
    assert_eq!(aligned.aligned(), Some(870));
    let behind = Heads {
        executors: vec![Some(870), Some(869), Some(870)],
        validator: Some(870),
    };
    assert_eq!(behind.aligned(), None);
    let unreachable = Heads {
        executors: vec![Some(870), None, Some(870)],
        validator: Some(870),
    };
    assert_eq!(unreachable.aligned(), None);
    assert_eq!(
        unreachable.to_string(),
        "executors=[870, unreachable, 870] validator=870"
    );
    let idle = Heads {
        executors: vec![Some(0)],
        validator: Some(0),
    };
    assert_eq!(idle.aligned(), None);
}
