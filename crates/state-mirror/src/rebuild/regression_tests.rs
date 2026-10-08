use alloy_primitives::{Address, B256, U256};
use kardamom_state::{StateEnv, StateEnvBuilder, StateSnapshot, StateWriter, WriteBatch};
use kardamom_types::{AccountChange, BPosition, BlockBoundary, BlockDelta};

use super::RebuildSnapshot;

struct Fixture {
    _root: tempfile::TempDir,
    env: StateEnv,
    address: Address,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(root.path().join("state"))
            .open()
            .unwrap();
        let address = Address::repeat_byte(0x42);
        kardamom_state::genesis::seed_genesis(
            &env,
            &[AccountChange {
                address,
                nonce: 0,
                balance: U256::from(100),
                code_hash: B256::ZERO,
            }],
            &[],
        )
        .unwrap();
        Self {
            _root: root,
            env,
            address,
        }
    }

    fn commit(&self, count: u64) {
        {
            let mut writer = StateWriter::spawn(self.env.clone()).unwrap();
            writer
                .delta_tx
                .send(WriteBatch::new(
                    BlockBoundary {
                        block_number: count,
                        end_tx_idx: BPosition::from_index(count),
                        l2_timestamp: count,
                        l1_origin: 0,
                        base_fee: 0,
                        gas_used: 0,
                    },
                    BlockDelta {
                        block_number: count,
                        accounts: vec![AccountChange {
                            address: self.address,
                            nonce: count,
                            balance: U256::from(100 - count),
                            code_hash: B256::ZERO,
                        }],
                        ..BlockDelta::default()
                    },
                ))
                .unwrap();
            writer.shutdown().unwrap();
        }
    }

    fn restore(&self, first_live: BPosition) -> Option<RebuildSnapshot> {
        RebuildSnapshot::new(StateSnapshot::open(&self.env).unwrap(), first_live).unwrap()
    }
}

#[test]
fn checkpoint_must_include_the_first_live_receipt() {
    let fixture = Fixture::new();
    fixture.commit(1);
    let first_live = BPosition::from_index(1);
    assert!(
        fixture.restore(first_live).is_none(),
        "one completed transaction excludes receipt at index one"
    );

    fixture.commit(2);
    let restored = fixture.restore(first_live).unwrap();
    assert_eq!(restored.end, first_live);
    let mut rows = Vec::new();
    restored
        .snapshot
        .for_each_account(|address, nonce, balance| {
            rows.push((address, nonce, balance));
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .unwrap();
    assert_eq!(rows, vec![(fixture.address, 2, U256::from(98))]);
    assert!(restored.end.as_index() < 2, "the next receipt must win");
}
