use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256, U256};
use kardamom_state::{StateEnv, StateEnvBuilder, StateSnapshot, StateWriter, WriteBatch};
use kardamom_types::{AccountChange, BPosition, BlockBoundary, BlockDelta};

use super::RebuildSnapshot;

/// One state env of a one-account chain, seeded from a genesis TOML.
pub(super) struct Fixture {
    root: tempfile::TempDir,
    env: StateEnv,
    pub(super) address: Address,
}

impl Fixture {
    pub(super) fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(root.path().join("state"))
            .open()
            .unwrap();
        let address = Address::repeat_byte(0x42);
        let fixture = Self { root, env, address };
        let genesis = kardamom_state::load_genesis(&fixture.genesis_file("genesis", 100)).unwrap();
        kardamom_state::genesis::seed_genesis(&fixture.env, &genesis.to_alloc()).unwrap();
        fixture
    }

    /// Write a genesis TOML that gives the account `balance`, and return
    /// its path.
    fn genesis_file(&self, name: &str, balance: u64) -> PathBuf {
        let path = self.root.path().join(format!("{name}.toml"));
        let toml = format!(
            "chain_id = 1\n\n[[alloc]]\naddress = \"{}\"\nbalance = \"{balance}\"\n",
            self.address
        );
        std::fs::write(&path, toml).unwrap();
        path
    }

    /// The genesis TOML of this chain.
    pub(super) fn chain(&self) -> PathBuf {
        self.root.path().join("genesis.toml")
    }

    /// The genesis TOML of another chain.
    pub(super) fn foreign_chain(&self) -> PathBuf {
        self.genesis_file("foreign", 7)
    }

    /// Write a checkpoint of the committed state into `dir`, and return
    /// its path.
    pub(super) fn checkpoint(&self, dir: &Path) -> PathBuf {
        kardamom_state::create_checkpoint(&self.env, dir)
            .unwrap()
            .path
    }

    pub(super) fn commit(&self, count: u64) {
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
    assert_eq!(restored.rows(), vec![(fixture.address, 2, U256::from(98))]);
    assert!(restored.end.as_index() < 2, "the next receipt must win");
}

impl RebuildSnapshot {
    /// Every account row of the snapshot.
    pub(super) fn rows(&self) -> Vec<(Address, u64, U256)> {
        let mut rows = Vec::new();
        self.snapshot
            .for_each_account(|address, nonce, balance| {
                rows.push((address, nonce, balance));
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .unwrap();
        rows
    }
}
