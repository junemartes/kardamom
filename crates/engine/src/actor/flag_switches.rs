//! A test chain that turns `FEATURE_SEALER_V3` on, off and on again with
//! `setFeature` deposits in the middle of blocks, for the block drivers of
//! other crates (the Block-STM strategy, the validator's parallel path and
//! the stateless re-execution).
//!
//! [`FlagSwitches::run`] runs the blocks one by one. Each block freezes
//! its flags from the state after the block before it
//! ([`BlockFeatures::read`]), so a deposit changes the flags from the next
//! block. The sequential driver is the reference: the driver under test
//! must give the same receipts and writes in every block. Each on-phase
//! has a burning skip, and the expected burns are
//! [`FlagSwitches::EXPECTED_BURNS`].

use alloy_primitives::{Address, U256, address, keccak256};
use kardamom_types::epoch::{LockboxLog, UpgradeLog, derive_epoch};
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};
use kardamom_types::{BPosition, BlockBoundaryStart, Deposit, Receipt};
use revm::primitives::KECCAK_EMPTY;

use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::exec_types::TxIndex;
use crate::features::BlockFeatures;
use crate::state::MockStateDatabase;
use crate::stateless::{BlockExecOutput, BufferedRecord, execute_block};

use super::fixtures::{LegacyTx, seeded_signer};

const PAYEE: Address = address!("00000000000000000000000000000000000E0003");
/// One transfer costs at most 2.1e15 wei at this gas price.
const GAS_PRICE: u128 = 100_000_000_000;
/// The poor sender pays one transfer of [`SPEND`], and skips a transfer of
/// [`TOO_MUCH`].
const POOR_FUNDS: u128 = 10u128.pow(16);
const SPEND: u64 = 1_000;
const TOO_MUCH: u64 = 10u64.pow(17);
/// An activation time that every block reaches, and one that no block
/// reaches (the off switch).
const NOW: u64 = 1;
const NEVER: u64 = 10u64.pow(12);
/// Block `k` executes at `STEP_MS * (k - 1)`: block 1 at the genesis
/// time 0.
const STEP_MS: u64 = 1_000;

/// One record of a test block.
#[derive(Clone, Copy)]
enum Item {
    /// A `setFeature` deposit of `FEATURE_SEALER_V3` at this time.
    Schedule(u64),
    /// A transfer of the poor sender at `nonce`, of `value`.
    Poor { nonce: u64, value: u64 },
    /// A transfer of the rich sender at a nonce.
    Rich(u64),
}

const fn skip(nonce: u64) -> Item {
    Item::Poor {
        nonce,
        value: TOO_MUCH,
    }
}

/// The blocks. The comment of each block names its frozen flag.
const BLOCKS: [&[Item]; 4] = [
    // Off (block 1 executes at 0). The deposit turns the flag on from
    // block 2; the skip after it burns nothing.
    &[Item::Rich(0), Item::Schedule(NOW), skip(0), Item::Rich(1)],
    // On: the skip burns nonce 0. The deposit turns the flag off from
    // block 3; the skip after it still burns.
    &[skip(0), Item::Rich(2), Item::Schedule(NEVER), skip(1)],
    // Off: no skip burns. The deposit turns the flag on from block 4.
    &[skip(2), Item::Schedule(NOW), skip(2), Item::Rich(3)],
    // On: the skip burns nonce 2, and nonce 3 lands.
    &[
        skip(2),
        Item::Poor {
            nonce: 3,
            value: SPEND,
        },
        Item::Rich(4),
    ],
];

/// The chain state before block 1, and the records of each block.
pub struct FlagSwitches {
    snap: MockStateDatabase,
    blocks: Vec<Vec<BufferedRecord>>,
}

impl Default for FlagSwitches {
    fn default() -> Self {
        Self::new()
    }
}

impl FlagSwitches {
    /// For each block, whether each receipt burns a nonce.
    pub const EXPECTED_BURNS: [&[bool]; 4] = [
        &[false, false, false, false],
        &[true, false, false, true],
        &[false, false, false, false],
        &[true, false, false],
    ];

    /// The genesis state: the chain-state predeploy of the dev genesis,
    /// a poor sender and a rich sender.
    ///
    /// # Panics
    ///
    /// Panics if the dev genesis does not parse or has no predeploy.
    #[must_use]
    pub fn new() -> Self {
        let poor = seeded_signer(0x683);
        let rich = seeded_signer(0x684);
        let genesis: kardamom_types::Genesis =
            toml::from_str(include_str!("../../../../chains/dev-withdrawals.toml"))
                .expect("parse the genesis");
        let (alloc, code) = genesis.to_alloc();
        let predeploy = alloc
            .iter()
            .find(|a| a.address == CHAIN_STATE)
            .expect("the genesis holds the chain-state predeploy");
        let predeploy_code = code
            .iter()
            .find(|c| c.code_hash == predeploy.code_hash)
            .expect("the predeploy code");
        let snap = MockStateDatabase::builder()
            .account(poor.address(), U256::from(POOR_FUNDS), 0, KECCAK_EMPTY)
            .account(rich.address(), U256::from(10u128.pow(20)), 0, KECCAK_EMPTY)
            .account(
                CHAIN_STATE,
                U256::ZERO,
                predeploy.nonce,
                predeploy.code_hash,
            )
            .code(predeploy.code_hash, predeploy_code.code.clone())
            .build();
        // The canonical record of an item at block index `i`. A deposit
        // takes L1 block `40 + i`, so each one in a block has its own
        // source hash.
        let record = |item: &Item, i: u64| {
            let tx_idx = TxIndex(i);
            let position = BPosition::from_index(i);
            let (signer, nonce, value) = match *item {
                Item::Schedule(at) => {
                    return BufferedRecord::Deposit {
                        tx_idx,
                        deposit: Self::set_feature(40 + i, at),
                        position,
                    };
                }
                Item::Poor { nonce, value } => (&poor, nonce, value),
                Item::Rich(nonce) => (&rich, nonce, SPEND),
            };
            let envelope = LegacyTx {
                chain_id: 1,
                to: PAYEE,
                nonce,
                value,
                gas_limit: 21_000,
                gas_price: GAS_PRICE,
                ..Default::default()
            }
            .sign(signer);
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            }
        };
        let blocks = BLOCKS
            .iter()
            .map(|items| {
                items
                    .iter()
                    .zip(0u64..)
                    .map(|(item, i)| record(item, i))
                    .collect()
            })
            .collect();
        Self { snap, blocks }
    }

    /// The state before block 1.
    #[must_use]
    pub fn snapshot(&self) -> &MockStateDatabase {
        &self.snap
    }

    /// Run every block through `driver` and through the sequential
    /// driver, block by block, and return the receipts of each block.
    /// `driver` gets the state of the earlier blocks as the parent layer
    /// over [`Self::snapshot`], the records, and the env with the frozen
    /// flags.
    ///
    /// # Panics
    ///
    /// Panics if the sequential driver fails, or if `driver` gives other
    /// receipts or writes than the sequential driver in a block.
    pub fn run(
        &self,
        mut driver: impl FnMut(&PendingDelta, &[BufferedRecord], ExecEnv) -> BlockExecOutput,
    ) -> Vec<Vec<Receipt>> {
        let mut parent = PendingDelta::new();
        self.blocks
            .iter()
            .zip(1u64..)
            .map(|(records, number)| {
                let env = self.env(&parent, number);
                let seq =
                    execute_block(&self.snap, Some(&parent), records, env).expect("sequential");
                let out = driver(&parent, records, env);
                assert_eq!(out.receipts, seq.receipts, "receipts of block {number}");
                assert_eq!(out.delta.accounts, seq.delta.accounts, "block {number}");
                assert_eq!(out.delta.storage, seq.delta.storage, "block {number}");
                parent.merge_from(&seq.delta);
                seq.receipts
            })
            .collect()
    }

    /// Check the burns of `receipts`, the output of [`Self::run`].
    ///
    /// # Panics
    ///
    /// Panics if a receipt burns where the chain expects none, or the
    /// other way round, or if the last transfer of the poor sender did
    /// not land.
    pub fn assert_burns(receipts: &[Vec<Receipt>]) {
        let burns: Vec<Vec<bool>> = receipts
            .iter()
            .map(|block| block.iter().map(Receipt::burns_nonce).collect())
            .collect();
        assert_eq!(burns, Self::EXPECTED_BURNS.map(<[bool]>::to_vec));
        assert!(receipts[3][1].status, "nonce 3 lands: {:?}", receipts[3][1]);
    }

    /// The env of block `number`, with the flags that the state after the
    /// earlier blocks (`parent` over the snapshot) freezes for it.
    fn env(&self, parent: &PendingDelta, number: u64) -> ExecEnv {
        let exec_ts = (number - 1) * STEP_MS;
        let start = BlockBoundaryStart {
            block_number: number,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: exec_ts,
            l1_origin: 0,
        };
        let features = BlockFeatures::read(&self.snap, Some(parent), exec_ts).expect("flags");
        ExecEnv::new(1, &start).with_features(features)
    }

    /// The system deposit of an upgrade transaction in L1 block
    /// `l1_number` that schedules `FEATURE_SEALER_V3` at `activation_ms`.
    fn set_feature(l1_number: u64, activation_ms: u64) -> Deposit {
        let l1_hash = keccak256(l1_number.to_be_bytes());
        let upgrade = LockboxLog::Upgrade(UpgradeLog {
            block_number: l1_number,
            block_hash: l1_hash,
            log_index: 0,
            feature_id: U256::from(FEATURE_SEALER_V3),
            activation_timestamp: activation_ms,
        });
        derive_epoch(l1_number, l1_hash, &[upgrade])
            .expect("one block's logs")
            .deposits
            .remove(0)
    }
}
