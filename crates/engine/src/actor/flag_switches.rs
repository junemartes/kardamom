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
//!
//! The genesis holds the chain-state predeploy or not
//! ([`ChainStateGenesis`]). Without it, the first `setFeature` deposit
//! installs the code, and the chain switches at the same blocks.

use alloy_primitives::{Address, U256, address};
use kardamom_types::upgrades::{
    CHAIN_STATE, CHAIN_STATE_CODE, CHAIN_STATE_CODE_HASH, FEATURE_SEALER_V3,
};
use kardamom_types::{BPosition, BlockBoundaryStart, Deposit, Receipt};
use revm::primitives::KECCAK_EMPTY;

use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::exec_types::TxIndex;
use crate::features::BlockFeatures;
use crate::state::MockStateDatabase;
use crate::stateless::{BlockExecOutput, BufferedRecord, execute_block};

use super::fixtures::{ChainStateGenesis, ChainStatePredeploy, LegacyTx, seeded_signer};

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
    genesis: ChainStateGenesis,
    snap: MockStateDatabase,
    blocks: Vec<Vec<BufferedRecord>>,
}

impl Default for FlagSwitches {
    fn default() -> Self {
        Self::new(ChainStateGenesis::Predeploy)
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

    /// The genesis state: a poor sender, a rich sender, and the
    /// chain-state predeploy of the dev genesis when `genesis` holds it.
    ///
    /// # Panics
    ///
    /// Panics if the dev genesis does not parse or has no predeploy.
    #[must_use]
    pub fn new(genesis: ChainStateGenesis) -> Self {
        let poor = seeded_signer(0x683);
        let rich = seeded_signer(0x684);
        let funded = MockStateDatabase::builder()
            .account(poor.address(), U256::from(POOR_FUNDS), 0, KECCAK_EMPTY)
            .account(rich.address(), U256::from(10u128.pow(20)), 0, KECCAK_EMPTY);
        let snap = match genesis.predeploy() {
            Some(p) => funded
                .account(
                    CHAIN_STATE,
                    U256::ZERO,
                    p.account.nonce,
                    p.account.code_hash,
                )
                .code(p.code.code_hash, p.code.code),
            None => funded,
        }
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
        Self {
            genesis,
            snap,
            blocks,
        }
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
    /// Panics if the sequential driver fails, if `driver` gives other
    /// receipts or writes than the sequential driver in a block, or if
    /// the chain does not end with the code at [`CHAIN_STATE`].
    pub fn run(
        &self,
        mut driver: impl FnMut(&PendingDelta, &[BufferedRecord], ExecEnv) -> BlockExecOutput,
    ) -> Vec<Vec<Receipt>> {
        let mut parent = PendingDelta::new();
        let receipts = self
            .blocks
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
                assert_eq!(out.delta.code, seq.delta.code, "block {number}");
                parent.merge_from(&seq.delta);
                seq.receipts
            })
            .collect();
        self.assert_chain_state_code(&parent);
        receipts
    }

    /// The writes of the chain (`parent`) install the code at
    /// [`CHAIN_STATE`] only on a genesis without it.
    fn assert_chain_state_code(&self, parent: &PendingDelta) {
        let hash = CHAIN_STATE_CODE_HASH;
        let installed = parent
            .accounts
            .get(&CHAIN_STATE)
            .is_some_and(|a| a.code_hash == hash)
            && parent
                .code
                .get(&hash)
                .is_some_and(|c| c.as_ref() == CHAIN_STATE_CODE);
        assert_eq!(
            installed,
            self.genesis == ChainStateGenesis::Empty,
            "{:?} genesis: the install of the chain-state code",
            self.genesis
        );
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
        ExecEnv::new(1, &start, features)
    }

    /// The system deposit of an upgrade transaction in L1 block
    /// `l1_number` that schedules `FEATURE_SEALER_V3` at `activation_ms`.
    fn set_feature(l1_number: u64, activation_ms: u64) -> Deposit {
        ChainStatePredeploy::upgrade_epoch(l1_number, FEATURE_SEALER_V3, &[activation_ms])
            .deposits
            .remove(0)
    }
}
