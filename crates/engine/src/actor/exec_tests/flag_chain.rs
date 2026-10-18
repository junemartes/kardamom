//! A chain with one poor sender, with or without the chain-state
//! predeploy in its genesis, whose upgrade transactions schedule
//! `FEATURE_SEALER_V3`. It runs through the
//! live exec thread (streaming or whole-block) and the offline replay,
//! each on libMDBX with the trie on, and reports what each run leaves:
//! the root, the nonce of the poor sender, the code hash at the chain-state
//! address, and which transfers of each block burned their nonce.

use alloy_primitives::{Address, B256, U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_state::{StateEnv, StateSnapshot, StateWriter, TrieMode, seed_genesis};
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};
use kardamom_types::{
    AccountChange, EpochRecord, StateDatabase, TxEnvelope, TxRef, delta::CodeEntry,
};

use crate::actor::StateWriterSignal;
use crate::actor::fixtures::{ChainStateGenesis, ChainStatePredeploy};
use crate::actor::test_hooks::SequentialBlockExec;
use crate::actor::test_support::{
    ExecRig, boundary_msg, feed, fresh_env, genesis_for, legacy, pos,
};
use crate::persist::{MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal};
use crate::reader::ReaderToExec;
use crate::replay::{CanonicalEnd, ReplayBlock, ReplayGenesis, replay_blocks};

/// The balance of the poor sender, and a value above it.
const POOR_FUNDS: u64 = 1_000;
const TOO_MUCH: u64 = 10_000;
const PAYEE: Address = address!("00000000000000000000000000000000000E0002");
/// The L1 block of the first upgrade epoch. Each later one takes the next
/// L1 block.
const FIRST_L1: u64 = 40;

/// One record of a block on the canonical stream.
pub(super) enum Item {
    /// An L1 epoch whose upgrade transactions schedule `FEATURE_SEALER_V3`
    /// at these times, in ms, in log order.
    Schedule(Vec<u64>),
    /// A transfer of the poor sender at `nonce` that it cannot pay: a skip
    /// at that nonce. Each skip has its own value, so its own hash.
    Skip(u64),
    /// A transfer of the poor sender at `nonce`, of `value`.
    Tx { nonce: u64, value: u64 },
}

/// One block: its header time and its records, in order.
pub(super) struct Block {
    pub(super) ts: u64,
    pub(super) items: Vec<Item>,
}

/// How the live exec thread runs a block.
#[derive(Clone, Copy, Debug)]
pub(super) enum Mode {
    /// Record by record, in the block scope.
    Streaming,
    /// At the boundary, through the sequential whole-block strategy.
    Whole,
}

/// What a run leaves in its state DB.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Rebuilt {
    pub(super) root: B256,
    pub(super) poor_nonce: u64,
    pub(super) payee_balance: U256,
    /// The code hash at the chain-state address, or `None` without an
    /// account there.
    pub(super) chain_state_code: Option<B256>,
    /// For each block, whether each of its transfers burned its nonce.
    pub(super) burns: Vec<Vec<bool>>,
}

/// One record of a block, resolved: an epoch, or a signed transfer.
enum Record {
    Epoch(EpochRecord),
    Tx(TxEnvelope),
}

impl Record {
    /// The canonical slots the record takes: an epoch marker and each of
    /// its deposits, or one transaction.
    fn slots(&self) -> u64 {
        match self {
            Self::Epoch(epoch) => 1 + epoch.deposits.len() as u64,
            Self::Tx(_) => 1,
        }
    }

    fn epoch(&self) -> Option<&EpochRecord> {
        match self {
            Self::Epoch(epoch) => Some(epoch),
            Self::Tx(_) => None,
        }
    }

    fn tx(&self) -> Option<&TxEnvelope> {
        match self {
            Self::Epoch(_) => None,
            Self::Tx(envelope) => Some(envelope),
        }
    }

    /// The reader messages of the record at canonical slot `at`.
    fn live(&self, at: u64) -> Vec<ReaderToExec> {
        match self {
            Self::Epoch(epoch) => std::iter::once(ReaderToExec::Epoch(epoch.clone()))
                .chain(epoch.deposits.iter().cloned().map(ReaderToExec::Deposit))
                .collect(),
            Self::Tx(envelope) => {
                let position = pos(i32::try_from(at).expect("a small fixture"));
                vec![ReaderToExec::Tx {
                    tx_ref: TxRef::new(envelope.tx_hash, 0, position, 0),
                    envelope: envelope.clone(),
                    position,
                }]
            }
        }
    }
}

/// One resolved block: its number, header time, records, and the
/// canonical slot range `start..end` that its records take.
struct Numbered<'a> {
    number: u64,
    ts: u64,
    records: &'a [Record],
    start: u64,
    end: u64,
}

pub(super) struct FlagChain {
    poor: PrivateKeySigner,
    accounts: Vec<AccountChange>,
    code: Vec<CodeEntry>,
    /// Each block's header time and resolved records.
    blocks: Vec<(u64, Vec<Record>)>,
}

impl FlagChain {
    pub(super) fn new(genesis: ChainStateGenesis, blocks: Vec<Block>) -> Self {
        let poor = PrivateKeySigner::random();
        let predeploy = genesis.predeploy();
        let mut accounts = genesis_for(Address::repeat_byte(0x01));
        accounts.push(AccountChange {
            address: poor.address(),
            nonce: 0,
            balance: U256::from(POOR_FUNDS),
            code_hash: B256::ZERO,
        });
        accounts.extend(predeploy.as_ref().map(|p| p.account.clone()));
        let mut l1 = FIRST_L1..;
        let mut skips = 0u64..;
        let mut resolve = |item: Item| match item {
            Item::Schedule(times) => Record::Epoch(ChainStatePredeploy::upgrade_epoch(
                l1.next().expect("an L1 block"),
                FEATURE_SEALER_V3,
                &times,
            )),
            Item::Skip(nonce) => {
                let value = TOO_MUCH + skips.next().expect("a skip");
                Record::Tx(legacy(&poor, PAYEE, nonce, value))
            }
            Item::Tx { nonce, value } => Record::Tx(legacy(&poor, PAYEE, nonce, value)),
        };
        let blocks = blocks
            .into_iter()
            .map(|block| {
                (
                    block.ts,
                    block.items.into_iter().map(&mut resolve).collect(),
                )
            })
            .collect();
        Self {
            poor,
            accounts,
            code: predeploy.into_iter().map(|p| p.code).collect(),
            blocks,
        }
    }

    fn numbered(&self) -> impl Iterator<Item = Numbered<'_>> {
        self.blocks
            .iter()
            .zip(1u64..)
            .scan(0u64, |end, ((ts, records), number)| {
                let start = *end;
                *end += records.iter().map(Record::slots).sum::<u64>();
                Some(Numbered {
                    number,
                    ts: *ts,
                    records,
                    start,
                    end: *end,
                })
            })
    }

    fn live_records(&self) -> Vec<ReaderToExec> {
        self.numbered()
            .flat_map(|block| {
                let positions = block.records.iter().scan(block.start, |at, record| {
                    let here = *at;
                    *at += record.slots();
                    Some(here)
                });
                block
                    .records
                    .iter()
                    .zip(positions)
                    .flat_map(|(record, at)| record.live(at))
                    .chain(std::iter::once(boundary_msg(
                        block.number,
                        i32::try_from(block.end).expect("a small fixture"),
                        block.ts,
                    )))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The blocks as the DA payload carries them. Replay runs each epoch
    /// at the head of its block, so a block for replay lists its
    /// schedules before its transfers.
    fn replay_blocks(&self) -> Vec<ReplayBlock> {
        self.numbered()
            .map(|block| ReplayBlock {
                block_number: block.number,
                l2_timestamp: block.ts,
                canonical_end: Some(CanonicalEnd {
                    end_tx_idx: block.end,
                    l1_origin: FIRST_L1,
                }),
                l1_epochs: block
                    .records
                    .iter()
                    .filter_map(Record::epoch)
                    .cloned()
                    .collect(),
                remote_epochs: Vec::new(),
                txs: block
                    .records
                    .iter()
                    .filter_map(Record::tx)
                    .cloned()
                    .collect(),
            })
            .collect()
    }

    /// Whether the stored receipt of `tx` burns its nonce.
    fn burns(snap: &StateSnapshot, tx: &TxEnvelope) -> bool {
        let position = snap
            .get_tx_position(tx.tx_hash)
            .unwrap()
            .expect("the transfer is stored");
        snap.get_receipt(position)
            .unwrap()
            .expect("the receipt is stored")
            .burns_nonce()
    }

    fn rebuilt(&self, env: &StateEnv) -> Rebuilt {
        let snap = StateSnapshot::open(env).unwrap();
        Rebuilt {
            root: snap.state_root().unwrap().expect("the trie is on"),
            poor_nonce: snap.basic(self.poor.address()).unwrap().expect("poor").0,
            payee_balance: snap.basic(PAYEE).unwrap().map_or(U256::ZERO, |a| a.1),
            chain_state_code: snap.basic(CHAIN_STATE).unwrap().map(|a| a.2),
            burns: self
                .blocks
                .iter()
                .map(|(_, records)| {
                    records
                        .iter()
                        .filter_map(Record::tx)
                        .map(|tx| Self::burns(&snap, tx))
                        .collect()
                })
                .collect(),
        }
    }

    /// Run the chain through the live exec thread.
    pub(super) fn live(&self, mode: Mode) -> Rebuilt {
        let (_dir, env) = fresh_env();
        seed_genesis(&env, &self.accounts, &self.code).unwrap();
        let handle = StateWriter::spawn_with_trie(env.clone(), TrieMode::Incremental).unwrap();
        let rig = ExecRig::new(
            MdbxSnapshotSource::new(handle.snapshot_rx.clone()),
            MdbxWriterSignal::new(handle.snapshot_rx.clone()),
            MdbxWriterQueue::new(handle.delta_tx.clone()),
        );
        let records = feed(self.live_records());
        let (h, _rx_e2c) = match mode {
            Mode::Streaming => rig.spawn(records),
            Mode::Whole => rig.block_exec(SequentialBlockExec).spawn(records),
        };
        h.join().expect("no panic").expect("exec ok");
        MdbxWriterSignal::new(handle.snapshot_rx.clone())
            .wait_committed(self.blocks.len() as u64)
            .unwrap();
        self.rebuilt(&env)
    }

    /// Run the chain through the offline replay.
    pub(super) fn replayed(&self) -> Rebuilt {
        let (_dir, env) = fresh_env();
        let genesis = ReplayGenesis {
            chain_id: 1,
            accounts: &self.accounts,
            code: &self.code,
            fees: None,
        };
        replay_blocks(env.clone(), &genesis, self.replay_blocks()).unwrap();
        self.rebuilt(&env)
    }
}
