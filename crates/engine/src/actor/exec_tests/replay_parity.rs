//! The offline replay and the live exec thread give the same state when
//! the replay gets the L1 epochs the live stream carried. Both sides write
//! through the real libMDBX writer with the trie on, so the check compares
//! state roots, not a model of them.

use alloy_primitives::{Address, B256, U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_state::{StateEnv, StateSnapshot, StateWriter, TrieMode, seed_genesis};
use kardamom_types::{AccountChange, BPosition, EpochRecord, StateDatabase};

use crate::actor::StateWriterSignal;
use crate::actor::test_support::{
    ExecRig, boundary_msg, deposit_epoch, feed, fresh_env, genesis_for, legacy, tx_msg,
};
use crate::persist::{MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal};
use crate::reader::ReaderToExec;
use crate::replay::{CanonicalEnd, ReplayBlock, ReplayGenesis, replay_blocks};

const T1: u64 = 1_700_000_000;
const T2: u64 = 1_700_000_001;
/// Each deposit mints this much to its recipient.
const MINT: u128 = 1_000;

/// Two blocks, each led by an L1 epoch. Block 1: epoch 40 mints to
/// `minted` and to `to`; then `funded` pays `to` 100, and `minted` pays
/// `to` 7 from the deposit, which only works after the mint. Block 2: the
/// empty epoch 41, then `funded` pays `to` 25.
struct Scenario {
    funded: PrivateKeySigner,
    minted: PrivateKeySigner,
    to: Address,
    epochs: [EpochRecord; 2],
    genesis: Vec<AccountChange>,
}

/// What a run leaves in its state DB.
#[derive(Debug, PartialEq, Eq)]
struct Rebuilt {
    root: B256,
    /// Where the receipt of epoch 40's first deposit is.
    deposit_position: Option<BPosition>,
    to_balance: U256,
}

impl Rebuilt {
    fn read(env: &StateEnv, deposit: B256, to: Address) -> Self {
        let snap = StateSnapshot::open(env).unwrap();
        Self {
            root: snap.state_root().unwrap().expect("the trie is on"),
            deposit_position: snap.get_tx_position(deposit).unwrap(),
            to_balance: snap.basic(to).unwrap().expect("to exists").1,
        }
    }
}

impl Scenario {
    fn new() -> Self {
        let funded = PrivateKeySigner::random();
        let minted = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000E0001");
        let epochs = [
            deposit_epoch(40, &[minted.address(), to], MINT),
            deposit_epoch(41, &[], MINT),
        ];
        let genesis = genesis_for(funded.address());
        Self {
            funded,
            minted,
            to,
            epochs,
            genesis,
        }
    }

    /// The epoch as the live reader expands it: the marker, then one
    /// record per deposit.
    fn expanded(epoch: &EpochRecord) -> impl Iterator<Item = ReaderToExec> + '_ {
        std::iter::once(ReaderToExec::Epoch(epoch.clone()))
            .chain(epoch.deposits.iter().cloned().map(ReaderToExec::Deposit))
    }

    fn live_records(&self) -> Vec<ReaderToExec> {
        let [e40, e41] = &self.epochs;
        Self::expanded(e40)
            .chain([
                tx_msg(&self.funded, self.to, 3, 0, 100),
                tx_msg(&self.minted, self.to, 4, 0, 7),
                boundary_msg(1, 5, T1),
            ])
            .chain(Self::expanded(e41))
            .chain([
                tx_msg(&self.funded, self.to, 6, 1, 25),
                boundary_msg(2, 7, T2),
            ])
            .collect()
    }

    fn replay_blocks(&self) -> Vec<ReplayBlock> {
        let [e40, e41] = self.epochs.clone();
        vec![
            ReplayBlock {
                block_number: 1,
                l2_timestamp: T1,
                canonical_end: Some(CanonicalEnd {
                    end_tx_idx: 5,
                    l1_origin: 40,
                }),
                l1_epochs: vec![e40],
                remote_epochs: Vec::new(),
                txs: vec![
                    legacy(&self.funded, self.to, 0, 100),
                    legacy(&self.minted, self.to, 0, 7),
                ],
            },
            ReplayBlock {
                block_number: 2,
                l2_timestamp: T2,
                canonical_end: Some(CanonicalEnd {
                    end_tx_idx: 7,
                    l1_origin: 41,
                }),
                l1_epochs: vec![e41],
                remote_epochs: Vec::new(),
                txs: vec![legacy(&self.funded, self.to, 1, 25)],
            },
        ]
    }

    fn rebuilt(&self, env: &StateEnv) -> Rebuilt {
        Rebuilt::read(env, self.epochs[0].deposits[0].source_hash, self.to)
    }

    /// Run the live exec thread over the scenario's records, into a
    /// trie-on state DB.
    fn live(&self) -> Rebuilt {
        let (_dir, env) = fresh_env();
        seed_genesis(&env, &self.genesis, &[]).unwrap();
        let handle = StateWriter::spawn_with_trie(env.clone(), TrieMode::Incremental).unwrap();
        let rig = ExecRig::new(
            MdbxSnapshotSource::new(handle.snapshot_rx.clone()),
            MdbxWriterSignal::new(handle.snapshot_rx.clone()),
            MdbxWriterQueue::new(handle.delta_tx.clone()),
        );
        let (h, _rx_e2c) = rig.spawn(feed(self.live_records()));
        h.join().expect("no panic").expect("exec ok");
        MdbxWriterSignal::new(handle.snapshot_rx.clone())
            .wait_committed(2)
            .unwrap();
        self.rebuilt(&env)
    }

    fn replayed(&self) -> Rebuilt {
        let (_dir, env) = fresh_env();
        let genesis = ReplayGenesis {
            chain_id: 1,
            accounts: &self.genesis,
            code: &[],
            fees: None,
        };
        replay_blocks(env.clone(), &genesis, self.replay_blocks()).unwrap();
        self.rebuilt(&env)
    }
}

/// The replay applies each epoch's deposits at the head of its block,
/// through the live deposit execution: the same root, and the same
/// receipt position, as the live exec thread.
#[test]
fn replayed_deposits_give_the_live_root_and_positions() {
    let scenario = Scenario::new();
    let live = scenario.live();
    assert_eq!(scenario.replayed(), live);
    // The marker takes slot 0, so the first deposit is at slot 1.
    assert_eq!(live.deposit_position, Some(BPosition::from_index(1)));
    // The mint to `to`, then 100 + 7 + 25 in transfers. The 7 only moves
    // when the deposit to `minted` applied before its transfer.
    assert_eq!(live.to_balance, U256::from(MINT + 132));
}
