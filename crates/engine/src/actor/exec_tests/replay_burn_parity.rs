//! The offline replay and the live exec thread give the same state when
//! `FEATURE_SEALER_V3` activates at a time between two block headers.
//! Block N executes with block N-1's header time on both sides, so a block
//! whose own header reaches the activation time, and whose execution time
//! does not, burns no nonce on either side.

use alloy_primitives::{Address, B256, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_state::{StateEnv, StateSnapshot, StateWriter, TrieMode, seed_genesis};
use kardamom_types::epoch::{LockboxLog, UpgradeLog, derive_epoch};
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};
use kardamom_types::{AccountChange, EpochRecord, StateDatabase, delta::CodeEntry};

use crate::actor::StateWriterSignal;
use crate::actor::test_support::{
    ExecRig, boundary_msg, feed, fresh_env, genesis_for, legacy, tx_msg,
};
use crate::persist::{MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal};
use crate::reader::ReaderToExec;
use crate::replay::{CanonicalEnd, ReplayBlock, ReplayGenesis, replay_blocks};

/// The header times of blocks 1, 2 and 3. Block 2 executes with `T1`, and
/// block 3 with `T2`.
const T1: u64 = 1_000;
const T2: u64 = 2_000;
const T3: u64 = 3_000;
/// The activation time: after the execution time of block 2, at or before
/// its header time.
const ACTIVATION: u64 = 1_500;
/// The balance of the poor sender, and a value above it.
const POOR_FUNDS: u64 = 1_000;
const TOO_MUCH: u64 = 10_000;
const PAYEE: Address = address!("00000000000000000000000000000000000E0002");

/// Block 1: the epoch whose upgrade transaction schedules the feature at
/// [`ACTIVATION`]. Block 2: the poor sender skips nonce 0. Block 3: it
/// sends 7 at nonce 0, and skips nonce 1.
struct Scenario {
    poor: PrivateKeySigner,
    epoch: EpochRecord,
    accounts: Vec<AccountChange>,
    code: Vec<CodeEntry>,
}

/// What a run leaves in its state DB.
#[derive(Debug, PartialEq, Eq)]
struct Rebuilt {
    root: B256,
    poor_nonce: u64,
    payee_balance: U256,
}

impl Scenario {
    fn new() -> Self {
        let poor = PrivateKeySigner::random();
        let l1_hash = keccak256(40u64.to_be_bytes());
        let upgrade = LockboxLog::Upgrade(UpgradeLog {
            block_number: 40,
            block_hash: l1_hash,
            log_index: 0,
            feature_id: U256::from(FEATURE_SEALER_V3),
            activation_timestamp: ACTIVATION,
        });
        let epoch = derive_epoch(40, l1_hash, &[upgrade]).expect("one block's logs");
        let chain: kardamom_types::Genesis =
            toml::from_str(include_str!("../../../../../chains/dev-withdrawals.toml"))
                .expect("parse the genesis");
        let (alloc, all_code) = chain.to_alloc();
        let predeploy = alloc
            .into_iter()
            .find(|a| a.address == CHAIN_STATE)
            .expect("the genesis holds the chain-state predeploy");
        let code = all_code
            .into_iter()
            .filter(|c| c.code_hash == predeploy.code_hash)
            .collect();
        let mut accounts = genesis_for(Address::repeat_byte(0x01));
        accounts.extend([
            AccountChange {
                address: poor.address(),
                nonce: 0,
                balance: U256::from(POOR_FUNDS),
                code_hash: B256::ZERO,
            },
            predeploy,
        ]);
        Self {
            poor,
            epoch,
            accounts,
            code,
        }
    }

    /// The epoch takes slots 0 and 1, so block 1 ends at 2.
    fn live_records(&self) -> Vec<ReaderToExec> {
        std::iter::once(ReaderToExec::Epoch(self.epoch.clone()))
            .chain(
                self.epoch
                    .deposits
                    .iter()
                    .cloned()
                    .map(ReaderToExec::Deposit),
            )
            .chain([
                boundary_msg(1, 2, T1),
                tx_msg(&self.poor, PAYEE, 2, 0, TOO_MUCH),
                boundary_msg(2, 3, T2),
                tx_msg(&self.poor, PAYEE, 3, 0, 7),
                tx_msg(&self.poor, PAYEE, 4, 1, TOO_MUCH),
                boundary_msg(3, 5, T3),
            ])
            .collect()
    }

    fn block(number: u64, ts: u64, end: u64) -> ReplayBlock {
        ReplayBlock {
            block_number: number,
            l2_timestamp: ts,
            canonical_end: Some(CanonicalEnd {
                end_tx_idx: end,
                l1_origin: 40,
            }),
            l1_epochs: Vec::new(),
            remote_epochs: Vec::new(),
            txs: Vec::new(),
        }
    }

    fn replay_blocks(&self) -> Vec<ReplayBlock> {
        vec![
            ReplayBlock {
                l1_epochs: vec![self.epoch.clone()],
                ..Self::block(1, T1, 2)
            },
            ReplayBlock {
                txs: vec![legacy(&self.poor, PAYEE, 0, TOO_MUCH)],
                ..Self::block(2, T2, 3)
            },
            ReplayBlock {
                txs: vec![
                    legacy(&self.poor, PAYEE, 0, 7),
                    legacy(&self.poor, PAYEE, 1, TOO_MUCH),
                ],
                ..Self::block(3, T3, 5)
            },
        ]
    }

    fn rebuilt(&self, env: &StateEnv) -> Rebuilt {
        let snap = StateSnapshot::open(env).unwrap();
        Rebuilt {
            root: snap.state_root().unwrap().expect("the trie is on"),
            poor_nonce: snap.basic(self.poor.address()).unwrap().expect("poor").0,
            payee_balance: snap.basic(PAYEE).unwrap().map_or(U256::ZERO, |a| a.1),
        }
    }

    fn live(&self) -> Rebuilt {
        let (_dir, env) = fresh_env();
        seed_genesis(&env, &self.accounts, &self.code).unwrap();
        let handle = StateWriter::spawn_with_trie(env.clone(), TrieMode::Incremental).unwrap();
        let rig = ExecRig::new(
            MdbxSnapshotSource::new(handle.snapshot_rx.clone()),
            MdbxWriterSignal::new(handle.snapshot_rx.clone()),
            MdbxWriterQueue::new(handle.delta_tx.clone()),
        );
        let (h, _rx_e2c) = rig.spawn(feed(self.live_records()));
        h.join().expect("no panic").expect("exec ok");
        MdbxWriterSignal::new(handle.snapshot_rx.clone())
            .wait_committed(3)
            .unwrap();
        self.rebuilt(&env)
    }

    fn replayed(&self) -> Rebuilt {
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

/// Block 2's header reaches the activation time, and its execution time
/// does not: its skip uses no nonce, so block 3's transfer at nonce 0
/// executes. Block 3 executes past the activation time: its skip of
/// nonce 1 uses the nonce. The replay ends at the live root.
#[test]
fn a_replay_burns_only_where_the_live_chain_burns() {
    let scenario = Scenario::new();
    let live = scenario.live();
    assert_eq!(
        (live.poor_nonce, live.payee_balance),
        (2, U256::from(7u64)),
        "nonce 0 executes in block 3, and nonce 1 burns"
    );
    assert_eq!(scenario.replayed(), live);
}
