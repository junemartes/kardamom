//! The choice of the rollback checkpoint: local first, then a peer, and a
//! stop when no checkpoint is below the divergence block.

use std::path::{Path, PathBuf};

use alloy_primitives::{Address, U256};
use kardamom_state::{StateEnv, StateEnvBuilder, StateWriter, WriteBatch};
use kardamom_types::{AccountChange, BPosition, BlockBoundary, BlockDelta};

use super::*;
use crate::marker::Markers;

/// A state DB with genesis and blocks `1..=upto`, and a checkpoint of each
/// block in `checkpoints`, in `ckpt_dir`.
fn chain(state_dir: &Path, ckpt_dir: &Path, upto: u64, checkpoints: &[u64]) -> StateEnv {
    let env = StateEnvBuilder::new(state_dir).open().unwrap();
    let genesis = [AccountChange {
        address: Address::repeat_byte(1),
        nonce: 0,
        balance: U256::from(1u64),
        code_hash: B256::ZERO,
    }];
    kardamom_state::seed_genesis(&env, &genesis, &[]).unwrap();
    (1..=upto).for_each(|block| {
        commit(&env, block);
        if checkpoints.contains(&block) {
            kardamom_state::create_checkpoint(&env, ckpt_dir).unwrap();
        }
    });
    env
}

/// Commit `block` and wait until it is durable.
fn commit(env: &StateEnv, block: u64) {
    let mut writer = StateWriter::spawn(env.clone()).unwrap();
    commit_on(&mut writer, block);
    writer.shutdown().unwrap();
}

/// Commit `block` through `writer`, and wait until it is durable.
fn commit_on(writer: &mut kardamom_state::WriterHandle, block: u64) {
    let boundary = BlockBoundary {
        block_number: block,
        end_tx_idx: BPosition::from_index(block),
        ..BlockBoundary::default()
    };
    let delta = BlockDelta {
        block_number: block,
        ..BlockDelta::default()
    };
    writer
        .delta_tx
        .send(WriteBatch::new(boundary, delta))
        .unwrap();
    std::iter::from_fn(|| writer.snapshot_rx.recv())
        .find(|s| s.block_number() >= block)
        .expect("the block commits");
}

/// [`chain`] with the block log of the checkpoints: a writer that feeds
/// the log thread commits each block.
fn chain_logged(state_dir: &Path, ckpt_dir: &Path, upto: u64, checkpoints: &[u64]) {
    let env = chain(state_dir, ckpt_dir, 0, &[]);
    let cfg = kardamom_state::checkpoint::log_thread::LogConfig {
        dir: ckpt_dir.join(kardamom_state::checkpoint::replay::LOG_DIR),
        segment_bytes: std::num::NonZeroU64::new(1 << 20).unwrap(),
        queue: std::num::NonZeroUsize::new(64).unwrap(),
        sync_every: std::time::Duration::from_millis(10),
    };
    let (feed, log_thread) =
        kardamom_state::checkpoint::log_thread::BlockLogThread::spawn(&env, &cfg).unwrap();
    let mut writer = StateWriter::spawn_with_log(env.clone(), feed).unwrap();
    for block in 1..=upto {
        commit_on(&mut writer, block);
        if checkpoints.contains(&block) {
            kardamom_state::create_checkpoint(&env, ckpt_dir).unwrap();
        }
    }
    writer.shutdown().unwrap();
    log_thread.join().unwrap();
}

/// The blocks of the checkpoints in `dir`, oldest first.
fn blocks(dir: &Path) -> Vec<u64> {
    let mut found: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|e| {
            e.file_name()
                .to_str()?
                .strip_prefix("checkpoint-")?
                .parse()
                .ok()
        })
        .collect();
    found.sort_unstable();
    found
}

fn diverged(block: u64) -> ExecutorError {
    ExecutorError::StreamDiverged {
        block,
        index: 0,
        stream: B256::repeat_byte(7),
    }
}

/// The receipt at `index` of `block`.
fn receipt(index: u64, block: u64) -> kardamom_types::ReceiptRows {
    kardamom_types::ReceiptRows::bare(kardamom_types::Receipt {
        tx_idx: BPosition::from_index(index),
        block_number: block,
        ..kardamom_types::Receipt::default()
    })
}

/// The indexes of the receipts in `ledger`.
fn published(ledger: &ReceiptLedger) -> Vec<u64> {
    match ledger.tail(0, true).unwrap() {
        kardamom_engine::reader::carried::PublishedTail::Known(published) => {
            published.into_keys().collect()
        }
        kardamom_engine::reader::carried::PublishedTail::Unknown => panic!("a known tail"),
    }
}

struct Dirs {
    _root: tempfile::TempDir,
    state: PathBuf,
    ckpt: PathBuf,
    ledger: ReceiptLedger,
}

impl Dirs {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("state");
        let ckpt = root.path().join("ckpt");
        std::fs::create_dir_all(&ckpt).unwrap();
        std::fs::create_dir_all(&state).unwrap();
        Self {
            ledger: ReceiptLedger::open(Markers::new(&state)).unwrap(),
            state,
            ckpt,
            _root: root,
        }
    }

    fn repair<'a>(&'a self, peers: &'a [String]) -> RollbackRepair<'a> {
        RollbackRepair {
            checkpoint_dir: Some(&self.ckpt),
            checkpoint_peers: peers,
            state_dir: &self.state,
            expected_genesis: None,
            ledger: &self.ledger,
        }
    }
}

#[test]
fn another_error_stages_nothing() {
    let dirs = Dirs::new();
    drop(chain(&dirs.state, &dirs.ckpt, 4, &[2, 4]));
    let staged = dirs
        .repair(&[])
        .stage(Some(&ExecutorError::TxOrderingClosed))
        .unwrap();
    assert!(!staged);
    assert_eq!(dirs.ledger.markers.pending().unwrap(), None);
    assert_eq!(blocks(&dirs.ckpt), [2, 4]);
    assert!(kardamom_state::has_state_db(&dirs.state).unwrap());
}

#[test]
fn a_local_checkpoint_below_the_divergence_is_staged() {
    let dirs = Dirs::new();
    drop(chain(&dirs.state, &dirs.ckpt, 8, &[2, 4, 6, 8]));
    dirs.ledger.record(&[receipt(3, 3), receipt(9, 9)]).unwrap();
    assert!(dirs.repair(&[]).stage(Some(&diverged(6))).unwrap());
    assert_eq!(
        published(&dirs.ledger),
        [] as [u64; 0],
        "a restore clears the whole ledger"
    );
    let pending = dirs
        .ledger
        .markers
        .pending()
        .unwrap()
        .expect("a rollback marker");
    assert_eq!(
        pending,
        Void {
            kept: 5,
            stream: B256::repeat_byte(7)
        }
        .record(),
        "the marker is on disk before the park"
    );
    assert_eq!(blocks(&dirs.ckpt), [2, 4], "the diverged checkpoints go");
    assert!(
        !kardamom_state::has_state_db(&dirs.state).unwrap(),
        "the diverged state DB is parked"
    );
    let restored = kardamom_state::restore_best_checkpoint(
        &dirs.ckpt,
        &dirs.state,
        BlockRange {
            max: pending.to_block,
            ..BlockRange::ANY
        },
        None,
    )
    .unwrap()
    .expect("a checkpoint below the block");
    assert_eq!(restored.0, 4, "the newest below the block");
}

/// With the block log, the rollback restores the block just before the
/// divergence, not the newest checkpoint below it. The log of the diverged
/// blocks is cut, so no later restore reaches them.
#[test]
fn a_rollback_with_the_block_log_restores_the_block_before_the_divergence() {
    let dirs = Dirs::new();
    chain_logged(&dirs.state, &dirs.ckpt, 8, &[2, 6]);
    assert!(dirs.repair(&[]).stage(Some(&diverged(6))).unwrap());
    assert_eq!(blocks(&dirs.ckpt), [2], "the diverged checkpoint goes");
    let restored =
        kardamom_state::restore_best_checkpoint(&dirs.ckpt, &dirs.state, BlockRange::ANY, None)
            .unwrap()
            .expect("the base below the block");
    assert_eq!(restored.0, 5, "the base at 2 and the log to 5");
}

#[test]
fn no_checkpoint_below_the_divergence_keeps_the_state() {
    let dirs = Dirs::new();
    drop(chain(&dirs.state, &dirs.ckpt, 6, &[6]));
    assert!(!dirs.repair(&[]).stage(Some(&diverged(6))).unwrap());
    assert_eq!(blocks(&dirs.ckpt), [] as [u64; 0]);
    assert_eq!(dirs.ledger.markers.pending().unwrap(), None);
    assert!(
        kardamom_state::has_state_db(&dirs.state).unwrap(),
        "the state DB stays for the operator"
    );
}

#[test]
fn no_checkpoint_dir_stages_nothing() {
    let dirs = Dirs::new();
    drop(chain(&dirs.state, &dirs.ckpt, 2, &[]));
    let repair = RollbackRepair {
        checkpoint_dir: None,
        ..dirs.repair(&[])
    };
    assert!(!repair.stage(Some(&diverged(2))).unwrap());
}

/// With no local checkpoint below the block, a peer checkpoint below it is
/// fetched; the peer's newer checkpoint is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_checkpoint_below_the_divergence_is_fetched() {
    let peer = Dirs::new();
    drop(chain(&peer.state, &peer.ckpt, 7, &[3, 7]));
    let server =
        kardamom_state::serve_checkpoints("127.0.0.1:0".parse().unwrap(), peer.ckpt.clone())
            .unwrap();
    let peers = vec![server.addr.to_string()];

    let dirs = Dirs::new();
    let staged = tokio::task::spawn_blocking({
        let dirs_state = dirs.state.clone();
        let dirs_ckpt = dirs.ckpt.clone();
        let ledger = dirs.ledger.clone();
        move || {
            drop(chain(&dirs_state, &dirs_ckpt, 6, &[6]));
            RollbackRepair {
                checkpoint_dir: Some(&dirs_ckpt),
                checkpoint_peers: &peers,
                state_dir: &dirs_state,
                expected_genesis: None,
                ledger: &ledger,
            }
            .stage(Some(&diverged(5)))
            .unwrap()
        }
    })
    .await
    .unwrap();
    assert!(staged);
    assert_eq!(
        dirs.ledger.markers.pending().unwrap().map(|r| r.to_block),
        Some(4)
    );
    assert_eq!(
        blocks(&dirs.ckpt),
        [3],
        "the peer checkpoint below the block"
    );
}

/// The stream differs only from receipts published above the committed
/// history: the record voids them, the ledger drops them, and the state
/// and the checkpoints stay.
#[test]
fn a_published_divergence_keeps_the_state() {
    let dirs = Dirs::new();
    drop(chain(&dirs.state, &dirs.ckpt, 4, &[2, 4]));
    dirs.ledger.record(&[receipt(4, 5), receipt(5, 6)]).unwrap();
    let void = ExecutorError::PublishedDiverged {
        block: 6,
        index: 5,
        stream: B256::repeat_byte(7),
    };
    assert!(dirs.repair(&[]).stage(Some(&void)).unwrap());
    assert_eq!(
        dirs.ledger.markers.pending().unwrap(),
        Some(
            Void {
                kept: 5,
                stream: B256::repeat_byte(7)
            }
            .record()
        )
    );
    assert_eq!(published(&dirs.ledger), [4], "the receipts of block 6 go");
    assert_eq!(blocks(&dirs.ckpt), [2, 4], "the checkpoints stay");
    assert!(
        kardamom_state::has_state_db(&dirs.state).unwrap(),
        "the state stays"
    );
}
