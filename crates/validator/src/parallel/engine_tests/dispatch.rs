//! Real-mdbx dispatch test for the pooled parallel path.

use alloy_primitives::{U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::BufferedRecord;
use kardamom_types::StateDatabase;

use crate::parallel::{BlockInputs, execute_block_parallel};

use super::fixtures::{env, honest_claims, nz, nz16, test_pool, tx};

/// Real-mdbx equivalence: the pooled path with per-worker snapshot forks
/// (`fork_view`) produces byte-identical output to sequential execution
/// against the same mdbx snapshot. Mock tests cannot exercise the fork
/// path's mdbx anchor semantics, so this test uses a real state env.
#[test]
fn pooled_with_forks_matches_sequential_on_mdbx() {
    use kardamom_engine::stateless::execute_block;

    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");

    let dir = tempfile::tempdir().expect("tmpdir");
    let env_ = kardamom_state::StateEnvBuilder::new(dir.path())
        .durability(kardamom_state::Durability::SafeNoSync)
        .open()
        .expect("open state env");
    // Fund the sender at genesis so transfers execute.
    kardamom_state::seed_genesis(
        &env_,
        &[kardamom_types::AccountChange {
            address: from,
            nonce: 0,
            balance: U256::from(10u128.pow(18)),
            code_hash: revm::primitives::KECCAK_EMPTY,
        }],
        &[],
    )
    .expect("seed genesis");
    let mut writer = kardamom_state::StateWriter::spawn(env_).expect("spawn writer");
    let snap = writer.snapshot_rx.recv().expect("genesis snapshot");
    // Sanity check: forks mint while the writer is quiet at genesis.
    assert!(
        snap.fork_view().is_some(),
        "fork must mint at a quiet anchor"
    );

    let txs: Vec<BufferedRecord> = (0..17).map(|i| tx(&signer, to, i, 50 + i, i)).collect();
    let claims = honest_claims(&snap, &txs);

    let sequential = execute_block(&snap, None, &txs, env()).expect("sequential");
    let pooled = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &txs,
            claims: &claims,
            env: env(),
            granularity: nz16(1),
        },
        nz(8),
    )
    .expect("pooled with forks");

    assert_eq!(pooled.receipts, sequential.receipts);
    assert_eq!(pooled.delta.accounts, sequential.delta.accounts);
    assert_eq!(pooled.delta.storage, sequential.delta.storage);
    drop(snap);
    writer.shutdown().expect("writer shutdown");
}
