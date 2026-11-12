use std::path::Path;
use std::time::Instant;

use alloy_primitives::{B256, U256, keccak256};
use kardamom_types::{BPosition, CodeEntry, GenesisAlloc, Receipt, StateDatabase};

use super::container::{Case, Container};
use super::{BOUND, Fixture, Volume};
use crate::checkpoint::tests::{ADDR, block_batch};
use crate::{StateEnvBuilder, StateSnapshot, StateWriter, WriteBatch, read_recovery_point};

#[test]
#[ignore = "requires Linux Docker and one disposable container with an eight-MiB tmpfs"]
fn state_enospc_stops_without_false_publication_and_resumes_after_reopen() {
    Container::run(Case::State);
}

#[test]
#[ignore = "child entry point; the container parent supplies an isolated tmpfs"]
fn state_full_child() {
    assert_eq!(std::env::var("KARDAMOM_CHECKPOINT_COMPONENT").unwrap(), "1");
    assert!(Path::new("/.dockerenv").exists());
    let pending = Pending::new();
    pending.refuse_on_full_volume();
    Volume::clear();
    pending.reopen_and_resume();
    println!("State ENOSPC proof: commit=28 no_false_snapshot=1 recovered=1 resumed=2");
}

struct Pending {
    batch: WriteBatch,
    hash: B256,
}

impl Pending {
    fn new() -> Self {
        let code = vec![0x5a; 1 << 20];
        let hash = keccak256(&code);
        let mut batch = block_batch(ADDR, 2, 2);
        batch.delta.accounts[0].code_hash = hash;
        batch.delta.code.push(CodeEntry {
            code_hash: hash,
            code: code.into(),
        });
        batch.delta.receipts.push(Receipt {
            tx_idx: BPosition::from_index(2),
            tx_hash: hash,
            from: ADDR,
            nonce: 2,
            status: true,
            ..Receipt::default()
        });
        Self { batch, hash }
    }

    fn refuse_on_full_volume(&self) {
        let env = StateEnvBuilder::new("/dev/shm/state").open().unwrap();
        crate::genesis::seed_genesis(&env, &GenesisAlloc::of_accounts(Vec::new())).unwrap();
        let mut writer = StateWriter::spawn(env).unwrap();
        Fixture::commit_on(&writer, 1);
        Volume::fill();
        writer.delta_tx.send(self.batch.clone()).unwrap();
        assert!(
            writer
                .snapshot_rx
                .recv_deadline(Instant::now().checked_add(BOUND).unwrap())
                .is_none(),
            "a failed commit must not publish a snapshot"
        );
        let error = writer
            .shutdown()
            .expect_err("the writer must report its failed commit");
        assert!(
            matches!(
                error,
                crate::StateError::Mdbx(signet_libmdbx::MdbxError::Other(28))
            ),
            "{error}"
        );
        assert!(
            writer.snapshot_rx.watch().has_changed().is_err(),
            "the failed writer must close publication"
        );
        Fixture::assert_state(&writer.snapshot_rx.current().unwrap(), 1);
    }

    fn reopen_and_resume(&self) {
        let env = StateEnvBuilder::new("/dev/shm/state").open().unwrap();
        let recovered = read_recovery_point(&env).unwrap();
        assert_eq!(recovered.last_committed_block, 1);
        assert_eq!(recovered.last_committed_end_tx_position.as_index(), 1);
        assert_eq!(recovered.last_fsynced_reader_position.as_index(), 1);
        Self::assert_old(&StateSnapshot::open(&env).unwrap(), self.hash);
        let mut writer = StateWriter::spawn(env).unwrap();
        writer.delta_tx.send(self.batch.clone()).unwrap();
        let deadline = Instant::now().checked_add(BOUND).unwrap();
        let snapshot = std::iter::from_fn(|| writer.snapshot_rx.recv_deadline(deadline))
            .find(|snapshot| snapshot.block_number() == 2)
            .expect("writer must resume after freeing space");
        Fixture::assert_state(&snapshot, 2);
        assert_eq!(snapshot.basic(ADDR).unwrap().unwrap().2, self.hash);
        assert_eq!(
            snapshot.code_by_hash(self.hash).unwrap(),
            self.batch.delta.code[0].code
        );
        assert_eq!(
            snapshot.get_receipt(BPosition::from_index(2)).unwrap(),
            Some(self.batch.delta.receipts[0].clone())
        );
        assert_eq!(
            snapshot.get_tx_position(self.hash).unwrap(),
            Some(BPosition::from_index(2))
        );
        writer.shutdown().unwrap();
    }

    fn assert_old(snapshot: &StateSnapshot, hash: B256) {
        Fixture::assert_state(snapshot, 1);
        assert_eq!(snapshot.basic(ADDR).unwrap().unwrap().2, B256::ZERO);
        assert!(snapshot.code_by_hash(hash).unwrap().is_empty());
        assert!(
            snapshot
                .get_receipt(BPosition::from_index(2))
                .unwrap()
                .is_none()
        );
        assert!(snapshot.get_tx_position(hash).unwrap().is_none());
        assert!(snapshot.header(2).unwrap().is_none());
        assert_eq!(
            snapshot
                .storage(ADDR, B256::from(U256::from(2_000_000)))
                .unwrap(),
            U256::ZERO
        );
    }
}

#[test]
#[ignore = "requires Linux Docker and one disposable container with an eight-MiB tmpfs"]
fn unsynchronized_commit_failure_preserves_data_and_allows_retry() {
    Container::run(Case::Unsync);
}

#[test]
#[ignore = "child entry point; the container parent supplies an isolated tmpfs"]
fn unsync_full_child() {
    assert_eq!(std::env::var("KARDAMOM_CHECKPOINT_COMPONENT").unwrap(), "1");
    assert!(Path::new("/.dockerenv").exists());
    let env = StateEnvBuilder::new("/dev/shm/unsync").open().unwrap();
    {
        let txn = env.raw().begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        txn.put(
            db,
            b"fault-stable",
            b"prior",
            signet_libmdbx::WriteFlags::UPSERT,
        )
        .unwrap();
        txn.commit().unwrap();
    }
    Volume::fill();
    let payload = vec![0x5a; 1 << 20];
    {
        let txn = env.raw().begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        txn.put(
            db,
            b"fault-pending",
            &payload,
            signet_libmdbx::WriteFlags::UPSERT,
        )
        .unwrap();
        assert_eq!(
            txn.commit().unwrap_err(),
            signet_libmdbx::MdbxError::Other(28)
        );
    }
    Volume::clear();
    {
        let txn = env.raw().begin_ro_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        assert_eq!(
            txn.get::<Vec<u8>>(db.dbi(), b"fault-stable").unwrap(),
            Some(b"prior".to_vec())
        );
        assert_eq!(
            txn.get::<Vec<u8>>(db.dbi(), b"fault-pending").unwrap(),
            None
        );
    }
    {
        let txn = env.raw().begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        txn.put(
            db,
            b"fault-pending",
            &payload,
            signet_libmdbx::WriteFlags::UPSERT,
        )
        .unwrap();
        txn.commit().unwrap();
    }
    let txn = env.raw().begin_ro_unsync().unwrap();
    let db = txn.open_db(None).unwrap();
    assert_eq!(
        txn.get::<Vec<u8>>(db.dbi(), b"fault-pending").unwrap(),
        Some(payload)
    );
    println!("Unsync ENOSPC proof: commit=28 prior_value=1 retry=1");
}
