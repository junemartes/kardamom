//! Kill the real writer before commit and after commit but before snapshot publication.

#![cfg(unix)]

mod common;
#[path = "writer_crash/support.rs"]
mod support;

use alloy_primitives::{Address, B256, U256};
use kardamom_state::{StateEnvBuilder, StateSnapshot, StateWriter, read_recovery_point};
use kardamom_types::StateDatabase;
use support::{CrashChild, Cut};

const ACCOUNT: Address = Address::repeat_byte(0x42);

#[test]
fn process_loss_before_commit_discards_the_entire_uncommitted_block() {
    CrashChild::run(Cut::Accounts).verify(1);
}

#[test]
fn process_loss_after_commit_preserves_a_block_without_a_published_snapshot() {
    CrashChild::run(Cut::Committed).verify(2);
}

#[test]
#[ignore = "child entry point; the parent supplies owned storage and kills this process"]
fn writer_crash_child() {
    let root = std::env::var_os("KARDAMOM_WRITER_CRASH_DIR").unwrap();
    let root = std::path::PathBuf::from(root);
    let cut = Cut::parse(&std::env::var("KARDAMOM_WRITER_CRASH_CUT").unwrap());
    let gate = support::Recorder::install(root.join("cut"), cut);
    let env = StateEnvBuilder::new(root.join("state")).open().unwrap();
    let writer = StateWriter::spawn(env).unwrap();
    common::commit_block(&writer, 1, ACCOUNT, 101, 7, 1001);
    gate.arm();
    writer
        .delta_tx
        .send(common::simple_delta(2, ACCOUNT, 102, 7, 1002))
        .unwrap();
    common::wait_for_block(&writer, 2);
    panic!("the parent must kill the writer before snapshot publication");
}

impl CrashChild {
    fn verify(&self, block: u64) {
        let env = StateEnvBuilder::new(self.root().join("state"))
            .open()
            .unwrap();
        let recovered = read_recovery_point(&env).unwrap();
        assert_eq!(recovered.last_committed_block, block);
        assert_eq!(
            recovered.last_committed_end_tx_position,
            common::bpos(block)
        );
        assert_eq!(recovered.last_fsynced_reader_position, common::bpos(block));
        let snapshot = StateSnapshot::open(&env).unwrap();
        Self::assert_snapshot(&snapshot, block);
        assert_eq!(snapshot.header(2).unwrap().is_some(), block == 2);
        assert_eq!(
            snapshot.get_receipt(common::bpos(2)).unwrap().is_some(),
            block == 2
        );
        assert_eq!(
            snapshot.get_tx_position(B256::from(U256::from(2))).unwrap(),
            (block == 2).then(|| common::bpos(2))
        );

        let mut writer = StateWriter::spawn(env).unwrap();
        let next = block.checked_add(1).unwrap();
        let next_snapshot = common::commit_block(
            &writer,
            next,
            ACCOUNT,
            100_u64.checked_add(next).unwrap(),
            7,
            1000_u64.checked_add(next).unwrap(),
        );
        Self::assert_snapshot(&next_snapshot, next);
        writer.shutdown().unwrap();
    }

    fn assert_snapshot(snapshot: &StateSnapshot, block: u64) {
        assert_eq!(snapshot.block_number(), block);
        assert_eq!(snapshot.end_tx_position().unwrap(), common::bpos(block));
        assert_eq!(
            snapshot.basic(ACCOUNT).unwrap(),
            Some((
                block,
                U256::from(100_u64.checked_add(block).unwrap()),
                B256::ZERO
            ))
        );
        assert_eq!(
            snapshot.storage(ACCOUNT, common::slot_key(7)).unwrap(),
            U256::from(1000_u64.checked_add(block).unwrap())
        );
        let expected = common::simple_delta(block, ACCOUNT, 0, 7, 0).delta.receipts[0].clone();
        assert_eq!(
            snapshot.get_receipt(common::bpos(block)).unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            snapshot.get_tx_position(expected.tx_hash).unwrap(),
            Some(common::bpos(block))
        );
    }
}
