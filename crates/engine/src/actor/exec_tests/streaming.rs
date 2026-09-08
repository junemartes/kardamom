//! Streaming execution: a two-tx block's slim boundary, and same-block
//! deposit-to-transfer visibility.

use alloy_primitives::{U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_types::BlockBoundary;

use crate::exec_types::TxIndex;
use crate::reader::ReaderToExec;
use crate::state::StaticSnapshotSource;

use crate::actor::ExecToCommit;
use crate::actor::test_support::{
    ExecRig, ImmediateCommit, boundary_msg, feed, funded, legacy, pos, tx_msg,
};

#[test]
fn exec_runs_two_txs_and_emits_slim_boundary() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");

    let snap = funded(&signer, 0);

    // Two canonical records apply here, so the cumulative count is 2.
    // end_tx_idx encodes that count (pos(2) == BPosition::from_index(2)).
    let rx_r2e = feed(vec![
        tx_msg(&signer, to, 0, 0, 100),
        tx_msg(&signer, to, 1, 1, 50),
        boundary_msg(1, 2, 1_700_000_000),
    ]);

    let (rig, writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, _rx_e2c) = rig.spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    let log = writer_log.lock().unwrap();
    assert_eq!(log.len(), 1);
    let (boundary, delta) = &log[0];
    assert_eq!(boundary.block_number, 1);
    assert_eq!(boundary.end_tx_idx, pos(2));
    assert_eq!(boundary.l2_timestamp, 1_700_000_000);
    // The recipient gets 150 total from both transfers. Verify this by
    // iterating the canonical Vec<AccountChange> that the wire form holds.
    let to_acc = delta
        .accounts
        .iter()
        .find(|a| a.address == to)
        .expect("recipient");
    assert_eq!(to_acc.balance, U256::from(150u64));
    // The block's receipts travel inside the BlockDelta, in arrival order.
    // The writer persists them (receipts and tx_hash_index). This lets
    // eth_getTransactionReceipt answer from durable state after a restart.
    assert_eq!(delta.receipts.len(), 2, "both txs' receipts persisted");
    assert!(delta.receipts.iter().all(|r| r.block_number == 1));
    assert_eq!(delta.receipts[0].nonce, 0);
    assert_eq!(delta.receipts[1].nonce, 1);
    // This destructure is a regression guard. It forces the
    // compiler to check the shape of BlockBoundary, confirming no
    // state-root commitment field sneaks in. `l1_origin` is a deliberate
    // field: the L1 epoch this block belongs to. The guard also catches
    // any new field added without a spec behind it.
    let BlockBoundary {
        block_number: _,
        end_tx_idx: _,
        l1_origin: _,
        l2_timestamp: _,
    } = boundary;
}

/// This test checks scope-cache visibility across record kinds. A
/// deposit credits an account mid-block. A later transaction in the same
/// block spends that credit. The deposit runs outside the `ExecScope`, with
/// its own commit semantics, so its writes must fold into the scope cache
/// explicitly. Without this fold, the spend fails as an
/// insufficient-funds skip.
#[test]
fn deposit_credit_is_visible_to_later_txs_in_the_block() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000BEEF0");
    // The sender does not exist before the block. Only the deposit funds it.
    let snap = crate::state::MockStateDatabase::builder().build();

    let rx_r2e = feed(vec![
        ReaderToExec::Deposit {
            tx_idx: TxIndex(0),
            deposit: kardamom_types::Deposit {
                source_hash: alloy_primitives::B256::repeat_byte(0x11),
                from,
                to: Some(from),
                mint: 10u128.pow(18),
                value: U256::ZERO,
                gas_limit: 100_000,
                is_system_transaction: false,
                input: bytes::Bytes::default(),
            },
            position: pos(0),
        },
        ReaderToExec::Tx {
            tx_idx: TxIndex(1),
            envelope: legacy(&signer, to, 1, 1_000),
            position: pos(64),
        },
        boundary_msg(1, 2, 1_700_000_000),
    ]);

    let (rig, _writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, rx_e2c) = rig.spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    // The transfer must execute (status true), not skip for missing funds.
    // This shows the deposit's credit reached the scope cache.
    let saw_transfer_success = rx_e2c.try_iter().any(|msg| {
        if let ExecToCommit::Receipt(r) = msg
            && r.tx_idx == pos(64)
        {
            assert!(
                r.status,
                "transfer after same-block deposit must execute: {r:?}"
            );
            assert!(r.gas_used > 0);
            true
        } else {
            false
        }
    });
    assert!(saw_transfer_success, "transfer receipt not observed");
}
