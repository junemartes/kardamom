//! Interop P1: remote epochs execute as 0x7D deliveries.

use std::sync::{Arc, Mutex};

use crate::error::ExecutorError;
use crate::state::{MockStateDatabase, StaticSnapshotSource};

use crate::actor::ExecToCommit;
use crate::actor::test_support::{
    ExecRig, ImmediateCommit, boundary_msg, feed, remote_epoch_fixture, remote_epoch_records,
};

struct RecordingRemoteObserver(Arc<Mutex<Vec<u64>>>);
impl crate::reader::RemoteEpochObserver for RecordingRemoteObserver {
    fn observe(
        &mut self,
        rec: &kardamom_types::xchain::RemoteEpochRecord,
    ) -> Result<(), ExecutorError> {
        self.0.lock().unwrap().push(rec.origin_chain_id);
        Ok(())
    }
}

/// The marker consumes a slot but applies no tx; each message executes as a
/// 0x7D receipt keyed by its remote source hash, from the aliased origin
/// Outbox — and the observer seam fires on the marker, before the messages.
#[test]
fn remote_epoch_messages_execute_as_0x7d_receipts() {
    use kardamom_types::xchain;

    let origin: u64 = 424_242;
    let record = remote_epoch_fixture(origin, 2);

    let snap = MockStateDatabase::builder().build();
    let mut records = remote_epoch_records(record);
    // Marker + 2 messages = 3 slots consumed.
    records.push(boundary_msg(1, 3, 1_700_000_000));
    let rx_r2e = feed(records);

    let observed = Arc::new(Mutex::new(Vec::new()));
    let (rig, writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, rx_e2c) = rig
        .remote(Box::new(RecordingRemoteObserver(observed.clone())))
        .spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    let receipts: Vec<_> = rx_e2c
        .try_iter()
        .filter_map(|m| match m {
            ExecToCommit::Receipt(r) => Some(r),
            ExecToCommit::Boundary(_) => None,
        })
        .collect();
    assert_eq!(
        receipts.len(),
        2,
        "marker applies no tx; each message applies one"
    );
    for (seq, r) in receipts.iter().enumerate() {
        assert_eq!(r.tx_type, kardamom_types::TX_TYPE_XCHAIN);
        assert_eq!(r.tx_hash, xchain::remote_source_hash(origin, seq as u64));
        assert!(r.status);
        assert_eq!(r.from, xchain::xchain_tx_sender(origin));
        assert_eq!(r.to, Some(xchain::INBOX));
    }
    assert_eq!(
        *observed.lock().unwrap(),
        vec![origin],
        "observer fired once, on the marker"
    );
    // The block's receipts also persist through the writer, like any tx's.
    let log = writer_log.lock().unwrap();
    assert_eq!(log[0].1.receipts.len(), 2);
}

/// Whole-block execution (the validator's parallel path) BUFFERS cross-chain
/// messages like deposits and hands them to the strategy at the boundary.
/// The strategy here dispatches through `execute_xchain_tx` exactly as the
/// streaming path does, and the receipts land on the commit channel
/// unchanged.
#[test]
fn whole_block_strategy_receives_buffered_xchain_records() {
    use crate::actor::types::{BlockExec, BlockExecOutput, BufferedRecord};
    use crate::delta::PendingDelta;
    use crate::executor::execute_xchain_tx;
    use kardamom_types::xchain;

    let origin: u64 = 424_242;
    let record = remote_epoch_fixture(origin, 2);

    let snap = MockStateDatabase::builder().build();
    let mut records = remote_epoch_records(record);
    // marker + 2 messages = 3 slots
    records.push(boundary_msg(1, 3, 1_700_000_000));
    let rx_r2e = feed(records);

    // A minimal whole-block strategy: record what arrived, execute the
    // XChain arms sequentially through the shared executor entry point.
    let seen_kinds = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let seen = seen_kinds.clone();
    let strategy: BlockExec<MockStateDatabase> =
        Box::new(move |snapshot, _parent, records, env, _block| {
            let mut receipts = Vec::new();
            let mut delta = PendingDelta::new();
            let mut cumulative = 0u64;
            for (i, rec) in records.iter().enumerate() {
                match rec {
                    BufferedRecord::Tx { .. } => seen.lock().unwrap().push("tx"),
                    BufferedRecord::Deposit { .. } => seen.lock().unwrap().push("deposit"),
                    BufferedRecord::XChain {
                        tx_idx,
                        origin_chain_id,
                        message,
                        position,
                    } => {
                        seen.lock().unwrap().push("xchain");
                        let (r, ws) = execute_xchain_tx(
                            snapshot,
                            None,
                            &delta,
                            env,
                            *tx_idx,
                            *position,
                            *origin_chain_id,
                            message,
                            i as u64,
                            cumulative,
                            None,
                        )?;
                        cumulative = r.cumulative_gas_used;
                        delta.apply(ws);
                        receipts.push(r);
                    }
                }
            }
            Ok(BlockExecOutput {
                receipts,
                delta,
                bal: None,
            })
        });

    let (rig, _writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, rx_e2c) = rig.block_exec(strategy).spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    assert_eq!(
        *seen_kinds.lock().unwrap(),
        vec!["xchain", "xchain"],
        "the strategy must receive the buffered 0x7D records (marker excluded)"
    );
    let receipts: Vec<_> = rx_e2c
        .try_iter()
        .filter_map(|m| match m {
            ExecToCommit::Receipt(r) => Some(r),
            ExecToCommit::Boundary(_) => None,
        })
        .collect();
    assert_eq!(receipts.len(), 2);
    for (seq, r) in receipts.iter().enumerate() {
        assert_eq!(r.tx_type, kardamom_types::TX_TYPE_XCHAIN);
        assert_eq!(r.tx_hash, xchain::remote_source_hash(origin, seq as u64));
    }
}

struct RejectingRemoteObserver;
impl crate::reader::RemoteEpochObserver for RejectingRemoteObserver {
    fn observe(
        &mut self,
        rec: &kardamom_types::xchain::RemoteEpochRecord,
    ) -> Result<(), ExecutorError> {
        Err(ExecutorError::State(format!(
            "remote epoch rejected (origin {})",
            rec.origin_chain_id
        )))
    }
}

/// A rejected record fail-stops on the MARKER — before any of its messages
/// execute — the same posture as a rejected L1 epoch.
#[test]
fn rejected_remote_epoch_halts_before_messages_execute() {
    let record = remote_epoch_fixture(424_242, 2);
    let snap = MockStateDatabase::builder().build();
    let rx_r2e = feed(remote_epoch_records(record));

    let (rig, _writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, rx_e2c) = rig.remote(Box::new(RejectingRemoteObserver)).spawn(rx_r2e);
    let res = h.join().expect("no panic");
    assert!(matches!(res, Err(ExecutorError::State(_))), "got {res:?}");
    assert!(
        !rx_e2c
            .try_iter()
            .any(|m| matches!(m, ExecToCommit::Receipt(_))),
        "no message may execute after its record is rejected"
    );
}
