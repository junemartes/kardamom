//! Interop P1: remote epochs execute as 0x7D deliveries.

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use crate::error::ExecutorError;
use crate::state::{MockStateDatabase, StaticSnapshotSource};

use crate::actor::ExecToCommit;
use crate::actor::test_support::{
    ExecRig, ImmediateCommit, boundary_msg, feed, remote_epoch_fixture, remote_epoch_records,
};

struct RecordingRemoteObserver(Arc<Mutex<Vec<u64>>>);
impl<S: kardamom_types::StateDatabase> crate::reader::RemoteEpochObserver<S>
    for RecordingRemoteObserver
{
    fn observe(
        &mut self,
        rec: &kardamom_types::xchain::RemoteEpochRecord,
        parent: &crate::delta::ParentState<'_, S>,
    ) -> Result<(), ExecutorError> {
        // The seam gives the observer a parent-state read. A fresh chain
        // reads zero for the Inbox lane cursor.
        let next_seq = parent.storage(
            kardamom_types::xchain::INBOX,
            kardamom_types::xchain::Inbox::next_seq_slot(rec.origin_chain_id),
        )?;
        assert_eq!(next_seq, alloy_primitives::U256::ZERO);
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
    let record = remote_epoch_fixture(origin, NonZeroU64::new(2).expect("2 is nonzero"));

    let snap = MockStateDatabase::builder().build();
    let mut records = remote_epoch_records(record);
    // Marker + 2 messages = 3 slots consumed.
    records.push(boundary_msg(1, 3, 1_700_000_000));
    let rx_r2e = feed(records);

    let observed = Arc::new(Mutex::new(Vec::new()));
    let (rig, writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, rx_e2c) = rig
        .remote(RecordingRemoteObserver(observed.clone()))
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

/// A minimal whole-block strategy for
/// [`whole_block_strategy_receives_buffered_xchain_records`]: it records
/// what arrived, and executes the `XChain` arms sequentially through the
/// shared executor entry point, exactly as the streaming path does.
struct RecordingXChainStrategy {
    seen: Arc<Mutex<Vec<&'static str>>>,
}

impl crate::actor::BlockExecStrategy<MockStateDatabase> for RecordingXChainStrategy {
    fn execute_block(
        &self,
        snapshot: &MockStateDatabase,
        _parent: Option<&crate::delta::PendingDelta>,
        records: &[crate::actor::types::BufferedRecord],
        env: crate::block_env::ExecEnv,
        _block_number: u64,
    ) -> Result<crate::actor::types::BlockExecOutput, ExecutorError> {
        let (delta, receipts, _) = records.iter().enumerate().try_fold(
            (crate::delta::PendingDelta::new(), Vec::new(), 0u64),
            |(mut delta, mut receipts, cumulative), (i, rec)| {
                let Some((r, ws)) =
                    self.apply_one(snapshot, &delta, env, i as u64, cumulative, rec)?
                else {
                    return Ok((delta, receipts, cumulative));
                };
                let cumulative = r.cumulative_gas_used;
                delta.apply(ws);
                receipts.push(r);
                Ok::<_, ExecutorError>((delta, receipts, cumulative))
            },
        )?;
        Ok(crate::actor::types::BlockExecOutput {
            receipts,
            delta,
            bal: None,
        })
    }
}

impl RecordingXChainStrategy {
    /// Handle one buffered record. `Tx` and `Deposit` only mark their kind
    /// seen. `XChain` also executes, and returns its receipt and write set
    /// for the caller to fold into the block's delta.
    fn apply_one(
        &self,
        snapshot: &MockStateDatabase,
        delta: &crate::delta::PendingDelta,
        env: crate::block_env::ExecEnv,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
        rec: &crate::actor::types::BufferedRecord,
    ) -> Result<Option<(kardamom_types::Receipt, crate::delta::WriteSet)>, ExecutorError> {
        use crate::actor::types::BufferedRecord;
        use crate::executor::execute_xchain_tx;

        match rec {
            BufferedRecord::Tx { .. } => {
                self.seen.lock().unwrap().push("tx");
                Ok(None)
            }
            BufferedRecord::Deposit { .. } => {
                self.seen.lock().unwrap().push("deposit");
                Ok(None)
            }
            BufferedRecord::XChain {
                tx_idx,
                origin_chain_id,
                message,
                position,
            } => {
                self.seen.lock().unwrap().push("xchain");
                let slot = kardamom_exec_core::exec_types::TxSlot {
                    tx_idx: *tx_idx,
                    tx_position: *position,
                    tx_index_in_block,
                    cumulative_gas_used_before,
                };
                let delivery = kardamom_exec_core::executor::XChainDelivery {
                    origin_chain_id: *origin_chain_id,
                    message,
                };
                execute_xchain_tx(snapshot, None, delta, env, slot, delivery, None).map(Some)
            }
        }
    }
}

/// Whole-block execution (the validator's parallel path) BUFFERS cross-chain
/// messages like deposits and hands them to the strategy at the boundary.
/// The strategy here dispatches through `execute_xchain_tx` exactly as the
/// streaming path does, and the receipts land on the commit channel
/// unchanged.
#[test]
fn whole_block_strategy_receives_buffered_xchain_records() {
    use kardamom_types::xchain;

    let origin: u64 = 424_242;
    let record = remote_epoch_fixture(origin, NonZeroU64::new(2).expect("2 is nonzero"));

    let snap = MockStateDatabase::builder().build();
    let mut records = remote_epoch_records(record);
    // marker + 2 messages = 3 slots
    records.push(boundary_msg(1, 3, 1_700_000_000));
    let rx_r2e = feed(records);

    let seen_kinds = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let strategy = RecordingXChainStrategy {
        seen: seen_kinds.clone(),
    };

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
impl<S: kardamom_types::StateDatabase> crate::reader::RemoteEpochObserver<S>
    for RejectingRemoteObserver
{
    fn observe(
        &mut self,
        rec: &kardamom_types::xchain::RemoteEpochRecord,
        _parent: &crate::delta::ParentState<'_, S>,
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
    let record = remote_epoch_fixture(424_242, NonZeroU64::new(2).expect("2 is nonzero"));
    let snap = MockStateDatabase::builder().build();
    let rx_r2e = feed(remote_epoch_records(record));

    let (rig, _writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, rx_e2c) = rig.remote(RejectingRemoteObserver).spawn(rx_r2e);
    let res = h.join().expect("no panic");
    assert!(matches!(res, Err(ExecutorError::State(_))), "got {res:?}");
    assert!(
        !rx_e2c
            .try_iter()
            .any(|m| matches!(m, ExecToCommit::Receipt(_))),
        "no message may execute after its record is rejected"
    );
}
