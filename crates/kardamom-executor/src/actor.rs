//! Executor actor: M tx_data reader threads + 1 tx_ordering reader thread +
//! sequential execution thread + commit thread.
//!
//! ## Topology change (S4-arch-update, D-Sh12 / spec §2.4)
//!
//! Pre-S4-arch-update there was **one** tx_ordering reader thread that pulled
//! full `TxEnvelope`s off tx_ordering. Post-D-Sh12 the inbound demux is split:
//!
//! - **M tx_data reader threads** (one per sequencer partition) each
//!   subscribe to their tx_data and stream full `TxEnvelope`s into a shared
//!   **join buffer** keyed by `(sequencer_id, tx_data_position)`.
//! - **One tx_ordering reader thread** pulls tiny `TxOrderingMessage` records
//!   (`TxRef | BoundaryStart`) in canonical order. For each `TxRef`, it joins
//!   against the buffer and hands `(b_position, TxEnvelope)` to the exec
//!   thread. For each `BoundaryStart`, it forwards verbatim.
//!
//! The exec thread, commit thread, state-snapshot swap protocol, write-set
//! hashing, and tx_receipts emission are **unchanged** — the executor's
//! external contract (consume canonical-ordered txs + boundaries, produce
//! ordered receipts + slim boundaries on tx_receipts) is identical. Only the
//! inbound demux moves.
//!
//! See `reader.rs` for the join-buffer + reader-thread implementation.
//!
//! Wiring:
//! ```text
//!   tx_data[0..M]    tx_ordering
//!        │                │
//!        ▼                ▼
//!   ┌─────────┐     ┌──────────┐
//!   │M readers│──►  │B reader  │──► exec ──► commit ──► tx_receipts
//!   │ (insert │join │(lookup+  │
//!   │ buffer) │buf  │ forward) │
//!   └─────────┘     └──────────┘
//! ```
//!
//! Each Aeron-touching thread (the M+1 reader threads in production) owns
//! its own `rusteron_client::Aeron` (`!Send + !Sync`) on a dedicated OS
//! thread; cross-thread coordination uses the `DashMap` join buffer and
//! crossbeam channels.

use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::debug;

use kardamom_types::{BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, SnapshotSource};

use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::error::ExecutorError;
use crate::executor::execute_tx;
use crate::reader::{
    JoinBuffer, ReaderConfig, ReaderToExec, TxDataSubscription, TxOrderingSubscription,
    spawn_tx_data_reader, spawn_tx_ordering_reader,
};
use crate::types::{CMessage, TxIndex};

/// Publication handle for tx_receipts.
pub trait ChannelCPublication: Send {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError>;
}

/// Signal from the state writer (S6): "block N is durable in mdbx; you may
/// swap to a snapshot >= N."
pub trait StateWriterSignal: Send {
    /// Block until the state writer reports a block number >= `await_at_least`
    /// has been committed. Returns the committed block number.
    fn wait_committed(&mut self, await_at_least: u64) -> Result<u64, ExecutorError>;
}

/// Hand-off queue from executor → state writer. The state writer (S6)
/// consumes these to apply the block delta to libmdbx.
pub trait StateWriterQueue: Send {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError>;
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub chain_id: u64,
    /// Bound on the receipt queue between exec and commit threads. Larger =
    /// more amortization, more memory.
    pub receipt_queue_depth: usize,
    /// Reader-layer tunables (join buffer timeout, growth warning
    /// threshold). See [`ReaderConfig`].
    pub reader: ReaderConfig,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            chain_id: 1,
            receipt_queue_depth: 1024,
            reader: ReaderConfig::default(),
        }
    }
}

/// Internal envelope routed from exec → commit thread.
enum ExecToCommit {
    Receipt(kardamom_types::Receipt),
    Boundary(BlockBoundary),
}

/// Owns the M+3 threads (M tx_data readers, 1 tx_ordering reader, 1 exec, 1
/// commit). `run` blocks until the tx_ordering subscription closes or an
/// error occurs.
pub struct Executor;

impl Executor {
    /// Spawn the readers, exec, commit threads and join them. Returns when
    /// tx_ordering closes cleanly or when any thread propagates a fatal
    /// error.
    ///
    /// `a_subs` holds one subscription per sequencer partition (M total).
    /// They may be supplied in any order — each subscription declares its
    /// own `sequencer_id`.
    #[allow(clippy::too_many_arguments)] // 8 args is the natural shape of the
    // executor's run-once API (config, M A-subs, B-sub, C-pub, snapshots,
    // state-writer signal, state-writer queue, initial block); packaging
    // them into a builder struct would shuffle the noise around without
    // reducing it.
    pub fn run<C, S, Q, P>(
        cfg: ExecutorConfig,
        a_subs: Vec<Box<dyn TxDataSubscription>>,
        b_sub: Box<dyn TxOrderingSubscription>,
        c_pub: C,
        snapshots: S,
        sw_signal: Q,
        sw_queue: P,
        initial_block: u64,
    ) -> Result<(), ExecutorError>
    where
        C: ChannelCPublication + 'static,
        S: SnapshotSource + 'static,
        Q: StateWriterSignal + 'static,
        P: StateWriterQueue + 'static,
    {
        let buffer = JoinBuffer::new();
        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(cfg.receipt_queue_depth);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(cfg.receipt_queue_depth);

        // M tx_data reader threads, one per sequencer partition. Each
        // owns its subscription for the duration; we collect the join
        // handles to surface any error.
        let mut a_handles: Vec<JoinHandle<Result<(), ExecutorError>>> =
            Vec::with_capacity(a_subs.len());
        for a in a_subs {
            // The trait object's `next` already advertises sequencer_id.
            a_handles.push(spawn_tx_data_reader(BoxedASub(a), buffer.clone()));
        }

        let b_handle =
            spawn_tx_ordering_reader(BoxedBSub(b_sub), buffer.clone(), cfg.reader.clone(), tx_r2e);

        let exec = spawn_exec(
            cfg.clone(),
            rx_r2e,
            tx_e2c,
            snapshots,
            sw_signal,
            sw_queue,
            initial_block,
        );
        let commit = spawn_commit(c_pub, rx_e2c);

        // Join in this order: B reader (closes first when tx_ordering is
        // exhausted), then exec, then commit, then A readers. TxData
        // subscriptions may keep producing after tx_ordering closes; we let
        // them drain to clean shutdown. Errors from any thread propagate;
        // the first error wins but every join still runs so threads tear
        // down cleanly.
        let r_b = b_handle.join().expect("tx_ordering reader panic");
        let r_exec = exec.join().expect("exec panic");
        let r_commit = commit.join().expect("commit panic");
        let mut r_a: Result<(), ExecutorError> = Ok(());
        for h in a_handles {
            let res = h.join().expect("tx_data reader panic");
            if r_a.is_ok() {
                r_a = res;
            }
        }
        r_b.and(r_exec).and(r_commit).and(r_a)
    }
}

// Trait-object adapters so the reader fns (generic on a concrete type) can
// own a `Box<dyn TxDataSubscription>` / `Box<dyn TxOrderingSubscription>`
// without requiring the caller to monomorphise per-M.
struct BoxedASub(Box<dyn TxDataSubscription>);
impl TxDataSubscription for BoxedASub {
    fn sequencer_id(&self) -> u8 {
        self.0.sequencer_id()
    }
    fn next(&mut self) -> Result<(BPosition, kardamom_types::TxEnvelope), ExecutorError> {
        self.0.next()
    }
}

struct BoxedBSub(Box<dyn TxOrderingSubscription>);
impl TxOrderingSubscription for BoxedBSub {
    fn next(&mut self) -> Result<(BPosition, kardamom_types::TxOrderingMessage), ExecutorError> {
        self.0.next()
    }
}

fn spawn_exec<S, Q, P>(
    cfg: ExecutorConfig,
    rx: Receiver<ReaderToExec>,
    tx: Sender<ExecToCommit>,
    snapshots: S,
    mut sw_signal: Q,
    mut sw_queue: P,
    initial_block: u64,
) -> JoinHandle<Result<(), ExecutorError>>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
{
    thread::Builder::new()
        .name("executor-exec".into())
        .spawn(move || -> Result<(), ExecutorError> {
            // The snapshot source hands back owned snapshots keyed by block
            // number. We always open the snapshot for the block *just
            // committed* (initial_block at startup; whatever the writer
            // signals after each boundary).
            let mut snapshot = snapshots.snapshot_after(initial_block);
            let mut delta = PendingDelta::new();
            // Block-number bookkeeping. We treat blocks 1-indexed (genesis
            // is block 0). The exec thread assumes every block boundary it
            // sees is for the *current* in-flight block; it doesn't try to
            // re-derive block numbers without sealer help.
            let mut current_block = initial_block + 1;
            let mut current_l2_ts: u64 = 0;
            // Last tx_ordering position the exec thread folded into a
            // receipt. Used to validate alignment with
            // `BlockBoundaryStart.end_tx_idx`.
            let mut last_processed_position: Option<BPosition> = None;
            // Sanity: tx_idx assigned by the tx_ordering reader is monotone.
            let mut expected_tx_idx = TxIndex::ZERO;

            loop {
                let msg = match rx.recv() {
                    Ok(m) => m,
                    Err(_) => return Ok(()),
                };
                match msg {
                    ReaderToExec::Tx {
                        tx_idx,
                        envelope,
                        position,
                    } => {
                        if tx_idx != expected_tx_idx {
                            return Err(ExecutorError::OutOfOrderTx {
                                got: tx_idx,
                                expected: expected_tx_idx,
                            });
                        }
                        expected_tx_idx = expected_tx_idx.next();
                        let env = ExecEnv {
                            chain_id: cfg.chain_id,
                            block_number: current_block,
                            l2_timestamp: current_l2_ts,
                        };
                        let (receipt, ws) =
                            execute_tx(&snapshot, &delta, env, tx_idx, position, &envelope)?;
                        delta.apply(ws);
                        last_processed_position = Some(position);
                        if tx.send(ExecToCommit::Receipt(receipt)).is_err() {
                            return Ok(());
                        }
                    }
                    ReaderToExec::Boundary(BlockBoundaryStart {
                        block_number,
                        end_tx_idx,
                        l2_timestamp,
                    }) => {
                        // Alignment: BlockBoundaryStart.end_tx_idx is a
                        // BPosition identifying the LAST tx_ordering record
                        // that belongs to the closing block. It must match
                        // the executor's most recent processed position.
                        if let Some(lp) = last_processed_position
                            && lp != end_tx_idx
                        {
                            return Err(ExecutorError::BoundaryMisaligned {
                                end: end_tx_idx,
                                last_seen: lp,
                            });
                        }

                        // S0 D-Sh11: NO state-root computation. The sealed
                        // BlockBoundary on tx_receipts is slim — three
                        // fields, no commitment.
                        let boundary = BlockBoundary {
                            block_number,
                            end_tx_idx,
                            l2_timestamp,
                        };

                        // Drain the delta. We swap it out so the writer
                        // owns it. The PendingDelta becomes a BlockDelta
                        // here; receipts are carried separately on
                        // tx_receipts, so the BlockDelta the writer
                        // receives has an empty receipts vec.
                        let pending = std::mem::take(&mut delta);
                        let bd: BlockDelta = pending.finalize(block_number);
                        sw_queue.submit(boundary.clone(), bd)?;

                        if tx.send(ExecToCommit::Boundary(boundary)).is_err() {
                            return Ok(());
                        }

                        // Wait for the writer to durably commit.
                        let committed = sw_signal.wait_committed(block_number)?;
                        debug!(
                            target: "executor",
                            committed,
                            block_number,
                            "snapshot-swap: writer caught up"
                        );

                        // Snapshot swap: open the new snapshot, drop the
                        // old. The trait returns an owned value.
                        snapshot = snapshots.snapshot_after(block_number);
                        current_block = block_number + 1;
                        // The next block's wall-clock timestamp arrives in
                        // its own BlockBoundaryStart; until then we keep
                        // the previous value as a deterministic
                        // placeholder for any txs that race ahead of the
                        // sealer (in v0 the sealer is single-leader so
                        // this branch is purely defensive).
                        current_l2_ts = l2_timestamp;
                    }
                }
            }
        })
        .expect("spawn exec")
}

fn spawn_commit<C>(
    mut c_pub: C,
    rx: Receiver<ExecToCommit>,
) -> JoinHandle<Result<(), ExecutorError>>
where
    C: ChannelCPublication + 'static,
{
    thread::Builder::new()
        .name("executor-commit".into())
        .spawn(move || -> Result<(), ExecutorError> {
            loop {
                let msg = match rx.recv() {
                    Ok(m) => m,
                    Err(_) => return Ok(()),
                };
                let c_msg = match msg {
                    ExecToCommit::Receipt(r) => CMessage::Receipt(r),
                    ExecToCommit::Boundary(b) => CMessage::BlockBoundary(b),
                };
                c_pub.publish(c_msg)?;
            }
        })
        .expect("spawn commit")
}

#[cfg(test)]
mod exec_tests {
    use super::*;
    use crate::reader::ReaderToExec;
    use crate::state::{MockStateDatabase, StaticSnapshotSource};
    use crate::types::TxIndex;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{
        Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, address, keccak256,
    };
    use alloy_signer_local::PrivateKeySigner;
    use bytes::Bytes;
    use kardamom_types::{BPosition, TxEnvelope as KtTxEnvelope};
    use revm::primitives::KECCAK_EMPTY;
    use std::sync::{Arc, Mutex};

    fn pos(off: i32) -> BPosition {
        BPosition {
            term_id: 0,
            term_offset: off,
        }
    }

    fn legacy(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64) -> KtTxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: APTxKind::Call(to),
            value: U256::from(value),
            input: AlloyBytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        KtTxEnvelope {
            correlation_id: 0,
            raw_tx,
            sender: signer.address(),
            tx_hash,
        }
    }

    struct ImmediateCommit;
    impl StateWriterSignal for ImmediateCommit {
        fn wait_committed(&mut self, at_least: u64) -> Result<u64, ExecutorError> {
            Ok(at_least)
        }
    }
    struct RecordingQueue(Arc<Mutex<Vec<(BlockBoundary, BlockDelta)>>>);
    impl StateWriterQueue for RecordingQueue {
        fn submit(&mut self, b: BlockBoundary, d: BlockDelta) -> Result<(), ExecutorError> {
            self.0.lock().unwrap().push((b, d));
            Ok(())
        }
    }

    #[test]
    fn exec_runs_two_txs_and_emits_slim_boundary() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("00000000000000000000000000000000000ABCDE");

        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let writer_log = Arc::new(Mutex::new(Vec::new()));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(0),
                envelope: legacy(&signer, to, 0, 100),
                position: pos(0),
            })
            .unwrap();
        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(1),
                envelope: legacy(&signer, to, 1, 50),
                position: pos(1),
            })
            .unwrap();
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: pos(1),
                l2_timestamp: 1_700_000_000,
            }))
            .unwrap();
        drop(tx_r2e);

        let cfg = ExecutorConfig::default();
        let h = spawn_exec(
            cfg,
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            ImmediateCommit,
            RecordingQueue(writer_log.clone()),
            0,
        );
        h.join().expect("no panic").expect("exec ok");
        drop(rx_e2c);

        let log = writer_log.lock().unwrap();
        assert_eq!(log.len(), 1);
        let (boundary, delta) = &log[0];
        assert_eq!(boundary.block_number, 1);
        assert_eq!(boundary.end_tx_idx, pos(1));
        assert_eq!(boundary.l2_timestamp, 1_700_000_000);
        // The recipient received 150 total across both transfers — verify
        // by iterating the canonical Vec<AccountChange> the wire form holds.
        let to_acc = delta
            .accounts
            .iter()
            .find(|a| a.address == to)
            .expect("recipient");
        assert_eq!(to_acc.balance, U256::from(150u64));
        // S0 D-Sh11 regression guard: destructure to enforce the 3-field
        // shape of BlockBoundary at compile time.
        let BlockBoundary {
            block_number: _,
            end_tx_idx: _,
            l2_timestamp: _,
        } = boundary;
    }

    #[test]
    fn exec_rejects_misaligned_boundary() {
        let writer_log = Arc::new(Mutex::new(Vec::new()));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
        let (tx_e2c, _rx_e2c) = bounded::<ExecToCommit>(8);

        let signer = PrivateKeySigner::random();
        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(0),
                envelope: legacy(&signer, Address::from([0x22u8; 20]), 0, 0),
                position: pos(0),
            })
            .unwrap();
        // Boundary claims end_tx_idx at offset 5 but we only processed offset 0.
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: pos(5),
                l2_timestamp: 0,
            }))
            .unwrap();
        drop(tx_r2e);

        // Pre-fund the signer so the tx doesn't fail before we hit the boundary.
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                KECCAK_EMPTY,
            )
            .build();

        let cfg = ExecutorConfig::default();
        let h = spawn_exec(
            cfg,
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            ImmediateCommit,
            RecordingQueue(writer_log),
            0,
        );
        let res = h.join().expect("no panic");
        assert!(matches!(res, Err(ExecutorError::BoundaryMisaligned { .. })));
    }
}

#[cfg(test)]
mod commit_tests {
    use super::*;
    use alloy_primitives::B256;
    use kardamom_types::{BPosition, BlockBoundary, Receipt};
    use std::sync::{Arc, Mutex};

    struct RecordPub(Arc<Mutex<Vec<CMessage>>>);
    impl ChannelCPublication for RecordPub {
        fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
            self.0.lock().unwrap().push(msg);
            Ok(())
        }
    }

    #[test]
    fn commit_thread_preserves_order() {
        let (tx, rx) = bounded::<ExecToCommit>(8);
        let log = Arc::new(Mutex::new(Vec::new()));
        let pos0 = BPosition {
            term_id: 0,
            term_offset: 0,
        };

        tx.send(ExecToCommit::Receipt(Receipt {
            tx_idx: pos0,
            tx_hash: B256::repeat_byte(0xAA),
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            write_set_hash: B256::ZERO,
        }))
        .unwrap();
        tx.send(ExecToCommit::Boundary(BlockBoundary {
            block_number: 1,
            end_tx_idx: pos0,
            l2_timestamp: 100,
        }))
        .unwrap();
        drop(tx);

        let h = spawn_commit(RecordPub(log.clone()), rx);
        h.join().expect("no panic").expect("ok");

        let l = log.lock().unwrap();
        assert_eq!(l.len(), 2);
        assert!(matches!(&l[0], CMessage::Receipt(r) if r.tx_idx == pos0));
        assert!(matches!(&l[1], CMessage::BlockBoundary(b) if b.block_number == 1));
    }
}
