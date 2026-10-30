//! A carry validator that stops while the chain runs on. It reads only
//! the egress. While it is stopped, the batcher posts past its cursor, and
//! the egress prunes the blocks below its reader window. The window still
//! holds the cursor of the validator. When the validator starts again, it
//! reads the range from the egress and verifies each block after its
//! cursor against the write-set and the receipts of the executor. It meets
//! no refused replay, adopts no peer checkpoint, and proves no divergence.
//!
//! The validator is the real engine pipeline with the real validator seams
//! and the real egress store, owner thread and HTTP side. The executor is a
//! controlled neighbor: a run of the same pipeline that records the
//! write-set of each block and each receipt, as the executor publishes
//! them on `tx_bal` and `tx_receipts`.

use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::{Receiver, Sender};
use kardamom_engine::actor::fixtures::{
    ChanTxDataSub, Imm, LegacyTx, Retention, TestEgress, TestEntry, TestStream,
};
use kardamom_engine::reader::carried::{CarriedSubscription, EgressTransport, NoSource};
use kardamom_engine::reader::cluster::ReplayCursor;
use kardamom_engine::{
    BlockBoundary, BlockBoundaryStart, BlockDelta, BPosition, CMessage, EngineWiring, ExecPorts,
    Executor, ExecutorConfig, ExecutorError, Inbound, MockStateDatabase, MutatingSnapshotSource,
    NoBlockExec, NoEpochCheck, NoExecStream, NoRemoteEpochCheck, Outbound, Receipt, ResumePoint,
    RoleHooks, StateDatabase, StateWriterQueue, TxReceiptsPublication, TxRef,
    VerifyRecordIdentity, WriterApplyingQueue,
};
use kardamom_validator::{BalBuffer, Divergence, ReceiptBuffer, ReplicaId, ValidatorWriterQueue};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use revm::primitives::KECCAK_EMPTY;

/// The blocks of the chain. Each holds one transfer.
const BLOCKS: u64 = 40;
/// The last block that the validator verified before it stopped.
const STOPPED_AT: u64 = 10;
/// The batcher posts through this block while the validator is stopped:
/// past the cursor of the validator.
const POSTED: u64 = 36;
/// The reader window of the egress, in blocks below its head.
const KEEP_BLOCKS: u64 = 32;
/// The blocks of one store segment.
const SEGMENT_BLOCKS: u64 = 4;
const FIRST_TIMESTAMP: u64 = 1_700_000_000;
const RECIPIENT: Address = Address::repeat_byte(0x22);
const VERIFIED: &str = "validator_blocks_verified_total";
/// The executor session that published the write-sets and the receipts.
const EXECUTOR: ReplicaId = ReplicaId::from_session(7);

/// The real engine pipeline over the egress, with the writer queue `Q` and
/// the receipt sink `R`.
struct Wiring<Q, R>(PhantomData<(Q, R)>);

impl<Q: StateWriterQueue + 'static, R> ExecPorts for Wiring<Q, R> {
    type Snapshots = MutatingSnapshotSource;
    type WriterSignal = Imm;
    type WriterQueue = Q;
    type Epoch = NoEpochCheck;
    type RemoteEpoch = NoRemoteEpochCheck;
    type BlockExec = NoBlockExec;
    type TxHook = VerifyRecordIdentity;
}

impl<Q: StateWriterQueue + 'static, R: TxReceiptsPublication + 'static> EngineWiring
    for Wiring<Q, R>
{
    type TxData = ChanTxDataSub;
    type TxOrdering = CarriedSubscription<EgressTransport, NoSource>;
    type TxReceipts = R;
    type ExecStream = NoExecStream;
}

/// The writer queue of the executor: it applies each block, and records
/// the write-set that the executor publishes as its BAL.
struct Recording {
    inner: WriterApplyingQueue,
    deltas: Sender<BlockDelta>,
}

impl StateWriterQueue for Recording {
    fn submit(
        &mut self,
        block: BlockBoundary,
        delta: BlockDelta,
        refs: Vec<TxRef>,
    ) -> Result<(), ExecutorError> {
        self.deltas.send(delta.clone()).unwrap();
        self.inner.submit(block, delta, refs)
    }
}

/// The receipt stream of the executor.
struct Published(Sender<Receipt>);

impl TxReceiptsPublication for Published {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        if let CMessage::Receipt(receipt) = msg {
            self.0.send(receipt).unwrap();
        }
        Ok(())
    }

    fn publish_rollback(
        &mut self,
        _rollback: kardamom_types::ReceiptRollback,
    ) -> Result<(), ExecutorError> {
        Ok(())
    }
}

/// The canonical stream: block `b` holds the transfer with nonce `b - 1`.
struct Chain {
    signer: PrivateKeySigner,
    stream: TestStream,
}

impl Chain {
    fn new() -> Self {
        let signer = PrivateKeySigner::from_bytes(&[0x31; 32].into()).unwrap();
        let entries = (1..=BLOCKS)
            .flat_map(|block| {
                let tx = LegacyTx {
                    chain_id: 1,
                    to: RECIPIENT,
                    nonce: block - 1,
                    value: 1,
                    gas_limit: 21_000,
                    gas_price: 0,
                    ..Default::default()
                }
                .sign(&signer);
                [
                    TestEntry::Tx(tx),
                    TestEntry::Boundary(BlockBoundaryStart {
                        block_number: block,
                        end_tx_idx: BPosition::from_index(block),
                        l2_timestamp: FIRST_TIMESTAMP + block,
                        l1_origin: 0,
                    }),
                ]
            })
            .collect();
        Self {
            signer,
            stream: TestStream(entries),
        }
    }

    /// The egress frames of the blocks through `last`: two frames a block.
    fn through(&self, last: u64) -> Vec<Vec<u8>> {
        let frames = self.stream.egress_frames();
        frames[..usize::try_from(last * 2).unwrap()].to_vec()
    }

    fn genesis(&self) -> MockStateDatabase {
        MockStateDatabase::builder()
            .account(
                self.signer.address(),
                U256::from(10u128.pow(18)),
                0,
                KECCAK_EMPTY,
            )
            .build()
    }

    /// The accounts that the chain changes, as `(nonce, balance)`.
    fn accounts(&self, state: &MockStateDatabase) -> Vec<Option<(u64, U256)>> {
        [self.signer.address(), RECIPIENT]
            .iter()
            .map(|address| {
                state
                    .basic(*address)
                    .unwrap()
                    .map(|(nonce, balance, _)| (nonce, balance))
            })
            .collect()
    }

    /// The nonce of the signer in `state`: the blocks that it applied.
    fn applied(&self, state: &MockStateDatabase) -> u64 {
        state
            .basic(self.signer.address())
            .unwrap()
            .map_or(0, |(nonce, _, _)| nonce)
    }
}

/// One run of the pipeline over a reader of an egress.
struct Run<'a> {
    chain: &'a Chain,
    snap: MockStateDatabase,
    rt: &'a tokio::runtime::Runtime,
}

impl Run<'_> {
    /// Run from `start` and `cursor` over `egress` until the state applied
    /// every block through `last`. Then the live stream of the egress
    /// closes, and the threads join.
    fn until<Q, R>(
        &self,
        egress: &TestEgress,
        (start, cursor): (ResumePoint, ReplayCursor),
        ports: (Q, R),
        last: u64,
    ) -> Result<(), ExecutorError>
    where
        Q: StateWriterQueue + 'static,
        R: TxReceiptsPublication + 'static,
    {
        let reader = egress.reader(self.rt, cursor);
        let threads = Executor::<Wiring<Q, R>>::new(
            ExecutorConfig::default(),
            Inbound {
                tx_data: Vec::new(),
                tx_ordering: reader.sub,
                join_recovery: None,
                exec_stream: NoExecStream,
            },
            Outbound {
                tx_receipts: ports.1,
                snapshots: MutatingSnapshotSource(self.snap.clone()),
                writer_signal: Imm,
                writer_queue: ports.0,
            },
            start,
            RoleHooks::none(),
        )
        .spawn();
        let deadline = Instant::now() + Duration::from_secs(30);
        let _ = std::iter::repeat_with(|| std::thread::sleep(Duration::from_millis(10)))
            .find(|()| self.chain.applied(&self.snap) >= last || Instant::now() >= deadline);
        drop_live(reader.live);
        threads.join()
    }
}

/// Close the live stream of an egress reader: the reader then stops.
fn drop_live(_live: Sender<Vec<u8>>) {}

/// The run ended at the close of its source, not on a fault.
fn closed_cleanly(result: Result<(), ExecutorError>) {
    match result {
        Ok(()) | Err(ExecutorError::TxOrderingClosed) => {}
        Err(e) => panic!("the run ends only when its source closes: {e:?}"),
    }
}

/// The metrics of this test binary.
fn recorder() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| PrometheusBuilder::new().install_recorder().unwrap())
}

fn verified() -> u64 {
    recorder()
        .render()
        .lines()
        .find(|line| line.starts_with(VERIFIED))
        .and_then(|line| line.rsplit(' ').next()?.parse().ok())
        .unwrap_or(0)
}

/// The write-sets and the receipts that the executor published for the
/// whole chain.
fn executor_outputs(chain: &Chain, rt: &tokio::runtime::Runtime) -> (MockStateDatabase, Receiver<BlockDelta>, Receiver<Receipt>) {
    let dir = tempfile::tempdir().unwrap();
    let egress = TestEgress::serve(rt, dir.path(), TestEgress::GENESIS, &chain.through(BLOCKS));
    let run = Run {
        chain,
        snap: chain.genesis(),
        rt,
    };
    let (deltas_tx, deltas) = crossbeam_channel::unbounded();
    let (receipts_tx, receipts) = crossbeam_channel::unbounded();
    let queue = Recording {
        inner: WriterApplyingQueue::new(run.snap.clone()),
        deltas: deltas_tx,
    };
    closed_cleanly(run.until(
        &egress,
        (ResumePoint::GENESIS, ReplayCursor::new(0, 1)),
        (queue, Published(receipts_tx)),
        BLOCKS,
    ));
    (run.snap, deltas, receipts)
}

#[test]
fn a_validator_held_past_the_posted_head_resumes_inside_the_window_and_verifies_every_block() {
    recorder();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let chain = Chain::new();
    let (reference, deltas, receipts) = executor_outputs(&chain, &rt);
    let bals = BalBuffer::new();
    let published = ReceiptBuffer::new();
    deltas
        .try_iter()
        .for_each(|delta| bals.insert(delta.block_number, EXECUTOR, delta));
    receipts
        .try_iter()
        .for_each(|receipt| published.receipts.insert(receipt.tx_idx, EXECUTOR, receipt));
    let divergence = Divergence::new();

    // The validator verifies the blocks through `STOPPED_AT` from the
    // egress, then stops.
    let dir = tempfile::tempdir().unwrap();
    let egress = TestEgress::serve_with(
        &rt,
        dir.path(),
        TestEgress::GENESIS,
        Retention {
            keep_blocks: NonZeroU64::new(KEEP_BLOCKS).unwrap(),
            segment_blocks: NonZeroU64::new(SEGMENT_BLOCKS).unwrap(),
        },
        &chain.through(STOPPED_AT),
    );
    let validator = Run {
        chain: &chain,
        snap: chain.genesis(),
        rt: &rt,
    };
    let ports = || {
        (
            ValidatorWriterQueue::new(
                WriterApplyingQueue::new(validator.snap.clone()),
                Arc::clone(&bals),
                Arc::clone(&divergence),
            ),
            kardamom_validator::ValidatorReceiptSink::new(
                Arc::clone(&published),
                Arc::clone(&divergence),
            ),
        )
    };
    let before = verified();
    closed_cleanly(validator.until(
        &egress,
        (ResumePoint::GENESIS, ReplayCursor::new(0, 1)),
        ports(),
        STOPPED_AT,
    ));
    assert_eq!(verified() - before, STOPPED_AT, "each block verifies once");

    // While the validator is stopped, the chain runs on, the batcher posts
    // past its cursor, and the egress prunes below its reader window.
    egress.append(&chain.through(BLOCKS)[usize::try_from(STOPPED_AT * 2).unwrap()..]);
    egress.post(POSTED);
    let window = BLOCKS + 1 - KEEP_BLOCKS;
    let floor = wait_for_floor(&egress, window);
    assert!(
        floor <= STOPPED_AT + 1,
        "the window keeps the cursor of the validator: floor {floor}"
    );

    let resumed = verified();
    closed_cleanly(validator.until(
        &egress,
        (
            ResumePoint {
                block: STOPPED_AT,
                record_count: STOPPED_AT,
                l2_timestamp: FIRST_TIMESTAMP + STOPPED_AT,
                base_fee: 0,
                gas_used: 21_000,
            },
            ReplayCursor::new(STOPPED_AT, STOPPED_AT + 1),
        ),
        ports(),
        BLOCKS,
    ));
    assert!(!divergence.is_halted(), "{:?}", divergence.reason());
    assert_eq!(
        verified() - resumed,
        BLOCKS - STOPPED_AT,
        "every block after the cursor verifies against the executor"
    );
    assert_eq!(
        chain.accounts(&validator.snap),
        chain.accounts(&reference),
        "the validator ends with the state of the executor"
    );
}

/// Wait until the egress prunes: its floor rises above genesis. The floor
/// stops at the segment of `window`, the first block of the reader window.
/// Returns the first block that the egress still serves.
fn wait_for_floor(egress: &TestEgress, window: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    let floor = std::iter::repeat_with(|| {
        std::thread::sleep(Duration::from_millis(10));
        egress.floor().map(|f| f.next_block).filter(|block| *block > 1)
    })
    .take_while(|_| Instant::now() < deadline)
    .find_map(|floor| floor)
    .expect("the egress prunes below its window");
    assert!(floor <= window, "the prune keeps the window: floor {floor}");
    floor
}
