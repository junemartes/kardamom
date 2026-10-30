//! The chain of the tests and its executor results. A reference executor
//! runs the production engine over a real egress and a real MDBX writer
//! with the trie off, as a deployed executor does. It records the write
//! set and the receipts of each block, which the feed publishes as the
//! BAL and receipt frames of an executor replica. At one block it copies
//! a real checkpoint, which a peer server serves to a joining validator.
//! A second writer with the trie on gives the state root at each head
//! that a test ends on.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::{Address, B256, address};
use kardamom_engine::actor::fixtures::{
    ChanTxDataSub, ChanTxOrderingSub, LegacyTx, TestEntry, TestStream,
};
use kardamom_engine::reader::NoExecStream;
use kardamom_engine::{
    BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, CMessage, EngineWiring, ExecPorts,
    Executor, ExecutorConfig, ExecutorError, Inbound, MdbxSnapshotSource, MdbxWriterQueue,
    MdbxWriterSignal, NoBlockExec, NoEpochCheck, NoRemoteEpochCheck, NoTxHook, Outbound,
    ResumePoint, RoleHooks, StateWriterQueue, StateWriterSignal, TxOrderingMessage,
    TxReceiptsPublication, TxRef,
};
use kardamom_state::{
    Durability, StateEnv, StateEnvBuilder, StateSnapshot, StateWriter, TrieMode, WriteBatch,
};
use kardamom_types::{Genesis, GenesisAlloc, ReceiptBatch, ReceiptRollback, ReceiptRows};

/// The chain id of `chains/dev.toml`.
pub(crate) const CHAIN_ID: u64 = 412_346;
/// Every transfer pays this account.
const RECIPIENT: Address = address!("000000000000000000000000000000000000c0de");
/// The transfers in each block.
const TXS_PER_BLOCK: u64 = 2;
/// The timestamp of block 0.
const T0: u64 = 1_700_000_000;
/// The longest wait for the reference writer.
const WRITER_WAIT: Duration = Duration::from_secs(60);

/// The published results of one block: its egress frames, the BAL frame
/// delta, and the receipt batches of the executor.
pub(crate) struct Block {
    pub(crate) frames: Vec<Vec<u8>>,
    pub(crate) delta: BlockDelta,
    pub(crate) receipts: Vec<ReceiptBatch>,
}

/// The whole chain: the genesis file, its blocks from block 1, and the
/// directory of the checkpoint that the reference executor copied.
pub(crate) struct Chain {
    pub(crate) genesis_file: PathBuf,
    pub(crate) blocks: Vec<Block>,
    pub(crate) checkpoints: PathBuf,
    pub(crate) checkpoint_block: u64,
    /// The canonical state root after each head that a test ends on.
    roots: BTreeMap<u64, B256>,
}

impl Chain {
    /// The canonical state root after `head`, one of the heads of
    /// [`Self::build`].
    pub(crate) fn root(&self, head: u64) -> B256 {
        self.roots[&head]
    }

    /// Build `count` blocks under `dir`, copy a checkpoint after block
    /// `checkpoint_block`, and keep the state root after each of `heads`.
    pub(crate) fn build(dir: &Path, count: u64, checkpoint_block: u64, heads: &[u64]) -> Self {
        let genesis_file = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../chains/dev.toml");
        let genesis: Genesis =
            toml::from_str(&std::fs::read_to_string(&genesis_file).unwrap()).unwrap();
        let alloc = genesis.to_alloc();
        let entries = Entries::of(count);
        let stream = entries.stream();
        let frames = TestStream(entries.stream()).egress_frames();
        let checkpoints = dir.join("peer-checkpoints");
        let run = Reference {
            dir: dir.join("reference"),
            alloc: alloc.clone(),
            checkpoints: checkpoints.clone(),
            checkpoint_block,
        }
        .run(&stream, count);
        let roots = Roots::at(&dir.join("roots"), &alloc, &run.deltas, heads);
        let blocks = entries
            .split(frames)
            .into_iter()
            .zip(run.deltas)
            .map(|(frames, (_, delta))| Block {
                receipts: run.receipts.of(delta.block_number),
                frames,
                delta,
            })
            .collect();
        Self {
            genesis_file,
            blocks,
            checkpoints,
            checkpoint_block,
            roots,
        }
    }
}

/// The canonical entries of the chain: each block holds its transfers,
/// then its boundary.
struct Entries {
    blocks: Vec<Vec<TestEntry>>,
}

impl Entries {
    fn of(count: u64) -> Self {
        let signer = kardamom_engine::actor::fixtures::anvil_signer_0();
        let blocks = (1..=count)
            .map(|block| {
                let first = (block - 1) * TXS_PER_BLOCK;
                (first..first + TXS_PER_BLOCK)
                    .map(|nonce| {
                        TestEntry::Tx(
                            LegacyTx {
                                chain_id: CHAIN_ID,
                                to: RECIPIENT,
                                nonce,
                                value: 1,
                                gas_limit: 21_000,
                                gas_price: 0,
                                ..LegacyTx::default()
                            }
                            .sign(&signer),
                        )
                    })
                    .chain(std::iter::once(TestEntry::Boundary(BlockBoundaryStart {
                        block_number: block,
                        end_tx_idx: BPosition::from_index(block * TXS_PER_BLOCK),
                        l2_timestamp: T0 + block,
                        l1_origin: 0,
                    })))
                    .collect()
            })
            .collect();
        Self { blocks }
    }

    fn stream(&self) -> Vec<TestEntry> {
        self.blocks
            .iter()
            .flatten()
            .map(|entry| match entry {
                TestEntry::Tx(envelope) => TestEntry::Tx(envelope.clone()),
                TestEntry::Boundary(b) => TestEntry::Boundary(b.clone()),
            })
            .collect()
    }

    /// Cut the frames of the whole stream into the frames of each block.
    fn split(&self, frames: Vec<Vec<u8>>) -> Vec<Vec<Vec<u8>>> {
        let mut frames = frames.into_iter();
        self.blocks
            .iter()
            .map(|entries| frames.by_ref().take(entries.len()).collect())
            .collect()
    }
}

/// The receipt batches the reference executor published.
#[derive(Clone, Default)]
struct Receipts(Arc<Mutex<Vec<ReceiptBatch>>>);

impl Receipts {
    /// The batches of `block`, in publish order.
    fn of(&self, block: u64) -> Vec<ReceiptBatch> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|b| b.receipts.first().is_some_and(|r| r.block_number == block))
            .cloned()
            .collect()
    }
}

impl TxReceiptsPublication for Receipts {
    fn publish(&mut self, _msg: CMessage) -> Result<(), ExecutorError> {
        Ok(())
    }

    fn publish_receipts(&mut self, items: &[ReceiptRows]) -> (usize, Option<ExecutorError>) {
        self.0.lock().unwrap().push(ReceiptBatch::merge(items));
        (items.len(), None)
    }

    fn publish_rollback(&mut self, _rollback: ReceiptRollback) -> Result<(), ExecutorError> {
        Ok(())
    }
}

/// The writer queue of the reference executor: it records each block's
/// write set, and copies a checkpoint once the checkpoint block is
/// durable, before the next block reaches the writer.
struct Recorder {
    inner: MdbxWriterQueue,
    signal: MdbxWriterSignal,
    env: StateEnv,
    checkpoints: PathBuf,
    checkpoint_block: u64,
    deltas: Arc<Mutex<Vec<(BlockBoundary, BlockDelta)>>>,
}

impl StateWriterQueue for Recorder {
    fn submit(
        &mut self,
        block: BlockBoundary,
        delta: BlockDelta,
        refs: Vec<TxRef>,
    ) -> Result<(), ExecutorError> {
        if block.block_number == self.checkpoint_block + 1 {
            self.signal.wait_committed(self.checkpoint_block)?;
            kardamom_state::create_checkpoint(&self.env, &self.checkpoints).unwrap();
        }
        self.deltas
            .lock()
            .unwrap()
            .push((block.clone(), delta.clone()));
        self.inner.submit(block, delta, refs)
    }
}

/// The port types of the reference executor.
struct ReferenceWiring;

impl ExecPorts for ReferenceWiring {
    type Snapshots = MdbxSnapshotSource;
    type WriterSignal = MdbxWriterSignal;
    type WriterQueue = Recorder;
    type Epoch = NoEpochCheck;
    type RemoteEpoch = NoRemoteEpochCheck;
    type BlockExec = NoBlockExec;
    type TxHook = NoTxHook;
}

impl EngineWiring for ReferenceWiring {
    type TxData = ChanTxDataSub;
    type TxOrdering = ChanTxOrderingSub;
    type TxReceipts = Receipts;
    type ExecStream = NoExecStream;
}

/// What the reference run records.
struct Recorded {
    deltas: Vec<(BlockBoundary, BlockDelta)>,
    receipts: Receipts,
}

/// The canonical stream as the cluster relays it: the reference of each
/// transaction on `tx_ordering` and its envelope on `tx_data` lane 0, at
/// the canonical index of the record. A boundary takes the index of the
/// next transaction. The egress serves the same records at the same
/// indexes, so both readers give the engine the same input.
struct Cluster;

impl Cluster {
    fn open(entries: &[TestEntry]) -> (ChanTxDataSub, ChanTxOrderingSub) {
        let (data, data_rx) = crossbeam_channel::unbounded();
        let (refs, refs_rx) = crossbeam_channel::unbounded();
        let indexed = entries.iter().scan(0u64, |next, entry| {
            let index = *next;
            *next = index + u64::from(matches!(entry, TestEntry::Tx(_)));
            Some((BPosition::from_index(index), entry))
        });
        indexed.for_each(|(position, entry)| {
            let msg = match entry {
                TestEntry::Tx(envelope) => {
                    data.send((position, envelope.clone())).unwrap();
                    TxOrderingMessage::TxRef(TxRef::new(envelope.tx_hash, 0, position, 0))
                }
                TestEntry::Boundary(b) => TxOrderingMessage::BoundaryStart(b.clone()),
            };
            refs.send((position, msg)).unwrap();
        });
        (
            ChanTxDataSub {
                sequencer_id: 0,
                rx: data_rx,
            },
            ChanTxOrderingSub(refs_rx),
        )
    }
}

/// One run of the reference executor.
struct Reference {
    dir: PathBuf,
    alloc: GenesisAlloc,
    checkpoints: PathBuf,
    checkpoint_block: u64,
}

impl Reference {
    /// The threads end once the closed streams are read to their end.
    /// `TxOrderingClosed` is the normal end of this run.
    fn stop(threads: kardamom_engine::Threads) {
        let ended = threads.join();
        assert!(
            matches!(ended, Ok(()) | Err(ExecutorError::TxOrderingClosed)),
            "{ended:?}"
        );
    }

    fn run(self, entries: &[TestEntry], count: u64) -> Recorded {
        let (lane, sub) = Cluster::open(entries);
        let env = open_env(&self.dir.join("state"));
        kardamom_state::seed_genesis(&env, &self.alloc).unwrap();
        let mut writer = StateWriter::spawn(env.clone()).unwrap();
        let deltas = Arc::new(Mutex::new(Vec::new()));
        let receipts = Receipts::default();
        let threads = Executor::<ReferenceWiring>::new(
            ExecutorConfig {
                chain_id: std::num::NonZeroU64::new(CHAIN_ID).unwrap(),
                ..ExecutorConfig::default()
            },
            Inbound {
                tx_data: vec![lane],
                tx_ordering: sub,
                join_recovery: None,
                exec_stream: NoExecStream,
            },
            Outbound {
                tx_receipts: receipts.clone(),
                snapshots: MdbxSnapshotSource::new(writer.snapshot_rx.clone()),
                writer_signal: MdbxWriterSignal::new(writer.snapshot_rx.clone()),
                writer_queue: Recorder {
                    inner: MdbxWriterQueue::new(writer.delta_tx.clone()),
                    signal: MdbxWriterSignal::new(writer.snapshot_rx.clone()),
                    env,
                    checkpoints: self.checkpoints,
                    checkpoint_block: self.checkpoint_block,
                    deltas: deltas.clone(),
                },
            },
            ResumePoint::GENESIS,
            RoleHooks::none(),
        )
        .spawn();
        wait_block(&writer, count);
        Self::stop(threads);
        writer.shutdown().unwrap();
        let deltas = deltas.lock().unwrap().clone();
        Recorded { deltas, receipts }
    }
}

/// The state root after each of the heads: the recorded write sets,
/// applied in order by a writer with the trie on.
struct Roots;

impl Roots {
    fn at(
        dir: &Path,
        alloc: &GenesisAlloc,
        deltas: &[(BlockBoundary, BlockDelta)],
        heads: &[u64],
    ) -> BTreeMap<u64, B256> {
        let env = open_env(dir);
        kardamom_state::seed_genesis(&env, alloc).unwrap();
        let mut writer = StateWriter::spawn_with_trie(env, TrieMode::Incremental).unwrap();
        let roots = deltas
            .iter()
            .filter_map(|(boundary, delta)| {
                writer
                    .delta_tx
                    .send(WriteBatch::new(boundary.clone(), delta.clone()))
                    .unwrap();
                let head = boundary.block_number;
                heads
                    .contains(&head)
                    .then(|| (head, Self::root(&writer, head)))
            })
            .collect();
        writer.shutdown().unwrap();
        roots
    }

    fn root(writer: &kardamom_state::WriterHandle, head: u64) -> B256 {
        wait_block(writer, head)
            .state_root()
            .unwrap()
            .expect("the trie is on")
    }
}

fn open_env(dir: &Path) -> StateEnv {
    StateEnvBuilder::new(dir)
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap()
}

fn wait_block(writer: &kardamom_state::WriterHandle, block: u64) -> StateSnapshot {
    kardamom_obs::testkit::poll_sync(
        &format!("reference writer committed block {block}"),
        WRITER_WAIT,
        Duration::from_millis(5),
        || {
            Ok(writer
                .snapshot_rx
                .current()
                .filter(|s| s.block_number() >= block))
        },
    )
    .unwrap_or_else(|e| {
        panic!(
            "{e}: the writer stands at {:?}",
            writer.snapshot_rx.current().map(|s| s.block_number())
        )
    })
}
