//! The chain, the egress child process and the readers of the egress
//! restart test.

use std::net::{SocketAddr, TcpListener};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use crossbeam_channel::{Receiver, Sender};
use kardamom_cluster_adapter::wire::{
    GuardHeader, encode_egress_record, encode_ingress_txref, split_ingress,
};
use kardamom_engine::actor::fixtures::{LegacyTx, TestEgress, TestEntry, TestStream, byte_signer};
use kardamom_engine::reader::Ordered;
use kardamom_engine::reader::carried::{CarriedSubscription, EgressTransport, NoSource};
use kardamom_engine::reader::cluster::ReplayCursor;
use kardamom_engine::{BPosition, BlockBoundaryStart, ExecutorError, TxOrderingMessage, TxRef};
use kardamom_engine::{TxOrderingSubscription, TxEnvelope};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// The longest wait for one event of a child or a reader.
const BOUND: Duration = Duration::from_secs(20);
/// The blocks of the chain. Block `b` holds the transaction at index
/// `b - 1`, then its boundary.
const BLOCKS: u64 = 10;
/// The blocks whose transactions the egress holds as references.
const REFERENCE_BLOCKS: u64 = 3;

const DIR_ENV: &str = "KARDAMOM_CARRY_EGRESS_DIR";
const PHASE_ENV: &str = "KARDAMOM_CARRY_EGRESS_PHASE";
const ADDR_ENV: &str = "KARDAMOM_CARRY_EGRESS_ADDR";

/// The reference mark of the egress: the canonical index and the block of
/// the highest stored reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Mark {
    index: u64,
    block: u64,
}

impl Mark {
    /// Whether this side file value is above `mark` in both fields, as
    /// the lead of the store leaves it.
    pub(super) fn leads(self, mark: Self) -> bool {
        self.index > mark.index && self.block > mark.block
    }

    /// The cursor just past the mark: the next index, and the next block.
    pub(super) fn next_cursor(self) -> (u64, u64) {
        (self.index + 1, self.block + 1)
    }

    /// The side file of the mark in the store under `root`.
    pub(super) fn in_file(root: &Path) -> Self {
        let bytes = std::fs::read(EgressChild::store(root).join("ref-mark")).unwrap();
        let word = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        Self {
            index: word(0),
            block: word(8),
        }
    }
}

/// The two runs of the egress child.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Phase {
    /// Blocks 1 to 6: the references, then the first entries with bytes.
    First,
    /// Blocks 7 to 10, after the restart: entries with bytes only.
    Second,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::First => "first",
            Self::Second => "second",
        }
    }

    fn parse(name: &str) -> Self {
        match name {
            "first" => Self::First,
            "second" => Self::Second,
            _ => panic!("unknown phase {name}"),
        }
    }

    pub(super) fn blocks(self) -> RangeInclusive<u64> {
        match self {
            Self::First => 1..=6,
            Self::Second => 7..=BLOCKS,
        }
    }

    /// The blocks that the copy of an executor sends to the egress of
    /// this phase. After a restart the copy starts with the last stored
    /// block: a copy chains only behind a frame that the store holds.
    fn copy_blocks(self) -> RangeInclusive<u64> {
        match self {
            Self::First => self.blocks(),
            Self::Second => Self::First.last_block()..=BLOCKS,
        }
    }

    pub(super) fn last_block(self) -> u64 {
        *self.blocks().end()
    }

    /// The blocks of this phase past R.
    pub(super) fn blocks_past_r(self) -> RangeInclusive<u64> {
        REFERENCE_BLOCKS + 1..=self.last_block()
    }
}

/// The chain of the test, and the frames of the egress for it.
pub(super) struct Chain {
    txs: Vec<TxEnvelope>,
}

impl Chain {
    /// R: the transaction of the last reference block, at that block.
    pub(super) const R: Mark = Mark {
        index: REFERENCE_BLOCKS - 1,
        block: REFERENCE_BLOCKS,
    };

    pub(super) fn new() -> Self {
        let signer = byte_signer(0x31);
        let txs = (0..BLOCKS)
            .map(|nonce| {
                LegacyTx {
                    to: Address::repeat_byte(0x32),
                    nonce,
                    value: 1,
                    correlation_id: nonce,
                    ..LegacyTx::default()
                }
                .sign(&signer)
            })
            .collect();
        Self { txs }
    }

    pub(super) fn last_index(&self) -> u64 {
        BLOCKS - 1
    }

    fn boundary(block: u64) -> BlockBoundaryStart {
        BlockBoundaryStart {
            block_number: block,
            end_tx_idx: BPosition::from_index(block),
            l2_timestamp: 1_700_000_000 + block,
            l1_origin: 0,
        }
    }

    /// The frames of `blocks`: the transaction of each block, as a
    /// reference in a reference block and else with its bytes, then its
    /// boundary.
    fn frames(&self, blocks: RangeInclusive<u64>) -> Vec<Vec<u8>> {
        let entries: Vec<TestEntry> = blocks
            .clone()
            .flat_map(|b| {
                [
                    TestEntry::Tx(self.tx(b).clone()),
                    TestEntry::Boundary(Self::boundary(b)),
                ]
            })
            .collect();
        let full = TestStream(self.prefix(*blocks.start(), entries)).egress_frames();
        let skip = usize::try_from(2 * (blocks.start() - 1)).unwrap();
        full.into_iter()
            .skip(skip)
            .zip(blocks.flat_map(|b| [Some(b), None]))
            .map(|(frame, tx_block)| match tx_block {
                Some(b) if b <= REFERENCE_BLOCKS => self.reference(b),
                _ => frame,
            })
            .collect()
    }

    /// `entries`, after the entries of the blocks below `first`: the
    /// stream from genesis gives each frame its canonical index.
    fn prefix(&self, first: u64, entries: Vec<TestEntry>) -> Vec<TestEntry> {
        (1..first)
            .flat_map(|b| {
                [
                    TestEntry::Tx(self.tx(b).clone()),
                    TestEntry::Boundary(Self::boundary(b)),
                ]
            })
            .chain(entries)
            .collect()
    }

    fn tx(&self, block: u64) -> &TxEnvelope {
        &self.txs[usize::try_from(block - 1).unwrap()]
    }

    /// The relayed frame of the reference of the transaction of `block`.
    fn reference(&self, block: u64) -> Vec<u8> {
        let index = block - 1;
        let tx_ref = TxRef::new(self.tx(block).tx_hash, 0, BPosition::from_index(index), 0);
        let ingress = encode_ingress_txref(&tx_ref, GuardHeader::EXEMPT);
        let (_, relayed) = split_ingress(&ingress).unwrap();
        encode_egress_record(index, relayed).unwrap()
    }
}

/// The egress in a child process.
pub(super) struct EgressChild {
    child: Child,
    root: PathBuf,
    addr: SocketAddr,
    phase: Phase,
    client: tokio::runtime::Runtime,
}

impl EgressChild {
    /// Start the egress of `phase` on the store under `root`. The second
    /// phase takes the address of the first.
    pub(super) fn start(root: &Path, phase: Phase) -> Self {
        let addr = match phase {
            Phase::First => TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap(),
            Phase::Second => std::fs::read_to_string(root.join("addr")).unwrap().parse().unwrap(),
        };
        std::fs::write(root.join("addr"), addr.to_string()).unwrap();
        let log = std::fs::File::create(root.join(format!("{}.log", phase.name()))).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "carry_egress_child", "--ignored", "--nocapture", "--test-threads=1"])
            .env(DIR_ENV, root)
            .env(PHASE_ENV, phase.name())
            .env(ADDR_ENV, addr.to_string())
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        Self {
            child,
            root: root.to_path_buf(),
            addr,
            phase,
            client: tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap(),
        }
    }

    fn store(root: &Path) -> PathBuf {
        root.join("store")
    }

    fn metrics_file(root: &Path, phase: Phase) -> PathBuf {
        root.join(format!("metrics-{}", phase.name()))
    }

    pub(super) fn base(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Wait until the store of the child holds every block to `block`.
    pub(super) fn await_head(&self, block: u64) {
        let url = format!("{}/v1/frames?from_index=0&from_block=1&limit=1", self.base());
        self.wait(|| {
            self.client
                .block_on(reqwest::get(&url))
                .ok()
                .and_then(|answer| answer.headers().get("x-egress-next-block")?.to_str().ok()?.parse().ok())
                .is_some_and(|next: u64| next > block)
        });
    }

    /// R as the gauges of the child report it, in a write after this call.
    pub(super) fn mark(&self) -> Mark {
        let path = Self::metrics_file(&self.root, self.phase);
        let asked = std::time::SystemTime::now();
        let mut mark = None;
        self.wait(|| {
            let fresh = std::fs::metadata(&path)
                .and_then(|m| m.modified())
                .is_ok_and(|written| written > asked);
            mark = fresh.then(|| std::fs::read_to_string(&path).ok()).flatten().and_then(|text| {
                Some(Mark {
                    index: gauge(&text, "kardamom_egress_ref_frame_max_index")?,
                    block: gauge(&text, "kardamom_egress_ref_frame_max_block")?,
                })
            });
            mark.is_some()
        });
        mark.unwrap()
    }

    /// Kill the child with SIGKILL, and require that the signal ended it.
    pub(super) fn kill(mut self) {
        use std::os::unix::process::ExitStatusExt;
        self.child.kill().unwrap();
        assert_eq!(self.child.wait().unwrap().signal(), Some(9));
    }

    fn wait(&self, mut done: impl FnMut() -> bool) {
        let end = Instant::now() + BOUND;
        let reached = std::iter::repeat_with(Instant::now)
            .take_while(|now| *now < end)
            .any(|_| done() || Self::pause());
        assert!(
            reached,
            "the egress child misses its deadline; {}",
            std::fs::read_to_string(self.root.join(format!("{}.log", self.phase.name()))).unwrap_or_default()
        );
    }

    fn pause() -> bool {
        std::thread::sleep(Duration::from_millis(10));
        false
    }

    /// The child: serve the frames of its phase, and write the gauges until
    /// the parent kills it.
    pub(super) fn serve() {
        let root = PathBuf::from(std::env::var_os(DIR_ENV).unwrap());
        let phase = Phase::parse(&std::env::var(PHASE_ENV).unwrap());
        let addr = std::env::var(ADDR_ENV).unwrap().parse().unwrap();
        let handle = PrometheusBuilder::new().install_recorder().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let frames = Chain::new().frames(phase.copy_blocks());
        let _egress = TestEgress::serve_on(&rt, addr, &Self::store(&root), TestEgress::GENESIS, &frames);
        let out = Self::metrics_file(&root, phase);
        std::iter::repeat_with(|| Self::publish(&handle, &out)).for_each(drop);
    }

    /// Write the rendered gauges to `out` in one rename, then pause.
    fn publish(handle: &PrometheusHandle, out: &Path) {
        let temp = out.with_extension("tmp");
        std::fs::write(&temp, handle.render()).unwrap();
        std::fs::rename(&temp, out).unwrap();
        std::thread::sleep(Duration::from_millis(20));
    }
}

impl Drop for EgressChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The value of the unlabelled gauge `name` in `text`.
fn gauge(text: &str, name: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .and_then(|value| value.trim().parse::<f64>().ok())
        .map(|value| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "a test gauge holds a small index")]
            let whole = value as u64;
            whole
        })
}

/// One reader of the egress on its own thread: a carried subscription with
/// the egress as its only source, as the batcher and the validator run it.
pub(super) struct Reader {
    items: Receiver<Result<(BPosition, Ordered), ExecutorError>>,
    live: Sender<Vec<u8>>,
    metrics: PrometheusHandle,
    gauge: &'static str,
}

impl Reader {
    pub(super) const BATCHER: &'static str = "kardamom_batcher_egress_cursor_index";
    pub(super) const VALIDATOR: &'static str = "kardamom_validator_egress_cursor_index";

    /// Read the egress at `base` from `cursor`, with the cursor gauge `gauge`.
    pub(super) fn start(base: String, cursor: (u64, u64), gauge: &'static str) -> Self {
        let (live, frames) = crossbeam_channel::unbounded();
        let (sent, items) = crossbeam_channel::unbounded();
        let recorder = PrometheusBuilder::new().build_recorder();
        let metrics = recorder.handle();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let transport = EgressTransport::new(base, frames, rt.handle().clone());
            metrics::with_local_recorder(&recorder, || {
                let mut sub =
                    CarriedSubscription::new(transport, NoSource, ReplayCursor::new(cursor.0, cursor.1))
                        .with_cursor_gauge(gauge)
                        .suppress_sealer_metrics();
                std::iter::repeat_with(|| sub.next())
                    .map(|item| (item.is_ok(), sent.send(item)))
                    .take_while(|(ok, sent)| *ok && sent.is_ok())
                    .for_each(drop);
            });
        });
        Self {
            items,
            live,
            metrics,
            gauge,
        }
    }

    fn next(&self) -> Result<(BPosition, Ordered), ExecutorError> {
        self.items.recv_timeout(BOUND).expect("the reader delivers within its bound")
    }

    /// Whether the first item of the reader is its stop.
    pub(super) fn stops(&self) -> bool {
        self.next().is_err()
    }

    /// Require the transaction with its bytes and the boundary of each of
    /// `blocks`, in order.
    pub(super) fn expect(&self, chain: &Chain, blocks: RangeInclusive<u64>) {
        blocks.for_each(|block| {
            let (position, tx) = self.next().unwrap();
            assert_eq!(position, BPosition::from_index(block - 1));
            assert!(
                matches!(&tx, Ordered::Tx(entry) if entry.canonical_id == chain.tx(block).tx_hash && entry.envelope == *chain.tx(block)),
                "block {block}: {tx:?}"
            );
            let (_, boundary) = self.next().unwrap();
            assert!(
                matches!(&boundary, Ordered::Message(TxOrderingMessage::BoundaryStart(b)) if *b == Chain::boundary(block)),
                "block {block}: {boundary:?}"
            );
        });
    }

    /// Send the live frames of `phase`, without its first block: the hole
    /// makes the reader ask the egress again.
    pub(super) fn live_after_a_hole(&self, chain: &Chain, phase: Phase) {
        chain
            .frames(phase.blocks())
            .into_iter()
            .skip(2)
            .for_each(|frame| self.live.send(frame).unwrap());
    }

    /// Require the cursor gauge of the reader at `index` or above.
    pub(super) fn assert_cursor_past(&self, index: u64) {
        let value = gauge(&self.metrics.render(), self.gauge);
        assert!(value.is_some_and(|v| v >= index), "{}: {value:?}", self.gauge);
    }
}
