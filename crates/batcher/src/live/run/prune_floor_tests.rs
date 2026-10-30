//! The batcher stops while a carry chain runs past the reader window of
//! the egress. Each egress copy keeps every block above the posted head:
//! the copies serve the same bytes from the cursor of the batcher, far
//! below the window. When the batcher starts again, it reads the range
//! from the egress and posts it to the settlement contract. The posts are
//! contiguous, each block lands once, and the payloads hold every
//! transaction. Once the posts are final, the copies prune back inside the
//! window.

use std::num::NonZeroU64;

use kardamom_engine::actor::fixtures::{
    LegacyTx, Retention, TestEgress, TestEntry, TestStream, anvil_signer_0,
};
use kardamom_engine::reader::cluster::ReplayCursor;
use kardamom_engine::reader::{
    JoinBuffer, NoExecStream, ReaderConfig, ReaderToExec, TxOrderingInputs, TxOrderingReader,
};
use kardamom_types::{BPosition, BlockBoundaryStart};

use super::null_receipts_tests::{Fixture, Post};
use super::*;

/// The blocks that the batcher posts before it stops.
const POSTED: u64 = 4;
/// The head of the chain when the batcher starts again.
const HEAD: u64 = 28;
const KEEP_BLOCKS: u64 = 8;
const SEGMENT_BLOCKS: u64 = 4;
const COPIES: usize = 3;
const PAGE_BYTES: usize = 512;

/// The canonical stream: block `b` holds one transfer with nonce `b - 1`.
fn chain() -> TestStream {
    TestStream(
        (1..=HEAD)
            .flat_map(|block| {
                let tx = LegacyTx {
                    chain_id: 42,
                    to: Address::repeat_byte(0x22),
                    nonce: block - 1,
                    value: 1,
                    gas_limit: 21_000,
                    gas_price: 0,
                    ..Default::default()
                }
                .sign(&anvil_signer_0());
                [
                    TestEntry::Tx(tx),
                    TestEntry::Boundary(BlockBoundaryStart {
                        block_number: block,
                        end_tx_idx: BPosition::from_index(block),
                        l2_timestamp: 1_000 + block,
                        l1_origin: 0,
                    }),
                ]
            })
            .collect(),
    )
}

/// The egress copies, each with its own store, and the runtime of their
/// HTTP sides and of the reader of the batcher.
struct Copies {
    rt: Option<tokio::runtime::Runtime>,
    copies: Vec<TestEgress>,
    frames: Vec<Vec<u8>>,
    _dirs: Vec<tempfile::TempDir>,
}

impl Copies {
    /// Start the copies with the blocks through `last`. The runtime starts
    /// on its own thread: the test runs on another runtime.
    fn start(last: u64) -> Self {
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let frames = chain().egress_frames();
            let dirs: Vec<_> = (0..COPIES).map(|_| tempfile::tempdir().unwrap()).collect();
            let retention = Retention {
                keep_blocks: NonZeroU64::new(KEEP_BLOCKS).unwrap(),
                segment_blocks: NonZeroU64::new(SEGMENT_BLOCKS).unwrap(),
            };
            let copies = dirs
                .iter()
                .map(|dir| {
                    TestEgress::serve_with(&rt, dir.path(), TestEgress::GENESIS, retention, &[])
                })
                .collect();
            let started = Self {
                rt: Some(rt),
                copies,
                frames,
                _dirs: dirs,
            };
            started.through(0, last);
            started
        })
        .join()
        .unwrap()
    }

    /// Deliver the blocks after `from` through `last` to every copy.
    fn through(&self, from: u64, last: u64) {
        let range = usize::try_from(from * 2).unwrap()..usize::try_from(last * 2).unwrap();
        self.copies
            .iter()
            .for_each(|copy| copy.append(&self.frames[range.clone()]));
    }

    /// A post on L1 covers the blocks through `block`, on every copy.
    fn post(&self, block: u64) {
        self.copies.iter().for_each(|copy| copy.post(block));
    }

    /// The first block that each copy serves, once every copy pruned below
    /// `above`.
    fn floors_above(&self, above: u64) -> Vec<u64> {
        tokio::task::block_in_place(|| {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            std::iter::repeat_with(|| {
                std::thread::sleep(Duration::from_millis(10));
                self.copies
                    .iter()
                    .map(|copy| copy.floor().map_or(1, |floor| floor.next_block))
                    .collect::<Vec<_>>()
            })
            .take_while(|_| std::time::Instant::now() < deadline)
            .find(|floors| floors.iter().all(|floor| *floor > above))
            .expect("every copy prunes")
        })
    }

    /// The reader of the batcher on the first copy from `cursor`, as the
    /// binary with `--egress` opens it, and the channel of its records.
    fn reader(&self, cursor: ReplayCursor) -> (tokio::sync::mpsc::Receiver<ReaderToExec>, Live) {
        let reader = self.copies[0].reader(self.rt.as_ref().unwrap(), cursor);
        let (exec_out, records) = tokio::sync::mpsc::channel(1 << 14);
        TxOrderingReader::spawn(TxOrderingInputs {
            sub: reader.sub,
            buffer: JoinBuffer::new(),
            cfg: ReaderConfig::default(),
            exec_out,
            exec_stream: NoExecStream,
            recovery_factory: None,
        });
        (records, Live(reader.live))
    }

    /// Read the range from `cursor` to the head page by page on every copy.
    /// Each copy must serve the same bytes and the same cursors. Returns the
    /// pages.
    async fn replay(&self, cursor: (u64, u64)) -> usize {
        let http = reqwest::Client::new();
        let mut at = cursor;
        let mut pages = 0;
        while at.1 <= HEAD {
            at = self.compare_page(&http, at).await;
            pages += 1;
        }
        pages
    }

    /// Read one page from `at` on every copy, require the same page from
    /// each, and return the cursor after it.
    async fn compare_page(&self, http: &reqwest::Client, at: (u64, u64)) -> (u64, u64) {
        let mut read = Vec::with_capacity(self.copies.len());
        for copy in &self.copies {
            read.push(page(http, copy.base(), at).await);
        }
        assert!(
            read.windows(2).all(|pair| pair[0] == pair[1]),
            "every copy serves the same bytes from {at:?}"
        );
        assert_ne!(read[0].after, at, "the replay moves");
        read[0].after
    }
}

impl Drop for Copies {
    fn drop(&mut self) {
        self.rt.take().map(tokio::runtime::Runtime::shutdown_background);
    }
}

/// The live stream of the reader. The reader stops when it ends.
struct Live(#[allow(dead_code, reason = "held only to keep the live stream open")] crossbeam_channel::Sender<Vec<u8>>);

/// One page of a range read.
#[derive(Debug, PartialEq, Eq)]
struct Page {
    after: (u64, u64),
    bytes: Vec<u8>,
}

async fn page(http: &reqwest::Client, base: &str, (index, block): (u64, u64)) -> Page {
    let response = http
        .get(format!("{base}/v1/frames"))
        .query(&[
            ("from_index", index.to_string()),
            ("from_block", block.to_string()),
            ("limit", PAGE_BYTES.to_string()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK, "{base} serves {index}/{block}");
    let header = |name: &str| -> u64 {
        response.headers()[name].to_str().unwrap().parse().unwrap()
    };
    let after = (header("x-egress-after-index"), header("x-egress-after-block"));
    Page {
        after,
        bytes: response.bytes().await.unwrap().to_vec(),
    }
}

impl Fixture {
    /// Start the feed from the durable cursor, reading the first copy.
    async fn post_from_egress(&self, copies: &Copies) -> (Post, Live) {
        let resumed = self
            .args()
            .start_l1_side()
            .await
            .unwrap_or_else(|_| panic!("start the feed"));
        let cursor = ReplayCursor::new(resumed.cursor.next_index, resumed.cursor.next_block);
        let (records, live) = copies.reader(cursor);
        (self.feed_from(resumed, records), live)
    }

    /// Wait until the contract covers the blocks through `block`.
    async fn await_covered(&self, block: u64) {
        tokio::time::timeout(Duration::from_secs(30), async {
            while L1Truth::read(&self.rig.provider, self.settlement)
                .await
                .unwrap()
                .covered_through_block
                < block
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the batcher posts through the head");
    }

    /// Every post is contiguous with the one before, and the payloads hold
    /// each block once with its transaction.
    async fn assert_contiguous_through(&self, head: u64) {
        let records = crate::l1::read_posted_batches(&self.rig.provider, self.settlement, 0)
            .await
            .unwrap();
        let stored = self.da.stored();
        let blocks: Vec<(u64, usize)> = records
            .iter()
            .flat_map(|record| {
                crate::recon::reconstruct(stored.get(&record.da_cert).unwrap())
                    .unwrap()
                    .into_iter()
                    .map(|frame| (frame.block_number, frame.txs.len()))
            })
            .collect();
        assert_eq!(
            blocks,
            (1..=head).map(|block| (block, 1)).collect::<Vec<_>>(),
            "each block once, in order, with its transaction"
        );
        records.windows(2).for_each(|pair| {
            assert_eq!(pair[1].l2_block_start, pair[0].l2_block_end + 1, "{records:?}");
            assert_eq!(pair[1].index, pair[0].index + 1);
        });
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Anvil and the compiled settlement contract"]
async fn unposted_blocks_past_the_reader_window_stay_on_every_copy_and_post_contiguously() {
    let fixture = Fixture::new().await;
    let copies = Copies::start(POSTED);
    let (post, live) = fixture.post_from_egress(&copies).await;
    fixture.await_covered(POSTED).await;
    post.cancel().await;
    drop_live(live);
    copies.post(POSTED);

    // The batcher is stopped. The chain runs on past the reader window,
    // and the copies prune every block below their window that a post
    // covers.
    copies.through(POSTED, HEAD);
    let floors = copies.floors_above(1);
    assert!(
        floors.iter().all(|floor| *floor <= POSTED + 1),
        "every copy keeps the first unposted block: {floors:?}"
    );
    assert!(
        HEAD - POSTED > KEEP_BLOCKS + SEGMENT_BLOCKS,
        "the unposted range is past the window"
    );
    let pages = copies.replay((POSTED, POSTED + 1)).await;
    assert!(pages > 1, "the replay spans pages");

    // The batcher starts again from its cursor.
    let (post, live) = fixture.post_from_egress(&copies).await;
    fixture.await_covered(HEAD).await;
    post.cancel().await;
    drop_live(live);
    fixture.assert_contiguous_through(HEAD).await;

    // The posts cover the head: the copies prune back inside the window.
    copies.post(HEAD);
    let floors = copies.floors_above(POSTED + 1);
    assert!(
        floors.iter().all(|floor| *floor > HEAD - KEEP_BLOCKS - SEGMENT_BLOCKS),
        "the retention is back inside the window: {floors:?}"
    );
}

/// End the live stream of a reader.
fn drop_live(_live: Live) {}
