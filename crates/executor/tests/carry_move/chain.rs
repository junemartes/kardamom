//! The chain of the carry move test, and the runs of its executors.

use alloy_primitives::{Address, B256, U256, keccak256};
use kardamom_engine::actor::fixtures::{
    ChannelHarness, HarnessInput, HarnessOutcome, HarnessSetup, LegacyTx, TestEgress, TestEntry,
    TestStream, byte_signer,
};
use kardamom_engine::reader::cluster::ReplayCursor;
use kardamom_engine::{
    BPosition, BlockBoundary, BlockBoundaryStart, CMessage, ExecutorConfig, ExecutorError,
    MockStateDatabase, Receipt, ResumePoint, StateDatabase, TxEnvelope, TxOrderingMessage, TxRef,
};
use kardamom_types::epoch::LockboxLog;
use kardamom_types::{DepositLog, EpochRecord, derive_epoch};
use revm::primitives::KECCAK_EMPTY;

const CHAIN_ID: std::num::NonZeroU64 = std::num::NonZeroU64::new(1).unwrap();
const BLOCKS: u64 = 8;
const SINK: Address = Address::repeat_byte(0x5c);
const RECIPIENT: Address = Address::repeat_byte(0xd4);
const FUNDS: u128 = 10u128.pow(18);
/// The blocks that an L1 epoch opens, with the L1 block and the mint of
/// its one deposit: before the outage, across it, and after it.
const DEPOSITS: [(u64, u64, u128); 3] = [(1, 101, 1_001), (5, 102, 2_003), (8, 103, 4_007)];

/// One entry of the chain, at its canonical index.
enum Kind {
    Epoch(EpochRecord),
    Tx(TxEnvelope),
    Boundary(BlockBoundaryStart),
}

/// The chain of the test: block `b` holds the epoch that opens it, if
/// any, then one transfer of `b` wei, then its boundary.
pub(super) struct Chain {
    sender: Address,
    entries: Vec<(u64, u64, Kind)>,
}

impl Chain {
    pub(super) fn new() -> Self {
        let signer = byte_signer(0x5a);
        let blocks: Vec<Vec<Kind>> = (1..=BLOCKS)
            .map(|b| {
                let tx = LegacyTx {
                    chain_id: CHAIN_ID.get(),
                    to: SINK,
                    nonce: b - 1,
                    value: b,
                    gas_limit: 21_000,
                    gas_price: 0,
                    ..LegacyTx::default()
                }
                .sign(&signer);
                Self::epoch_of(b)
                    .into_iter()
                    .map(Kind::Epoch)
                    .chain([Kind::Tx(tx)])
                    .collect()
            })
            .collect();
        let entries = blocks
            .into_iter()
            .zip(1u64..)
            .scan((0u64, 0u64), |(next, origin), (kinds, b)| {
                let mut block: Vec<(u64, u64, Kind)> = kinds
                    .into_iter()
                    .map(|kind| {
                        let index = *next;
                        *next += Self::slots(&kind);
                        if let Kind::Epoch(epoch) = &kind {
                            *origin = epoch.l1_number;
                        }
                        (b, index, kind)
                    })
                    .collect();
                block.push((b, *next, Kind::Boundary(Self::boundary(b, *next, *origin))));
                Some(block)
            })
            .flatten()
            .collect();
        Self {
            sender: signer.address(),
            entries,
        }
    }

    /// The epoch that opens block `b`: one deposit to the recipient.
    fn epoch_of(b: u64) -> Option<EpochRecord> {
        let (_, l1_number, mint) = DEPOSITS.into_iter().find(|(block, ..)| *block == b)?;
        let l1_hash = keccak256(l1_number.to_be_bytes());
        let log = DepositLog {
            block_number: l1_number,
            block_hash: l1_hash,
            log_index: 0,
            from: Address::repeat_byte(0xe1),
            to: RECIPIENT,
            mint,
            gas_limit: 100_000,
            data: alloy_primitives::Bytes::new(),
        };
        Some(derive_epoch(l1_number, l1_hash, &[LockboxLog::Deposit(log)]).expect("one deposit"))
    }

    fn slots(kind: &Kind) -> u64 {
        match kind {
            Kind::Epoch(epoch) => 1 + u64::try_from(epoch.deposits.len()).unwrap(),
            Kind::Tx(_) => 1,
            Kind::Boundary(_) => 0,
        }
    }

    fn boundary(block: u64, end: u64, l1_origin: u64) -> BlockBoundaryStart {
        BlockBoundaryStart {
            block_number: block,
            end_tx_idx: BPosition::from_index(end),
            l2_timestamp: 1_700_000_000 + block,
            l1_origin,
        }
    }

    fn entry(kind: &Kind) -> TestEntry {
        match kind {
            Kind::Epoch(epoch) => TestEntry::Epoch(epoch.clone()),
            Kind::Tx(tx) => TestEntry::Tx(tx.clone()),
            Kind::Boundary(b) => TestEntry::Boundary(b.clone()),
        }
    }

    fn setup(start: ResumePoint, snap: MockStateDatabase) -> HarnessSetup {
        HarnessSetup {
            cfg: ExecutorConfig {
                chain_id: CHAIN_ID,
                ..ExecutorConfig::default()
            },
            start,
            snap,
            tx_hook: None,
        }
    }

    fn genesis(&self) -> MockStateDatabase {
        MockStateDatabase::builder()
            .account(self.sender, U256::from(FUNDS), 0, KECCAK_EMPTY)
            .build()
    }

    /// Run the blocks to `last` in reference mode: the records and the
    /// boundaries on the cluster egress, the envelopes on lane 0.
    fn by_reference(&self, last: u64) -> HarnessOutcome {
        let part = || self.entries.iter().filter(move |(b, ..)| *b <= last);
        let tx_data = part()
            .filter_map(|(_, index, kind)| match kind {
                Kind::Tx(tx) => Some((BPosition::from_index(*index), tx.clone())),
                Kind::Epoch(_) | Kind::Boundary(_) => None,
            })
            .collect();
        let tx_ordering = part()
            .map(|(_, index, kind)| {
                let position = BPosition::from_index(*index);
                let message = match kind {
                    Kind::Epoch(epoch) => TxOrderingMessage::Epoch(epoch.clone()),
                    Kind::Tx(tx) => {
                        TxOrderingMessage::TxRef(TxRef::new(tx.tx_hash, 0, position, 0))
                    }
                    Kind::Boundary(b) => TxOrderingMessage::BoundaryStart(b.clone()),
                };
                (position, message)
            })
            .collect();
        ChannelHarness::run(
            Self::setup(ResumePoint::GENESIS, self.genesis()),
            HarnessInput {
                tx_data: vec![tx_data],
                tx_ordering,
            },
        )
    }

    /// Run in carry mode from `start`, on the state `snap` that the run
    /// before committed: a reader of an egress that serves every entry
    /// with its bytes, from the cursor after the committed block.
    fn carried(&self, start: ResumePoint, snap: MockStateDatabase) -> HarnessOutcome {
        let frames = TestStream(
            self.entries
                .iter()
                .map(|(.., kind)| Self::entry(kind))
                .collect(),
        )
        .egress_frames();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let egress = TestEgress::serve(&rt, dir.path(), TestEgress::GENESIS, &frames);
        let cursor = ReplayCursor::new(start.record_count, start.block + 1);
        ChannelHarness::run_on_egress(Self::setup(start, snap), egress.reader(&rt, cursor))
    }

    /// The executor that never moves.
    pub(super) fn reference(&self) -> Run {
        Run::of(self.by_reference(BLOCKS))
    }

    /// An executor that moves to carry mode after block `cut`: it runs the
    /// blocks to `cut` by reference, stops, and runs the rest in carry mode
    /// from the state that it committed.
    pub(super) fn moving(&self, cut: u64) -> Run {
        let head = Run::of(self.by_reference(cut));
        let tail = Run::of(self.carried(head.resume(), head.state.clone()));
        head.then(tail)
    }

    /// The source hash of each deposit, in L1 order.
    fn source_hashes(&self) -> Vec<B256> {
        self.entries
            .iter()
            .filter_map(|(.., kind)| match kind {
                Kind::Epoch(epoch) => Some(epoch.deposits.iter().map(|d| d.source_hash)),
                Kind::Tx(_) | Kind::Boundary(_) => None,
            })
            .flatten()
            .collect()
    }
}

/// What one executor published and committed.
pub(super) struct Run {
    messages: Vec<CMessage>,
    state: MockStateDatabase,
}

impl Run {
    fn of(outcome: HarnessOutcome) -> Self {
        assert!(
            matches!(
                outcome.result,
                Ok(()) | Err(ExecutorError::TxOrderingClosed)
            ),
            "{:?}",
            outcome.result
        );
        Self {
            messages: outcome.receipts,
            state: outcome.state,
        }
    }

    /// The cursor after the last committed boundary.
    fn resume(&self) -> ResumePoint {
        let boundary = self
            .boundaries()
            .last()
            .copied()
            .cloned()
            .expect("a committed block");
        ResumePoint {
            block: boundary.block_number,
            record_count: boundary.end_tx_idx.as_index(),
            l2_timestamp: boundary.l2_timestamp,
            base_fee: boundary.base_fee,
            gas_used: boundary.gas_used,
        }
    }

    fn then(mut self, later: Self) -> Self {
        self.messages.extend(later.messages);
        Self {
            messages: self.messages,
            state: later.state,
        }
    }

    /// The published receipts, in order. The pipeline can publish the
    /// receipts of a block before the boundary of the block before it, so
    /// only each kind keeps its order.
    fn receipts(&self) -> Vec<&Receipt> {
        self.messages
            .iter()
            .filter_map(|m| match m {
                CMessage::Receipt(r) => Some(r),
                CMessage::BlockBoundary(_) => None,
            })
            .collect()
    }

    /// The published boundaries, in order.
    fn boundaries(&self) -> Vec<&BlockBoundary> {
        self.messages
            .iter()
            .filter_map(|m| match m {
                CMessage::BlockBoundary(b) => Some(b),
                CMessage::Receipt(_) => None,
            })
            .collect()
    }

    fn account(&self, address: Address) -> Option<(u64, U256, B256)> {
        self.state.basic(address).unwrap()
    }

    /// Every deposit has one successful receipt, in L1 order, and the
    /// recipient holds exactly the sum of the mints with its nonce
    /// unchanged.
    pub(super) fn assert_credits(&self, chain: &Chain) {
        let credited: Vec<(B256, BPosition)> = self
            .receipts()
            .into_iter()
            .filter(|r| chain.source_hashes().contains(&r.tx_hash))
            .map(|r| {
                assert!(r.status, "deposit {} succeeds", r.tx_hash);
                (r.tx_hash, r.tx_idx)
            })
            .collect();
        assert_eq!(
            credited.iter().map(|(hash, _)| *hash).collect::<Vec<_>>(),
            chain.source_hashes(),
            "one receipt for each deposit, in L1 order"
        );
        assert!(
            credited.windows(2).all(|pair| pair[0].1 < pair[1].1),
            "the receipt order is the L1 order"
        );
        let minted: u128 = DEPOSITS.iter().map(|(.., mint)| mint).sum();
        let (nonce, balance, _) = self.account(RECIPIENT).expect("the recipient");
        assert_eq!((nonce, balance), (0, U256::from(minted)), "an exact credit");
    }

    /// The state audit: the same receipts and boundaries in the same
    /// order, the same accounts, and the exact credits.
    pub(super) fn assert_matches(&self, reference: &Self, chain: &Chain) {
        assert_eq!(self.receipts(), reference.receipts(), "receipts");
        assert_eq!(self.boundaries(), reference.boundaries(), "boundaries");
        let accounts = |run: &Self| [chain.sender, SINK, RECIPIENT].map(|a| run.account(a));
        assert_eq!(accounts(self), accounts(reference), "accounts");
        let paid: u64 = (1..=BLOCKS).sum();
        assert_eq!(
            self.account(SINK).map(|(_, balance, _)| balance),
            Some(U256::from(paid))
        );
        assert_eq!(
            self.account(chain.sender).map(|(nonce, ..)| nonce),
            Some(BLOCKS)
        );
        self.assert_credits(chain);
    }
}
