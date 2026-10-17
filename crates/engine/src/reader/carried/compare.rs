//! [`Compare`]: the check of the canonical stream against the committed
//! history of this consumer, before the reader delivers a frame.
//!
//! A member can lose the unsynced tail of its log, for example after a
//! power loss on a majority of the members. The cluster then continues
//! from a shorter log, and the next entries take the lost indexes. A
//! paired executor can have committed entries of the lost tail. So at each
//! start, and at each new connection, the reader replays the source from a
//! floor below its cursor. It compares each frame below the cursor with
//! its committed history: the transaction hash of each receipt by index,
//! and the end, the timestamp and the L1 origin of each header. The first
//! difference is the divergence point.
//!
//! A consumer publishes the receipts of a block before it commits the
//! block. So the comparison also reads on above the cursor, through the
//! last block with a receipt that the consumer published there
//! ([`PublishedTail`]). A difference there makes those receipts void, and
//! the state stays. With no difference, delivery starts at the cursor.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;

use alloy_primitives::{B256, keccak256};
use kardamom_state::{StateError, StateSnapshot};
use kardamom_types::{BPosition, BlockBoundaryStart, TxOrderingMessage};

use crate::ExecutorError;
use crate::reader::Ordered;
use crate::reader::cluster::{BoundaryMetrics, Reorder, ReplayCursor};

/// The committed history that the comparison reads: the headers and the
/// receipt hashes from the floor up to the delivery cursor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalHistory {
    /// The first point of the comparison: a canonical index and the block
    /// that starts at it.
    floor: (u64, u64),
    /// The delivery cursor: the end of the committed history.
    cursor: (u64, u64),
    /// The header of each block from the floor to the last committed
    /// block, by block number.
    headers: BTreeMap<u64, BlockBoundaryStart>,
    /// The transaction hash of each receipt from the floor to the cursor,
    /// by canonical index. A skipped transaction has no receipt.
    receipts: HashMap<u64, B256>,
    /// The receipts published above the cursor.
    tail: PublishedTail,
}

/// The receipts that a consumer published above its delivery cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishedTail {
    /// Every receipt published above the cursor, by canonical index: the
    /// block and the transaction hash. Empty when none was published.
    Known(BTreeMap<u64, (u64, B256)>),
    /// The record of the published receipts can be short, for example
    /// after a restart of the host. Every receipt above the cursor can be
    /// void.
    Unknown,
}

impl Default for PublishedTail {
    fn default() -> Self {
        Self::Known(BTreeMap::new())
    }
}

impl PublishedTail {
    /// Whether the slot `index` of `block` in the stream differs from the
    /// receipt published at `index`. `hash` is the transaction hash of the
    /// slot, or `None` for a slot with no receipt. An index with no
    /// published receipt does not differ.
    fn differs_at(&self, index: u64, block: u64, hash: Option<B256>) -> bool {
        match self {
            Self::Known(published) => published
                .get(&index)
                .is_some_and(|(at, receipt)| (*at, Some(*receipt)) != (block, hash)),
            Self::Unknown => true,
        }
    }

    /// Whether the boundary `b` of the stream seals its block before a
    /// receipt that was published in that block or in a block below it.
    /// The receipts are in index order, so the first one at or above the
    /// end of the block has the lowest block of them.
    fn cut_by(&self, b: &BlockBoundaryStart) -> bool {
        match self {
            Self::Known(published) => published
                .range(b.end_tx_idx.as_index()..)
                .next()
                .is_some_and(|(_, (block, _))| *block <= b.block_number),
            Self::Unknown => true,
        }
    }
}

impl LocalHistory {
    /// Read the history of the last `window` committed blocks of
    /// `snapshot`. `cursor` is the delivery cursor of the snapshot: the
    /// next canonical index and the next block. Returns `None` at genesis:
    /// there is nothing to compare.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if a read fails, or if a header in the window
    /// is absent.
    pub fn read(
        snapshot: &StateSnapshot,
        cursor: (u64, u64),
        window: NonZeroU64,
    ) -> Result<Option<Self>, StateError> {
        // Block 0 is genesis: the stream carries blocks from block 1.
        let Some(last) = cursor.1.checked_sub(1).and_then(NonZeroU64::new) else {
            return Ok(None);
        };
        // A window longer than the chain starts at the first block of the
        // chain: the subtraction saturates to that meaning.
        let floor_block =
            NonZeroU64::new(cursor.1.saturating_sub(window.get())).unwrap_or(NonZeroU64::MIN);
        let headers = (floor_block.get()..=last.get())
            .map(|block| Self::header_of(snapshot, block))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let floor_index = match floor_block.get().checked_sub(1).and_then(NonZeroU64::new) {
            Some(before) => Self::header_of(snapshot, before.get())?
                .1
                .end_tx_idx
                .as_index(),
            None => 0,
        };
        let receipts = snapshot
            .receipt_hashes(
                BPosition::from_index(floor_index),
                BPosition::from_index(cursor.0),
            )?
            .into_iter()
            .map(|(pos, hash)| (pos.as_index(), hash))
            .collect();
        Ok(Some(Self {
            floor: (floor_index, floor_block.get()),
            cursor,
            headers,
            receipts,
            tail: PublishedTail::default(),
        }))
    }

    /// This history with the receipts published above its cursor.
    #[must_use]
    pub fn with_tail(self, tail: PublishedTail) -> Self {
        Self { tail, ..self }
    }

    /// The committed header of `block`, as the boundary that sealed it.
    fn header_of(
        snapshot: &StateSnapshot,
        block: u64,
    ) -> Result<(u64, BlockBoundaryStart), StateError> {
        let header = snapshot.header(block)?.ok_or_else(|| {
            StateError::Recovery(format!("block {block} has no committed header"))
        })?;
        Ok((
            block,
            BlockBoundaryStart {
                block_number: block,
                end_tx_idx: header.end_tx_idx,
                l2_timestamp: header.l2_timestamp,
                l1_origin: header.l1_origin,
            },
        ))
    }

    /// A history from its parts, for a test.
    #[cfg(test)]
    pub(crate) fn of(
        floor: (u64, u64),
        cursor: (u64, u64),
        headers: impl IntoIterator<Item = BlockBoundaryStart>,
        receipts: impl IntoIterator<Item = (u64, B256)>,
    ) -> Self {
        Self {
            floor,
            cursor,
            headers: headers.into_iter().map(|h| (h.block_number, h)).collect(),
            receipts: receipts.into_iter().collect(),
            tail: PublishedTail::default(),
        }
    }

    /// The delivery cursor: the end of the committed history.
    pub(crate) fn cursor(&self) -> (u64, u64) {
        self.cursor
    }
}

/// The first difference between the stream and the committed history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Divergence {
    pub(crate) block: u64,
    pub(crate) index: u64,
    /// The fingerprint of the first frame of `block` in the stream: the
    /// canonical id of an entry, or the hash of the boundary of an empty
    /// block. Every reader of one stream has that frame, also a reader
    /// that finds the difference at a later frame of the block.
    pub(crate) stream: B256,
}

impl Divergence {
    /// The error of a divergence above the committed history: the
    /// published receipts at and above the block are void.
    fn published(self) -> ExecutorError {
        ExecutorError::PublishedDiverged {
            block: self.block,
            index: self.index,
            stream: self.stream,
        }
    }
}

impl From<Divergence> for ExecutorError {
    fn from(d: Divergence) -> Self {
        Self::StreamDiverged {
            block: d.block,
            index: d.index,
            stream: d.stream,
        }
    }
}

/// Where the comparison stands after the frames that it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Progress {
    /// It needs more frames.
    Pending,
    /// The stream equals the history up to the delivery cursor.
    Equal,
    /// The source retains no frame below the delivery cursor, so nothing is
    /// compared. The retained log of a member starts at its snapshot, which
    /// is on its disk, and the frames below a snapshot survive a power
    /// loss. So the source holds no lost entry below the cursor.
    Skipped,
    /// The stream differs from a block that equals the history before it.
    Diverged(Divergence),
    /// The committed history is equal, and the stream differs from a
    /// receipt published above it.
    Published(Divergence),
    /// The stream differs at the floor of the window: the first compared
    /// block, or a floor that the source refuses as no point of its
    /// stream. The divergence can be below the floor. A wider window can
    /// find it, unless the floor is the first block of the chain or the
    /// retention of the source.
    BelowWindow { floor_block: u64, widen: bool },
}

impl Progress {
    /// What the reader does with this progress: wait for more frames
    /// (`None`), deliver from the cursor (`Some`), or end the run.
    ///
    /// # Errors
    ///
    /// Returns the divergence that ends the run.
    pub(crate) fn settle(self) -> Result<Option<Settled>, ExecutorError> {
        match self {
            Self::Pending => Ok(None),
            Self::Equal => Ok(Some(Settled::Equal)),
            Self::Skipped => Ok(Some(Settled::Skipped)),
            Self::Diverged(d) => Err(d.into()),
            Self::Published(d) => Err(d.published()),
            Self::BelowWindow { floor_block, widen } => {
                Err(ExecutorError::DivergedBelowWindow { floor_block, widen })
            }
        }
    }
}

/// How a comparison that lets the reader deliver ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Settled {
    Equal,
    Skipped,
}

impl Settled {
    pub(crate) fn log(self) {
        match self {
            Self::Equal => tracing::info!("the stream equals the committed history"),
            Self::Skipped => tracing::warn!(
                "the source retains no frame of the comparison window; delivering without a comparison"
            ),
        }
    }
}

/// The comparison of one run of the reader.
pub(crate) struct Compare {
    history: LocalHistory,
    /// The cursor and the pending set of the compared frames. It starts at
    /// the floor, and it delivers nothing downstream.
    reorder: Reorder,
    /// A block of the window compared equal. A difference before it is at
    /// the floor.
    equal_block: bool,
    /// The retention of the source moved the floor up, so a wider window
    /// finds nothing more.
    raised: bool,
    /// The fingerprint of the first frame of the block that the comparison
    /// reads now.
    head: Option<B256>,
}

impl Compare {
    pub(crate) fn new(history: LocalHistory) -> Self {
        let reorder = Self::reorder_at(history.floor);
        Self {
            history,
            reorder,
            equal_block: false,
            raised: false,
            head: None,
        }
    }

    fn reorder_at((index, block): (u64, u64)) -> Reorder {
        let mut reorder = Reorder::new(ReplayCursor::new(index, block));
        reorder.metrics = BoundaryMetrics::Off;
        reorder.restart();
        reorder
    }

    /// The next frame that the comparison needs.
    pub(crate) fn cursor(&self) -> (u64, u64) {
        self.reorder.next()
    }

    /// The pending set of the compared frames.
    pub(crate) fn reorder(&mut self) -> &mut Reorder {
        &mut self.reorder
    }

    /// Start again from the floor. A new connection can serve another
    /// stream, so the frames compared on the old one prove nothing.
    pub(crate) fn rewind(&mut self) {
        self.reorder = Self::reorder_at(self.history.floor);
        self.equal_block = false;
        self.head = None;
    }

    /// The progress of a difference at the floor.
    fn below_window(&self) -> Progress {
        let floor_block = self.history.floor.1;
        Progress::BelowWindow {
            floor_block,
            widen: !self.raised && floor_block > NonZeroU64::MIN.get(),
        }
    }

    /// The source refused the cursor with its retained floor.
    ///
    /// - A cursor at or above the retained floor on both axes names no
    ///   point of the stream of the source: the blocks before it differ.
    ///   The comparison ends below the window.
    /// - A cursor below the retained floor moves up to the first committed
    ///   block that starts at or above the floor.
    pub(crate) fn refused(&mut self, oldest_index: u64, oldest_block: u64) -> Progress {
        let (index, block) = self.cursor();
        if oldest_index <= index && oldest_block <= block {
            return self.below_window();
        }
        // A header is below the cursor block, so the next block fits.
        let raised = self
            .history
            .headers
            .range(block..)
            .filter_map(|(b, h)| Some((b.checked_add(1)?, h.end_tx_idx.as_index())))
            .find(|(next, end)| *next >= oldest_block && *end >= oldest_index)
            .map(|(next, end)| (end, next))
            .filter(|cursor| cursor.1 < self.history.cursor.1);
        let Some(raised) = raised else {
            return Progress::Skipped;
        };
        self.history.floor = raised;
        self.raised = true;
        self.rewind();
        Progress::Pending
    }

    /// Compare every frame that the pending set can deliver.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the pending set fails: a cursor overflow, or a
    /// boundary that seals below the cursor.
    pub(crate) fn drain(&mut self) -> Result<Progress, ExecutorError> {
        loop {
            let Some(progress) = self.step()? else {
                continue;
            };
            return Ok(progress);
        }
    }

    /// Whether the comparison has every frame that it needs: the stream
    /// is at the delivery cursor, and it sealed each block with a receipt
    /// published above the cursor. With an unknown tail, the first frame
    /// above the cursor ends the comparison instead.
    fn reached(&self) -> bool {
        let next = self.reorder.next();
        match &self.history.tail {
            PublishedTail::Known(published) => published
                .last_key_value()
                .map_or(next == self.history.cursor, |(_, (last, _))| next.1 > *last),
            PublishedTail::Unknown => false,
        }
    }

    /// Compare one frame. `None` when the frame is equal and the
    /// comparison needs more frames.
    fn step(&mut self) -> Result<Option<Progress>, ExecutorError> {
        if self.reached() {
            return Ok(Some(Progress::Equal));
        }
        let Some((pos, item)) = self.reorder.try_deliver()? else {
            return Ok(Some(Progress::Pending));
        };
        let head = *self.head.get_or_insert(Self::fingerprint(&item));
        if item.is_boundary() {
            self.head = None;
        }
        let Some((block, index)) = self.check(pos, &item) else {
            self.equal_block |= item.is_boundary();
            return Ok(None);
        };
        let divergence = Divergence {
            block,
            index,
            stream: head,
        };
        let progress = match (block >= self.history.cursor.1, self.equal_block) {
            (true, _) => Progress::Published(divergence),
            (false, true) => Progress::Diverged(divergence),
            (false, false) => self.below_window(),
        };
        Ok(Some(progress))
    }

    /// The fingerprint of a frame: the canonical id of an entry, an epoch
    /// or a reference, the hash of a void, or the hash of the fields of a
    /// boundary.
    fn fingerprint(item: &Ordered) -> B256 {
        match item {
            Ordered::Tx(tx) => tx.canonical_id,
            Ordered::Message(TxOrderingMessage::BoundaryStart(b)) => {
                let fields = [
                    b.block_number,
                    b.end_tx_idx.as_index(),
                    b.l2_timestamp,
                    b.l1_origin,
                ];
                keccak256(fields.map(u64::to_be_bytes).concat())
            }
            Ordered::Message(TxOrderingMessage::Epoch(e)) => e.canonical_id(),
            Ordered::Message(TxOrderingMessage::RemoteEpoch(r)) => r.canonical_id(),
            Ordered::Message(TxOrderingMessage::TxRef(r)) => r.tx_hash,
            Ordered::Message(TxOrderingMessage::DepositRef(r)) => r.source_hash,
            Ordered::Message(TxOrderingMessage::Void(v)) => v.tx_hash,
        }
    }

    /// The receipt of each slot of a frame at `pos`: the transaction hash,
    /// or `None` for a slot with no receipt. An epoch has the marker and
    /// then one slot for each deposit or message, whose receipt has its
    /// source hash. A void has a slot with no receipt. A boundary has no
    /// slot.
    fn slots(pos: BPosition, item: &Ordered) -> Vec<(u64, Option<B256>)> {
        let index = pos.as_index();
        let marked =
            |hashes: Vec<B256>| {
                std::iter::once((index, None))
                    .chain((1..).zip(hashes).filter_map(|(k, hash)| {
                        index.checked_add(k).map(|slot| (slot, Some(hash)))
                    }))
                    .collect()
            };
        match item {
            Ordered::Tx(tx) => vec![(index, Some(tx.canonical_id))],
            Ordered::Message(TxOrderingMessage::Epoch(e)) => {
                marked(e.deposits.iter().map(|d| d.source_hash).collect())
            }
            Ordered::Message(TxOrderingMessage::RemoteEpoch(r)) => {
                marked(r.messages.into_iter().map(|m| m.source_hash).collect())
            }
            Ordered::Message(TxOrderingMessage::TxRef(r)) => vec![(index, Some(r.tx_hash))],
            Ordered::Message(TxOrderingMessage::DepositRef(r)) => {
                vec![(index, Some(r.source_hash))]
            }
            Ordered::Message(TxOrderingMessage::Void(_)) => vec![(index, None)],
            Ordered::Message(TxOrderingMessage::BoundaryStart(_)) => Vec::new(),
        }
    }

    /// Whether the slot `index` of `block` in the stream, with the receipt
    /// `hash`, differs from this history. Below the cursor, a committed
    /// receipt at the index must have the same hash; above it, see
    /// [`PublishedTail::differs_at`]. An index with no receipt on this side
    /// does not differ: a skipped transaction has no receipt.
    fn differs(&self, index: u64, block: u64, hash: Option<B256>) -> bool {
        if index < self.history.cursor.0 {
            return self
                .history
                .receipts
                .get(&index)
                .is_some_and(|local| Some(*local) != hash);
        }
        self.history.tail.differs_at(index, block, hash)
    }

    /// The block and the index of a difference at one frame, if any.
    ///
    /// - Each slot of the frame compares its receipt with the history. A
    ///   slot with no receipt in the stream, such as the marker of an
    ///   epoch, differs from a receipt at its index.
    /// - Below the cursor, a boundary compares itself with the committed
    ///   header of its block. Above it, a boundary must not seal its block
    ///   before a published receipt of that block.
    fn check(&self, pos: BPosition, item: &Ordered) -> Option<(u64, u64)> {
        let block = self.reorder.next().1;
        let cursor = self.history.cursor;
        match item {
            Ordered::Message(TxOrderingMessage::BoundaryStart(b)) if b.block_number < cursor.1 => {
                (self.history.headers.get(&b.block_number) != Some(b))
                    .then_some((b.block_number, b.end_tx_idx.as_index()))
            }
            Ordered::Message(TxOrderingMessage::BoundaryStart(b)) => self
                .history
                .tail
                .cut_by(b)
                .then_some((b.block_number, b.end_tx_idx.as_index())),
            _ => Self::slots(pos, item)
                .into_iter()
                .find(|(index, hash)| self.differs(*index, block, *hash))
                .map(|(index, _)| (block, index)),
        }
    }
}
