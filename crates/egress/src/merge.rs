//! [`Merge`]: one canonical stream from the copies of every executor.
//!
//! Each executor sends the canonical frames that it reads, once each and in
//! canonical order, but best effort: a copy can miss a frame. Each message
//! is `[seq:u64][frame]`, and the sequence counts the missed frames too. A
//! frame is a relayed record, keyed by its canonical index, or a block
//! boundary, keyed by its block.
//!
//! The merge delivers a frame when a copy shows that the frame follows the
//! last delivered frame: that copy sent it right after the last delivered
//! frame, with the next sequence. So a gap in one copy never reorders the
//! stream: the frame of that copy after the gap names no frame before it,
//! and the copy of another executor fills the gap. Later frames of that copy
//! chain again once the copy reaches the delivered head.
//!
//! The first frame of a copy names no frame before it. It goes out alone
//! only where its place is certain:
//! - the boundary of the next block that closes at the next index: no
//!   record of that block is missing;
//! - at the start of the chain, the first frame of an executor that starts
//!   there (its sequence is 0).
//!
//! A record is never certain alone: the boundary of an empty block can come
//! before it. So after a restart of the egress the copies chain only from a
//! frame that another source delivers: the frames of a peer egress, in
//! order ([`Merge::take_ordered`]), or a new start after a boundary
//! ([`Merge::rebase`]). [`Merge::joined`] says when a copy chained.
//!
//! A frame whose key the merge delivered is a copy. The merge compares its
//! bytes with the first copy while it keeps the digest of that key, and
//! counts a difference. It never stops on one: the validator decides which
//! copy is wrong.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

use kardamom_cluster_adapter::wire::{
    EGRESS_KIND_BOUNDARY, EGRESS_KIND_RELAYED, EgressItem, RT_EPOCH, RT_REMOTE_EPOCH, epoch_slots,
    remote_epoch_slots,
};
use kardamom_types::TxOrderingMessage;

use crate::metrics;

/// The key of one frame: the canonical index of a record, or the block of a
/// boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Key {
    Record(u64),
    Boundary(u64),
}

impl Key {
    /// The key of `frame`: the u64 after the kind byte of a record or a
    /// boundary. `None` for any other frame.
    #[must_use]
    pub fn of(frame: &[u8]) -> Option<Self> {
        let (&kind, rest) = frame.split_first()?;
        let key = u64::from_le_bytes(*rest.first_chunk::<8>()?);
        match kind {
            EGRESS_KIND_RELAYED => Some(Self::Record(key)),
            EGRESS_KIND_BOUNDARY => Some(Self::Boundary(key)),
            _ => None,
        }
    }
}

/// Where the merged stream stands: the next record index and the next block
/// to deliver, and the last delivered frame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Head {
    pub next_index: u64,
    pub next_block: u64,
    /// The last delivered frame. `None` at the start of the chain.
    pub last: Option<Key>,
}

impl Head {
    /// Whether the stream passed `key`: the frame is behind this head.
    #[must_use]
    pub fn passed(&self, key: Key) -> bool {
        match key {
            Key::Record(index) => index < self.next_index,
            Key::Boundary(block) => block < self.next_block,
        }
    }

    /// Whether `frame` takes the next place with no frame of its copy
    /// before it: the boundary of the next block that closes at the next
    /// index, or at the start of the chain the first frame (`seq` 0) of an
    /// executor that starts there.
    fn fits(&self, key: Key, frame: &[u8], seq: u64) -> bool {
        let closes_here =
            key == Key::Boundary(self.next_block) && boundary_end(frame) == Some(self.next_index);
        let starts_here = self.last.is_none() && seq == 0 && key == Key::Record(self.next_index);
        closes_here || starts_here
    }

    /// The head after the boundary `frame`: the next block, the next index
    /// at its end, and the boundary as the last frame. `None` for a frame
    /// that is no boundary.
    #[must_use]
    pub fn after_boundary(frame: &[u8]) -> Option<Self> {
        let Some(Key::Boundary(block)) = Key::of(frame) else {
            return None;
        };
        Some(Self {
            next_index: boundary_end(frame)?,
            next_block: block.checked_add(1)?,
            last: Some(Key::Boundary(block)),
        })
    }

    /// Move the head past `frame`. A record moves the next index by its
    /// slots: an epoch takes its marker and one slot for each deposit, so
    /// the next record is that many indexes on.
    pub(crate) fn advance(&mut self, frame: &[u8]) {
        let Some(key) = Key::of(frame) else {
            return;
        };
        match key {
            Key::Record(index) => self.next_index = index + slots(frame),
            Key::Boundary(block) => self.next_block = block + 1,
        }
        self.last = Some(key);
    }
}

/// The limits of the merge.
#[derive(Clone, Copy, Debug)]
pub struct MergeConfig {
    /// The frames that may wait for their place. Past it, the merge drops
    /// every waiting frame: every copy missed a frame before them, so the
    /// stream stalls until a copy sends the missing frame again.
    pub max_waiting: NonZeroUsize,
    /// The delivered keys whose digests the merge keeps, to compare copies.
    pub compared: NonZeroUsize,
}

/// One canonical stream from the frames of every copy.
pub struct Merge {
    cfg: MergeConfig,
    head: Head,
    /// The sequence and the key of the last frame that each copy sent, by
    /// the session of the copy.
    copies: HashMap<i32, (u64, Key)>,
    /// The frames of the copies ahead of the head, by the key of the frame
    /// before them in their copy.
    waiting: HashMap<Key, (Key, Vec<u8>)>,
    /// The digests of the newest delivered keys.
    digests: Digests,
    /// Whether a frame of a copy went out since the start: the merge
    /// follows the copies.
    joined: bool,
}

impl Merge {
    #[must_use]
    pub fn new(cfg: MergeConfig, head: Head) -> Self {
        Self {
            cfg,
            head,
            copies: HashMap::new(),
            waiting: HashMap::new(),
            digests: Digests::new(cfg.compared),
            joined: false,
        }
    }

    /// Whether a frame of a copy went out since the start or the last
    /// rebase.
    #[must_use]
    pub fn joined(&self) -> bool {
        self.joined
    }

    /// Start the stream again at `head`, after a boundary: the frames
    /// that wait are of the old head, and go.
    pub fn rebase(&mut self, head: Head) {
        self.head = head;
        self.waiting.clear();
    }

    /// Take one frame of a peer egress, which serves the stream in order
    /// from this head. A frame that the stream passed is a copy. Each other
    /// frame is the next one: it goes out, and then each frame of a copy
    /// that waits behind it.
    pub fn take_ordered(&mut self, frame: &[u8], out: &mut Vec<Vec<u8>>) {
        let Some(key) = Key::of(frame) else {
            return;
        };
        if self.head.passed(key) {
            self.compare(0, key, frame);
            return;
        }
        self.deliver(key, frame.to_vec(), out);
        while self.release_next(out) {}
    }

    /// Where the merged stream stands.
    #[must_use]
    pub fn head(&self) -> Head {
        self.head
    }

    /// Take one message `[seq:u64][frame]` of the copy `session`. The frames
    /// that it lets go, in canonical order, go to `out`. A message that holds
    /// neither a record nor a boundary carries nothing for the stream.
    pub fn take(&mut self, session: i32, message: &[u8], out: &mut Vec<Vec<u8>>) {
        let Some((seq, frame)) = message.split_first_chunk::<8>() else {
            return;
        };
        let seq = u64::from_le_bytes(*seq);
        let Some(key) = Key::of(frame) else {
            return;
        };
        let before = self
            .copies
            .insert(session, (seq, key))
            .filter(|(last, _)| last.checked_add(1) == Some(seq))
            .map(|(_, key)| key);
        if self.head.passed(key) {
            self.compare(session, key, frame);
            return;
        }
        let chains = before.is_some() && before == self.head.last;
        if chains || self.head.fits(key, frame, seq) {
            self.deliver_chain(key, frame.to_vec(), out);
            return;
        }
        let frame = frame.to_vec();
        if let Some(before) = before.filter(|b| !self.head.passed(*b)) {
            self.wait(before, key, frame);
        }
    }

    /// Deliver `frame` of a copy, then every waiting frame that chains
    /// behind it.
    fn deliver_chain(&mut self, key: Key, frame: Vec<u8>, out: &mut Vec<Vec<u8>>) {
        self.joined = true;
        self.deliver(key, frame, out);
        while self.release_next(out) {}
    }

    /// Hold `frame` until `before`, the frame before it in its copy, is
    /// delivered. A copy whose frame before is delivered but is not the last
    /// one has a gap: its frame cannot chain, and it does not wait.
    fn wait(&mut self, before: Key, key: Key, frame: Vec<u8>) {
        self.waiting.entry(before).or_insert((key, frame));
        if self.waiting.len() > self.cfg.max_waiting.get() {
            tracing::error!(
                waiting = self.waiting.len(),
                head = ?self.head,
                "every copy missed a frame: the egress drops the frames that wait"
            );
            self.waiting.clear();
            metrics::count_waiting_dropped();
        }
    }

    /// Deliver the frame that waits for the last delivered frame. Returns
    /// whether one was there.
    fn release_next(&mut self, out: &mut Vec<Vec<u8>>) -> bool {
        let Some(last) = self.head.last else {
            return false;
        };
        let Some((key, frame)) = self.waiting.remove(&last) else {
            return false;
        };
        if self.head.passed(key) {
            return true;
        }
        self.joined = true;
        self.deliver(key, frame, out);
        true
    }

    fn deliver(&mut self, key: Key, frame: Vec<u8>, out: &mut Vec<Vec<u8>>) {
        self.digests.keep(key, digest(&frame));
        self.head.advance(&frame);
        out.push(frame);
    }

    /// Compare a copy of a delivered frame with the first copy, while the
    /// merge keeps the digest of its key.
    fn compare(&self, session: i32, key: Key, frame: &[u8]) {
        if self
            .digests
            .of(key)
            .is_some_and(|first| first != digest(frame))
        {
            tracing::error!(
                session,
                ?key,
                "a copy of a canonical frame differs from the first copy"
            );
            metrics::count_copy_mismatch();
        }
    }
}

/// The digests of the newest delivered keys, the oldest out first. Each key
/// is delivered once, so each comes in once.
struct Digests {
    cap: NonZeroUsize,
    order: VecDeque<Key>,
    of: HashMap<Key, u64>,
}

impl Digests {
    fn new(cap: NonZeroUsize) -> Self {
        Self {
            cap,
            order: VecDeque::new(),
            of: HashMap::new(),
        }
    }

    fn keep(&mut self, key: Key, digest: u64) {
        self.order.push_back(key);
        self.of.insert(key, digest);
        if self.order.len() > self.cap.get()
            && let Some(old) = self.order.pop_front()
        {
            self.of.remove(&old);
        }
    }

    fn of(&self, key: Key) -> Option<u64> {
        self.of.get(&key).copied()
    }
}

/// The offset of the record type in a record frame:
/// `[kind:u8][index:u64][len:u32]`, then `[canonical_id:32][record_type:u8]`.
const RECORD_TYPE_AT: usize = 1 + 8 + 4 + 32;

/// The canonical slots of a record frame. Only an epoch and a remote epoch
/// take more than one, so only they decode.
fn slots(frame: &[u8]) -> u64 {
    let epoch = matches!(
        frame.get(RECORD_TYPE_AT),
        Some(&(RT_EPOCH | RT_REMOTE_EPOCH))
    );
    if !epoch {
        return 1;
    }
    match EgressItem::decode(frame) {
        Ok(EgressItem::Record {
            msg: TxOrderingMessage::Epoch(e),
            ..
        }) => epoch_slots(&e),
        Ok(EgressItem::Record {
            msg: TxOrderingMessage::RemoteEpoch(r),
            ..
        }) => remote_epoch_slots(&r),
        _ => 1,
    }
}

/// The end index of a boundary frame:
/// `[kind:u8][block:u64][end_tx_idx:u64]...`.
fn boundary_end(frame: &[u8]) -> Option<u64> {
    let end = frame.get(9..17)?.first_chunk::<8>()?;
    Some(u64::from_le_bytes(*end))
}

fn digest(frame: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    frame.hash(&mut h);
    h.finish()
}

#[cfg(test)]
#[path = "merge_tests.rs"]
mod tests;
