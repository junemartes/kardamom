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
//! and the copy of another executor fills the gap. The first frame of a copy
//! after its start names no frame before it either, so it chains only at
//! the start of the stream. Later frames of that copy chain again once the
//! copy reaches the delivered head.
//!
//! A frame whose key the merge delivered is a copy. The merge compares its
//! bytes with the first copy while it keeps the digest of that key, and
//! counts a difference. It never stops on one: the validator decides which
//! copy is wrong.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

use kardamom_cluster_adapter::wire::{EGRESS_KIND_BOUNDARY, EGRESS_KIND_RELAYED};

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

    /// Move the head past `key`.
    pub(crate) fn advance(&mut self, key: Key) {
        match key {
            Key::Record(index) => self.next_index = index + 1,
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
        }
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
        let frame = frame.to_vec();
        if before == self.head.last {
            self.deliver_chain(key, frame, out);
            return;
        }
        if let Some(before) = before.filter(|b| !self.head.passed(*b)) {
            self.wait(before, key, frame);
        }
    }

    /// Deliver `frame`, then every waiting frame that chains behind it.
    fn deliver_chain(&mut self, key: Key, frame: Vec<u8>, out: &mut Vec<Vec<u8>>) {
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
        self.deliver(key, frame, out);
        true
    }

    fn deliver(&mut self, key: Key, frame: Vec<u8>, out: &mut Vec<Vec<u8>>) {
        self.digests.keep(key, digest(&frame));
        self.head.advance(key);
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

fn digest(frame: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    frame.hash(&mut h);
    h.finish()
}

#[cfg(test)]
#[path = "merge_tests.rs"]
mod tests;
