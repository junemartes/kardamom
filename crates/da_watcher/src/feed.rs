//! The da-watcher's view of the `l1_blocks` stream: the records the L1
//! follower publishes, one per finalized L1 block.
//!
//! The da-watcher has no L1 access. A record on the stream is a block that
//! two L1 sources agreed on and that descends from the record before it;
//! the watcher checks only the parent link against its own head. Two
//! follower instances publish every block, so the records hold copies,
//! and a lying instance can publish a second hash for one number: the
//! watcher's [`kardamom_types::L1BlockDedup`] keeps the first and halts
//! on the second.

use async_trait::async_trait;
use kardamom_types::L1Block;

/// The stream, live and from its archives.
#[async_trait]
pub trait BlockFeed: Send + 'static {
    /// The records of the blocks from `from` on that the archives hold,
    /// in block order, copies included. The list can stop short of the
    /// newest record: the live records follow it. An empty list means no
    /// archive answered or none holds `from`; the watcher asks again later.
    async fn history(&mut self, from: u64) -> Vec<L1Block>;

    /// The next live record. `None` once the stream closed. Cancel-safe:
    /// a record is never lost when the call is dropped.
    async fn next(&mut self) -> Option<L1Block>;

    /// The next live record that already arrived, without a wait.
    fn try_next(&mut self) -> Option<L1Block>;
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use alloy_primitives::B256;
    use async_trait::async_trait;
    use kardamom_types::L1Block;
    use kardamom_types::epoch::{EpochRecord, LockboxLog, derive_epoch};

    use super::BlockFeed;

    /// A scripted chain and stream, shared between a test and the feeds it
    /// hands to its watchers (a restart builds a new feed on the same
    /// chain).
    #[derive(Default)]
    struct Chain {
        /// Every block the follower published, by number.
        blocks: BTreeMap<u64, L1Block>,
        /// The newest published block.
        head: Option<u64>,
        /// The tips a test scripted: each `try_next` on an empty live
        /// queue publishes up to the next one.
        tips: VecDeque<u64>,
        /// Extra live records a test injected: copies, lies, gaps.
        injected: VecDeque<L1Block>,
        /// Whether the archives answer `history`.
        archive_down: bool,
        /// Records the archives hold beside the chain: another instance's.
        archived: Vec<L1Block>,
    }

    /// The test's handle on the scripted stream.
    #[derive(Clone, Default)]
    pub struct ScriptedStream {
        chain: Arc<Mutex<Chain>>,
        /// Lockbox logs by block, for the epochs the follower derives.
        logs: Arc<Mutex<BTreeMap<u64, Vec<LockboxLog>>>>,
    }

    impl ScriptedStream {
        /// The hash of block `number` on the scripted chain.
        #[must_use]
        #[allow(
            clippy::cast_possible_truncation,
            reason = "deliberate: repeats number mod 256"
        )]
        pub fn hash(number: u64) -> B256 {
            let mut bytes = [number as u8; 32];
            bytes[..8].copy_from_slice(&number.to_be_bytes());
            B256::from(bytes)
        }

        /// The record the follower publishes for block `number`.
        ///
        /// # Panics
        /// Panics when a scripted log does not derive.
        #[must_use]
        pub fn record(&self, number: u64) -> L1Block {
            let logs = self
                .logs
                .lock()
                .unwrap()
                .get(&number)
                .cloned()
                .unwrap_or_default();
            let hash = Self::hash(number);
            L1Block {
                number,
                hash,
                parent_hash: Self::hash(number.saturating_sub(1)),
                timestamp: number.saturating_mul(12),
                epoch: derive_epoch(number, hash, &logs).expect("a scripted log derives"),
                batches: Vec::new(),
            }
        }

        /// The epoch of block `number`, as the follower derives it.
        #[must_use]
        pub fn epoch(&self, number: u64) -> EpochRecord {
            self.record(number).epoch
        }

        /// Script the lockbox logs of block `number`.
        ///
        /// # Panics
        /// Panics when a test thread panicked while it held the lock.
        pub fn set_logs(&self, number: u64, logs: Vec<LockboxLog>) {
            self.logs.lock().unwrap().insert(number, logs);
        }

        /// Script the next finalized tips, one per `try_next` on an empty
        /// live queue. The first tip of a new stream publishes only that
        /// block: a subscriber that joins sees the stream from there.
        ///
        /// # Panics
        /// Panics when a test thread panicked while it held the lock.
        pub fn tips(&self, tips: &[u64]) {
            self.chain.lock().unwrap().tips.extend(tips);
        }

        /// Publish blocks up to `tip` at once: the archives hold them, and
        /// no live record carries them.
        ///
        /// # Panics
        /// Panics when a test thread panicked while it held the lock.
        pub fn archive_up_to(&self, tip: u64) {
            let from = self
                .chain
                .lock()
                .unwrap()
                .head
                .map_or(1, |h| h.saturating_add(1));
            let records: Vec<L1Block> = (from..=tip).map(|n| self.record(n)).collect();
            let mut chain = self.chain.lock().unwrap();
            chain
                .blocks
                .extend(records.into_iter().map(|r| (r.number, r)));
            chain.head = Some(tip.max(chain.head.unwrap_or(0)));
        }

        /// Inject one live record, out of the script: a copy, a lie, or a
        /// record past a gap.
        ///
        /// # Panics
        /// Panics when a test thread panicked while it held the lock.
        pub fn inject(&self, record: L1Block) {
            self.chain.lock().unwrap().injected.push_back(record);
        }

        /// Put a record into the archives beside the chain's own record of
        /// its block, as another follower instance's recording holds it.
        ///
        /// # Panics
        /// Panics when a test thread panicked while it held the lock.
        pub fn add_archived(&self, record: L1Block) {
            self.chain.lock().unwrap().archived.push(record);
        }

        /// Turn the archives off or on.
        ///
        /// # Panics
        /// Panics when a test thread panicked while it held the lock.
        pub fn archive_down(&self, down: bool) {
            self.chain.lock().unwrap().archive_down = down;
        }

        /// A new subscriber's feed.
        #[must_use]
        pub fn feed(&self) -> ScriptedFeed {
            ScriptedFeed {
                stream: self.clone(),
                live: VecDeque::new(),
                filled: false,
            }
        }

        /// Publish up to the next scripted tip; returns the new records.
        fn advance(&self, joined: bool) -> Vec<L1Block> {
            let Some(tip) = self.chain.lock().unwrap().tips.pop_front() else {
                return Vec::new();
            };
            let head = self.chain.lock().unwrap().head;
            let from = match (joined, head) {
                (true, Some(h)) => h.saturating_add(1),
                _ => tip,
            };
            let records: Vec<L1Block> = (from..=tip).map(|n| self.record(n)).collect();
            self.archive_up_to(tip);
            records
        }
    }

    /// One subscriber of the scripted stream. A round of `try_next`
    /// calls, up to the `None` that ends it, delivers the injected records
    /// and one scripted tip: one pass of the watcher sees one tip.
    pub struct ScriptedFeed {
        stream: ScriptedStream,
        /// Live records that arrived and were not read yet.
        live: VecDeque<L1Block>,
        /// The current round already delivered its tip.
        filled: bool,
    }

    impl ScriptedFeed {
        /// The injected records, then the live records of the next
        /// scripted tip.
        fn fill(&mut self) {
            let injected: Vec<L1Block> = self
                .stream
                .chain
                .lock()
                .unwrap()
                .injected
                .drain(..)
                .collect();
            self.live.extend(injected);
            let joined = self.stream.chain.lock().unwrap().head.is_some();
            self.live.extend(self.stream.advance(joined));
        }
    }

    #[async_trait]
    impl BlockFeed for ScriptedFeed {
        async fn history(&mut self, from: u64) -> Vec<L1Block> {
            let chain = self.stream.chain.lock().unwrap();
            if chain.archive_down {
                return Vec::new();
            }
            let extra = chain.archived.iter().filter(|r| r.number >= from).cloned();
            extra
                .chain(chain.blocks.range(from..).map(|(_, r)| r.clone()))
                .collect()
        }

        async fn next(&mut self) -> Option<L1Block> {
            match self.try_next() {
                Some(record) => Some(record),
                None => std::future::pending().await,
            }
        }

        fn try_next(&mut self) -> Option<L1Block> {
            if self.live.is_empty() && !self.filled {
                self.fill();
                self.filled = true;
            }
            let record = self.live.pop_front();
            self.filled &= record.is_some();
            record
        }
    }
}
