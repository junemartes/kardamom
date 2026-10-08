//! Where the follower publishes each block's record: the `l1_blocks`
//! Aeron stream in a deployment, a list in a test.

use kardamom_log::aeron_live::L1BlocksPublisherHandle;

use crate::{IndexerError, L1Block};

/// The receiver of the follower's records, in block order.
pub trait BlockSink: Send + Sync + 'static {
    /// Publish one record. The follower writes its cursor only after
    /// every record of a range is published, so a record that fails here
    /// is published again on the next read, and the consumers drop the
    /// copies.
    ///
    /// # Errors
    /// Returns [`IndexerError::Publish`] when the stream does not take
    /// the record.
    fn publish(&self, block: &L1Block) -> Result<(), IndexerError>;
}

impl BlockSink for L1BlocksPublisherHandle {
    fn publish(&self, block: &L1Block) -> Result<(), IndexerError> {
        Self::publish(self, block)
            .map(|_| ())
            .map_err(|e| IndexerError::Publish(e.to_string()))
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::sync::Mutex;

    use super::BlockSink;
    use crate::{IndexerError, L1Block};

    /// The records a test follower published, in order. A test reads
    /// them from its own thread while the follower writes, so the list
    /// sits behind a lock.
    #[derive(Default)]
    pub struct RecordedBlocks {
        blocks: Mutex<Vec<L1Block>>,
        /// When set, every publish fails: a stream with no subscriber.
        pub refuse: Mutex<bool>,
    }

    impl RecordedBlocks {
        /// The records published so far.
        ///
        /// # Panics
        /// Panics when a test thread panicked while it held the lock.
        #[must_use]
        pub fn blocks(&self) -> Vec<L1Block> {
            self.blocks.lock().unwrap().clone()
        }
    }

    impl BlockSink for RecordedBlocks {
        fn publish(&self, block: &L1Block) -> Result<(), IndexerError> {
            if *self.refuse.lock().unwrap() {
                return Err(IndexerError::Publish("NOT_CONNECTED".into()));
            }
            self.blocks.lock().unwrap().push(block.clone());
            Ok(())
        }
    }

    impl BlockSink for std::sync::Arc<RecordedBlocks> {
        fn publish(&self, block: &L1Block) -> Result<(), IndexerError> {
            (**self).publish(block)
        }
    }
}
