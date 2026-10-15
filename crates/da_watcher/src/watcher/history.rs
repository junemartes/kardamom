//! The history the archives give, resolved by the parent chain.
//!
//! The archives record every follower instance, so the history of a
//! range can hold two records of one block: copies, or, after a follower
//! instance read a lie, two blocks. Live, a second hash for one number is
//! a halt that an operator clears. A history read cannot tell which
//! record came first, so it keeps the chain that descends from the
//! watcher's head: for each block after the head, the record whose
//! parent is the record kept before it. A block whose records all name
//! another parent ends the history with one of them, so the consumer rule
//! sees the break (`l1_chain_break`); a block with no record ends it too,
//! and the watcher reads it again a tick later.

use kardamom_types::{L1Block, L1BlockDedup};

use super::{L1Watcher, Position};
use crate::feed::BlockFeed;
use crate::publisher::EpochPublisher;

/// The chain kept so far, as a fold over the history in block order.
struct Chain {
    kept: Vec<L1Block>,
    /// The hash the next kept record must name as its parent.
    parent: alloy_primitives::B256,
    /// The block the next kept record must be.
    next: u64,
    /// The first record of the next block that names another parent,
    /// while no record of that block links.
    breaker: Option<L1Block>,
    /// A breaking record is kept; nothing after it is.
    broken: bool,
}

impl Chain {
    fn take(mut self, record: L1Block) -> Self {
        if self.broken || record.number < self.next {
            return self;
        }
        if record.number > self.next {
            self.end();
            return self;
        }
        if record.parent_hash == self.parent {
            self.breaker = None;
            self.parent = record.hash;
            self.next = self.next.saturating_add(1);
            self.kept.push(record);
        } else if self.breaker.is_none() {
            self.breaker = Some(record);
        }
        self
    }

    /// The history ends: a breaking record left over is kept, so the
    /// consumer rule sees it.
    fn end(&mut self) {
        if let Some(breaker) = self.breaker.take() {
            self.kept.push(breaker);
        }
        self.broken = true;
    }

    fn finish(mut self) -> Vec<L1Block> {
        self.end();
        self.kept
    }
}

impl<F: BlockFeed, P: EpochPublisher> L1Watcher<F, P> {
    /// `history`, in block order, reduced to the chain that descends from
    /// the head. Before the anchor there is no head: the history stays as
    /// it is, and the consumer rule judges it.
    pub(super) fn linked(&self, mut history: Vec<L1Block>) -> Vec<L1Block> {
        history.sort_by_key(|record| record.number);
        let Position::Anchored(window, _) = &self.position else {
            return history;
        };
        let head = window.head();
        let start = Chain {
            kept: Vec::new(),
            parent: head.hash,
            next: head.number.saturating_add(1),
            breaker: None,
            broken: false,
        };
        history.into_iter().fold(start, Chain::take).finish()
    }

    /// Forget the records seen before the head: an operator cleared the
    /// halt they caused, after the runbook's steps.
    pub(super) fn forget_seen(&mut self) {
        if let Position::Anchored(window, dedup) = &mut self.position {
            let head = window.head();
            *dedup = L1BlockDedup::after(head.number, head.hash);
        }
    }
}
