use super::config::MAX_BLOCK_TXS;
use super::graph::{Node, TxSlot};
use super::metrics::TxResult;
use crate::mv::MvCache;
use crate::mv::ReadRecord;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::error::ExecutorError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

/// Recycle pools (steady-state zero-allocation blocks): the reaper
/// scrubs spent block structures in place (drops entries, keeps every
/// buffer) and parks them here. Session build pops from here instead of
/// mapping fresh arenas per block. mv caches outlive their block as
/// mv-as-layer references, so a still-shared cache parks until its
/// last Arc drops, and is swept at the next build.
pub(super) struct RecyclePools {
    pub(super) arenas: Mutex<Vec<SpentArena>>,
    pub(super) mv_clean: Mutex<Vec<MvCache>>,
    pub(super) mv_parked: Mutex<Vec<Arc<MvCache>>>,
    /// Cleared per-transaction read-record buffers, returned by the
    /// reaper in one batch per block, taken by workers in batches of
    /// 64. The per-transaction `Vec::with_capacity` and its growth
    /// reallocations were the largest STM-specific allocation.
    pub(super) read_bufs: Mutex<Vec<Vec<ReadRecord>>>,
    /// Cleared `PendingDelta` shells for the fold (the maps' tables are
    /// the other huge per-block allocation), returned by the consumer
    /// through [`PoolHandle::recycle_delta`] once a release settles.
    pub(super) deltas: Mutex<Vec<PendingDelta>>,
}

pub(super) struct SpentArena {
    pub(super) slots: Vec<std::sync::OnceLock<TxSlot>>,
    pub(super) results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
    pub(super) nodes: Vec<Node>,
}

/// A finished block's recyclable structures, on their way to the
/// reaper.
pub(super) struct SpentBlock {
    pub(super) slots: Vec<std::sync::OnceLock<TxSlot>>,
    pub(super) results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
    pub(super) nodes: Arc<Vec<Node>>,
    pub(super) mv: Arc<MvCache>,
    pub(super) pools: std::sync::Arc<RecyclePools>,
}

impl SpentBlock {
    /// Scrub and park this block's structures: clear the result slots,
    /// harvest their read-record buffers, reset and park the node
    /// arena (only when this call holds the last `Arc`, since a
    /// still-shared arena is still in use), and park or scrub the mv
    /// cache the same way.
    pub(super) fn reap(self) {
        let Self {
            mut slots,
            mut results,
            nodes,
            mv,
            pools,
        } = self;
        for c in &mut slots {
            let _ = c.take();
        }
        // Harvest read-record buffers on the way out. Spent results
        // drop here; their read buffers return to the pool.
        let bufs: Vec<Vec<ReadRecord>> = results
            .iter_mut()
            .filter_map(std::sync::OnceLock::take)
            .filter_map(Result::ok)
            .map(|mut r| {
                let mut b = std::mem::take(&mut r.reads);
                b.clear();
                b
            })
            .collect();
        if !bufs.is_empty() {
            let mut g = pools.read_bufs.lock().expect("pools poisoned");
            let room = MAX_BLOCK_TXS.saturating_sub(g.len());
            g.extend(bufs.into_iter().take(room));
        }
        if let Ok(nodes) = Arc::try_unwrap(nodes) {
            Self::park_nodes(nodes, slots, results, &pools);
        }
        match Arc::try_unwrap(mv) {
            Ok(cache) => {
                cache.scrub();
                pools.mv_clean.lock().expect("pools poisoned").push(cache);
            }
            Err(shared) => {
                pools.mv_parked.lock().expect("pools poisoned").push(shared);
            }
        }
    }

    /// Reset every node in a no-longer-shared arena, and park it in the
    /// pool for reuse. [`Self::reap`]'s branch stays free of a loop.
    fn park_nodes(
        nodes: Vec<Node>,
        slots: Vec<std::sync::OnceLock<TxSlot>>,
        results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
        pools: &RecyclePools,
    ) {
        for nd in &nodes {
            nd.open.store(false, Ordering::Relaxed);
            nd.children.lock().expect("node poisoned").clear();
            nd.indegree.store(0, Ordering::Relaxed);
            nd.worker.store(0, Ordering::Relaxed);
            nd.queued.store(false, Ordering::Relaxed);
            nd.fifo_preds.lock().expect("node poisoned").clear();
        }
        pools
            .arenas
            .lock()
            .expect("pools poisoned")
            .push(SpentArena {
                slots,
                results,
                nodes,
            });
    }
}
