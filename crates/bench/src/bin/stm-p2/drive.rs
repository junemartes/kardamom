//! The pipelined measurement's two drive loops: speculative (the
//! production shape, layering block N+1 on the engine's own deltas)
//! and baseline (layering on pass A's deltas, known up front, so there
//! is no bind-wait). Both share [`DriveState`]'s bookkeeping.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Instant;

use kardamom_engine::delta::PendingDelta;
use kardamom_footprint::classifier::Stats;
use kardamom_state::{StateEnv, StateSnapshot, WriterHandle};
use kardamom_stm::execute::{BlockTicket, DeltaRelease, MvRelease, PoolHandle};
use kardamom_stm::mv::MvCache;

use super::common::{BlockOutputs, FeedPayload, Workload};

/// The read-only context both drive loops need.
pub(crate) struct DriveParams<'a> {
    pub(crate) w: &'a Workload<'a>,
    pub(crate) env_b: &'a StateEnv,
    pub(crate) writer_b: &'a WriterHandle,
    pub(crate) stats_b: &'a Stats,
    pub(crate) warm: usize,
    pub(crate) wk: usize,
    pub(crate) n_flow: usize,
    pub(crate) timing: bool,
}

/// The mv-as-layer channels the speculative drive loop binds on.
pub(crate) struct MvChannels {
    pub(crate) mv_tx: mpsc::Sender<MvRelease>,
    pub(crate) rel_tx: mpsc::Sender<DeltaRelease>,
    pub(crate) rel_rx: mpsc::Receiver<DeltaRelease>,
    pub(crate) mv_rx: mpsc::Receiver<MvRelease>,
}

/// The mutable state both drive loops carry across their `fi` iterations.
pub(crate) struct DriveState {
    feed_payloads: Vec<FeedPayload>,
    /// The engine's own released deltas (speculative mode) or `None`
    /// until advanced (baseline mode never fills this).
    engine_deltas: Vec<Option<Arc<PendingDelta>>>,
    /// The pass-A baseline deltas, wrapped in an Arc up front (baseline
    /// mode only; empty in speculative mode).
    deltas_arc: Vec<Arc<PendingDelta>>,
    /// The mv caches of unsettled predecessors (speculative mode only).
    engine_mvs: Vec<Option<(Arc<MvCache>, Option<revm::state::AccountInfo>)>>,
    advanced_to: usize,
}

impl DriveState {
    /// Build the drive state for one worker count's pool pass. In
    /// baseline mode, `deltas_arc` holds pass A's deltas, wrapped in an
    /// Arc up front, which measures pipeline mechanics with the answer
    /// already known. In speculative mode (the production shape), the
    /// engine's own deltas arrive later, from the tail's streaming
    /// release at each block's fold, so `deltas_arc` stays empty and
    /// `engine_deltas` fills in as releases land.
    pub(crate) fn new(
        feed_payloads: Vec<FeedPayload>,
        speculative: bool,
        baseline: &[BlockOutputs],
        n_flow: usize,
    ) -> Self {
        let deltas_arc = if speculative {
            Vec::new()
        } else {
            baseline.iter().map(|b| Arc::new(b.delta.clone())).collect()
        };
        Self {
            feed_payloads,
            engine_deltas: vec![None; n_flow],
            deltas_arc,
            engine_mvs: vec![None; n_flow],
            advanced_to: 0,
        }
    }

    /// The newest-first layer for flow block `k`: the engine's own
    /// delta in speculative mode, or pass A's baseline delta otherwise.
    fn layer_of(&self, speculative: bool, k: usize) -> Arc<PendingDelta> {
        if speculative {
            self.engine_deltas[k]
                .clone()
                .expect("release drained in order")
        } else {
            self.deltas_arc[k].clone()
        }
    }

    /// Feed block `fi`'s upstream-prepared records into `sess`,
    /// consuming this block's payload. Both drive loops call this right
    /// after opening the block's session.
    fn feed_block(
        &mut self,
        fi: usize,
        sess: &mut kardamom_stm::execute::BlockSession<'_, '_, StateSnapshot>,
    ) -> anyhow::Result<()> {
        for f in std::mem::take(&mut self.feed_payloads[fi]) {
            sess.push_prepared(f.idx, f.position, f.envelope, f.prepared)?;
        }
        Ok(())
    }
}

impl DriveState {
    /// Drain the fold releases that have arrived so far, without
    /// blocking, into `engine_deltas`. Speculative mode only: baseline
    /// mode's deltas are all known up front, in `deltas_arc`.
    ///
    /// `rel.block` stays far under `usize::MAX` for any run this
    /// benchmark does.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "rel.block stays far under usize::MAX for any run this benchmark does"
    )]
    fn drain_releases(&mut self, ch: &MvChannels, warm: usize) {
        while let Ok(rel) = ch.rel_rx.try_recv() {
            assert!(
                !rel.corrected,
                "wound in pipeline bench (block {}) — scenario must be wound-free",
                rel.block
            );
            let k = (rel.block as usize)
                .checked_sub(warm + 1)
                .expect("flow-range release");
            self.engine_deltas[k] = Some(rel.delta);
        }
    }

    /// Advance the pool's base cache for every settled block up to
    /// `fi`. In speculative mode, a block counts as settled once its
    /// own delta has released (`engine_deltas[k]` is `Some`) and the
    /// writer has published it; the delta shell is then recycled to
    /// the fold pool. In baseline mode every delta is already known up
    /// front (`deltas_arc`), so only the writer's publish gates
    /// advancement.
    fn advance_settled(
        &mut self,
        pool: &PoolHandle<'_, StateSnapshot>,
        speculative: bool,
        fi: usize,
        published_block: u64,
        warm: usize,
    ) {
        while self.advanced_to < fi {
            let bn = (warm + self.advanced_to) as u64 + 1;
            if bn > published_block
                || (speculative && self.engine_deltas[self.advanced_to].is_none())
            {
                break;
            }
            pool.advance_base(&self.layer_of(speculative, self.advanced_to));
            // Once settled, hand the shell back to the fold pool.
            if speculative
                && let Some(arc) = self.engine_deltas[self.advanced_to].take()
                && let Ok(d) = Arc::try_unwrap(arc)
            {
                pool.recycle_delta(d);
            }
            self.advanced_to += 1;
        }
    }

    /// Block until block `fi - 1`'s mv cache has released. A no-op for
    /// `fi == 0`, which has no predecessor to wait on.
    ///
    /// `rel.block` stays far under `usize::MAX` for any run this
    /// benchmark does.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "rel.block stays far under usize::MAX for any run this benchmark does"
    )]
    fn wait_prev_mv(&mut self, ch: &MvChannels, fi: usize, warm: usize) {
        if fi == 0 {
            return;
        }
        while self.engine_mvs[fi - 1].is_none() {
            let rel = ch.mv_rx.recv().expect("mv release channel");
            let k = (rel.block as usize)
                .checked_sub(warm + 1)
                .expect("flow-range release");
            self.engine_mvs[k] = Some((rel.mv, rel.sink_final));
        }
    }

    /// Drive one speculative block, with late-bound layers. It is
    /// built, fed, and submitted while fi-1 still executes, since
    /// admission is layer-independent. Its read base binds only when
    /// fi-1's delta releases at the fold. Build concurrently with the
    /// release, not after it, to keep the feed off the critical path.
    fn drive_one_speculative(
        &mut self,
        pool: &PoolHandle<'_, StateSnapshot>,
        p: &DriveParams<'_>,
        settle_tx: &mpsc::Sender<(usize, BlockTicket)>,
        ch: &MvChannels,
        fi: usize,
    ) -> anyhow::Result<()> {
        let t1 = Instant::now();
        let views = super::common::open_views(p.env_b, p.wk)?;
        let t_views = t1.elapsed();
        let t2 = Instant::now();
        let (mut sess, binder) = pool.begin_block_deferred(
            views,
            PendingDelta::new(),
            p.w.env_for(p.warm + fi),
            p.stats_b,
        )?;
        self.feed_block(fi, &mut sess)?;
        let ticket = sess.submit_streaming_mv(ch.mv_tx.clone(), ch.rel_tx.clone())?;
        settle_tx.send((fi, ticket)).expect("settler alive");
        let t_feed = t2.elapsed();
        // Bind: wait out fi-1's early release, drain and extract,
        // pre-fold, per the mv-as-layer design. Releases arrive in
        // submission order.
        // Do advancement bookkeeping first. It overlaps fi-1's
        // still-running execution, instead of adding to the cadence
        // after the bind-wait. Fold deltas arrive lazily; drain them
        // without blocking.
        let t_a = Instant::now();
        self.drain_releases(ch, p.warm);
        let h = p
            .writer_b
            .snapshot_rx
            .current()
            .map_or(0, |s| s.block_number());
        self.advance_settled(pool, true, fi, h, p.warm);
        let t_adv = t_a.elapsed();
        let t0 = Instant::now();
        self.wait_prev_mv(ch, fi, p.warm);
        // Newest first: the mv caches of unsettled predecessors. The
        // sink rides the newest release.
        let mv_layers: Vec<Arc<MvCache>> = (self.advanced_to..fi)
            .rev()
            .map(|k| {
                self.engine_mvs[k]
                    .as_ref()
                    .expect("drained in order")
                    .0
                    .clone()
            })
            .collect();
        let sink = if fi > 0 {
            Some(
                self.engine_mvs[fi - 1]
                    .as_ref()
                    .expect("just drained")
                    .1
                    .clone(),
            )
        } else {
            None
        };
        binder
            .bind_with(mv_layers, Vec::new(), sink)
            .map_err(|e| anyhow::anyhow!("bind block {fi}: {e}"))?;
        if p.timing {
            eprintln!(
                "pipe block {fi}: views {t_views:?} feed {t_feed:?} adv {t_adv:?} bind-wait {:?}",
                t0.elapsed()
            );
        }
        Ok(())
    }

    /// Drive every speculative block in order.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "settle_tx is owned on purpose, it closes when this function returns so the settler thread's recv() loop sees EOF"
    )]
    pub(crate) fn drive_speculative(
        &mut self,
        pool: &PoolHandle<'_, StateSnapshot>,
        p: &DriveParams<'_>,
        settle_tx: mpsc::Sender<(usize, BlockTicket)>,
        ch: &MvChannels,
    ) -> anyhow::Result<()> {
        for fi in 0..p.n_flow {
            self.drive_one_speculative(pool, p, &settle_tx, ch, fi)?;
        }
        Ok(())
    }

    /// Drive one baseline block: its read base is pass A's delta,
    /// known up front, so this measures pipeline mechanics with the
    /// answer already known, with no bind-wait.
    fn drive_one_baseline(
        &mut self,
        pool: &PoolHandle<'_, StateSnapshot>,
        p: &DriveParams<'_>,
        settle_tx: &mpsc::Sender<(usize, BlockTicket)>,
        fi: usize,
    ) -> anyhow::Result<()> {
        let t0 = Instant::now();
        let h = p
            .writer_b
            .snapshot_rx
            .current()
            .map_or(0, |s| s.block_number());
        self.advance_settled(pool, false, fi, h, p.warm);
        // Newest first.
        let layers: Vec<Arc<PendingDelta>> = (self.advanced_to..fi)
            .rev()
            .map(|k| self.layer_of(false, k))
            .collect();
        let t_base = t0.elapsed();
        let t1 = Instant::now();
        let views = super::common::open_views(p.env_b, p.wk)?;
        let t_views = t1.elapsed();
        let t2 = Instant::now();
        let mut sess = pool.begin_block_layered(
            views,
            PendingDelta::new(),
            layers,
            p.w.env_for(p.warm + fi),
            p.stats_b,
        )?;
        self.feed_block(fi, &mut sess)?;
        let t_feed = t2.elapsed();
        let t3 = Instant::now();
        settle_tx.send((fi, sess.submit()?)).expect("settler alive");
        if p.timing {
            eprintln!(
                "pipe block {fi}: base {t_base:?} views {t_views:?} feed {t_feed:?} hand {:?}",
                t3.elapsed()
            );
        }
        Ok(())
    }

    /// Drive every baseline block in order.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "settle_tx is owned on purpose, it closes when this function returns so the settler thread's recv() loop sees EOF"
    )]
    pub(crate) fn drive_baseline(
        &mut self,
        pool: &PoolHandle<'_, StateSnapshot>,
        p: &DriveParams<'_>,
        settle_tx: mpsc::Sender<(usize, BlockTicket)>,
    ) -> anyhow::Result<()> {
        for fi in 0..p.n_flow {
            self.drive_one_baseline(pool, p, &settle_tx, fi)?;
        }
        Ok(())
    }
}
