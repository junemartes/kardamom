//! The rebuild: scan the co-located executor's newest checkpoint into
//! Redis, tagged with the checkpoint's end position.
//!
//! The mirror is already subscribed and applying live batches when this
//! runs, so nothing is replayed. The rebuild waits for a checkpoint whose
//! end position is at or beyond the first live position, restores it into
//! the mirror's scratch directory (never the live env), opens it read
//! only, and streams the accounts table to Redis in chunks. A live row
//! for the same account wins by position.

use std::ops::ControlFlow;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use kardamom_cache::AccountCache;
use kardamom_state::checkpoint::latest_checkpoint;
use kardamom_state::{StateEnvBuilder, StateSnapshot, restore_newest_readable};
use kardamom_types::{AccountRow, BPosition};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Rows per Redis pipeline during a scan.
const CHUNK: usize = 512;
/// How long to wait between checks for a new checkpoint. The executor
/// writes one every 20 s.
const CHECKPOINT_POLL: Duration = Duration::from_secs(5);
/// How long to wait after a failed Redis write before the retry.
const WRITE_RETRY: Duration = Duration::from_millis(200);

/// The rebuild inputs: where the checkpoints are, and where to restore.
#[derive(Clone)]
pub(crate) struct Rebuild {
    checkpoints_dir: PathBuf,
    scratch: PathBuf,
}

/// The checkpoint block a rebuild already restored. A restore copies
/// and hashes the whole state image, which costs seconds to minutes
/// with three mirrors on one disk. Restoring the same block again on
/// every poll gains nothing, and the work starves the restores that
/// would make progress.
struct TriedCheckpoint {
    block: Option<u64>,
}

impl TriedCheckpoint {
    fn new() -> Self {
        Self { block: None }
    }

    fn is_new(&self, block: u64) -> bool {
        self.block.is_none_or(|tried| block > tried)
    }

    fn mark(&mut self, block: u64) {
        self.block = Some(block);
    }
}

impl Rebuild {
    pub(crate) fn new(checkpoints_dir: PathBuf, scratch: PathBuf) -> Self {
        Self {
            checkpoints_dir,
            scratch,
        }
    }

    /// Run one rebuild. `first_live` is the first live batch end this
    /// mirror applied; the checkpoint must reach it. Returns the
    /// checkpoint's end position.
    pub(crate) async fn run(
        &self,
        cache: &AccountCache,
        first_live: BPosition,
        shutdown: &CancellationToken,
    ) -> Result<BPosition> {
        let started = Instant::now();
        let snapshot = self.wait_for_checkpoint(first_live, shutdown).await?;
        let end = snapshot.end_tx_position()?;
        info!(
            block = snapshot.block_number(),
            end = end.as_index(),
            "rebuild: scanning"
        );
        let (tx, rx) = mpsc::channel::<Vec<AccountRow>>(4);
        let scan = tokio::task::spawn_blocking(move || scan(&snapshot, &tx));
        let written = write_chunks(cache, end, rx, shutdown).await;
        scan.await.context("join the scan")??;
        let written = written?;
        let _ = std::fs::remove_dir_all(&self.scratch);
        info!(
            rows = written,
            seconds = started.elapsed().as_secs_f64(),
            "rebuild: done"
        );
        Ok(end)
    }

    /// Restore the newest checkpoint into the scratch directory and open
    /// it read only, once one reaches `first_live`.
    async fn wait_for_checkpoint(
        &self,
        first_live: BPosition,
        shutdown: &CancellationToken,
    ) -> Result<StateSnapshot> {
        let mut tried = TriedCheckpoint::new();
        loop {
            if let Some(snapshot) = self.try_newest(first_live, &mut tried).await? {
                return Ok(snapshot);
            }
            pause(shutdown, CHECKPOINT_POLL, "the rebuild wait").await?;
        }
    }

    /// One attempt at the newest checkpoint, on a blocking thread. The
    /// restore copies and hashes the whole image, so it must not run on
    /// an async worker. A block this rebuild already restored is
    /// skipped: only a newer one can carry the first live batch.
    async fn try_newest(
        &self,
        first_live: BPosition,
        tried: &mut TriedCheckpoint,
    ) -> Result<Option<StateSnapshot>> {
        let Some(newest) = latest_checkpoint(&self.checkpoints_dir)? else {
            warn!(dir = %self.checkpoints_dir.display(), "rebuild: no checkpoint yet");
            return Ok(None);
        };
        if !tried.is_new(newest.block) {
            return Ok(None);
        }
        tried.mark(newest.block);
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.open_newest(newest.block, first_live))
            .await
            .context("join the checkpoint restore")?
    }

    /// The newest checkpoint as a read-only snapshot, when it reaches
    /// `first_live`. `None` when no checkpoint reads back, or the
    /// newest one is still behind. Runs on a blocking thread.
    ///
    /// This reads the executor's directory and never writes to it. The
    /// executor prunes its old checkpoints, so one can vanish between
    /// the listing and the read; `restore_newest_readable` skips such a
    /// checkpoint instead of renaming it out of the executor's way.
    fn open_newest(&self, newest: u64, first_live: BPosition) -> Result<Option<StateSnapshot>> {
        let restored = restore_newest_readable(&self.checkpoints_dir, &self.scratch, None)?;
        let Some((block, _)) = restored else {
            warn!("rebuild: no restorable checkpoint");
            return Ok(None);
        };
        let env = StateEnvBuilder::new(&self.scratch)
            .read_only(true)
            .open()
            .context("open the restored checkpoint")?;
        let snapshot = StateSnapshot::open(&env)?;
        let end = snapshot.end_tx_position()?;
        if end < first_live {
            info!(
                block,
                end = end.as_index(),
                first_live = first_live.as_index(),
                "rebuild: newest checkpoint {newest} is behind the first live batch; waiting"
            );
            return Ok(None);
        }
        Ok(Some(snapshot))
    }
}

/// Sleep `wait`, or end the step when shutdown comes first.
async fn pause(shutdown: &CancellationToken, wait: Duration, during: &str) -> Result<()> {
    tokio::select! {
        () = shutdown.cancelled() => anyhow::bail!("shutdown during {during}"),
        () = tokio::time::sleep(wait) => Ok(()),
    }
}

/// Walk the accounts table and hand chunks to the writer. Runs on a
/// blocking thread. A closed receiver ends the scan early.
fn scan(snapshot: &StateSnapshot, tx: &mpsc::Sender<Vec<AccountRow>>) -> Result<()> {
    let mut chunk = Vec::with_capacity(CHUNK);
    snapshot.for_each_account(|address, nonce, balance| {
        chunk.push(AccountRow {
            address,
            nonce,
            balance,
        });
        if chunk.len() < CHUNK {
            return Ok(ControlFlow::Continue(()));
        }
        let full = std::mem::replace(&mut chunk, Vec::with_capacity(CHUNK));
        // A closed receiver means the writer is gone: stop the scan.
        Ok(tx
            .blocking_send(full)
            .map_or(ControlFlow::Break(()), |()| ControlFlow::Continue(())))
    })?;
    if !chunk.is_empty() {
        let _ = tx.blocking_send(chunk);
    }
    Ok(())
}

/// Write every chunk through the monotone rule, tagged with `end`. A
/// failed write retries until it lands or shutdown. Returns the row count.
async fn write_chunks(
    cache: &AccountCache,
    end: BPosition,
    mut rx: mpsc::Receiver<Vec<AccountRow>>,
    shutdown: &CancellationToken,
) -> Result<u64> {
    let mut rows = 0u64;
    while let Some(chunk) = rx.recv().await {
        write_chunk(cache, end, &chunk, shutdown).await?;
        rows = rows.saturating_add(chunk.len() as u64);
    }
    Ok(rows)
}

/// One chunk, retried until it lands or shutdown.
async fn write_chunk(
    cache: &AccountCache,
    end: BPosition,
    chunk: &[AccountRow],
    shutdown: &CancellationToken,
) -> Result<()> {
    loop {
        match cache.write_rows(end, chunk).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                crate::metrics::record_write_retry();
                warn!(error = %e, "rebuild: write failed; retrying");
            }
        }
        pause(shutdown, WRITE_RETRY, "the rebuild").await?;
    }
}

#[cfg(test)]
mod tests {
    use super::TriedCheckpoint;

    /// A restore copies and hashes the whole state image. The wait loop
    /// polls every five seconds, so it must restore one block once and
    /// then wait for a newer one.
    #[test]
    fn a_checkpoint_is_tried_once_until_a_newer_one_appears() {
        let mut tried = TriedCheckpoint::new();
        assert!(tried.is_new(683), "the first checkpoint is always new");
        tried.mark(683);
        assert!(!tried.is_new(683), "the same block must not restore twice");
        assert!(!tried.is_new(680), "an older block carries less, not more");
        assert!(
            tried.is_new(696),
            "a newer block may carry the first live batch"
        );
    }
}
