//! The block log in the peer transfer.
//!
//! A peer that fetched a base also fetches the block log after it, so the
//! base serves as a restore point up to the end of that log:
//!
//! ```text
//! GET /checkpoint/log?from_block=<u64>&to_block=<u64> HTTP/1.0
//!
//! HTTP/1.0 200 OK
//! x-checkpoint-genesis: <0x hash>
//!
//! <records>
//! ```
//!
//! The body is the records of the blocks from `from_block` to `to_block`,
//! in order, each as it is on disk: `[len:u32][block:u64][crc32:u32][batch]`.
//! The body ends at the first block that the log does not hold, or at
//! `to_block`, and the connection closes. The client keeps the records up
//! to the first torn or bad one. The server answers `404` when it has no
//! base, and a server of an older release answers `404` for the unknown
//! path. The client then restores the base alone.

use std::num::NonZeroU64;
use std::path::Path;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{info, warn};

use super::{CheckpointFetch, IO_TIMEOUT, PeerResponse, framing};
use crate::checkpoint::block_log::{
    BlockLog, BlockLogWriter, DEFAULT_SEGMENT_BYTES, LogIdentity, Records,
};
use crate::checkpoint::replay::{LOG_DIR, LogReplay};
use crate::checkpoint::{BlockRange, CheckpointInfo, latest_checkpoint, read_manifest};
use crate::error::StateError;
use crate::meta::SCHEMA_VERSION;

/// The bytes of records that the server sends at a time.
const CHUNK: usize = 256 * 1024;

/// One request for the log: the blocks from `from` to `to`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LogRequest {
    from: NonZeroU64,
    to: u64,
}

impl LogRequest {
    fn line(self) -> String {
        format!(
            "{}?from_block={}&to_block={} HTTP/1.0\r\n\r\n",
            framing::LOG_PATH,
            self.from,
            self.to
        )
    }

    /// Parse a request line. `None` for a request of another path, or a
    /// query that does not parse.
    pub(super) fn parse(line: &str) -> Option<Self> {
        let query = line.strip_prefix(framing::LOG_PATH)?.strip_prefix('?')?;
        let query = query.split_whitespace().next()?;
        let value = |key: &str| {
            query
                .split('&')
                .filter_map(|pair| pair.split_once('='))
                .find(|(k, _)| *k == key)
                .and_then(|(_, v)| v.parse::<u64>().ok())
        };
        Some(Self {
            from: NonZeroU64::new(value("from_block")?)?,
            to: value("to_block")?,
        })
    }

    /// Answer the request on `wr`: the records, or `404` with no base.
    pub(super) async fn serve(self, dir: &Path, mut wr: OwnedWriteHalf) -> std::io::Result<()> {
        let dir = dir.to_path_buf();
        let log = tokio::task::spawn_blocking(move || Self::open_log(&dir))
            .await
            .map_err(std::io::Error::other)?;
        let Some((log, genesis)) = log else {
            timeout(IO_TIMEOUT, wr.write_all(b"HTTP/1.0 404 Not Found\r\n\r\n")).await??;
            return Ok(());
        };
        let head = format!(
            "HTTP/1.0 200 OK\r\n{}: {genesis:#x}\r\n\r\n",
            framing::HDR_GENESIS
        );
        timeout(IO_TIMEOUT, wr.write_all(head.as_bytes())).await??;
        let (tx, mut rx) = mpsc::channel(8);
        let produce = tokio::task::spawn_blocking(move || self.produce(&log, &tx));
        while let Some(chunk) = rx.recv().await {
            timeout(IO_TIMEOUT, wr.write_all(&chunk)).await??;
        }
        timeout(IO_TIMEOUT, wr.flush()).await??;
        let sent = produce.await.map_err(std::io::Error::other)?;
        info!(
            from = self.from,
            to = self.to,
            blocks = sent,
            "served the block log to a peer"
        );
        Ok(())
    }

    /// The log of `dir` and its chain, named by the manifest of the newest
    /// base. `None` with no base.
    fn open_log(dir: &Path) -> Option<(BlockLog, alloy_primitives::B256)> {
        let base = latest_checkpoint(dir, BlockRange::ANY)
            .inspect_err(|e| warn!(error = %e, "the checkpoint lookup of a log request failed"))
            .ok()??;
        let genesis = read_manifest(&base.path).ok()?.genesis_digest;
        let identity = LogIdentity {
            genesis,
            schema: SCHEMA_VERSION,
        };
        let log = BlockLog::open(&dir.join(LOG_DIR), identity).ok()?;
        Some((log, genesis))
    }

    /// Send the records of the request on `tx`, in chunks. Returns how many
    /// records went out. A closed channel ends the send.
    fn produce(self, log: &BlockLog, tx: &mpsc::Sender<Vec<u8>>) -> u64 {
        let mut chunk = Vec::with_capacity(CHUNK);
        let mut sent = 0u64;
        let flowed = log
            .after(self.from.get() - 1, self.to)
            .try_for_each(|record| {
                // The send stops at an encode error, and the client keeps the
                // records before it.
                record.write_to(&mut chunk).map_err(|e| {
                    warn!(error = %e, "a block log record did not encode for a peer");
                })?;
                sent += 1;
                if chunk.len() < CHUNK {
                    return Ok(());
                }
                tx.blocking_send(std::mem::take(&mut chunk)).map_err(|_| ())
            });
        if flowed.is_ok() && !chunk.is_empty() {
            let _ = tx.blocking_send(chunk);
        }
        sent
    }
}

/// Fetch the log after `base` up to block `to` from `peer`, into the log
/// of `checkpoints_dir`. The fetch asks only for the blocks after the
/// local reach of the base, and cuts the local log there, so the records
/// of the peer follow the last good local record. A fetch never shortens
/// the local log: a peer with a shorter log sends no record. This matters
/// because a node fetches from each peer in turn, and the peer list may
/// name the node itself, with its own stale log. Returns the last block
/// that the local log now holds after the base.
///
/// # Errors
///
/// Returns an error when the peer does not answer, answers for another
/// chain, or the local log does not write.
pub(super) fn fetch_log(
    peer: &str,
    checkpoints_dir: &Path,
    base: &CheckpointInfo,
    to: u64,
) -> Result<u64, StateError> {
    let genesis = read_manifest(&base.path)?.genesis_digest;
    let reach = LogReplay::new(&base.path, genesis, to).reach(base.block)?;
    let Some(from) = reach.checked_add(1).and_then(NonZeroU64::new) else {
        return Ok(reach);
    };
    let request = LogRequest { from, to };
    let mut fetch = CheckpointFetch::connect(peer, &request.line(), Some(genesis))?;
    let head = fetch.read_head_text()?;
    let mut lines = head.lines();
    let status = PeerResponse::parse_status_line(lines.next().unwrap_or_default())?;
    if status == 404 {
        return Ok(reach);
    }
    if status != 200 {
        return Err(StateError::Recovery(format!(
            "checkpoint peer {peer} returned status {status} for the log"
        )));
    }
    let served = PeerResponse::parse_headers(lines).genesis.ok_or_else(|| {
        StateError::Recovery(format!("checkpoint peer {peer} sent no chain for the log"))
    })?;
    if served != genesis {
        return Err(StateError::Recovery(format!(
            "checkpoint peer {peer} serves the log of chain {served:#x}, not {genesis:#x}"
        )));
    }
    let identity = LogIdentity {
        genesis,
        schema: SCHEMA_VERSION,
    };
    let mut log = BlockLogWriter::open(
        &checkpoints_dir.join(LOG_DIR),
        identity,
        DEFAULT_SEGMENT_BYTES,
    )?;
    log.cut_after(reach)?;
    let last = Records::new(&mut fetch.reader)
        .take_while(|r| r.block <= to)
        .map_while(|r| r.batch().ok())
        .try_fold(reach, |_, batch| {
            log.append(&batch).map(|()| batch.boundary.block_number)
        })?;
    log.sync()?;
    info!(
        peer,
        base = base.block,
        last,
        "fetched the block log from a peer"
    );
    Ok(last)
}

#[cfg(test)]
#[path = "log_fetch_tests.rs"]
mod tests;
