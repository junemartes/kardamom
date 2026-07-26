//! Peer-to-peer checkpoint transfer: a minimal HTTP server that publishes a
//! node's newest state checkpoint, and a client that fetches one from a peer.
//!
//! This is the network half of "restore-from-peer" ([`crate::checkpoint`]):
//! executor replicas are deterministic state machines at the same block, so any
//! replica's checkpoint is a valid restore source for another. The consumer is
//! the resync fallback — a node whose replay cursor aged out of the cluster's
//! bounded retention window (`REPLAY_UNAVAILABLE`) can only be repaired with
//! state from at-or-above the retention floor, which by construction never
//! exists locally (a local checkpoint's block is always <= the local cursor).
//!
//! The protocol is deliberately tiny (HTTP/1.0 GET, one request per
//! connection, std-only — no HTTP dependency):
//!
//! ```text
//! GET /checkpoint/latest HTTP/1.0
//!
//! HTTP/1.0 200 OK
//! x-checkpoint-block: <u64>
//! content-length: <bytes>
//!
//! <mdbx image>
//! ```
//!
//! `404` when the serving node has no checkpoint yet. Only complete checkpoints
//! are ever visible under `checkpoint-*` names (compact-to-tmp + rename), so a
//! served image is always a full, consistent snapshot.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::{info, warn};

use crate::checkpoint::{CheckpointInfo, checkpoint_data_file, checkpoint_name, latest_checkpoint};
use crate::error::StateError;

/// Per-socket read/write timeout. Transfers stream in bounded chunks, so this
/// caps per-syscall stalls (a dead peer), not total transfer time.
const IO_TIMEOUT: Duration = Duration::from_secs(20);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap on request/response head size — nothing legitimate comes close.
const MAX_HEAD: usize = 4096;

/// Serve the newest checkpoint under `checkpoints_dir` on `addr`, forever, on
/// a dedicated thread. Binding happens before the thread spawns so a bad
/// address fails startup loudly rather than logging from a background thread.
pub fn serve_checkpoints(
    addr: SocketAddr,
    checkpoints_dir: PathBuf,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    let listener = TcpListener::bind(addr)?;
    info!(%addr, dir = %checkpoints_dir.display(), "serving checkpoints to peers");
    std::thread::Builder::new()
        .name("kardamom-ckpt-serve".into())
        .spawn(move || {
            for conn in listener.incoming() {
                let stream = match conn {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "checkpoint server accept failed");
                        continue;
                    }
                };
                if let Err(e) = serve_one(stream, &checkpoints_dir) {
                    warn!(error = %e, "checkpoint transfer to peer failed");
                }
            }
        })
}

fn serve_one(stream: TcpStream, checkpoints_dir: &Path) -> std::io::Result<()> {
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader
        .by_ref()
        .take(MAX_HEAD as u64)
        .read_line(&mut request_line)?;
    let mut stream = stream;
    if !request_line.starts_with("GET /checkpoint/latest") {
        stream.write_all(b"HTTP/1.0 404 Not Found\r\n\r\n")?;
        return Ok(());
    }
    let ckpt = match latest_checkpoint(checkpoints_dir) {
        Ok(Some(c)) => c,
        Ok(None) => {
            stream.write_all(b"HTTP/1.0 404 Not Found\r\n\r\n")?;
            return Ok(());
        }
        Err(e) => {
            warn!(error = %e, "checkpoint lookup failed while serving peer");
            stream.write_all(b"HTTP/1.0 500 Internal Server Error\r\n\r\n")?;
            return Ok(());
        }
    };
    let data = match checkpoint_data_file(&ckpt.path) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "checkpoint data file missing while serving peer");
            stream.write_all(b"HTTP/1.0 500 Internal Server Error\r\n\r\n")?;
            return Ok(());
        }
    };
    let mut file = std::fs::File::open(&data)?;
    let len = file.metadata()?.len();
    stream.write_all(
        format!(
            "HTTP/1.0 200 OK\r\nx-checkpoint-block: {}\r\ncontent-length: {len}\r\n\r\n",
            ckpt.block
        )
        .as_bytes(),
    )?;
    std::io::copy(&mut file, &mut stream)?;
    info!(block = ckpt.block, bytes = len, "served checkpoint to peer");
    Ok(())
}

/// Fetch the newest checkpoint from `peer` (`host:port`) into
/// `checkpoints_dir`, returning its info. `Ok(None)` when the peer has no
/// checkpoint, or its newest is below `min_block` (the advertised block is in
/// the response head, so a useless image is never downloaded). An
/// already-present same-block checkpoint is returned without a transfer.
///
/// The image lands under the hidden `.checkpoint-<block>.fetch.tmp` name and is
/// renamed into place only when the full advertised length arrived, preserving
/// the "visible checkpoints are complete" invariant for any concurrent reader.
pub fn fetch_latest_checkpoint(
    peer: &str,
    checkpoints_dir: &Path,
    min_block: u64,
) -> Result<Option<CheckpointInfo>, StateError> {
    let addr: SocketAddr = peer
        .parse()
        .map_err(|_| StateError::Recovery(format!("bad checkpoint peer address: {peer}")))?;
    let stream = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let mut stream = stream;
    stream.write_all(b"GET /checkpoint/latest HTTP/1.0\r\n\r\n")?;

    let mut reader = BufReader::new(stream);
    let (status, block, content_length) = read_response_head(&mut reader)?;
    if status == 404 {
        return Ok(None);
    }
    if status != 200 {
        return Err(StateError::Recovery(format!(
            "checkpoint peer {peer} returned status {status}"
        )));
    }
    let block = block.ok_or_else(|| {
        StateError::Recovery(format!(
            "checkpoint peer {peer}: missing x-checkpoint-block"
        ))
    })?;
    let len = content_length.ok_or_else(|| {
        StateError::Recovery(format!("checkpoint peer {peer}: missing content-length"))
    })?;
    if block < min_block {
        // Too old to be useful (e.g. below the cluster's retention floor) —
        // don't download the body.
        return Ok(None);
    }

    std::fs::create_dir_all(checkpoints_dir)?;
    let dest = checkpoints_dir.join(checkpoint_name(block));
    if dest.exists() {
        // Already have this exact checkpoint — nothing to transfer.
        return Ok(Some(CheckpointInfo { block, path: dest }));
    }
    let tmp = checkpoints_dir.join(format!(".{}.fetch.tmp", checkpoint_name(block)));
    let mut out = std::fs::File::create(&tmp)?;
    let copied = std::io::copy(&mut reader.by_ref().take(len), &mut out)?;
    if copied != len {
        let _ = std::fs::remove_file(&tmp);
        return Err(StateError::Recovery(format!(
            "checkpoint transfer from {peer} truncated: got {copied} of {len} bytes"
        )));
    }
    out.sync_all()?;
    drop(out);
    std::fs::rename(&tmp, &dest)?;
    info!(block, bytes = len, peer, "fetched checkpoint from peer");
    Ok(Some(CheckpointInfo { block, path: dest }))
}

/// Fetch from each peer in turn and keep the newest checkpoint at or above
/// `min_block`. Individual peer failures are logged and skipped — one live
/// peer is enough. As the best-so-far rises, later peers advertising nothing
/// newer are skipped without downloading.
pub fn fetch_best_checkpoint(
    peers: &[String],
    checkpoints_dir: &Path,
    min_block: u64,
) -> Option<CheckpointInfo> {
    let mut best: Option<CheckpointInfo> = None;
    for peer in peers {
        let floor = best.as_ref().map_or(min_block, |b| b.block + 1);
        match fetch_latest_checkpoint(peer, checkpoints_dir, floor) {
            Ok(Some(c)) => best = Some(c),
            Ok(None) => {}
            Err(e) => warn!(peer, error = %e, "checkpoint fetch from peer failed"),
        }
    }
    best
}

fn read_response_head<R: BufRead>(
    reader: &mut R,
) -> Result<(u16, Option<u64>, Option<u64>), StateError> {
    let mut head = String::new();
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        head.push_str(&line);
        if head.len() > MAX_HEAD {
            return Err(StateError::Recovery(
                "checkpoint peer response head too large".into(),
            ));
        }
    }
    let mut lines = head.lines();
    let status_line = lines
        .next()
        .ok_or_else(|| StateError::Recovery("empty response from checkpoint peer".into()))?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            StateError::Recovery(format!(
                "bad status line from checkpoint peer: {status_line}"
            ))
        })?;
    let mut block = None;
    let mut content_length = None;
    for l in lines {
        let Some((k, v)) = l.split_once(':') else {
            continue;
        };
        match k.trim().to_ascii_lowercase().as_str() {
            "x-checkpoint-block" => block = v.trim().parse().ok(),
            "content-length" => content_length = v.trim().parse().ok(),
            _ => {}
        }
    }
    Ok((status, block, content_length))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_checkpoint(dir: &Path, block: u64, contents: &[u8]) -> PathBuf {
        let p = dir.join(checkpoint_name(block));
        std::fs::write(&p, contents).unwrap();
        p
    }

    fn serve_ephemeral(dir: PathBuf) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(stream) = conn else { break };
                let _ = serve_one(stream, &dir);
            }
        });
        addr
    }

    #[test]
    fn fetch_round_trips_newest_checkpoint() {
        let served = tempfile::tempdir().unwrap();
        write_checkpoint(served.path(), 3, b"old image");
        write_checkpoint(served.path(), 7, b"newest image bytes");
        let addr = serve_ephemeral(served.path().to_path_buf());

        let local = tempfile::tempdir().unwrap();
        let got = fetch_latest_checkpoint(&addr.to_string(), local.path(), 0)
            .unwrap()
            .unwrap();
        assert_eq!(got.block, 7);
        assert_eq!(std::fs::read(&got.path).unwrap(), b"newest image bytes");
        // No tmp residue.
        assert!(
            std::fs::read_dir(local.path()).unwrap().all(|e| !e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp"))
        );
    }

    #[test]
    fn fetch_from_empty_peer_returns_none() {
        let served = tempfile::tempdir().unwrap();
        let addr = serve_ephemeral(served.path().to_path_buf());
        let local = tempfile::tempdir().unwrap();
        assert!(
            fetch_latest_checkpoint(&addr.to_string(), local.path(), 0)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn fetch_best_picks_newest_across_peers_and_survives_dead_peer() {
        let served_a = tempfile::tempdir().unwrap();
        write_checkpoint(served_a.path(), 5, b"a5");
        let served_b = tempfile::tempdir().unwrap();
        write_checkpoint(served_b.path(), 9, b"b9");
        let addr_a = serve_ephemeral(served_a.path().to_path_buf());
        let addr_b = serve_ephemeral(served_b.path().to_path_buf());

        let local = tempfile::tempdir().unwrap();
        let peers = vec![
            "127.0.0.1:1".to_string(), // dead peer: connection refused, skipped
            addr_a.to_string(),
            addr_b.to_string(),
        ];
        let best = fetch_best_checkpoint(&peers, local.path(), 0).unwrap();
        assert_eq!(best.block, 9);
        assert_eq!(std::fs::read(&best.path).unwrap(), b"b9");
    }

    #[test]
    fn min_block_filters_stale_peer_checkpoint() {
        let served = tempfile::tempdir().unwrap();
        write_checkpoint(served.path(), 6, b"below the floor");
        let addr = serve_ephemeral(served.path().to_path_buf());
        let local = tempfile::tempdir().unwrap();
        // Peer's newest (6) is below the required floor (10): skipped, nothing
        // written locally.
        assert!(
            fetch_latest_checkpoint(&addr.to_string(), local.path(), 10)
                .unwrap()
                .is_none()
        );
        assert_eq!(std::fs::read_dir(local.path()).unwrap().count(), 0);
    }

    #[test]
    fn existing_local_checkpoint_short_circuits_transfer() {
        let served = tempfile::tempdir().unwrap();
        write_checkpoint(served.path(), 4, b"peer bytes");
        let addr = serve_ephemeral(served.path().to_path_buf());

        let local = tempfile::tempdir().unwrap();
        write_checkpoint(local.path(), 4, b"local bytes");
        let got = fetch_latest_checkpoint(&addr.to_string(), local.path(), 0)
            .unwrap()
            .unwrap();
        assert_eq!(got.block, 4);
        // The local copy is kept, not overwritten by the peer's.
        assert_eq!(std::fs::read(&got.path).unwrap(), b"local bytes");
    }
}
