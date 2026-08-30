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
//! connection, no HTTP dependency). The server is a tokio task (one task per
//! connection; the checkpoint lookup runs on `spawn_blocking`); the client is
//! std-sync because its callers are sync startup/repair code:
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
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tracing::{info, warn};

use alloy_primitives::B256;

use crate::checkpoint::{CheckpointInfo, checkpoint_data_file, checkpoint_name, latest_checkpoint};
use crate::error::StateError;

/// Per-socket read/write timeout. Transfers stream in bounded chunks, so this
/// caps per-syscall stalls (a dead peer), not total transfer time.
const IO_TIMEOUT: Duration = Duration::from_secs(20);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap on request/response head size — nothing legitimate comes close.
const MAX_HEAD: usize = 4096;

/// Removes the wrapped tmp dir on drop; set the field to `None` to defuse
/// once the dir has been published. Keeps every fetch refusal path
/// crash-clean without a per-arm cleanup line.
struct TmpDirGuard<'a>(Option<&'a Path>);

impl Drop for TmpDirGuard<'_> {
    fn drop(&mut self) {
        if let Some(p) = self.0 {
            let _ = std::fs::remove_dir_all(p);
        }
    }
}

/// A running checkpoint server: the bound address (useful when binding port
/// 0) and the accept-loop task. Dropping the handle does NOT stop the
/// server; abort the task to stop it.
pub struct CheckpointServer {
    pub addr: SocketAddr,
    pub task: tokio::task::JoinHandle<()>,
}

/// Serve the newest checkpoint under `checkpoints_dir` on `addr`, forever, as
/// a tokio task (one task per connection). Binding happens before the task
/// spawns so a bad address fails startup loudly rather than logging from a
/// background task. Must be called inside a tokio runtime.
pub fn serve_checkpoints(
    addr: SocketAddr,
    checkpoints_dir: PathBuf,
) -> std::io::Result<CheckpointServer> {
    let std_listener = std::net::TcpListener::bind(addr)?;
    std_listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(std_listener)?;
    let addr = listener.local_addr()?;
    info!(%addr, dir = %checkpoints_dir.display(), "serving checkpoints to peers");
    let task = tokio::spawn(async move {
        loop {
            let stream = match listener.accept().await {
                Ok((s, _)) => s,
                Err(e) => {
                    warn!(error = %e, "checkpoint server accept failed");
                    continue;
                }
            };
            let dir = checkpoints_dir.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_one(stream, &dir).await {
                    warn!(error = %e, "checkpoint transfer to peer failed");
                }
            });
        }
    });
    Ok(CheckpointServer { addr, task })
}

/// Response head + open image file for the newest checkpoint, or the error
/// response to send instead. Blocking (directory scan, manifest read, file
/// open): runs on `spawn_blocking`.
fn prepare_response(
    checkpoints_dir: &Path,
) -> Result<(String, std::fs::File, u64, u64), &'static [u8]> {
    const NOT_FOUND: &[u8] = b"HTTP/1.0 404 Not Found\r\n\r\n";
    const SERVER_ERROR: &[u8] = b"HTTP/1.0 500 Internal Server Error\r\n\r\n";
    let ckpt = match latest_checkpoint(checkpoints_dir) {
        Ok(Some(c)) => c,
        Ok(None) => return Err(NOT_FOUND),
        Err(e) => {
            warn!(error = %e, "checkpoint lookup failed while serving peer");
            return Err(SERVER_ERROR);
        }
    };
    let data = match checkpoint_data_file(&ckpt.path) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "checkpoint data file missing while serving peer");
            return Err(SERVER_ERROR);
        }
    };
    // Serve the manifest fields as headers so the peer can verify the bytes
    // it receives (and refuse a foreign chain) without a second round trip.
    // A checkpoint we cannot describe is one we must not hand out.
    let manifest = match crate::checkpoint::read_manifest(&ckpt.path) {
        Ok(m) => m,
        Err(e) => {
            warn!(error = %e, "checkpoint has no valid manifest; refusing to serve");
            return Err(SERVER_ERROR);
        }
    };
    let (file, len) = match std::fs::File::open(&data).and_then(|f| {
        let len = f.metadata()?.len();
        Ok((f, len))
    }) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "checkpoint data file unreadable while serving peer");
            return Err(SERVER_ERROR);
        }
    };
    let head = format!(
        "HTTP/1.0 200 OK\r\nx-checkpoint-block: {}\r\nx-checkpoint-keccak: {:#x}\r\n\
         x-checkpoint-genesis: {:#x}\r\ncontent-length: {len}\r\n\r\n",
        ckpt.block, manifest.image_keccak, manifest.genesis_digest
    );
    Ok((head, file, len, ckpt.block))
}

async fn serve_one(stream: tokio::net::TcpStream, checkpoints_dir: &Path) -> std::io::Result<()> {
    let (rd, mut wr) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(rd).take(MAX_HEAD as u64);
    let mut request_line = String::new();
    timeout(IO_TIMEOUT, reader.read_line(&mut request_line)).await??;
    if !request_line.starts_with("GET /checkpoint/latest") {
        timeout(IO_TIMEOUT, wr.write_all(b"HTTP/1.0 404 Not Found\r\n\r\n")).await??;
        return Ok(());
    }
    let dir = checkpoints_dir.to_path_buf();
    let prepared = tokio::task::spawn_blocking(move || prepare_response(&dir))
        .await
        .map_err(std::io::Error::other)?;
    let (head, file, len, block) = match prepared {
        Ok(v) => v,
        Err(response) => {
            timeout(IO_TIMEOUT, wr.write_all(response)).await??;
            return Ok(());
        }
    };
    timeout(IO_TIMEOUT, wr.write_all(head.as_bytes())).await??;
    // Chunked copy with a per-syscall stall cap (the std version used socket
    // timeouts): a dead peer fails within IO_TIMEOUT, a slow one streams on.
    let mut file = tokio::fs::File::from_std(file);
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        timeout(IO_TIMEOUT, wr.write_all(&buf[..n])).await??;
    }
    timeout(IO_TIMEOUT, wr.flush()).await??;
    info!(block, bytes = len, "served checkpoint to peer");
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
    expected_genesis: Option<B256>,
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
    let ResponseHead {
        status,
        block,
        content_length,
        keccak,
        genesis,
    } = read_response_head(&mut reader)?;
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
        // Already have this exact checkpoint — nothing to transfer. Logged
        // because this is a RECOVERY decision point: the retention-overrun
        // chaos case once read a silent short-circuit here as "the repair
        // path never ran" while the node recovered fine.
        info!(
            block,
            peer, "peer's newest checkpoint already present locally; skipping transfer"
        );
        return Ok(Some(CheckpointInfo { block, path: dest }));
    }
    // Build the same shape `create_checkpoint` produces — a directory holding
    // `mdbx.dat` + `MANIFEST` — under a hidden tmp name, so the checkpoint we
    // publish is self-contained and re-verifiable from disk.
    let tmp = checkpoints_dir.join(format!(".{}.fetch.tmp", checkpoint_name(block)));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp)?;
    // Every refusal below must leave no half-fetched tmp entry behind; the
    // guard replaces the per-arm cleanup and is defused only by the publish.
    let mut tmp_guard = TmpDirGuard(Some(&tmp));
    let tmp_data = tmp.join("mdbx.dat");
    let mut out = std::fs::File::create(&tmp_data)?;
    let copied = std::io::copy(&mut reader.by_ref().take(len), &mut out)?;
    if copied != len {
        return Err(StateError::Recovery(format!(
            "checkpoint transfer from {peer} truncated: got {copied} of {len} bytes"
        )));
    }
    out.sync_all()?;
    drop(out);

    // Integrity + chain identity BEFORE the image becomes visible — the
    // shared refusal checks (`checkpoint::check_image_identity`). Without
    // this the transfer was plain HTTP with a length check: silent bit rot,
    // a lying peer, or a checkpoint from a previous chain all became this
    // node's state (the recovery-C incident: a stale checkpoint wedged a
    // fresh node into an endless replay request loop).
    let got = crate::checkpoint::file_keccak(&tmp_data)?;
    let Some(want) = keccak else {
        return Err(StateError::Recovery(format!(
            "checkpoint peer {peer} sent no x-checkpoint-keccak — refusing an \
             unverifiable image"
        )));
    };
    let Some(genesis_digest) = genesis else {
        return Err(StateError::Recovery(format!(
            "checkpoint peer {peer} sent no x-checkpoint-genesis — refusing an \
             unidentifiable image"
        )));
    };
    crate::checkpoint::check_image_identity(
        &format!("from peer {peer}"),
        "peer",
        got,
        want,
        genesis_digest,
        expected_genesis,
    )?;

    // Persist the manifest INSIDE the checkpoint dir so a LATER restore
    // re-verifies from disk rather than trusting that this fetch happened.
    let manifest = crate::checkpoint::CheckpointManifest {
        block,
        image_keccak: got,
        genesis_digest,
    };
    crate::checkpoint::publish_checkpoint(&tmp, &dest, &manifest)?;
    tmp_guard.0 = None;
    info!(
        block,
        bytes = len,
        peer,
        "fetched checkpoint from peer (verified)"
    );
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
    expected_genesis: Option<B256>,
) -> Option<CheckpointInfo> {
    let mut best: Option<CheckpointInfo> = None;
    for peer in peers {
        let floor = best.as_ref().map_or(min_block, |b| b.block + 1);
        match fetch_latest_checkpoint(peer, checkpoints_dir, floor, expected_genesis) {
            Ok(Some(c)) => best = Some(c),
            Ok(None) => {}
            Err(e) => warn!(peer, error = %e, "checkpoint fetch from peer failed"),
        }
    }
    best
}

/// Parsed response head from a checkpoint peer.
struct ResponseHead {
    status: u16,
    block: Option<u64>,
    content_length: Option<u64>,
    keccak: Option<B256>,
    genesis: Option<B256>,
}

fn read_response_head<R: BufRead>(reader: &mut R) -> Result<ResponseHead, StateError> {
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
    let mut keccak = None;
    let mut genesis = None;
    for l in lines {
        let Some((k, v)) = l.split_once(':') else {
            continue;
        };
        match k.trim().to_ascii_lowercase().as_str() {
            "x-checkpoint-block" => block = v.trim().parse().ok(),
            "content-length" => content_length = v.trim().parse().ok(),
            "x-checkpoint-keccak" => keccak = v.trim().parse().ok(),
            "x-checkpoint-genesis" => genesis = v.trim().parse().ok(),
            _ => {}
        }
    }
    Ok(ResponseHead {
        status,
        block,
        content_length,
        keccak,
        genesis,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_checkpoint(dir: &Path, block: u64, contents: &[u8]) -> PathBuf {
        write_checkpoint_as(dir, block, contents, B256::repeat_byte(0x6E))
    }

    /// Write an image + a manifest that HONESTLY describes it, under a given
    /// chain identity.
    fn write_checkpoint_as(dir: &Path, block: u64, contents: &[u8], genesis: B256) -> PathBuf {
        let p = dir.join(checkpoint_name(block));
        std::fs::create_dir_all(&p).unwrap();
        std::fs::write(p.join("mdbx.dat"), contents).unwrap();
        let manifest = crate::checkpoint::CheckpointManifest {
            block,
            image_keccak: alloy_primitives::keccak256(contents),
            genesis_digest: genesis,
        };
        std::fs::write(crate::checkpoint::manifest_path(&p), manifest.encode()).unwrap();
        p
    }

    /// Read the image bytes of a dir-mode checkpoint.
    fn image_bytes(checkpoint: &Path) -> Vec<u8> {
        std::fs::read(crate::checkpoint::checkpoint_data_file(checkpoint).unwrap()).unwrap()
    }

    fn serve_ephemeral(dir: PathBuf) -> SocketAddr {
        serve_checkpoints("127.0.0.1:0".parse().unwrap(), dir)
            .unwrap()
            .addr
    }

    /// The client is sync; run it off the runtime so the server task runs.
    async fn fetch(
        addr: SocketAddr,
        local: PathBuf,
        min_block: u64,
    ) -> Result<Option<CheckpointInfo>, StateError> {
        tokio::task::spawn_blocking(move || {
            fetch_latest_checkpoint(&addr.to_string(), &local, min_block, None)
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_round_trips_newest_checkpoint() {
        let served = tempfile::tempdir().unwrap();
        write_checkpoint(served.path(), 3, b"old image");
        write_checkpoint(served.path(), 7, b"newest image bytes");
        let addr = serve_ephemeral(served.path().to_path_buf());

        let local = tempfile::tempdir().unwrap();
        let got = fetch(addr, local.path().to_path_buf(), 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.block, 7);
        assert_eq!(image_bytes(&got.path), b"newest image bytes");
        // No tmp residue.
        assert!(
            std::fs::read_dir(local.path()).unwrap().all(|e| !e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp"))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_from_empty_peer_returns_none() {
        let served = tempfile::tempdir().unwrap();
        let addr = serve_ephemeral(served.path().to_path_buf());
        let local = tempfile::tempdir().unwrap();
        assert!(
            fetch(addr, local.path().to_path_buf(), 0)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_best_picks_newest_across_peers_and_survives_dead_peer() {
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
        let local_dir = local.path().to_path_buf();
        let best =
            tokio::task::spawn_blocking(move || fetch_best_checkpoint(&peers, &local_dir, 0, None))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(best.block, 9);
        assert_eq!(image_bytes(&best.path), b"b9");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn min_block_filters_stale_peer_checkpoint() {
        let served = tempfile::tempdir().unwrap();
        write_checkpoint(served.path(), 6, b"below the floor");
        let addr = serve_ephemeral(served.path().to_path_buf());
        let local = tempfile::tempdir().unwrap();
        // Peer's newest (6) is below the required floor (10): skipped, nothing
        // written locally.
        assert!(
            fetch(addr, local.path().to_path_buf(), 10)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(std::fs::read_dir(local.path()).unwrap().count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn existing_local_checkpoint_short_circuits_transfer() {
        let served = tempfile::tempdir().unwrap();
        write_checkpoint(served.path(), 4, b"peer bytes");
        let addr = serve_ephemeral(served.path().to_path_buf());

        let local = tempfile::tempdir().unwrap();
        write_checkpoint(local.path(), 4, b"local bytes");
        let got = fetch(addr, local.path().to_path_buf(), 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.block, 4);
        // The local copy is kept, not overwritten by the peer's.
        assert_eq!(image_bytes(&got.path), b"local bytes");
    }
}
