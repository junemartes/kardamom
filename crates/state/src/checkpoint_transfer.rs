//! Peer-to-peer checkpoint transfer. A minimal HTTP server publishes a
//! node's newest state checkpoint. A client fetches one from a peer.
//!
//! This is the network half of "restore-from-peer" ([`crate::checkpoint`]).
//! Executor replicas are deterministic state machines at the same block, so
//! any replica's checkpoint is a valid restore source for another.
//!
//! The consumer is the resync fallback. A node whose replay cursor aged out
//! of the cluster's bounded retention window (`REPLAY_UNAVAILABLE`) can only
//! be repaired with state at or above the retention floor. Such state never
//! exists locally, because a local checkpoint's block is always at most the
//! local cursor.
//!
//! The protocol is deliberately small: HTTP/1.0 GET, one request per
//! connection, and no HTTP dependency. The server runs as a tokio task, one
//! task per connection. The checkpoint lookup runs on `spawn_blocking`. The
//! client stays std-sync, because its callers are sync startup and repair
//! code:
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
//! The server returns `404` when it has no checkpoint yet. Only complete
//! checkpoints are visible under `checkpoint-*` names, because each one is
//! built in a temp directory and then renamed into place. So a served image
//! is always a full, consistent snapshot.

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

/// The read/write timeout for each socket. Transfers stream in bounded
/// chunks, so this caps per-syscall stalls, such as a dead peer. It does
/// not cap total transfer time.
const IO_TIMEOUT: Duration = Duration::from_secs(20);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// The maximum size for a request or response head. No legitimate one
/// comes close to this limit.
const MAX_HEAD: usize = 4096;

/// The `x-checkpoint-*` header names, shared between `prepare_response`
/// (which writes them) and `PeerResponse::parse_headers` (which reads
/// them). A rename on one side, without this, turns into a silent "peer
/// sent no x-checkpoint-keccak" instead of a compile error.
mod framing {
    pub(super) const HDR_BLOCK: &str = "x-checkpoint-block";
    pub(super) const HDR_KECCAK: &str = "x-checkpoint-keccak";
    pub(super) const HDR_GENESIS: &str = "x-checkpoint-genesis";
}

/// Removes the wrapped temp directory on drop. Set the field to `None`
/// to defuse the guard once the directory has been published.
///
/// This keeps every fetch-refusal path crash-clean, without a cleanup
/// line in each branch.
struct TmpDirGuard<'a>(Option<&'a Path>);

impl Drop for TmpDirGuard<'_> {
    fn drop(&mut self) {
        if let Some(p) = self.0 {
            let _ = std::fs::remove_dir_all(p);
        }
    }
}

/// A running checkpoint server. It holds the bound address (useful when
/// binding to port 0) and the accept-loop task. Dropping the handle does
/// not stop the server. Call `task.abort()` to stop it.
pub struct CheckpointServer {
    pub addr: SocketAddr,
    pub task: tokio::task::JoinHandle<()>,
}

/// Serve the newest checkpoint under `checkpoints_dir` on `addr`, forever.
/// This runs as a tokio task, one task per connection. Binding happens
/// before the task spawns. So a bad address fails startup with a clear
/// error, instead of only logging from a background task. Call this inside
/// a tokio runtime.
///
/// # Errors
///
/// Returns an [`std::io::Error`] if `addr` cannot be bound.
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
    // Serve the manifest fields as headers, so the peer can verify the
    // bytes it receives, and refuse a foreign chain, without a second
    // round trip. We must not hand out a checkpoint we cannot describe.
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
        "HTTP/1.0 200 OK\r\n{}: {}\r\n{}: {:#x}\r\n{}: {:#x}\r\ncontent-length: {len}\r\n\r\n",
        framing::HDR_BLOCK,
        ckpt.block,
        framing::HDR_KECCAK,
        manifest.image_keccak,
        framing::HDR_GENESIS,
        manifest.genesis_digest
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
    // Chunked copy with a per-syscall stall cap: a dead peer fails within
    // IO_TIMEOUT, a slow one streams on.
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
/// `checkpoints_dir`, and return its info.
///
/// Returns `Ok(None)` when the peer has no checkpoint, or its newest
/// checkpoint is below `min_block`. The advertised block is in the
/// response head, so this function never downloads a useless image. If a
/// checkpoint for the same block already exists locally, this function
/// returns it without a transfer.
///
/// The image lands under the hidden `.checkpoint-<block>.fetch.tmp` name.
/// It is renamed into place only after the full advertised length
/// arrives. This preserves the invariant that a visible checkpoint is
/// always complete, for any concurrent reader.
pub(crate) fn fetch_latest_checkpoint(
    peer: &str,
    checkpoints_dir: &Path,
    min_block: u64,
    expected_genesis: Option<B256>,
) -> Result<Option<CheckpointInfo>, StateError> {
    let mut fetch = CheckpointFetch::connect(peer, expected_genesis)?;
    let head = match fetch.response()? {
        PeerResponse::NotFound => return Ok(None),
        PeerResponse::Image(head) => head,
    };
    if head.block < min_block {
        // Too old to be useful, for example below the cluster's retention
        // floor. Do not download the body.
        return Ok(None);
    }

    std::fs::create_dir_all(checkpoints_dir)?;
    let dest = checkpoints_dir.join(checkpoint_name(head.block));
    if dest.exists() {
        // Already have this exact checkpoint locally, so there is nothing
        // to transfer. This is a recovery decision point, so we log it: a
        // silent skip here can look like the repair path never ran, even
        // though the node is fine.
        info!(
            block = head.block,
            peer, "peer's newest checkpoint already present locally; skipping transfer"
        );
        return Ok(Some(CheckpointInfo {
            block: head.block,
            path: dest,
        }));
    }
    // Build the same shape that `create_checkpoint` produces: a directory
    // holding `mdbx.dat` and `MANIFEST`, under a hidden temp name. This
    // makes the published checkpoint self-contained and re-verifiable
    // from disk.
    let tmp = checkpoints_dir.join(format!(".{}.fetch.tmp", checkpoint_name(head.block)));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp)?;
    // Every refusal below must leave no half-fetched temp entry behind.
    // This guard replaces a cleanup line in each branch, and only the
    // publish step defuses it.
    let mut tmp_guard = TmpDirGuard(Some(&tmp));
    let tmp_data = tmp.join("mdbx.dat");
    fetch.download_image(head.len, &tmp_data)?;

    let manifest = fetch.verify_image(&tmp_data, &head)?;

    // Store the manifest inside the checkpoint directory. This lets a
    // later restore re-verify from disk, instead of trusting that this
    // fetch happened correctly.
    crate::checkpoint::publish_checkpoint(&tmp, &dest, &manifest)?;
    tmp_guard.0 = None;
    info!(
        block = head.block,
        bytes = head.len,
        peer,
        "fetched checkpoint from peer (verified)"
    );
    Ok(Some(CheckpointInfo {
        block: head.block,
        path: dest,
    }))
}

/// Fetch from each peer in turn, and keep the newest checkpoint at or
/// above `min_block`.
///
/// This function logs and skips individual peer failures; one live peer
/// is enough. As the best-so-far block rises, later peers with nothing
/// newer are skipped without a download.
pub fn fetch_best_checkpoint(
    peers: &[String],
    checkpoints_dir: &Path,
    min_block: u64,
    expected_genesis: Option<B256>,
) -> Option<CheckpointInfo> {
    let mut best: Option<CheckpointInfo> = None;
    for peer in peers {
        // A peer advertising `u64::MAX` must not wrap the floor back to 0
        // and disable the "newer only" filter for every later peer.
        let floor = best
            .as_ref()
            .map_or(min_block, |b| b.block.saturating_add(1));
        match fetch_latest_checkpoint(peer, checkpoints_dir, floor, expected_genesis) {
            Ok(Some(c)) => best = Some(c),
            Ok(None) => {}
            Err(e) => warn!(peer, error = %e, "checkpoint fetch from peer failed"),
        }
    }
    best
}

/// One peer checkpoint fetch: the connection, and the context every step
/// after parsing the response head needs.
struct CheckpointFetch<'a> {
    peer: &'a str,
    reader: BufReader<TcpStream>,
    expected_genesis: Option<B256>,
}

impl<'a> CheckpointFetch<'a> {
    /// Connect to `peer` (`host:port`) and send the checkpoint-fetch
    /// request.
    fn connect(peer: &'a str, expected_genesis: Option<B256>) -> Result<Self, StateError> {
        let addr: SocketAddr = peer
            .parse()
            .map_err(|_| StateError::Recovery(format!("bad checkpoint peer address: {peer}")))?;
        let mut stream = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)?;
        stream.set_read_timeout(Some(IO_TIMEOUT))?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        stream.write_all(b"GET /checkpoint/latest HTTP/1.0\r\n\r\n")?;
        Ok(Self {
            peer,
            reader: BufReader::new(stream),
            expected_genesis,
        })
    }

    /// Read and parse the response head.
    fn response(&mut self) -> Result<PeerResponse, StateError> {
        let head = self.read_head_text()?;
        PeerResponse::parse(&head, self.peer)
    }

    /// Read the raw head, line by line, up to the blank line that ends it.
    fn read_head_text(&mut self) -> Result<String, StateError> {
        let mut head = String::new();
        let mut line = String::new();
        loop {
            line.clear();
            let n = self.reader.read_line(&mut line)?;
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
        Ok(head)
    }

    /// Copy exactly `len` bytes from the response body into a fresh file
    /// at `path`, then sync it. The file closes when this returns,
    /// before the caller reopens `path` to hash it.
    fn download_image(&mut self, len: u64, path: &Path) -> Result<(), StateError> {
        let mut out = std::fs::File::create(path)?;
        let copied = std::io::copy(&mut self.reader.by_ref().take(len), &mut out)?;
        if copied != len {
            return Err(StateError::Recovery(format!(
                "checkpoint transfer from {} truncated: got {copied} of {len} bytes",
                self.peer
            )));
        }
        out.sync_all()?;
        Ok(())
    }

    /// Check integrity and chain identity before the image becomes
    /// visible, using the shared refusal checks
    /// (`checkpoint::check_image_identity`). Without this check, a
    /// transfer is plain HTTP with only a length check: silent bit rot,
    /// a lying peer, or a checkpoint from a different chain could all
    /// become this node's state. Returns the manifest to publish.
    fn verify_image(
        &self,
        tmp_data: &Path,
        head: &CheckpointHead,
    ) -> Result<crate::checkpoint::CheckpointManifest, StateError> {
        let got = crate::checkpoint::file_keccak(tmp_data)?;
        crate::checkpoint::check_image_identity(
            &format!("from peer {}", self.peer),
            "peer",
            got,
            head.keccak,
            head.genesis,
            self.expected_genesis,
        )?;
        Ok(crate::checkpoint::CheckpointManifest {
            block: head.block,
            image_keccak: got,
            genesis_digest: head.genesis,
        })
    }
}

/// A peer's response to the checkpoint-fetch request, parsed once.
enum PeerResponse {
    /// The peer has no checkpoint (`404`).
    NotFound,
    /// The peer's newest checkpoint image, with everything needed to
    /// fetch and verify it.
    Image(CheckpointHead),
}

/// Everything `x-checkpoint-*` and `content-length` say about a peer's
/// image, all required and already checked present.
struct CheckpointHead {
    block: u64,
    len: u64,
    keccak: B256,
    genesis: B256,
}

impl PeerResponse {
    /// Parse a full response head: the status line, then the headers
    /// this client understands. `peer` names the source, for error
    /// messages only.
    fn parse(head: &str, peer: &str) -> Result<Self, StateError> {
        let mut lines = head.lines();
        let status_line = lines
            .next()
            .ok_or_else(|| StateError::Recovery("empty response from checkpoint peer".into()))?;
        let status = Self::parse_status_line(status_line)?;
        if status == 404 {
            return Ok(Self::NotFound);
        }
        if status != 200 {
            return Err(StateError::Recovery(format!(
                "checkpoint peer {peer} returned status {status}"
            )));
        }
        let headers = Self::parse_headers(lines);
        let block = headers.block.ok_or_else(|| {
            StateError::Recovery(format!(
                "checkpoint peer {peer}: missing x-checkpoint-block"
            ))
        })?;
        let len = headers.content_length.ok_or_else(|| {
            StateError::Recovery(format!("checkpoint peer {peer}: missing content-length"))
        })?;
        let keccak = headers.keccak.ok_or_else(|| {
            StateError::Recovery(format!(
                "checkpoint peer {peer} sent no x-checkpoint-keccak — refusing an \
                 unverifiable image"
            ))
        })?;
        let genesis = headers.genesis.ok_or_else(|| {
            StateError::Recovery(format!(
                "checkpoint peer {peer} sent no x-checkpoint-genesis — refusing an \
                 unidentifiable image"
            ))
        })?;
        Ok(Self::Image(CheckpointHead {
            block,
            len,
            keccak,
            genesis,
        }))
    }

    /// Parse the HTTP status code out of the response's first line.
    fn parse_status_line(status_line: &str) -> Result<u16, StateError> {
        status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| {
                StateError::Recovery(format!(
                    "bad status line from checkpoint peer: {status_line}"
                ))
            })
    }

    /// Parse the checkpoint-specific headers this client understands. Any
    /// other header, or one it fails to parse, is ignored.
    fn parse_headers<'a>(lines: impl Iterator<Item = &'a str>) -> ParsedHeaders {
        let mut headers = ParsedHeaders::default();
        for l in lines {
            let Some((k, v)) = l.split_once(':') else {
                continue;
            };
            match k.trim().to_ascii_lowercase().as_str() {
                framing::HDR_BLOCK => headers.block = v.trim().parse().ok(),
                "content-length" => headers.content_length = v.trim().parse().ok(),
                framing::HDR_KECCAK => headers.keccak = v.trim().parse().ok(),
                framing::HDR_GENESIS => headers.genesis = v.trim().parse().ok(),
                _ => {}
            }
        }
        headers
    }
}

/// The subset of headers [`PeerResponse::parse_headers`] extracts, still
/// optional: presence is checked once, by [`PeerResponse::parse`].
#[derive(Default)]
struct ParsedHeaders {
    block: Option<u64>,
    content_length: Option<u64>,
    keccak: Option<B256>,
    genesis: Option<B256>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_checkpoint(dir: &Path, block: u64, contents: &[u8]) -> PathBuf {
        write_checkpoint_as(dir, block, contents, B256::repeat_byte(0x6E))
    }

    /// Write an image and a manifest that correctly describes it, under a
    /// given chain identity.
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
        // No temp files remain.
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
            "127.0.0.1:1".to_string(), // A dead peer: connection is refused, so it is skipped.
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
        // The peer's newest checkpoint (block 6) is below the required
        // floor (block 10). It is skipped, and nothing is written locally.
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
        // The local copy is kept. The peer's copy does not overwrite it.
        assert_eq!(image_bytes(&got.path), b"local bytes");
    }
}
