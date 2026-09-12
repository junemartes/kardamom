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
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
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
            let Some(stream) = accept_logged(&listener).await else {
                continue;
            };
            spawn_serve(stream, checkpoints_dir.clone());
        }
    });
    Ok(CheckpointServer { addr, task })
}

/// Accept one connection, logging and swallowing an accept error instead
/// of returning it: one bad accept must not take down the server loop.
async fn accept_logged(listener: &TcpListener) -> Option<tokio::net::TcpStream> {
    match listener.accept().await {
        Ok((stream, _)) => Some(stream),
        Err(e) => {
            warn!(error = %e, "checkpoint server accept failed");
            None
        }
    }
}

/// Serve one accepted connection on its own task, logging a transfer
/// failure instead of propagating it: one peer's failure must not affect
/// any other.
fn spawn_serve(stream: tokio::net::TcpStream, dir: PathBuf) {
    tokio::spawn(async move {
        if let Err(e) = serve_one(stream, &dir).await {
            warn!(error = %e, "checkpoint transfer to peer failed");
        }
    });
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
    while let Some(n) = std::num::NonZeroUsize::new(file.read(&mut buf).await?) {
        timeout(IO_TIMEOUT, wr.write_all(&buf[..n.get()])).await??;
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
#[must_use]
pub fn fetch_best_checkpoint(
    peers: &[String],
    checkpoints_dir: &Path,
    min_block: u64,
    expected_genesis: Option<B256>,
) -> Option<CheckpointInfo> {
    peers.iter().fold(None, |best, peer| {
        // A peer advertising `u64::MAX` must not wrap the floor back to 0
        // and disable the "newer only" filter for every later peer.
        let floor = best
            .as_ref()
            .map_or(min_block, |b: &CheckpointInfo| b.block.saturating_add(1));
        fetch_one_peer(peer, checkpoints_dir, floor, expected_genesis).or(best)
    })
}

/// Fetch from one peer, logging and swallowing a failure: one bad peer
/// must not stop the scan of the rest.
fn fetch_one_peer(
    peer: &str,
    checkpoints_dir: &Path,
    floor: u64,
    expected_genesis: Option<B256>,
) -> Option<CheckpointInfo> {
    match fetch_latest_checkpoint(peer, checkpoints_dir, floor, expected_genesis) {
        Ok(c) => c,
        Err(e) => {
            warn!(peer, error = %e, "checkpoint fetch from peer failed");
            None
        }
    }
}

/// One step of [`CheckpointFetch::read_head_line`].
enum HeadLine {
    /// The blank line or EOF that ends the head.
    Done,
    /// The accumulated head exceeds [`MAX_HEAD`].
    TooLarge,
    /// An ordinary line, already appended to the caller's `head`.
    More,
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
        let mut stream = Self::open(peer)?;
        stream.set_read_timeout(Some(IO_TIMEOUT))?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        stream.write_all(b"GET /checkpoint/latest HTTP/1.0\r\n\r\n")?;
        Ok(Self {
            peer,
            reader: BufReader::new(stream),
            expected_genesis,
        })
    }

    /// Open a connection to the first address of `peer` that accepts one.
    /// The host part may be a name, such as a Consul node record; every
    /// address it resolves to is tried in order.
    fn open(peer: &str) -> Result<TcpStream, StateError> {
        let addrs = peer.to_socket_addrs().map_err(|e| {
            StateError::Recovery(format!("bad checkpoint peer address {peer}: {e}"))
        })?;
        let mut refused = None;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
                Ok(stream) => return Ok(stream),
                Err(e) => refused = Some(e),
            }
        }
        Err(refused.map_or_else(
            || StateError::Recovery(format!("checkpoint peer {peer} resolves to no address")),
            StateError::from,
        ))
    }

    /// Read and parse the response head.
    fn response(&mut self) -> Result<PeerResponse, StateError> {
        let head = self.read_head_text()?;
        PeerResponse::parse(&head, self.peer)
    }

    /// Read the raw head, line by line, up to the blank line that ends it.
    fn read_head_text(&mut self) -> Result<String, StateError> {
        let mut head = String::new();
        loop {
            match self.read_head_line(&mut head)? {
                HeadLine::Done => break,
                HeadLine::TooLarge => {
                    return Err(StateError::Recovery(
                        "checkpoint peer response head too large".into(),
                    ));
                }
                HeadLine::More => {}
            }
        }
        Ok(head)
    }

    /// Read one line into `head`. `Done` at the blank line or EOF that
    /// ends the head, `TooLarge` once `head` exceeds [`MAX_HEAD`],
    /// `More` for every other line.
    fn read_head_line(&mut self, head: &mut String) -> Result<HeadLine, StateError> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line)?;
        if n == 0 || line == "\r\n" || line == "\n" {
            return Ok(HeadLine::Done);
        }
        head.push_str(&line);
        Ok(if head.len() > MAX_HEAD {
            HeadLine::TooLarge
        } else {
            HeadLine::More
        })
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
        lines
            .filter_map(|l| l.split_once(':'))
            .fold(ParsedHeaders::default(), |h, (k, v)| {
                h.with(&k.trim().to_ascii_lowercase(), v.trim())
            })
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

impl ParsedHeaders {
    /// Fold in one already-lowercased header key and trimmed value. An
    /// unrecognized key is ignored; a recognized key that fails to parse
    /// clears that field, matching a malformed or duplicate header
    /// overwriting an earlier valid one.
    fn with(mut self, k: &str, v: &str) -> Self {
        match k {
            framing::HDR_BLOCK => self.block = v.parse().ok(),
            "content-length" => self.content_length = v.parse().ok(),
            framing::HDR_KECCAK => self.keccak = v.parse().ok(),
            framing::HDR_GENESIS => self.genesis = v.parse().ok(),
            _ => {}
        }
        self
    }
}

#[cfg(test)]
#[path = "checkpoint_transfer_tests.rs"]
mod tests;
