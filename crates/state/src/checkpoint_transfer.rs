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
//! connection, and no HTTP library dependency.
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
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

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

/// Serve the newest checkpoint under `checkpoints_dir` on `addr`, forever,
/// on a dedicated thread.
///
/// Binding happens before the thread spawns. So a bad address fails
/// startup with a clear error, instead of only logging from a background
/// thread.
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
    // Serve the manifest fields as headers, so the peer can verify the
    // bytes it receives, and refuse a foreign chain, without a second
    // round trip. We must not hand out a checkpoint we cannot describe.
    let manifest = match crate::checkpoint::read_manifest(&ckpt.path) {
        Ok(m) => m,
        Err(e) => {
            warn!(error = %e, "checkpoint has no valid manifest; refusing to serve");
            stream.write_all(b"HTTP/1.0 500 Internal Server Error\r\n\r\n")?;
            return Ok(());
        }
    };
    let mut file = std::fs::File::open(&data)?;
    let len = file.metadata()?.len();
    stream.write_all(
        format!(
            "HTTP/1.0 200 OK\r\nx-checkpoint-block: {}\r\nx-checkpoint-keccak: {:#x}\r\n\
             x-checkpoint-genesis: {:#x}\r\ncontent-length: {len}\r\n\r\n",
            ckpt.block, manifest.image_keccak, manifest.genesis_digest
        )
        .as_bytes(),
    )?;
    std::io::copy(&mut file, &mut stream)?;
    info!(block = ckpt.block, bytes = len, "served checkpoint to peer");
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
        // Too old to be useful, for example below the cluster's retention
        // floor. Do not download the body.
        return Ok(None);
    }

    std::fs::create_dir_all(checkpoints_dir)?;
    let dest = checkpoints_dir.join(checkpoint_name(block));
    if dest.exists() {
        // Already have this exact checkpoint locally, so there is nothing
        // to transfer. This is a recovery decision point, so we log it: a
        // silent skip here can look like the repair path never ran, even
        // though the node is fine.
        info!(
            block,
            peer, "peer's newest checkpoint already present locally; skipping transfer"
        );
        return Ok(Some(CheckpointInfo { block, path: dest }));
    }
    // Build the same shape that `create_checkpoint` produces: a directory
    // holding `mdbx.dat` and `MANIFEST`, under a hidden temp name. This
    // makes the published checkpoint self-contained and re-verifiable
    // from disk.
    let tmp = checkpoints_dir.join(format!(".{}.fetch.tmp", checkpoint_name(block)));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp)?;
    // Every refusal below must leave no half-fetched temp entry behind.
    // This guard replaces a cleanup line in each branch, and only the
    // publish step defuses it.
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

    // Check integrity and chain identity before the image becomes visible,
    // using the shared refusal checks (`checkpoint::check_image_identity`).
    // Without this check, this transfer was plain HTTP with only a length
    // check. Silent bit rot, a lying peer, or a checkpoint from a
    // different chain could all become this node's state.
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

    // Store the manifest inside the checkpoint directory. This lets a
    // later restore re-verify from disk, instead of trusting that this
    // fetch happened correctly.
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
        let got = fetch_latest_checkpoint(&addr.to_string(), local.path(), 0, None)
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

    #[test]
    fn fetch_from_empty_peer_returns_none() {
        let served = tempfile::tempdir().unwrap();
        let addr = serve_ephemeral(served.path().to_path_buf());
        let local = tempfile::tempdir().unwrap();
        assert!(
            fetch_latest_checkpoint(&addr.to_string(), local.path(), 0, None)
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
            "127.0.0.1:1".to_string(), // A dead peer: connection is refused, so it is skipped.
            addr_a.to_string(),
            addr_b.to_string(),
        ];
        let best = fetch_best_checkpoint(&peers, local.path(), 0, None).unwrap();
        assert_eq!(best.block, 9);
        assert_eq!(image_bytes(&best.path), b"b9");
    }

    #[test]
    fn min_block_filters_stale_peer_checkpoint() {
        let served = tempfile::tempdir().unwrap();
        write_checkpoint(served.path(), 6, b"below the floor");
        let addr = serve_ephemeral(served.path().to_path_buf());
        let local = tempfile::tempdir().unwrap();
        // The peer's newest checkpoint (block 6) is below the required
        // floor (block 10). It is skipped, and nothing is written locally.
        assert!(
            fetch_latest_checkpoint(&addr.to_string(), local.path(), 10, None)
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
        let got = fetch_latest_checkpoint(&addr.to_string(), local.path(), 0, None)
            .unwrap()
            .unwrap();
        assert_eq!(got.block, 4);
        // The local copy is kept. The peer's copy does not overwrite it.
        assert_eq!(image_bytes(&got.path), b"local bytes");
    }
}
