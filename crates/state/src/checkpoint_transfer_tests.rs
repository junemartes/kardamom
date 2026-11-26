//! Integration tests for peer-to-peer checkpoint transfer
//! (`checkpoint_transfer.rs`).

use super::*;

fn write_checkpoint(dir: &Path, block: u64, contents: &[u8]) -> PathBuf {
    write_checkpoint_as(dir, block, contents, B256::repeat_byte(0x6E))
}

/// Write an image and a manifest that correctly describes it, under a
/// given chain identity.
fn write_checkpoint_as(dir: &Path, block: u64, contents: &[u8], genesis: B256) -> PathBuf {
    write_checkpoint_of_schema(dir, block, contents, genesis, ImageSchema::CURRENT)
}

/// [`write_checkpoint_as`] with the state schema the manifest states.
fn write_checkpoint_of_schema(
    dir: &Path,
    block: u64,
    contents: &[u8],
    genesis: B256,
    schema: ImageSchema,
) -> PathBuf {
    let p = dir.join(checkpoint_name(block));
    std::fs::create_dir_all(&p).unwrap();
    std::fs::write(p.join("mdbx.dat"), contents).unwrap();
    let manifest = crate::checkpoint::CheckpointManifest {
        block,
        image_keccak: alloy_primitives::keccak256(contents),
        genesis_digest: genesis,
        schema_version: schema,
    };
    std::fs::write(crate::checkpoint::manifest_path(&p), manifest.encode()).unwrap();
    p
}

/// A peer that holds an image of another state schema is skipped before
/// the download, and a fleet scan takes the next peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_image_of_another_schema_is_skipped() {
    let later = tempfile::tempdir().unwrap();
    let current = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let genesis = B256::repeat_byte(0x6E);
    let unreadable = crate::meta::SCHEMA_VERSION + 1;
    write_checkpoint_of_schema(
        later.path(),
        90,
        b"later schema",
        genesis,
        ImageSchema::of(unreadable),
    );
    write_checkpoint(current.path(), 40, b"current schema");
    let later_addr = serve_ephemeral(later.path().to_path_buf());
    let current_addr = serve_ephemeral(current.path().to_path_buf());

    let err = fetch(later_addr, local.path().to_path_buf(), BlockRange::ANY)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StateError::UnreadableCheckpointSchema { schema, .. } if schema == unreadable),
        "{err}"
    );
    assert!(
        latest_checkpoint(local.path(), BlockRange::ANY)
            .unwrap()
            .is_none(),
        "nothing was downloaded"
    );

    let peers = vec![later_addr.to_string(), current_addr.to_string()];
    let local_dir = local.path().to_path_buf();
    let best = tokio::task::spawn_blocking(move || {
        fetch_best_checkpoint(&peers, &local_dir, BlockRange::ANY, None)
    })
    .await
    .unwrap()
    .expect("the readable peer serves");
    assert_eq!(best.block, 40);
}

/// A peer of an older release sends no schema header. Its image is
/// fetched, and the stored manifest states no schema either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_without_a_schema_header_is_fetched() {
    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    write_checkpoint_of_schema(
        remote.path(),
        12,
        b"old release",
        B256::repeat_byte(0x6E),
        ImageSchema::default(),
    );
    let addr = serve_ephemeral(remote.path().to_path_buf());
    let got = fetch(addr, local.path().to_path_buf(), BlockRange::ANY)
        .await
        .unwrap()
        .expect("fetched");
    assert_eq!(got.block, 12);
    let manifest = crate::checkpoint::read_manifest(&got.path).unwrap();
    assert_eq!(manifest.schema_version, ImageSchema::default());
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
    range: BlockRange,
) -> Result<Option<CheckpointInfo>, StateError> {
    tokio::task::spawn_blocking(move || {
        fetch_latest_checkpoint(&addr.to_string(), &local, range, None)
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
    let got = fetch(addr, local.path().to_path_buf(), BlockRange::ANY)
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
        fetch(addr, local.path().to_path_buf(), BlockRange::ANY)
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
    let best = tokio::task::spawn_blocking(move || {
        fetch_best_checkpoint(&peers, &local_dir, BlockRange::ANY, None)
    })
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
        fetch(
            addr,
            local.path().to_path_buf(),
            BlockRange::at_or_above(std::num::NonZeroU64::new(10).unwrap())
        )
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
    let got = fetch(addr, local.path().to_path_buf(), BlockRange::ANY)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.block, 4);
    // The local copy is kept. The peer's copy does not overwrite it.
    assert_eq!(image_bytes(&got.path), b"local bytes");
}

/// A fetch with a cap gets the newest checkpoint at or below the cap, and
/// nothing when the server holds none there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capped_fetch_gets_the_newest_checkpoint_below_the_cap() {
    let served = tempfile::tempdir().unwrap();
    write_checkpoint(served.path(), 3, b"old image");
    write_checkpoint(served.path(), 7, b"newest image bytes");
    let addr = serve_ephemeral(served.path().to_path_buf());
    let local = tempfile::tempdir().unwrap();

    let below_seven = BlockRange::below(7).unwrap();
    let got = fetch(addr, local.path().to_path_buf(), below_seven)
        .await
        .unwrap()
        .expect("block 3 is below the cap");
    assert_eq!(got.block, 3);
    assert_eq!(image_bytes(&got.path), b"old image");

    let below_three = BlockRange::below(3).unwrap();
    assert_eq!(
        fetch(addr, local.path().to_path_buf(), below_three)
            .await
            .unwrap(),
        None
    );
}

/// The request line round-trips its cap. A line with no query, the request
/// of an older client, asks for every block.
#[test]
fn the_request_line_carries_the_cap() {
    let capped = FetchRequest {
        range: BlockRange {
            max: 41,
            ..BlockRange::ANY
        },
    };
    assert_eq!(
        capped.line(),
        "GET /checkpoint/latest?max_block=41 HTTP/1.0\r\n\r\n"
    );
    assert_eq!(FetchRequest::parse(&capped.line()), Some(capped));
    let any = FetchRequest {
        range: BlockRange::ANY,
    };
    assert_eq!(any.line(), "GET /checkpoint/latest HTTP/1.0\r\n\r\n");
    assert_eq!(
        FetchRequest::parse("GET /checkpoint/latest HTTP/1.0\r\n"),
        Some(any)
    );
    assert_eq!(FetchRequest::parse("GET /other HTTP/1.0\r\n"), None);
}

/// The names of the checkpoints in `dir`.
fn local_checkpoints(dir: &Path) -> Vec<u64> {
    crate::checkpoint::checkpoints_newest_first(dir)
        .unwrap()
        .iter()
        .map(|c| c.block)
        .collect()
}

/// Run [`fetch_best_checkpoint`] off the runtime, so the server tasks run.
async fn fetch_best(peers: Vec<String>, local: PathBuf, range: BlockRange) -> Option<u64> {
    tokio::task::spawn_blocking(move || fetch_best_checkpoint(&peers, &local, range, None))
        .await
        .unwrap()
        .map(|c| c.block)
}

/// A peer with one checkpoint at each of `blocks`. The temp directory
/// lives as long as the peer.
struct FakePeer {
    dir: tempfile::TempDir,
    addr: SocketAddr,
}

impl FakePeer {
    fn new(blocks: &[u64]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        for &block in blocks {
            write_checkpoint(dir.path(), block, format!("image {block}").as_bytes());
        }
        let addr = serve_ephemeral(dir.path().to_path_buf());
        Self { dir, addr }
    }

    fn name(&self) -> String {
        self.addr.to_string()
    }
}

/// The list order puts the oldest peer first. The fetch still downloads
/// one image only, from the newest peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_peers_with_rising_blocks_give_one_download_from_the_newest() {
    let peers = [
        FakePeer::new(&[5]),
        FakePeer::new(&[7]),
        FakePeer::new(&[9]),
    ];
    let local = tempfile::tempdir().unwrap();
    let names = peers.iter().map(FakePeer::name).collect();
    let best = fetch_best(names, local.path().to_path_buf(), BlockRange::ANY).await;
    assert_eq!(best, Some(9));
    assert_eq!(local_checkpoints(local.path()), [9]);
}

/// The best peer serves an image that fails its keccak check. The fetch
/// takes the next best peer, and leaves no part of the bad image behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_best_peer_that_fails_its_download_falls_back_to_the_next() {
    let peers = [
        FakePeer::new(&[5]),
        FakePeer::new(&[9]),
        FakePeer::new(&[7]),
    ];
    let bad = peers[1]
        .dir
        .path()
        .join(checkpoint_name(9))
        .join("mdbx.dat");
    std::fs::write(bad, b"rotted bytes").unwrap();
    let local = tempfile::tempdir().unwrap();
    let names = peers.iter().map(FakePeer::name).collect();
    let best = fetch_best(names, local.path().to_path_buf(), BlockRange::ANY).await;
    assert_eq!(best, Some(7));
    assert_eq!(local_checkpoints(local.path()), [7]);
    let tmp = std::fs::read_dir(local.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        })
        .count();
    assert_eq!(tmp, 0, "no temp entry remains");
}

/// A peer that refuses the connection, and a peer that closes it with no
/// answer, are skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_that_does_not_answer_is_skipped() {
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent_addr = silent.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let _ = silent.accept().await;
        }
    });
    let live = FakePeer::new(&[4]);
    let local = tempfile::tempdir().unwrap();
    let names = vec![
        "127.0.0.1:1".to_string(),
        silent_addr.to_string(),
        live.name(),
    ];
    let best = fetch_best(names, local.path().to_path_buf(), BlockRange::ANY).await;
    assert_eq!(best, Some(4));
    assert_eq!(local_checkpoints(local.path()), [4]);
}

/// A capped fetch takes the newest checkpoint at or below the cap over
/// every peer. A peer with a newer image serves an older one in the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cap_holds_over_every_peer() {
    let peers = [
        FakePeer::new(&[3, 9]),
        FakePeer::new(&[7]),
        FakePeer::new(&[11]),
    ];
    let local = tempfile::tempdir().unwrap();
    let names = peers.iter().map(FakePeer::name).collect();
    let cap = BlockRange::below(8).unwrap();
    let best = fetch_best(names, local.path().to_path_buf(), cap).await;
    assert_eq!(best, Some(7));
    assert_eq!(local_checkpoints(local.path()), [7]);
}

/// A probe reads the response head and closes the connection. The server
/// sees the write error of a closed peer, which it logs as a normal end,
/// and not as a failed transfer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_probe_that_closes_early_ends_the_serve_as_a_closed_peer() {
    let served = tempfile::tempdir().unwrap();
    // The image is larger than the socket buffers, so the server is still
    // writing when the probe closes.
    write_checkpoint(served.path(), 6, &vec![0x5A; 64 << 20]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap().to_string();
    let probe = tokio::task::spawn_blocking(move || {
        let request = FetchRequest {
            range: BlockRange::ANY,
        }
        .line();
        let mut fetch = CheckpointFetch::connect(&peer, &request, None).unwrap();
        fetch.offer(BlockRange::ANY).unwrap().map(|head| head.block)
    });
    let (stream, _) = listener.accept().await.unwrap();
    let served = serve_one(stream, served.path()).await;
    assert_eq!(probe.await.unwrap(), Some(6));
    let kind = served
        .expect_err("the probe closes before the body ends")
        .kind();
    assert!(PEER_CLOSED.contains(&kind), "{kind:?}");
}
