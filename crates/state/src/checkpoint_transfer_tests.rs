//! Integration tests for peer-to-peer checkpoint transfer
//! (`checkpoint_transfer.rs`).

use super::*;

fn write_checkpoint(dir: &Path, block: u64, contents: &[u8]) -> PathBuf {
    write_checkpoint_as(dir, block, contents, B256::repeat_byte(0x6E))
}

/// Write an image and a manifest that correctly describes it, under a
/// given chain identity.
fn write_checkpoint_as(dir: &Path, block: u64, contents: &[u8], genesis: B256) -> PathBuf {
    write_checkpoint_of_schema(
        dir,
        block,
        contents,
        genesis,
        Some(crate::meta::SCHEMA_VERSION),
    )
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
    write_checkpoint_of_schema(later.path(), 90, b"later schema", genesis, Some(unreadable));
    write_checkpoint(current.path(), 40, b"current schema");
    let later_addr = serve_ephemeral(later.path().to_path_buf());
    let current_addr = serve_ephemeral(current.path().to_path_buf());

    let err = fetch(later_addr, local.path().to_path_buf(), 0)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StateError::UnreadableCheckpointSchema { schema, .. } if schema == unreadable),
        "{err}"
    );
    assert!(
        latest_checkpoint(local.path()).unwrap().is_none(),
        "nothing was downloaded"
    );

    let peers = vec![later_addr.to_string(), current_addr.to_string()];
    let local_dir = local.path().to_path_buf();
    let best =
        tokio::task::spawn_blocking(move || fetch_best_checkpoint(&peers, &local_dir, 0, None))
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
        None,
    );
    let addr = serve_ephemeral(remote.path().to_path_buf());
    let got = fetch(addr, local.path().to_path_buf(), 0)
        .await
        .unwrap()
        .expect("fetched");
    assert_eq!(got.block, 12);
    let manifest = crate::checkpoint::read_manifest(&got.path).unwrap();
    assert_eq!(manifest.schema_version, None);
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
