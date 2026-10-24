use super::*;
use crate::env::{Durability, StateEnvBuilder};
use crate::snapshot::StateSnapshot;
use crate::writer::{StateWriter, WriteBatch};
use alloy_primitives::{Address, B256, U256};
use kardamom_types::{
    AccountChange, BPosition, BlockBoundary, BlockDelta, StateDatabase, StorageChange,
};

fn commit_blocks(env: &StateEnv, addr: Address, upto: u64) {
    commit_blocks_with_slots(env, addr, upto, 0);
}

/// Commit blocks `1..=upto` for `addr`. Each block also writes `slots`
/// storage slots of `addr`, with keys that differ from block to block.
pub(crate) fn commit_blocks_with_slots(env: &StateEnv, addr: Address, upto: u64, slots: u64) {
    let handle = StateWriter::spawn(env.clone()).unwrap();
    commit_through(handle, (1..=upto).map(|b| block_batch(addr, b, slots)));
}

/// The batch of block `b` for `addr`. It also writes `slots` storage slots
/// of `addr`, with keys that differ from block to block.
pub(crate) fn block_batch(addr: Address, b: u64, slots: u64) -> WriteBatch {
    let boundary = BlockBoundary {
        block_number: b,
        end_tx_idx: BPosition::from_index(b),
        l2_timestamp: 1_700_000_000 + b,
        l1_origin: 0,
        base_fee: 0,
        gas_used: 0,
    };
    let delta = BlockDelta {
        block_number: b,
        accounts: vec![AccountChange {
            address: addr,
            nonce: b,
            balance: U256::from(b * 100),
            code_hash: B256::ZERO,
        }],
        storage: (0..slots)
            .map(|slot| StorageChange {
                address: addr,
                key: B256::from(U256::from(b * 1_000_000 + slot)),
                value: U256::from(slot + 1),
            })
            .collect(),
        code: Vec::new(),
        receipts: Vec::new(),
    };
    WriteBatch::new(boundary, delta)
}

/// Send `batches` to the writer of `handle`, wait until the last one
/// commits, and shut the writer down.
pub(crate) fn commit_through(
    mut handle: crate::writer::WriterHandle,
    batches: impl Iterator<Item = WriteBatch>,
) {
    let last = batches.fold(0, |_, batch| {
        let block = batch.boundary.block_number;
        handle.delta_tx.send(batch).unwrap();
        block
    });
    while let Some(s) = handle.snapshot_rx.recv() {
        if s.block_number() >= last {
            break;
        }
    }
    handle.shutdown().unwrap();
}

/// The account of the batches of the block log tests.
pub(crate) const ADDR: Address = Address::repeat_byte(0x31);

/// Commit `blocks` of [`ADDR`] through a writer that feeds a log thread
/// in `log_dir`, and wait for both threads to end.
pub(crate) fn commit_logged(
    env: &StateEnv,
    log_dir: &std::path::Path,
    blocks: std::ops::RangeInclusive<u64>,
) {
    let cfg = log_thread::LogConfig {
        dir: log_dir.to_path_buf(),
        segment_bytes: NonZeroU64::new(1 << 20).unwrap(),
        queue: NonZeroUsize::new(64).unwrap(),
        sync_every: std::time::Duration::from_millis(10),
    };
    let (feed, log_thread) = log_thread::BlockLogThread::spawn(env, &cfg).unwrap();
    let writer = StateWriter::spawn_with_log(env.clone(), feed).unwrap();
    commit_through(writer, blocks.map(|b| block_batch(ADDR, b, 2)));
    log_thread.join().unwrap();
}

/// Every real env is genesis-seeded at startup. Checkpoints inherit
/// that digest as their chain identity, so test envs must be seeded too.
fn seed_test_genesis(env: &StateEnv) {
    let accounts = [kardamom_types::AccountChange {
        address: Address::from([0x01; 20]),
        nonce: 0,
        balance: alloy_primitives::U256::from(1u64),
        code_hash: B256::ZERO,
    }];
    crate::genesis::seed_genesis(
        env,
        &kardamom_types::GenesisAlloc::of_accounts(accounts.to_vec()),
    )
    .unwrap();
}

/// Open a fresh tempdir-backed env at `dir`, seed genesis, and commit
/// blocks `1..=upto` for `addr`. Every checkpoint test's fixture setup:
/// open, seed, commit.
fn seeded_env_with_blocks(dir: &std::path::Path, addr: Address, upto: u64) -> StateEnv {
    let env = seeded_env(dir);
    commit_blocks(&env, addr, upto);
    env
}

/// Open a fresh tempdir-backed env at `dir`, and seed genesis.
pub(crate) fn seeded_env(dir: &std::path::Path) -> StateEnv {
    let env = StateEnvBuilder::new(dir)
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    seed_test_genesis(&env);
    env
}

#[test]
fn checkpoint_restore_roundtrips_state() {
    let src_dir = tempfile::tempdir().unwrap();
    let ckpt_dir = tempfile::tempdir().unwrap();
    let restore_dir = tempfile::tempdir().unwrap();
    let addr = Address::from([0x42; 20]);

    // Build a DB with 5 committed blocks.
    {
        let env = seeded_env_with_blocks(src_dir.path(), addr, 5);

        let info = create_checkpoint(&env, ckpt_dir.path()).unwrap();
        assert_eq!(info.block, 5);
        assert!(info.path.exists());
    }

    // The latest checkpoint is block 5.
    let latest = latest_checkpoint(ckpt_dir.path(), BlockRange::ANY)
        .unwrap()
        .unwrap();
    assert_eq!(latest.block, 5);

    // Restore into a fresh dir and confirm the state matches exactly.
    let restored_block =
        restore_checkpoint(&latest.path, restore_dir.path(), None, u64::MAX).unwrap();
    assert_eq!(restored_block, 5);

    let snap =
        StateSnapshot::open(&StateEnvBuilder::new(restore_dir.path()).open().unwrap()).unwrap();
    assert_eq!(snap.block_number(), 5);
    let (nonce, balance, _) = snap.basic(addr).unwrap().unwrap();
    assert_eq!(nonce, 5);
    assert_eq!(balance, U256::from(500u64));
}

/// A checkpoint whose bytes changed under its manifest must be
/// refused, not adopted. An image is only as trustworthy as something
/// that pins its content.
#[test]
fn restore_refuses_a_tampered_image() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x21; 20]);
    let env = seeded_env_with_blocks(src.path(), addr, 3);
    let c = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    // The checkpoint verifies while it is untouched.
    verify_checkpoint(&c.path, None).expect("untouched checkpoint must verify");

    // Flip one byte deep inside the image, and keep the same length.
    let data = checkpoint_data_file(&c.path).unwrap();
    let mut bytes = std::fs::read(&data).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x01;
    std::fs::write(&data, &bytes).unwrap();

    let err = restore_checkpoint(&c.path, dst.path(), None, u64::MAX).unwrap_err();
    assert!(
        format!("{err}").contains("CORRUPT"),
        "expected a corruption refusal, got: {err}"
    );
    // And nothing was staged into the destination.
    assert!(!has_state_db(dst.path()).unwrap());
}

/// A checkpoint from another chain must be refused. Without this
/// check, a fresh node could adopt a foreign checkpoint, then request a
/// canonical index that chain never produced, and loop forever.
#[test]
fn restore_refuses_a_foreign_chain() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x22; 20]);
    let env = seeded_env_with_blocks(src.path(), addr, 2);
    let c = create_checkpoint(&env, ckpt.path()).unwrap();
    let ours = stored_genesis_digest(&env).unwrap();
    drop(env);

    // Same bytes, but this node belongs to a different chain.
    let theirs = B256::repeat_byte(0xAB);
    assert_ne!(ours, theirs);
    let err = restore_checkpoint(&c.path, dst.path(), Some(theirs), u64::MAX).unwrap_err();
    assert!(
        format!("{err}").contains("DIFFERENT CHAIN"),
        "expected a chain-identity refusal, got: {err}"
    );
    assert!(!has_state_db(dst.path()).unwrap());

    // Our own chain still restores.
    restore_checkpoint(&c.path, dst.path(), Some(ours), u64::MAX).expect("same chain must restore");
}

/// An image with no manifest at all is unverifiable and must be
/// refused. Otherwise, deleting a file would trivially bypass the check.
#[test]
fn restore_refuses_an_unmanifested_image() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x23; 20]);
    let env = seeded_env_with_blocks(src.path(), addr, 1);
    let c = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    std::fs::remove_file(manifest_path(&c.path)).unwrap();
    let err = restore_checkpoint(&c.path, dst.path(), None, u64::MAX).unwrap_err();
    assert!(
        format!("{err}").contains("manifest"),
        "expected an unverifiable-image refusal, got: {err}"
    );
}

/// A reader of another process's checkpoint directory takes the
/// next-newest checkpoint and leaves the directory as it found it. The
/// writer prunes its own checkpoints, so an unreadable one is a race,
/// not corruption.
#[test]
fn restore_newest_readable_skips_without_touching_the_directory() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x25; 20]);
    let env = seeded_env_with_blocks(src.path(), addr, 2);
    let good = create_checkpoint(&env, ckpt.path()).unwrap();
    commit_blocks(&env, addr, 5);
    let torn = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    // The newest one cannot be read back: its manifest never arrived.
    std::fs::remove_file(manifest_path(&torn.path)).unwrap();

    let (block, path) = restore_newest_readable(ckpt.path(), dst.path(), None)
        .expect("an unreadable checkpoint is not an error")
        .expect("the older readable checkpoint must restore");
    assert_eq!(block, good.block);
    assert_eq!(path, good.path);
    // The writer's directory is untouched: the skipped checkpoint keeps
    // its name, and nothing is quarantined.
    assert!(torn.path.exists(), "the skipped checkpoint must stay put");
    let hidden: Vec<_> = std::fs::read_dir(ckpt.path())
        .unwrap()
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with('.'))
        .collect();
    assert!(hidden.is_empty(), "nothing may be renamed: {hidden:?}");
}

/// A checkpoint that the writer prunes between the listing and the read
/// is skipped, and the reader still returns the next-newest.
#[test]
fn restore_newest_readable_skips_a_pruned_checkpoint() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x26; 20]);
    let env = seeded_env_with_blocks(src.path(), addr, 2);
    let good = create_checkpoint(&env, ckpt.path()).unwrap();
    commit_blocks(&env, addr, 5);
    let pruned = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    // The writer removed the newest one's contents, as a prune does.
    std::fs::remove_dir_all(&pruned.path).unwrap();
    std::fs::create_dir(&pruned.path).unwrap();

    let (block, _) = restore_newest_readable(ckpt.path(), dst.path(), None)
        .expect("a pruned checkpoint is not an error")
        .expect("the older readable checkpoint must restore");
    assert_eq!(block, good.block);
}

/// A bad newest checkpoint must cost one rung of the ladder, not wedge
/// the node. `restore_best_checkpoint` quarantines it and restores the
/// next-newest.
#[test]
fn restore_best_quarantines_bad_and_falls_back() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x24; 20]);
    let env = seeded_env_with_blocks(src.path(), addr, 2);
    let good = create_checkpoint(&env, ckpt.path()).unwrap();
    commit_blocks(&env, addr, 5);
    let torn = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    // Make the newest look like an image whose MANIFEST never arrived.
    std::fs::remove_file(manifest_path(&torn.path)).unwrap();

    let (block, path) = restore_best_checkpoint(ckpt.path(), dst.path(), BlockRange::ANY, None)
        .expect("restore_best must not error on a quarantinable checkpoint")
        .expect("the older good checkpoint must restore");
    assert_eq!(block, good.block);
    assert_eq!(path, good.path);
    // The torn one is quarantined under a hidden name, not retried forever.
    assert!(!torn.path.exists());
    let rejected: Vec<_> = std::fs::read_dir(ckpt.path())
        .unwrap()
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".rejected-"))
        .collect();
    assert_eq!(
        rejected.len(),
        1,
        "expected one quarantined checkpoint: {rejected:?}"
    );

    // Nothing restorable is left, so this returns Ok(None), never a crash loop.
    let dst2 = tempfile::tempdir().unwrap();
    std::fs::remove_file(manifest_path(&good.path)).unwrap();
    assert!(
        restore_best_checkpoint(ckpt.path(), dst2.path(), BlockRange::ANY, None)
            .unwrap()
            .is_none()
    );
}

/// A restore with a capped range takes the newest checkpoint below the
/// cap, and leaves the newer ones in place.
#[test]
fn restore_best_in_a_range_takes_the_newest_below_the_cap() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let addr = Address::from([0x31; 20]);
    let env = seeded_env_with_blocks(src.path(), addr, 2);
    let c2 = create_checkpoint(&env, ckpt.path()).unwrap();
    commit_blocks(&env, addr, 4);
    let c4 = create_checkpoint(&env, ckpt.path()).unwrap();
    commit_blocks(&env, addr, 6);
    let c6 = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    let dst = tempfile::tempdir().unwrap();
    let below_six = BlockRange::below(6).unwrap();
    let (block, path) = restore_best_checkpoint(ckpt.path(), dst.path(), below_six, None)
        .unwrap()
        .expect("block 4 is below the cap");
    assert_eq!((block, path), (c4.block, c4.path));
    assert!(c6.path.exists() && c2.path.exists());
    let empty = tempfile::tempdir().unwrap();
    assert_eq!(
        restore_best_checkpoint(
            ckpt.path(),
            empty.path(),
            BlockRange::below(2).unwrap(),
            None
        )
        .unwrap(),
        None
    );
}

#[test]
fn latest_picks_highest_block_and_prune_trims() {
    let src_dir = tempfile::tempdir().unwrap();
    let ckpt_dir = tempfile::tempdir().unwrap();
    let addr = Address::from([0x7; 20]);

    let env = seeded_env_with_blocks(src_dir.path(), addr, 3);
    let c3 = create_checkpoint(&env, ckpt_dir.path()).unwrap();
    assert_eq!(c3.block, 3);
    // Advance and checkpoint again.
    commit_blocks(&env, addr, 7);
    let c7 = create_checkpoint(&env, ckpt_dir.path()).unwrap();
    assert_eq!(c7.block, 7);

    assert_eq!(
        latest_checkpoint(ckpt_dir.path(), BlockRange::ANY)
            .unwrap()
            .unwrap()
            .block,
        7
    );

    // Keep only the newest checkpoint. This removes the block-3 checkpoint.
    assert_eq!(
        prune_checkpoints(ckpt_dir.path(), NonZeroUsize::MIN).unwrap(),
        1
    );
    assert_eq!(
        latest_checkpoint(ckpt_dir.path(), BlockRange::ANY)
            .unwrap()
            .unwrap()
            .block,
        7
    );
}

/// One prune case: checkpoints at `blocks`, a prune that keeps `keep`,
/// and the blocks that must stay.
struct PruneCase {
    blocks: &'static [u64],
    keep: usize,
    stays: &'static [u64],
}

impl PruneCase {
    /// Prunes empty checkpoint directories. The prune reads only the
    /// names, so the images need no mdbx content.
    fn check(&self) {
        let dir = tempfile::tempdir().unwrap();
        self.blocks
            .iter()
            .for_each(|b| std::fs::create_dir(dir.path().join(checkpoint_name(*b))).unwrap());
        let keep = NonZeroUsize::new(self.keep).unwrap();
        let removed = prune_checkpoints(dir.path(), keep).unwrap();
        let mut stays: Vec<u64> = checkpoints_newest_first(dir.path())
            .unwrap()
            .into_iter()
            .map(|c| c.block)
            .collect();
        stays.sort_unstable();
        assert_eq!(stays, self.stays, "keep {}", self.keep);
        assert_eq!(removed, self.blocks.len() - self.stays.len());
    }
}

#[test]
fn prune_keeps_the_newest_checkpoints_whatever_the_block_spacing() {
    [
        PruneCase {
            blocks: &[10, 20, 30, 40],
            keep: 3,
            stays: &[20, 30, 40],
        },
        PruneCase {
            blocks: &[10, 20, 30, 40],
            keep: 1,
            stays: &[40],
        },
        PruneCase {
            blocks: &[10, 20],
            keep: 3,
            stays: &[10, 20],
        },
        PruneCase {
            blocks: &[],
            keep: 3,
            stays: &[],
        },
    ]
    .iter()
    .for_each(PruneCase::check);
}

#[test]
fn create_sweeps_stale_tmp_and_leaves_no_residue() {
    let src_dir = tempfile::tempdir().unwrap();
    let ckpt_dir = tempfile::tempdir().unwrap();
    let addr = Address::from([0x5; 20]);
    let env = seeded_env_with_blocks(src_dir.path(), addr, 2);

    // Plant a stale temp directory, as if from a crashed earlier writer.
    let stale = ckpt_dir.path().join(".checkpoint-000000000000000001.tmp");
    std::fs::create_dir_all(&stale).unwrap();

    let c = create_checkpoint(&env, ckpt_dir.path()).unwrap();
    assert!(c.path.exists());
    assert!(!stale.exists());
    let tmp_residue = std::fs::read_dir(ckpt_dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        })
        .count();
    assert_eq!(tmp_residue, 0);
}

#[test]
fn restore_refuses_to_clobber_populated_dir() {
    let src_dir = tempfile::tempdir().unwrap();
    let ckpt_dir = tempfile::tempdir().unwrap();
    let addr = Address::from([0x9; 20]);
    let env = seeded_env_with_blocks(src_dir.path(), addr, 2);
    let c = create_checkpoint(&env, ckpt_dir.path()).unwrap();

    // Restoring over the live, populated source directory must be refused.
    let err = restore_checkpoint(&c.path, src_dir.path(), None, u64::MAX).unwrap_err();
    assert!(matches!(err, StateError::Recovery(_)));
}

#[test]
fn quarantine_failed_message_has_a_readable_gap() {
    // Defect: the message had "could not be" followed by 26 spaces and
    // then "quarantined", a stray run of literal spaces baked into the
    // format string instead of one space.
    let path = std::path::Path::new("/checkpoints/checkpoint-9");
    let verify_err = StateError::Recovery("bad manifest".into());
    let rename_err = std::io::Error::other("permission denied");
    let StateError::Recovery(msg) = quarantine_failed_message(path, &verify_err, &rename_err)
    else {
        panic!("expected StateError::Recovery");
    };
    assert!(msg.contains("could not be quarantined:"), "got {msg:?}");
    assert!(!msg.contains("be  quarantined"), "got {msg:?}");
}

/// The manifest states the image's schema. A manifest from before the
/// key parses with no schema, and a key the reader does not know is
/// ignored.
#[test]
fn manifest_round_trips_the_schema_version() {
    let manifest = CheckpointManifest {
        block: 9,
        image_keccak: B256::repeat_byte(0x11),
        genesis_digest: B256::repeat_byte(0x22),
        schema_version: ImageSchema::CURRENT,
    };
    let text = manifest.encode();
    assert!(text.contains(&format!("schema_version={}\n", crate::meta::SCHEMA_VERSION)));
    assert_eq!(CheckpointManifest::parse(&text).unwrap(), manifest);

    let old = text
        .lines()
        .filter(|l| !l.starts_with("schema_version"))
        .flat_map(|l| [l, "\n"])
        .collect::<String>();
    let parsed = CheckpointManifest::parse(&format!("{old}later_key=1\n")).unwrap();
    assert_eq!(parsed.schema_version, ImageSchema::default());
    assert_eq!(parsed.block, 9);
}

/// A checkpoint whose manifest states a schema this release does not
/// read is skipped before its image is copied. A reader of another
/// process's directory takes the next-newest and leaves the skipped one
/// in place.
#[test]
fn restore_skips_a_checkpoint_of_another_schema() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x27; 20]);
    let env = seeded_env_with_blocks(src.path(), addr, 2);
    let readable = create_checkpoint(&env, ckpt.path()).unwrap();
    commit_blocks(&env, addr, 5);
    let later = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    // The newer checkpoint comes from a release with the next schema.
    let mut manifest = read_manifest(&later.path).unwrap();
    manifest.schema_version = ImageSchema::of(crate::meta::SCHEMA_VERSION + 1);
    std::fs::write(manifest_path(&later.path), manifest.encode()).unwrap();

    let err = restore_checkpoint(&later.path, dst.path(), None, u64::MAX).unwrap_err();
    assert!(
        matches!(err, StateError::UnreadableCheckpointSchema { schema, .. } if schema == crate::meta::SCHEMA_VERSION + 1),
        "{err}"
    );
    assert!(!has_state_db(dst.path()).unwrap(), "nothing was copied");

    let (block, path) = restore_newest_readable(ckpt.path(), dst.path(), None)
        .unwrap()
        .expect("the readable checkpoint restores");
    assert_eq!(block, readable.block);
    assert_eq!(path, readable.path);
    assert!(later.path.exists(), "the skipped checkpoint stays put");
}

/// The engine path, `restore_best_checkpoint`, skips a checkpoint of a
/// schema this release does not read and restores the next-newest. The
/// skipped checkpoint keeps its name, so a later release still finds it,
/// and nothing is quarantined.
#[test]
fn restore_best_skips_a_checkpoint_of_another_schema_without_quarantine() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x28; 20]);
    let env = seeded_env_with_blocks(src.path(), addr, 2);
    let readable = create_checkpoint(&env, ckpt.path()).unwrap();
    commit_blocks(&env, addr, 5);
    let later = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    let mut manifest = read_manifest(&later.path).unwrap();
    manifest.schema_version = ImageSchema::of(crate::meta::SCHEMA_VERSION + 1);
    std::fs::write(manifest_path(&later.path), manifest.encode()).unwrap();

    let (block, path) = restore_best_checkpoint(ckpt.path(), dst.path(), BlockRange::ANY, None)
        .unwrap()
        .expect("the readable checkpoint restores");
    assert_eq!(block, readable.block);
    assert_eq!(path, readable.path);
    assert!(later.path.exists(), "the skipped checkpoint keeps its name");
    let hidden: Vec<_> = std::fs::read_dir(ckpt.path())
        .unwrap()
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with('.'))
        .collect();
    assert!(hidden.is_empty(), "nothing is quarantined: {hidden:?}");
}

#[test]
fn format_versions_match_the_registry() {
    use kardamom_formats::Registry;
    Registry::assert_exact("state-db", crate::meta::SCHEMA_VERSION);
    Registry::assert_exact("checkpoint-manifest", super::manifest::MANIFEST_VERSION);
}

/// The rows of each table, as key and value bytes, in cursor order.
type TableRows = Vec<(&'static str, Vec<(Vec<u8>, Vec<u8>)>)>;

/// Every row of every table, in cursor order.
fn table_rows(env: &StateEnv) -> TableRows {
    let txn = env.raw().begin_ro_sync().unwrap();
    crate::schema::ALL_TABLES
        .iter()
        .map(|table| {
            let db = txn.open_db(Some(table)).unwrap();
            let mut cur = txn.cursor(db).unwrap();
            let rows = cur
                .iter_start::<Vec<u8>, Vec<u8>>()
                .unwrap()
                .map(Result::unwrap)
                .collect();
            (*table, rows)
        })
        .collect()
}

/// A new checkpoint image holds only its used pages, not the trailing
/// hole of the growth step. The manifest hash and a peer transfer then
/// read about as many bytes as the state holds. The state here fills
/// many 64 KiB units, so a cut that drops a used page loses rows. The
/// image keeps every used page, opens in place read-only with every row
/// of the source, and restores.
#[test]
fn create_cuts_the_image_to_its_used_pages() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x28; 20]);
    let env = seeded_env(src.path());
    commit_blocks_with_slots(&env, addr, 4, 1_000);
    let c = create_checkpoint(&env, ckpt.path()).unwrap();

    let len = std::fs::metadata(checkpoint_data_file(&c.path).unwrap())
        .unwrap()
        .len();
    let in_place = StateEnvBuilder::new(&c.path)
        .read_only(true)
        .open()
        .unwrap();
    let pages = u64::try_from(in_place.raw().info().unwrap().last_pgno()).unwrap() + 1;
    let used = pages * u64::from(in_place.raw().stat().unwrap().page_size());
    assert!(
        used >= 4 * 64 * 1024,
        "the state fills many units: {used} bytes"
    );
    assert!(
        len >= used,
        "the image keeps every used page: {len} < {used}"
    );
    assert!(
        len < used + 64 * 1024,
        "the image keeps no hole: {len} bytes"
    );
    assert_eq!(len % (64 * 1024), 0, "the cut length is 64 KiB aligned");
    assert_eq!(table_rows(&in_place), table_rows(&env));
    assert_eq!(
        restore_checkpoint(&c.path, dst.path(), None, u64::MAX).unwrap(),
        4
    );
}

/// An image that is longer than its used pages still verifies and
/// restores, when its manifest has the hash of the whole file. The
/// reader does not use the file length. A short trailing hole keeps the
/// hash fast in a debug build.
#[test]
fn restore_reads_an_image_with_a_trailing_hole() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x29; 20]);
    let env = seeded_env_with_blocks(src.path(), addr, 2);
    let c = create_checkpoint(&env, ckpt.path()).unwrap();

    let data = checkpoint_data_file(&c.path).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&data)
        .unwrap()
        .set_len(4 * 1024 * 1024)
        .unwrap();
    let manifest = CheckpointManifest {
        image_keccak: file_keccak(&data).unwrap(),
        ..read_manifest(&c.path).unwrap()
    };
    std::fs::write(manifest_path(&c.path), manifest.encode()).unwrap();

    assert_eq!(
        restore_checkpoint(&c.path, dst.path(), None, u64::MAX).unwrap(),
        2
    );
}
