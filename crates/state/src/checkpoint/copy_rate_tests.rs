use std::fs::File;
use std::io::Write;
use std::num::NonZeroU64;
use std::os::unix::fs::MetadataExt;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256, U256};
use kardamom_types::StorageChange;

use super::super::image::CompactedImage;
use super::super::tests::{block_batch, commit_blocks_with_slots, seeded_env};
use super::super::{BlockRange, create_checkpoint_at, restore_best_checkpoint};
use super::{CopyRate, RateSink};
use crate::env::{StateEnv, StateEnvBuilder};
use crate::geometry::PAGE_SIZE;
use crate::integrity::deep_compare;
use crate::writer::{StateWriter, WriteBatch, WriterHandle};

const MIB: usize = 1 << 20;

/// The sink writes the data at the rate, and leaves a hole for each chunk
/// of zeros. The file reads back as the bytes of the pipe.
#[test]
fn the_sink_keeps_the_rate_and_leaves_holes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("image");
    let rate = NonZeroU64::new(4 << 20).unwrap();
    let (reader, mut writer) = std::io::pipe().unwrap();
    let sink = RateSink::new(File::create(&path).unwrap(), rate);
    let started = Instant::now();
    let drain = std::thread::spawn(move || sink.drain(reader));
    let parts: [(u8, usize); 3] = [(7, 2 * MIB), (0, 3 * MIB), (9, MIB)];
    for (byte, len) in parts {
        writer.write_all(&vec![byte; len]).unwrap();
    }
    {
        let _closed = writer;
    }
    drain.join().unwrap().unwrap();

    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(700),
        "3 MiB of data at 4 MiB/s takes about 750 ms, not {elapsed:?}"
    );
    let expected: Vec<u8> = parts
        .iter()
        .flat_map(|(byte, len)| std::iter::repeat_n(*byte, *len))
        .collect();
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let allocated = std::fs::metadata(&path).unwrap().blocks() * 512;
    assert!(
        allocated < 5 * MIB as u64,
        "the 3 MiB of zeros are a hole: {allocated} bytes allocated"
    );
}

/// A checkpoint made with the limited copy restores to the source state.
/// A compacted copy into a pipe fails this test: its image opens empty.
#[test]
fn a_limited_copy_makes_a_checkpoint_that_restores() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let env = seeded_env(src.path());
    commit_blocks_with_slots(&env, Address::repeat_byte(0x44), 3, 500);
    let rate = CopyRate::Limited(NonZeroU64::new(1 << 30).unwrap());
    let info = create_checkpoint_at(&env, ckpt.path(), rate).unwrap();
    assert_eq!(info.block, 3);

    let restored = restore_best_checkpoint(ckpt.path(), dst.path(), BlockRange::ANY, None).unwrap();
    assert_eq!(restored.map(|(block, _)| block), Some(3));
    let restored_env = StateEnvBuilder::new(dst.path()).open().unwrap();
    let diffs = deep_compare(&env, &restored_env).unwrap();
    assert!(diffs.is_empty(), "{diffs:?}");
}

/// The keys of the steady load: each block writes some of them again, so
/// the used pages stay the same size.
const LIVE_SLOTS: u64 = 20_000;
/// The keys that one block of the load writes again.
const SLOTS_PER_BLOCK: u64 = 50;
/// The pause between two blocks of the load.
const BLOCK_PAUSE: Duration = Duration::from_millis(2);

/// The pages of a state database: the high-water mark of the file, and the
/// pages of it that hold data.
#[derive(Debug, Clone, Copy)]
struct Pages {
    allocated: u64,
    used: u64,
}

impl Pages {
    fn of(env: &StateEnv) -> Self {
        let allocated = u64::try_from(env.raw().info().unwrap().last_pgno()).unwrap() + 1;
        let free = u64::try_from(env.raw().freelist().unwrap()).unwrap();
        Self {
            allocated,
            used: allocated - free,
        }
    }
}

/// A steady write load on a state of [`LIVE_SLOTS`] keys: a writer thread
/// that writes [`SLOTS_PER_BLOCK`] of the keys again in each block, until
/// the load drops.
struct SteadyLoad {
    stop: Option<crossbeam_channel::Sender<()>>,
    thread: Option<std::thread::JoinHandle<u64>>,
}

impl SteadyLoad {
    /// Fill `env` with the keys in block 1, then start the load.
    fn start(env: &StateEnv) -> Self {
        let mut writer = StateWriter::spawn(env.clone()).unwrap();
        Self::commit(&mut writer, Self::batch(1, 0..LIVE_SLOTS));
        let (stop, stopped) = crossbeam_channel::bounded(1);
        let thread = std::thread::spawn(move || Self::run(&mut writer, &stopped));
        Self {
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// Commit blocks until `stopped` ends. Returns the last block.
    fn run(writer: &mut WriterHandle, stopped: &crossbeam_channel::Receiver<()>) -> u64 {
        let mut block = 1;
        while stopped.try_recv() == Err(crossbeam_channel::TryRecvError::Empty) {
            block = Self::step(writer, block);
        }
        writer.shutdown().unwrap();
        block
    }

    /// Commit the block after `last`, then pause. Returns that block.
    fn step(writer: &mut WriterHandle, last: u64) -> u64 {
        let b = last + 1;
        let first = (b * SLOTS_PER_BLOCK) % LIVE_SLOTS;
        Self::commit(writer, Self::batch(b, first..first + SLOTS_PER_BLOCK));
        std::thread::sleep(BLOCK_PAUSE);
        b
    }

    fn commit(writer: &mut WriterHandle, batch: WriteBatch) {
        let block = batch.boundary.block_number;
        writer.delta_tx.send(batch).unwrap();
        std::iter::from_fn(|| writer.snapshot_rx.recv())
            .find(|s| s.block_number() == block)
            .unwrap();
    }

    /// Block `b`: the value `b` in each key of `slots`.
    fn batch(b: u64, slots: std::ops::Range<u64>) -> WriteBatch {
        let mut batch = block_batch(Address::repeat_byte(0x45), b, 0);
        batch.delta.storage = slots
            .map(|slot| StorageChange {
                address: Address::repeat_byte(0x45),
                key: B256::from(U256::from(slot)),
                value: U256::from(b),
            })
            .collect();
        batch
    }

    /// Stop the load, and return its last block.
    fn stop(mut self) -> u64 {
        self.stop.take();
        self.thread.take().unwrap().join().unwrap()
    }
}

/// The bytes of the image of `env` that `rate` copies to `dest`, cut to its
/// used pages as a checkpoint is.
fn image_len(env: &StateEnv, rate: CopyRate, dest: &std::path::Path) -> u64 {
    rate.copy(env, dest).unwrap();
    CompactedImage::new(dest).cut_to_used_pages().unwrap()
}

/// A rate that no test reaches: a copy of the pages as they are, at the
/// speed of the disk.
const FAST: CopyRate = CopyRate::Limited(NonZeroU64::new(1 << 40).unwrap());

/// A copy at a limit holds one read snapshot for its whole time. The writer
/// cannot reuse the pages that the snapshot holds, so under a steady load
/// the file grows while the used pages do not. The next copy of the pages
/// holds the free pages too, so it is larger and takes longer.
#[test]
fn a_slow_copy_under_load_grows_the_file_and_the_next_image() {
    let src = tempfile::tempdir().unwrap();
    let images = tempfile::tempdir().unwrap();
    let env = seeded_env(src.path());
    let load = SteadyLoad::start(&env);
    let first = image_len(&env, FAST, &images.path().join("first"));
    let before = Pages::of(&env);
    let slow = CopyRate::Limited(NonZeroU64::new(4 << 20).unwrap());
    let started = Instant::now();
    image_len(&env, slow, &images.path().join("slow"));
    let took = started.elapsed();
    let after = Pages::of(&env);
    let blocks = load.stop();
    let next = image_len(&env, FAST, &images.path().join("next"));
    let compacted = image_len(&env, CopyRate::Unlimited, &images.path().join("compacted"));
    let page = PAGE_SIZE as u64;
    println!(
        "slow copy {took:?}, {blocks} blocks: allocated {} -> {} pages, used {} -> {} pages; \
         image {first} -> {next} bytes, compacted {compacted} bytes",
        before.allocated, after.allocated, before.used, after.used
    );
    assert!(
        after.allocated >= 2 * before.allocated,
        "the file grows: {before:?} -> {after:?}"
    );
    assert!(
        after.used * 2 < after.allocated,
        "most new pages are free: {after:?}"
    );
    assert!(
        next >= after.allocated * page,
        "the next image holds the free pages"
    );
    assert!(
        next >= 2 * first,
        "the next image is larger: {first} -> {next}"
    );
    assert!(
        compacted * 2 < next,
        "a compacted image holds only the used pages: {compacted} of {next}"
    );
}

/// A copy with no limit is compacted and short. Under the same load the
/// file grows little, and the image holds only the used pages.
#[test]
fn a_copy_with_no_limit_under_load_holds_only_the_used_pages() {
    let src = tempfile::tempdir().unwrap();
    let images = tempfile::tempdir().unwrap();
    let env = seeded_env(src.path());
    let load = SteadyLoad::start(&env);
    std::thread::sleep(Duration::from_millis(500));
    let before = Pages::of(&env);
    let image = image_len(&env, CopyRate::Unlimited, &images.path().join("image"));
    let after = Pages::of(&env);
    load.stop();
    let page = PAGE_SIZE as u64;
    println!("copy with no limit: {before:?} -> {after:?}, image {image} bytes");
    assert!(
        after.allocated < before.allocated + before.allocated / 2,
        "the file grows little: {before:?} -> {after:?}"
    );
    assert!(
        image <= (after.used + 16) * page,
        "the image holds only the used pages: {image} bytes, {after:?}"
    );
}
