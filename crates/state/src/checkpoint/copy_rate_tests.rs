use std::fs::File;
use std::io::Write;
use std::num::NonZeroU64;
use std::os::unix::fs::MetadataExt;
use std::time::{Duration, Instant};

use alloy_primitives::Address;

use super::super::tests::{commit_blocks_with_slots, seeded_env};
use super::super::{BlockRange, create_checkpoint_at, restore_best_checkpoint};
use super::{CopyRate, RateSink};
use crate::env::StateEnvBuilder;
use crate::integrity::deep_compare;

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
