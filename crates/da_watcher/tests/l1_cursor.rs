//! The L1 watcher's durable cursor: a restart resumes after the stored
//! block and checks the parent link against the stored hash; a missing
//! file falls back to the first record; a file that does not read halts;
//! `--l1-resume-after` overrides the file.
//!
//! One test here raises the process halt. No other test in this binary
//! spawns a watcher, so no other test raises or clears it.

mod support;

use alloy_primitives::B256;
use kardamom_da_watcher::feed::fakes::ScriptedFeed;
use kardamom_da_watcher::publisher::fakes::InMemoryEpochPublisher;
use kardamom_da_watcher::{CursorError, L1Cursor, L1CursorError, L1Watcher, MonitorError};
use kardamom_obs::halt::{self, Clears, HaltCause};
use support::{Rig, at, wait};

#[test]
fn a_cursor_line_round_trips_and_a_bad_line_is_refused() {
    let cursor = L1Cursor {
        number: 1631,
        hash: B256::repeat_byte(0xab),
    };
    let line = cursor.to_string();
    assert_eq!(line, format!("1631 0x{}", "ab".repeat(32)));
    assert_eq!(line.parse::<L1Cursor>().unwrap(), cursor);
    assert_eq!("1631".parse::<L1Cursor>(), Err(L1CursorError::Fields(1)));
    assert!(matches!(
        format!("x {}", cursor.hash).parse::<L1Cursor>(),
        Err(L1CursorError::Number(_))
    ));
    assert!(matches!(
        "1631 0xabc".parse::<L1Cursor>(),
        Err(L1CursorError::Hash(_))
    ));
}

/// The first start stores its seed at once, so a restart before the first
/// publish does not move to a newer tip. The restart then publishes every
/// block after the stored one: no gap, no repeat.
#[tokio::test]
async fn a_restart_resumes_after_the_stored_block() {
    let rig = Rig::new();
    {
        let mut w = rig.start(&[100, 103], None).unwrap();
        assert_eq!(w.process_once().await.unwrap(), 0, "the first tick seeds");
        assert_eq!(rig.stored(), Some(at(100)), "the seed is stored");
        assert_eq!(w.process_once().await.unwrap(), 3);
        assert_eq!(rig.stored(), Some(at(103)));
    }

    let mut w = rig.start(&[106], None).unwrap();
    assert_eq!(w.cursor(), Some(103));
    assert_eq!(w.process_once().await.unwrap(), 3);

    assert_eq!(rig.published(), vec![101, 102, 103, 104, 105, 106]);
    assert_eq!(rig.stored(), Some(at(106)));
}

/// With no file, the watcher anchors at the first record of the stream,
/// and stores the anchor.
#[tokio::test]
async fn a_missing_file_falls_back_to_the_first_record() {
    let rig = Rig::new();
    let mut w = rig.start(&[500], None).unwrap();
    assert_eq!(w.cursor(), None);
    assert_eq!(w.process_once().await.unwrap(), 0);
    assert_eq!(w.cursor(), Some(500));
    assert!(
        rig.published().is_empty(),
        "the blocks before the tip are skipped"
    );
    assert_eq!(rig.stored(), Some(at(500)));
}

/// A file that does not parse, and a file that cannot be read, are both a
/// halt that an operator clears. Neither is ever a silent start at the tip.
#[test]
fn a_corrupt_or_unreadable_file_is_an_operator_halt() {
    let rig = Rig::new();
    rig.write("not a cursor\n");
    let err = rig.start(&[], None).err().expect("a corrupt file");
    assert!(matches!(err, CursorError::Corrupt { .. }), "got {err:?}");
    let halt = L1Watcher::<ScriptedFeed, InMemoryEpochPublisher>::cursor_halt(&err);
    assert_eq!(halt.cause, HaltCause::L1CursorUnreadable);
    assert_eq!(halt.clears, Clears::Operator);
    assert_eq!(
        halt.recovery.runbook(),
        "docs/runbooks/l1_cursor_unreadable.md"
    );

    let unreadable = Rig::new();
    std::fs::create_dir(unreadable.path()).unwrap();
    let err = unreadable
        .start(&[], None)
        .err()
        .expect("a directory is not a cursor file");
    assert!(matches!(err, CursorError::Io { .. }), "got {err:?}");
}

/// A stored hash that the chain does not hold is a chain break at the
/// first block after it. The watcher publishes nothing, keeps its cursor,
/// and keeps the file as it is.
#[tokio::test]
async fn a_stored_hash_that_the_chain_does_not_hold_halts() {
    let rig = Rig::new();
    let stored = L1Cursor {
        number: 50,
        hash: B256::repeat_byte(0xEE),
    };
    rig.write(&format!("{stored}\n"));
    let mut w = rig.start(&[52, 52], None).unwrap();

    let err = w.process_once().await.unwrap_err();
    assert!(
        matches!(err, MonitorError::ChainBreak { number: 51, expected, .. } if expected == stored.hash),
        "got {err}"
    );
    assert_eq!(err.halt().map(|h| h.cause), Some(HaltCause::L1ChainBreak));
    // The watcher reads the block again from the archives one tick later.
    tokio::time::sleep(Rig::config(None).tick * 2).await;
    let err = w.process_once().await.unwrap_err();
    assert!(matches!(err, MonitorError::ChainBreak { number: 51, .. }));

    assert!(rig.published().is_empty());
    assert_eq!(w.cursor(), Some(50));
    assert_eq!(rig.stored(), Some(stored));
}

/// `--l1-resume-after` wins over the file: the runbook sets it after a
/// seed, when the file holds a block of the old chain. The flag's block
/// is stored on the first tick. A file that does not parse is not read at
/// all, so it does not halt.
#[tokio::test]
async fn the_resume_flag_overrides_the_file() {
    let rig = Rig::new();
    rig.write(&format!("{}\n", at(100)));
    let mut w = rig.start(&[45], Some(41)).unwrap();
    assert_eq!(w.cursor(), Some(41));
    assert_eq!(w.process_once().await.unwrap(), 4);
    assert_eq!(rig.published(), vec![42, 43, 44, 45]);
    assert_eq!(rig.stored(), Some(at(45)));

    let corrupt = Rig::new();
    corrupt.write("garbage");
    let mut w = corrupt.start(&[42], Some(41)).unwrap();
    assert_eq!(w.process_once().await.unwrap(), 1);
    assert_eq!(corrupt.stored(), Some(at(42)));
}

/// The publish-then-persist window: a file one pass behind makes the
/// restart publish the same blocks again. The epochs are byte-identical,
/// with the same canonical id, so the sealer's first-seen dedup drops
/// them.
#[tokio::test]
async fn a_stale_file_publishes_identical_epochs_again() {
    let rig = Rig::new();
    {
        let mut w = rig.start(&[100, 103], None).unwrap();
        w.process_once().await.unwrap();
        w.process_once().await.unwrap();
    }
    // The process died after the publish of 101..=103, before the write.
    rig.write(&format!("{}\n", at(100)));
    let mut w = rig.start(&[103], None).unwrap();
    assert_eq!(w.process_once().await.unwrap(), 3);

    let epochs = rig.epochs();
    let (first, again) = epochs.split_at(3);
    assert_eq!(first, again);
    assert!(
        first
            .iter()
            .zip(again)
            .all(|(a, b)| a.canonical_id() == b.canonical_id())
    );
}

/// The spawned watcher halts on a corrupt file and stays up. It publishes
/// nothing until the operator fixes the file and clears the halt; then it
/// reads the file again and resumes after the stored block.
#[tokio::test]
async fn a_spawned_watcher_holds_the_halt_until_the_operator_clears_it() {
    let rig = Rig::new();
    rig.write("1631\n");
    rig.stream.tips(&[52]);
    let handle = L1Watcher::spawn(
        rig.publisher.clone(),
        rig.stream.feed(),
        Rig::config(None),
        Some(rig.file()),
    );

    wait("the cursor halt", || {
        halt::current().is_some_and(|h| h.cause == HaltCause::L1CursorUnreadable)
    })
    .await;
    assert!(!handle.task.is_finished(), "a halt is not an exit");
    assert!(rig.published().is_empty());

    rig.write(&format!("{}\n", at(50)));
    halt::clear();
    wait("the resume", || rig.published().len() == 2).await;

    assert_eq!(rig.published(), vec![51, 52]);
    assert!(halt::current().is_none());
    handle.join().await.unwrap();
    assert_eq!(rig.stored(), Some(at(52)));
}
