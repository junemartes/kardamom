//! The L1 watcher follows the sealer's commit. The boundaries' L1 origin
//! confirms the published epochs; the cursor file holds the confirmed
//! origin; the epochs no boundary confirms are published again; a start
//! resumes after the sealer's origin. Each test covers one race between
//! a publish and its commit.
//!
//! No test here raises the process halt.

mod support;

use kardamom_cluster_adapter::gateway::fakes::FakeEgress;
use kardamom_cluster_adapter::wire::{EGRESS_KIND_ORIGIN_GAP, encode_egress_boundary};
use kardamom_da_watcher::{BoundaryFeed, START_WAIT};
use kardamom_types::epoch_delivery::{PUBLISH_WINDOW, REPUBLISH_AFTER};
use support::{Rig, at, source, wait};
use tokio::sync::watch;

/// The sealer's origin feed, before its first boundary.
fn sealer() -> watch::Sender<Option<u64>> {
    watch::channel(None).0
}

/// The published epochs' canonical ids are the same for every copy, so
/// the sealer's first-seen dedup drops a copy.
fn ids(rig: &Rig) -> Vec<alloy_primitives::B256> {
    rig.epochs()
        .iter()
        .map(kardamom_types::EpochRecord::canonical_id)
        .collect()
}

/// A boundary confirms the epochs up to its origin. The file holds the
/// confirmed origin, not the last published block, from the next pass.
#[tokio::test]
async fn the_file_holds_the_confirmed_origin_not_the_last_publish() {
    let rig = Rig::new();
    let origins = sealer();
    let mut w = rig.follow(source(&[100, 103]), &origins);
    w.start_at(Some(0));
    assert_eq!(w.process_once().await.unwrap(), 0, "the first tick seeds");
    assert_eq!(w.process_once().await.unwrap(), 3);
    assert_eq!(w.cursor(), Some(103));
    assert_eq!(rig.stored(), Some(at(100)), "nothing is confirmed yet");

    w.on_sealer_origin(102);
    assert_eq!(w.confirmed(), Some(at(102)));
    assert!(w.process_once().await.is_err(), "no new tip");
    assert_eq!(rig.stored(), Some(at(102)));
}

/// The watcher dies after it published 101..=103 and before the sealer
/// committed 102. The file holds 101, and the restart resumes after the
/// sealer's origin 101: it publishes 102 and 103 again. No epoch is lost.
#[tokio::test]
async fn a_restart_in_the_publish_to_commit_window_loses_no_epoch() {
    let rig = Rig::new();
    let origins = sealer();
    {
        let mut w = rig.follow(source(&[100, 103]), &origins);
        w.start_at(Some(0));
        w.process_once().await.unwrap();
        w.process_once().await.unwrap();
        w.on_sealer_origin(101);
        assert!(w.process_once().await.is_err());
        assert_eq!(rig.stored(), Some(at(101)));
    }

    let mut w = rig.follow(source(&[104]), &origins);
    assert_eq!(w.confirmed(), Some(at(101)), "the file's block");
    w.start_at(Some(101));
    assert_eq!(w.process_once().await.unwrap(), 3);
    assert_eq!(rig.published(), [101, 102, 103, 102, 103, 104]);
    let ids = ids(&rig);
    assert_eq!(ids[1..3], ids[3..5], "the copies are byte-identical");
}

/// No boundary confirms 102 for the re-publish timeout: the watcher
/// publishes 102 and 103 again, in order, at most once for each period.
/// This heals a sequencer that restarted and lost its queue.
#[tokio::test(start_paused = true)]
async fn epochs_no_boundary_confirms_are_published_again_after_the_timeout() {
    let rig = Rig::new();
    let origins = sealer();
    let mut w = rig.follow(source(&[100, 103]), &origins);
    w.start_at(Some(0));
    w.process_once().await.unwrap();
    w.process_once().await.unwrap();
    w.on_sealer_origin(101);

    tokio::time::advance(REPUBLISH_AFTER / 2).await;
    assert_eq!(w.republish_due().unwrap(), 0, "not due yet");
    tokio::time::advance(REPUBLISH_AFTER / 2).await;
    assert_eq!(w.republish_due().unwrap(), 2);
    assert_eq!(w.republish_due().unwrap(), 0, "once for each period");
    tokio::time::advance(REPUBLISH_AFTER).await;
    assert_eq!(w.republish_due().unwrap(), 2);
    assert_eq!(rig.published(), [101, 102, 103, 102, 103, 102, 103]);

    w.on_sealer_origin(103);
    tokio::time::advance(REPUBLISH_AFTER).await;
    assert_eq!(w.republish_due().unwrap(), 0, "every epoch is confirmed");
}

/// A sealer fleet rebuilt from a seed at 100 sends boundaries below the
/// watcher's confirmed 108. The watcher follows 100: it reads 100's hash,
/// and publishes every block after it.
#[tokio::test]
async fn an_origin_that_moves_back_is_followed() {
    let rig = Rig::new();
    rig.write(&format!("{}\n", at(108)));
    let origins = sealer();
    let mut w = rig.follow(source(&[110, 103]), &origins);
    w.start_at(None);
    assert_eq!(w.process_once().await.unwrap(), 2);

    w.on_sealer_origin(100);
    assert_eq!(w.cursor(), Some(100), "anchored at the sealer's origin");
    assert_eq!(w.process_once().await.unwrap(), 3);
    assert_eq!(rig.published(), [109, 110, 101, 102, 103]);
    assert_eq!(rig.stored(), Some(at(100)), "the hash read from L1");
}

/// A stored cursor ahead of the sealer's origin at start (a seed at an
/// older origin): the start resumes after the sealer's origin.
#[tokio::test]
async fn a_start_resumes_after_the_sealer_origin_below_the_file() {
    let rig = Rig::new();
    rig.write(&format!("{}\n", at(120)));
    let origins = sealer();
    let mut w = rig.follow(source(&[102]), &origins);
    w.start_at(Some(100));
    assert_eq!(w.process_once().await.unwrap(), 2);
    assert_eq!(rig.published(), [101, 102]);
    assert_eq!(rig.stored(), Some(at(100)));
}

/// Another watcher published and the sealer confirmed past this watcher's
/// head. This watcher anchors at the sealer's origin: it never publishes
/// the confirmed epochs again, and it resumes after the origin. A stored
/// cursor behind the sealer's origin at start does the same.
#[tokio::test(start_paused = true)]
async fn an_origin_ahead_of_the_head_is_followed_without_a_republish() {
    let rig = Rig::new();
    let origins = sealer();
    let mut w = rig.follow(source(&[100, 103, 112]), &origins);
    w.start_at(Some(0));
    w.process_once().await.unwrap();
    w.process_once().await.unwrap();

    w.on_sealer_origin(110);
    tokio::time::advance(REPUBLISH_AFTER).await;
    assert_eq!(
        w.republish_due().unwrap(),
        0,
        "the confirmed epochs wait no more"
    );
    assert_eq!(w.process_once().await.unwrap(), 2);
    assert_eq!(rig.published(), [101, 102, 103, 111, 112]);

    let behind = Rig::new();
    behind.write(&format!("{}\n", at(90)));
    let mut w = behind.follow(source(&[101]), &origins);
    w.start_at(Some(100));
    assert_eq!(w.process_once().await.unwrap(), 1);
    assert_eq!(behind.published(), [101]);
}

/// Two watchers follow the same sealer. They publish the same epochs,
/// byte for byte, so the sealer orders one copy. The boundary that
/// confirms them empties both windows: neither publishes them again.
#[tokio::test(start_paused = true)]
async fn two_watchers_publish_identical_copies_and_follow_one_origin() {
    let origins = sealer();
    let (a, b) = (Rig::new(), Rig::new());
    let mut wa = a.follow(source(&[102]), &origins);
    let mut wb = b.follow(source(&[101, 102]), &origins);
    wa.start_at(Some(100));
    wb.start_at(Some(100));
    assert_eq!(wa.process_once().await.unwrap(), 2);
    assert_eq!(wb.process_once().await.unwrap(), 1);
    assert_eq!(wb.process_once().await.unwrap(), 1);
    assert_eq!(ids(&a), ids(&b));

    wa.on_sealer_origin(102);
    wb.on_sealer_origin(102);
    tokio::time::advance(REPUBLISH_AFTER).await;
    assert_eq!(wa.republish_due().unwrap(), 0);
    assert_eq!(wb.republish_due().unwrap(), 0);
}

/// With no boundary, the watcher publishes up to the window's bound and
/// stops. It drops nothing: a boundary frees the window, and the next
/// pass publishes the blocks after the bound.
#[tokio::test]
async fn a_full_window_stops_new_publishes_and_drops_nothing() {
    let rig = Rig::new();
    let origins = sealer();
    let bound = u64::try_from(PUBLISH_WINDOW.get()).unwrap();
    let tip = 1_000 + bound + 10;
    let mut w = rig.follow(source(&[1_000, tip, tip, tip]), &origins);
    w.start_at(Some(0));
    w.process_once().await.unwrap();
    assert_eq!(w.process_once().await.unwrap(), PUBLISH_WINDOW.get());
    assert_eq!(w.process_once().await.unwrap(), 0, "the window is full");

    w.on_sealer_origin(1_005);
    assert_eq!(w.process_once().await.unwrap(), 5);
    let published = rig.published();
    assert_eq!(published.first(), Some(&1_001));
    assert!(published.windows(2).all(|p| p[1] == p[0] + 1), "no hole");
}

/// A spawned watcher waits for the first boundary, and resumes after
/// the sealer's origin, not after the file's block.
#[tokio::test]
async fn a_spawned_watcher_resumes_after_the_first_boundary() {
    let rig = Rig::new();
    rig.write(&format!("{}\n", at(120)));
    let origins = sealer();
    let w = rig.follow(source(&[102]), &origins);
    let handle = w.start();
    origins.send_replace(Some(100));
    wait("the resume", || rig.published().len() == 2).await;
    assert_eq!(rig.published(), [101, 102]);
    handle.join().await.unwrap();
    assert_eq!(rig.stored(), Some(at(100)));
}

/// The boundary stream is silent at start (the sealer cluster is down):
/// after the start wait the watcher resumes from the file. It follows the
/// first boundary that arrives later.
#[tokio::test(start_paused = true)]
async fn a_silent_sealer_at_start_falls_back_to_the_file() {
    let rig = Rig::new();
    rig.write(&format!("{}\n", at(100)));
    let origins = sealer();
    let w = rig.follow(source(&[102]), &origins);
    let handle = w.start();
    tokio::time::sleep(START_WAIT).await;
    wait("the fallback", || rig.published().len() == 2).await;
    assert_eq!(rig.published(), [101, 102]);

    origins.send_replace(Some(102));
    wait("the confirm", || rig.stored() == Some(at(102))).await;
    handle.join().await.unwrap();
}

/// The boundary feed sends each new origin a boundary carries, ignores
/// every other frame, and ends when the egress closes.
#[test]
fn the_boundary_feed_sends_each_new_origin() {
    let egress = FakeEgress::new();
    let (feed, mut origins) = BoundaryFeed::new(egress.clone());
    assert_eq!(*origins.borrow(), None);
    egress.push(encode_egress_boundary(7, 70, 0, 100));
    let gap = [103_u64, 101].iter().flat_map(|n| n.to_le_bytes());
    egress.push(std::iter::once(EGRESS_KIND_ORIGIN_GAP).chain(gap).collect());
    egress.push(encode_egress_boundary(8, 71, 0, 100));
    egress.push(encode_egress_boundary(9, 72, 0, 101));
    egress.close();
    feed.run();
    assert_eq!(*origins.borrow_and_update(), Some(101));
}
