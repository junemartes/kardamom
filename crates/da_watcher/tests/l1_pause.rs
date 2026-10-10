//! A spawned watcher that waits for a record pauses with the L1 follower
//! as its root, and resumes by itself once the record arrives. This
//! binary owns the process lifecycle: no other test here pauses it.

mod support;

use std::time::Duration;

use kardamom_da_watcher::L1Watcher;
use kardamom_obs::lifecycle::process;
use kardamom_types::service::{HaltRef, PauseReason};
use support::{Rig, wait};

#[tokio::test]
async fn a_watcher_without_its_record_pauses_on_the_follower_and_resumes() {
    let rig = Rig::new();
    rig.stream.archive_down(true);
    rig.stream.tips(&[105]);
    let handle = L1Watcher::spawn(
        rig.publisher.clone(),
        rig.stream.feed(),
        Rig::config(Some(100)),
        Some(rig.file()),
    );

    wait("the pause on the follower", || {
        process()
            .slots()
            .pause
            .is_some_and(|p| p.reason == PauseReason::Upstream(HaltRef::l1_blocks_silent()))
    })
    .await;
    assert!(rig.published().is_empty(), "no epoch without the record");

    rig.stream.archive_down(false);
    wait("the resume", || rig.published().len() == 5).await;
    assert_eq!(rig.published(), (101..=105).collect::<Vec<_>>());
    wait("the end of the pause", || process().slots().pause.is_none()).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.join().await.unwrap();
}
