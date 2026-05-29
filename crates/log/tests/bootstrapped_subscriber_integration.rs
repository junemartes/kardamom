//! Integration coverage for the bootstrap state machine wired against the
//! `FakeBus` archive shim.
//!
//! The per-handle `open_bootstrapped` constructors in `aeron_live` need a
//! live Aeron daemon to exercise end-to-end (covered by `docker_e2e.rs`).
//! This file pins the underlying state machine: a late-joining subscriber
//! that consumes via the archive view sees pre-attach frames in order,
//! matching the `silent-drop-on-attach` bug fix described in
//! `docs/superpowers/specs/2026-05-29-aeron-archive-bootstrap-design.md`.

#![cfg(feature = "testing")]

use std::time::Duration;

use kardamom_log::bootstrap::{BootstrapDriver, ReplayWindow, StreamPosition};
use kardamom_log::testing::{FakeBus, FakeLiveImage};

#[test]
fn late_joiner_sees_pre_attach_frames_via_fake_archive() {
    let bus = FakeBus::new();
    let ch = "aeron:ipc?alias=test";
    let stream = 1;

    bus.publish_raw(ch, stream, StreamPosition(10), vec![1]);
    bus.publish_raw(ch, stream, StreamPosition(20), vec![2]);
    bus.publish_raw(ch, stream, StreamPosition(30), vec![3]);

    let archive = bus.archive_for(ch, stream);
    // Live image attached at the head of the recorded history; no live
    // frames available yet. Window sized so the anchor lands at-or-above
    // the earliest archived position (= 10).
    let mut live = FakeLiveImage::empty_at(StreamPosition(30));
    let mut driver = BootstrapDriver::new(
        &mut live,
        &archive,
        ReplayWindow::PositionBytes { bytes: 20 },
        Duration::from_secs(1),
    );

    let mut seen = vec![];
    driver.run(|_, b| seen.push(b[0])).unwrap();
    assert_eq!(seen, vec![1, 2, 3]);
}
