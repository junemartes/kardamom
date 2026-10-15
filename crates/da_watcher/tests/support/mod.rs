//! The fixture of the L1 watcher tests: one test's cursor file in its own
//! directory, the publisher that sees every epoch across the test's
//! restarts, and the scripted `l1_blocks` stream.

#![allow(dead_code, reason = "each test binary uses a part of the fixture")]

use std::cell::RefCell;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::Duration;

use kardamom_da_watcher::feed::fakes::{ScriptedFeed, ScriptedStream};
use kardamom_da_watcher::publisher::fakes::{InMemoryEpochPublisher, PublisherTap};
use kardamom_da_watcher::{
    CursorError, CursorFile, DaWatcherConfig, L1Cursor, L1ResumeAfter, L1Watcher,
};
use kardamom_types::EpochRecord;
use tokio::sync::watch;

pub(crate) type Watcher = L1Watcher<ScriptedFeed, InMemoryEpochPublisher>;

/// One test's cursor file in its own directory, the publisher that sees
/// every epoch across the test's restarts, and the stream.
pub(crate) struct Rig {
    pub(crate) dir: tempfile::TempDir,
    pub(crate) publisher: InMemoryEpochPublisher,
    pub(crate) stream: ScriptedStream,
    tap: PublisherTap,
    /// Every epoch the tap gave so far: the tap hands each epoch out once.
    seen: RefCell<Vec<EpochRecord>>,
}

impl Rig {
    pub(crate) fn new() -> Self {
        let (publisher, tap) = InMemoryEpochPublisher::new();
        Self {
            dir: tempfile::tempdir().unwrap(),
            publisher,
            stream: ScriptedStream::default(),
            tap,
            seen: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.dir.path().join("l1-cursor")
    }

    pub(crate) fn file(&self) -> CursorFile<L1Cursor> {
        CursorFile::open(self.path()).unwrap()
    }

    pub(crate) fn config(resume_after: Option<u64>) -> DaWatcherConfig {
        DaWatcherConfig {
            tick: Duration::from_millis(5),
            silence: Duration::from_secs(3600),
            resume_after: resume_after
                .map(|b| L1ResumeAfter::from(NonZeroU64::new(b).expect("a test block is not 0"))),
        }
    }

    /// A watcher that sees the next `tips` of the stream, one per pass,
    /// with this rig's cursor file, before its `load_cursor`.
    pub(crate) fn watcher(&self, tips: &[u64], resume_after: Option<u64>) -> Watcher {
        self.stream.tips(tips);
        L1Watcher::new(
            self.publisher.clone(),
            self.stream.feed(),
            Self::config(resume_after),
            Some(self.file()),
        )
    }

    /// The same, after its `load_cursor`. Dropping the watcher releases
    /// the file's lock: that is a restart.
    pub(crate) fn start(
        &self,
        tips: &[u64],
        resume_after: Option<u64>,
    ) -> Result<Watcher, CursorError> {
        let mut w = self.watcher(tips, resume_after);
        w.load_cursor()?;
        Ok(w)
    }

    /// A watcher that follows the sealer through `origins`, after its
    /// `load_cursor`.
    pub(crate) fn follow(&self, tips: &[u64], origins: &watch::Sender<Option<u64>>) -> Watcher {
        let mut w = self.watcher(tips, None).following(origins.subscribe());
        w.load_cursor().unwrap();
        w
    }

    /// The stored cursor, read from the file itself: a live watcher
    /// holds the file's lock, so a second handle cannot open it.
    pub(crate) fn stored(&self) -> Option<L1Cursor> {
        std::fs::read_to_string(self.path())
            .ok()
            .map(|line| line.trim().parse().unwrap())
    }

    pub(crate) fn write(&self, contents: &str) {
        std::fs::write(self.path(), contents).unwrap();
    }

    /// Every epoch published across the rig's watcher lifetimes, in order.
    pub(crate) fn epochs(&self) -> Vec<EpochRecord> {
        self.seen.borrow_mut().extend(self.tap.epochs());
        self.seen.borrow().clone()
    }

    pub(crate) fn published(&self) -> Vec<u64> {
        self.epochs().iter().map(|e| e.l1_number).collect()
    }
}

/// The cursor at block `number` of the scripted chain.
pub(crate) fn at(number: u64) -> L1Cursor {
    L1Cursor {
        number,
        hash: ScriptedStream::hash(number),
    }
}

/// Wait until `cond` holds, for at most ten seconds.
pub(crate) async fn wait(what: &str, mut cond: impl FnMut() -> bool) {
    kardamom_obs::testkit::poll_until(
        what,
        Duration::from_secs(10),
        Duration::from_millis(5),
        async || Ok(cond().then_some(())),
    )
    .await
    .unwrap();
}
