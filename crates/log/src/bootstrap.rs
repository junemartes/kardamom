//! Archive-backed bootstrap for Aeron subscribers.
//!
//! See `docs/superpowers/specs/2026-05-29-aeron-archive-bootstrap-design.md`.

use std::time::Duration;

/// Policy controlling how a subscription is brought online.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapPolicy {
    /// Replay from a recent archive position, merge into the live image,
    /// then continue live. Handlers MUST tolerate duplicates from the
    /// replay/live overlap.
    Replay {
        window: ReplayWindow,
        catch_up_timeout: Duration,
    },
    /// Wait for the live image to attach. No history is replayed.
    Connected { timeout: Duration },
}

/// How far back from the archive head a replay should start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayWindow {
    /// For `tx_ordering`: anchor at the last `BoundaryStart` whose archived
    /// position is <= `latest_recording_position`, then walk back `safety`
    /// additional boundaries.
    BlockBoundaries { safety: u32 },
    /// For `tx_data[i]` and `tx_deposits`: anchor at
    /// `max(0, latest_recording_position - bytes)`. Block-boundary alignment
    /// is not a property of these streams.
    PositionBytes { bytes: u64 },
}

/// Errors surfaced by bootstrap. Each is fatal for the affected subscription.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("anchor {anchor} below archive min {archive_min}")]
    AnchorBelowArchiveHead { anchor: i64, archive_min: i64 },
    #[error("start_replay failed: {0}")]
    ReplayFailed(String),
    #[error("catch-up did not complete within configured timeout")]
    CatchUpTimeout,
    #[error("live image did not attach within configured timeout")]
    ConnectTimeout,
    #[error("archive client error: {0}")]
    ArchiveClient(String),
}

impl BootstrapPolicy {
    /// Default policy for `tx_data[i]` subscribers: replay 16 MiB back,
    /// catch up within 30 s.
    pub fn default_tx_data() -> Self {
        Self::Replay {
            window: ReplayWindow::PositionBytes { bytes: 16 * 1024 * 1024 },
            catch_up_timeout: Duration::from_secs(30),
        }
    }

    /// Default for `tx_ordering`: 4 block boundaries back with no extra
    /// safety, 30 s catch-up budget.
    pub fn default_tx_ordering() -> Self {
        Self::Replay {
            window: ReplayWindow::BlockBoundaries { safety: 4 },
            catch_up_timeout: Duration::from_secs(30),
        }
    }

    /// Default for `tx_deposits`: 4 MiB back, 30 s catch-up.
    pub fn default_tx_deposits() -> Self {
        Self::Replay {
            window: ReplayWindow::PositionBytes { bytes: 4 * 1024 * 1024 },
            catch_up_timeout: Duration::from_secs(30),
        }
    }

    /// Default for read-only "tail" streams (`tx_receipts`, `tx_errors`,
    /// watermarks, block boundaries): wait up to 10 s for the live image
    /// to attach.
    pub fn default_connected() -> Self {
        Self::Connected { timeout: Duration::from_secs(10) }
    }
}

/// Position of a frame on its Aeron stream (or replay session). Newtype so
/// the state-machine code can't accidentally mix it with other integers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamPosition(pub i64);

/// Source of frames for the state machine — abstracts over a live Aeron
/// subscription so we can drive the state machine against a `FakeBus` in
/// unit tests.
pub trait LiveImage: Send {
    /// Current end position of the live image. None if the image has not
    /// attached yet.
    fn current_position(&self) -> Option<StreamPosition>;
    /// Position at which this image first became attached. None until then.
    fn join_position(&self) -> Option<StreamPosition>;
    /// Pull all currently-available frames, calling `f` for each.
    fn poll(&mut self, f: &mut dyn FnMut(StreamPosition, Vec<u8>));
}

/// Source of replayed frames — abstracts over a rusteron replay session.
pub trait ReplaySource: Send {
    /// Current position of the replay cursor. Equal to anchor at start;
    /// advances as `poll` is called.
    fn current_position(&self) -> StreamPosition;
    /// Pull all currently-available frames, calling `f` for each.
    fn poll(&mut self, f: &mut dyn FnMut(StreamPosition, Vec<u8>));
    /// Stop the replay session.
    fn stop(&mut self);
}

/// Bootstrap-time queries against the archive: position lookup and replay
/// session creation. The state machine calls these once at startup; live
/// frames flow through `LiveImage` and `ReplaySource`.
pub trait ArchiveSource {
    /// `latest_recording_position` for the recording mapped to this stream.
    /// Returns `None` if no recording exists yet (cold-start fallback).
    fn latest_recording_position(&self) -> Result<Option<StreamPosition>, BootstrapError>;
    /// Earliest position still archived for this stream (used to detect
    /// `AnchorBelowArchiveHead`).
    fn earliest_recording_position(&self) -> Result<StreamPosition, BootstrapError>;
    /// Walk back from `at` to the last `BoundaryStart` <= `at`, then back
    /// `safety` additional boundaries. Returns `None` if no boundaries
    /// exist before `at`. Only meaningful for `tx_ordering`.
    fn last_block_boundary(
        &self,
        at: StreamPosition,
        safety: u32,
    ) -> Result<Option<StreamPosition>, BootstrapError>;
    /// Start a replay anchored at `start` and return the session.
    fn start_replay(&self, start: StreamPosition) -> Result<Box<dyn ReplaySource>, BootstrapError>;
}

use std::collections::VecDeque;
use std::time::Instant;

/// Driver loop for one bootstrapped subscription. The state machine pulls
/// from `replay` until `replay.current_position() >= live.current_position()`,
/// at which point it stops the replay and switches to `live` exclusively.
/// All frames (replay + live) flow through `on_frame`.
///
/// Returns `Ok(())` once caught up. Returns an error if `catch_up_timeout`
/// elapses with replay still behind live.
pub struct BootstrapDriver<'a> {
    live: &'a mut dyn LiveImage,
    archive: &'a dyn ArchiveSource,
    window: ReplayWindow,
    catch_up_timeout: Duration,
    /// Polled by tests; production passes `Instant::now`.
    now: Box<dyn Fn() -> Instant>,
}

impl<'a> BootstrapDriver<'a> {
    pub fn new(
        live: &'a mut dyn LiveImage,
        archive: &'a dyn ArchiveSource,
        window: ReplayWindow,
        catch_up_timeout: Duration,
    ) -> Self {
        Self {
            live,
            archive,
            window,
            catch_up_timeout,
            now: Box::new(|| Instant::now()),
        }
    }

    #[cfg(test)]
    pub fn with_clock(mut self, clock: Box<dyn Fn() -> Instant>) -> Self {
        self.now = clock;
        self
    }

    /// Run a single bootstrap pass: open replay (if applicable), drain
    /// replay until caught up, switch to live. `on_frame` is called for
    /// every frame in order (replay frames first, then live frames).
    pub fn run(
        &mut self,
        mut on_frame: impl FnMut(StreamPosition, Vec<u8>),
    ) -> Result<(), BootstrapError> {
        // 1. Resolve anchor.
        let latest = self.archive.latest_recording_position()?;
        let Some(latest) = latest else {
            // Cold start: no recording yet. Drain whatever live has and
            // declare caught-up immediately.
            self.live.poll(&mut |p, b| on_frame(p, b));
            return Ok(());
        };
        let anchor = self.resolve_anchor(latest)?;

        // 2. Check anchor against archive head.
        let archive_min = self.archive.earliest_recording_position()?;
        if anchor < archive_min {
            return Err(BootstrapError::AnchorBelowArchiveHead {
                anchor: anchor.0,
                archive_min: archive_min.0,
            });
        }

        // 3. Start replay.
        let mut replay = self.archive.start_replay(anchor)?;

        // 4. Drain replay until caught up to live. Buffer live frames as
        //    we go so we don't lose them, and emit them after replay
        //    completes.
        let start = (self.now)();
        let mut live_buffer: VecDeque<(StreamPosition, Vec<u8>)> = VecDeque::new();
        loop {
            replay.poll(&mut |p, b| on_frame(p, b));
            self.live.poll(&mut |p, b| live_buffer.push_back((p, b)));

            let Some(live_pos) = self.live.current_position() else {
                // Live image not attached yet — keep pulling replay.
                if (self.now)().duration_since(start) > self.catch_up_timeout {
                    return Err(BootstrapError::CatchUpTimeout);
                }
                continue;
            };

            if replay.current_position() >= live_pos {
                break;
            }

            if (self.now)().duration_since(start) > self.catch_up_timeout {
                return Err(BootstrapError::CatchUpTimeout);
            }
        }

        replay.stop();

        // 5. Replay any buffered live frames in order, then continue with
        //    the live image directly.
        while let Some((p, b)) = live_buffer.pop_front() {
            on_frame(p, b);
        }
        self.live.poll(&mut |p, b| on_frame(p, b));

        Ok(())
    }

    fn resolve_anchor(
        &self,
        latest: StreamPosition,
    ) -> Result<StreamPosition, BootstrapError> {
        match self.window {
            ReplayWindow::BlockBoundaries { safety } => {
                let boundary = self.archive.last_block_boundary(latest, safety)?;
                Ok(boundary.unwrap_or(StreamPosition(0)))
            }
            ReplayWindow::PositionBytes { bytes } => {
                let raw = latest.0 - bytes as i64;
                Ok(StreamPosition(raw.max(0)))
            }
        }
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    struct MockLive {
        join: Option<StreamPosition>,
        current: Option<StreamPosition>,
        queue: VecDeque<(StreamPosition, Vec<u8>)>,
    }
    impl LiveImage for MockLive {
        fn current_position(&self) -> Option<StreamPosition> { self.current }
        fn join_position(&self) -> Option<StreamPosition> { self.join }
        fn poll(&mut self, f: &mut dyn FnMut(StreamPosition, Vec<u8>)) {
            while let Some((p, b)) = self.queue.pop_front() { f(p, b); }
        }
    }

    struct MockReplay {
        pos: StreamPosition,
        queue: VecDeque<(StreamPosition, Vec<u8>)>,
        stopped: Arc<Mutex<bool>>,
    }
    impl ReplaySource for MockReplay {
        fn current_position(&self) -> StreamPosition { self.pos }
        fn poll(&mut self, f: &mut dyn FnMut(StreamPosition, Vec<u8>)) {
            while let Some((p, b)) = self.queue.pop_front() {
                self.pos = p;
                f(p, b);
            }
        }
        fn stop(&mut self) { *self.stopped.lock().unwrap() = true; }
    }

    struct MockArchive {
        latest: Option<StreamPosition>,
        earliest: StreamPosition,
        boundary_at: Option<StreamPosition>,
        replay_frames: Mutex<Option<VecDeque<(StreamPosition, Vec<u8>)>>>,
        replay_stopped: Arc<Mutex<bool>>,
    }
    impl ArchiveSource for MockArchive {
        fn latest_recording_position(&self) -> Result<Option<StreamPosition>, BootstrapError> {
            Ok(self.latest)
        }
        fn earliest_recording_position(&self) -> Result<StreamPosition, BootstrapError> {
            Ok(self.earliest)
        }
        fn last_block_boundary(
            &self,
            _at: StreamPosition,
            _safety: u32,
        ) -> Result<Option<StreamPosition>, BootstrapError> {
            Ok(self.boundary_at)
        }
        fn start_replay(
            &self,
            _start: StreamPosition,
        ) -> Result<Box<dyn ReplaySource>, BootstrapError> {
            let queue = self.replay_frames.lock().unwrap().take().unwrap_or_default();
            Ok(Box::new(MockReplay {
                pos: StreamPosition(0),
                queue,
                stopped: self.replay_stopped.clone(),
            }))
        }
    }

    #[test]
    fn cold_start_when_no_recording_returns_immediately() {
        let mut live = MockLive {
            join: Some(StreamPosition(0)),
            current: Some(StreamPosition(0)),
            queue: VecDeque::new(),
        };
        let archive = MockArchive {
            latest: None,
            earliest: StreamPosition(0),
            boundary_at: None,
            replay_frames: Mutex::new(None),
            replay_stopped: Arc::new(Mutex::new(false)),
        };
        let mut driver = BootstrapDriver::new(
            &mut live,
            &archive,
            ReplayWindow::PositionBytes { bytes: 1024 },
            Duration::from_secs(1),
        );
        let mut seen: Vec<StreamPosition> = vec![];
        driver.run(|p, _| seen.push(p)).unwrap();
        assert!(seen.is_empty());
    }

    #[test]
    fn warm_replay_drains_then_switches_to_live() {
        let mut live = MockLive {
            join: Some(StreamPosition(100)),
            current: Some(StreamPosition(100)),
            queue: VecDeque::from(vec![(StreamPosition(100), vec![3])]),
        };
        let stopped = Arc::new(Mutex::new(false));
        let archive = MockArchive {
            latest: Some(StreamPosition(80)),
            earliest: StreamPosition(0),
            boundary_at: None,
            replay_frames: Mutex::new(Some(VecDeque::from(vec![
                (StreamPosition(50), vec![1]),
                (StreamPosition(100), vec![2]),
            ]))),
            replay_stopped: stopped.clone(),
        };
        let mut driver = BootstrapDriver::new(
            &mut live,
            &archive,
            ReplayWindow::PositionBytes { bytes: 1024 },
            Duration::from_secs(1),
        );
        let mut seen: Vec<u8> = vec![];
        driver.run(|_, b| seen.push(b[0])).unwrap();
        assert_eq!(seen, vec![1, 2, 3]);
        assert!(*stopped.lock().unwrap());
    }

    #[test]
    fn anchor_below_archive_min_is_fatal() {
        let mut live = MockLive {
            join: Some(StreamPosition(100)),
            current: Some(StreamPosition(100)),
            queue: VecDeque::new(),
        };
        let archive = MockArchive {
            latest: Some(StreamPosition(100)),
            earliest: StreamPosition(50),
            boundary_at: None,
            replay_frames: Mutex::new(Some(VecDeque::new())),
            replay_stopped: Arc::new(Mutex::new(false)),
        };
        let mut driver = BootstrapDriver::new(
            &mut live,
            &archive,
            ReplayWindow::PositionBytes { bytes: 1024 * 1024 }, // huge → anchor=0
            Duration::from_secs(1),
        );
        match driver.run(|_, _| {}) {
            Err(BootstrapError::AnchorBelowArchiveHead { .. }) => {}
            other => panic!("expected AnchorBelowArchiveHead, got {other:?}"),
        }
    }

    #[test]
    fn catch_up_timeout_when_replay_never_reaches_live() {
        let mut live = MockLive {
            join: Some(StreamPosition(1_000_000)),
            current: Some(StreamPosition(1_000_000)),
            queue: VecDeque::new(),
        };
        let archive = MockArchive {
            latest: Some(StreamPosition(100)),
            earliest: StreamPosition(0),
            boundary_at: None,
            replay_frames: Mutex::new(Some(VecDeque::new())), // replay stuck at 0
            replay_stopped: Arc::new(Mutex::new(false)),
        };
        let t0 = Instant::now();
        let clock_calls = Rc::new(RefCell::new(0usize));
        let calls_clone = clock_calls.clone();
        let mut driver = BootstrapDriver::new(
            &mut live,
            &archive,
            ReplayWindow::PositionBytes { bytes: 1024 },
            Duration::from_millis(5),
        )
        .with_clock(Box::new(move || {
            let mut c = calls_clone.borrow_mut();
            *c += 1;
            t0 + Duration::from_millis(*c as u64 * 10)
        }));
        let res = driver.run(|_, _| {});
        match res {
            Err(BootstrapError::CatchUpTimeout) => {}
            other => panic!("expected CatchUpTimeout, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policies_are_replay_or_connected() {
        match BootstrapPolicy::default_tx_data() {
            BootstrapPolicy::Replay { .. } => {}
            _ => panic!("expected Replay"),
        }
        match BootstrapPolicy::default_connected() {
            BootstrapPolicy::Connected { .. } => {}
            _ => panic!("expected Connected"),
        }
    }

    #[test]
    fn replay_window_variants_are_distinct() {
        assert_ne!(
            ReplayWindow::BlockBoundaries { safety: 0 },
            ReplayWindow::PositionBytes { bytes: 0 },
        );
    }

    #[test]
    fn stream_position_ordered() {
        assert!(StreamPosition(10) < StreamPosition(11));
        assert!(StreamPosition(0) <= StreamPosition(0));
    }
}
