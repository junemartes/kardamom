//! The controlled sealer. The parent appends the canonical log to a
//! file; each executor process reads it through one session that
//! speaks the production egress codec, the way the cluster client
//! serves a `REPLAY_FROM(cursor)`: a session start, the retained
//! frames from the cursor (with an overlap below it), `REPLAY_DONE`,
//! then the live frames. A cursor below the retained floor gets
//! `REPLAY_UNAVAILABLE`.

use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use kardamom_cluster_adapter::gateway::ClusterEgress;
use kardamom_cluster_adapter::wire::{
    encode_replay_done, encode_replay_unavailable, encode_session_start,
};

use super::chain::{Chain, Entry};

const LOG: &str = "log.bin";
const FLOOR: &str = "floor";
const DOWN: &str = "down";
/// How often a session reads the log for new frames.
const TAIL_EVERY: Duration = Duration::from_millis(10);
/// The records below the cursor that a replay sends again. The reader
/// must drop them as a replay/live overlap.
const OVERLAP: u64 = 4;

/// The parent's side: the retained log, its floor, and its liveness.
#[derive(Clone)]
pub(crate) struct SealerLog {
    dir: PathBuf,
}

impl SealerLog {
    pub(crate) fn create(dir: &Path) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::File::create(dir.join(LOG)).unwrap();
        Self {
            dir: dir.to_path_buf(),
        }
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// Append the entries of `block` in one write, so that a reader sees
    /// either none or a prefix that it reads again later.
    pub(crate) fn append_block(&self, chain: &Chain, block: u64) {
        let bytes: Vec<u8> = chain.block(block).iter().flat_map(Self::encode).collect();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(self.dir.join(LOG))
            .unwrap();
        file.write_all(&bytes).unwrap();
    }

    /// Retain only the frames at or above record `index` and block
    /// `block`: an older cursor gets `REPLAY_UNAVAILABLE`.
    pub(crate) fn set_floor(&self, index: u64, block: u64) {
        std::fs::write(self.dir.join(FLOOR), format!("{index} {block}")).unwrap();
    }

    /// Retain every frame again.
    pub(crate) fn clear_floor(&self) {
        let _ = std::fs::remove_file(self.dir.join(FLOOR));
    }

    /// Take every member down, or bring them back. A session waits for a
    /// member before it starts.
    pub(crate) fn set_down(&self, down: bool) {
        let path = self.dir.join(DOWN);
        if down {
            std::fs::write(path, b"down").unwrap();
        } else {
            std::fs::remove_file(path).unwrap();
        }
    }

    fn encode(entry: &Entry) -> Vec<u8> {
        let (kind, key, frame) = match entry {
            Entry::Record { index, frame } => (0u8, *index, frame),
            Entry::Boundary { block, frame, .. } => (1u8, *block, frame),
        };
        let len = u32::try_from(frame.len()).unwrap();
        [&[kind][..], &key.to_le_bytes(), &len.to_le_bytes(), frame].concat()
    }
}

/// The egress of one session: the frames the session thread sends. It
/// ends when the session thread hangs up, and at once when the session
/// closes, as a dropped cluster session ends its egress.
pub(crate) struct ChannelEgress {
    frames: Receiver<Vec<u8>>,
    stop: Arc<AtomicBool>,
}

impl ClusterEgress for ChannelEgress {
    fn recv(&mut self) -> Option<Vec<u8>> {
        loop {
            if let ControlFlow::Break(frame) = self.poll() {
                return frame;
            }
        }
    }
}

impl ChannelEgress {
    fn poll(&self) -> ControlFlow<Option<Vec<u8>>> {
        if self.stop.load(Ordering::Acquire) {
            return ControlFlow::Break(None);
        }
        match self.frames.recv_timeout(TAIL_EVERY) {
            Ok(frame) => ControlFlow::Break(Some(frame)),
            Err(RecvTimeoutError::Timeout) => ControlFlow::Continue(()),
            Err(RecvTimeoutError::Disconnected) => ControlFlow::Break(None),
        }
    }
}

/// One log entry as the session reads it.
struct Framed {
    kind: u8,
    key: u64,
    frame: Vec<u8>,
}

/// The child's side: one session from a cursor.
pub(crate) struct SealerSession {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// The state of the session thread.
struct Feed {
    dir: PathBuf,
    next: (u64, u64),
    out: Sender<Vec<u8>>,
    stop: Arc<AtomicBool>,
    offset: u64,
    head: (u64, u64),
}

impl SealerSession {
    /// Open a session that replays from `next` = (next index, next
    /// block), and its egress.
    pub(crate) fn open(dir: &Path, next: (u64, u64)) -> (Self, ChannelEgress) {
        let (out, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        let feed = Feed {
            dir: dir.to_path_buf(),
            next,
            out,
            stop: stop.clone(),
            offset: 0,
            head: (0, 1),
        };
        let thread = std::thread::spawn(move || feed.run());
        (
            Self {
                stop: stop.clone(),
                thread: Some(thread),
            },
            ChannelEgress { frames: rx, stop },
        )
    }

    /// End the session: the egress closes once its frames drain.
    pub(crate) fn close(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

impl Feed {
    fn run(mut self) {
        while self.dir.join(DOWN).exists() && !self.stop.load(Ordering::Acquire) {
            std::thread::sleep(TAIL_EVERY);
        }
        if self.stop.load(Ordering::Acquire) {
            return;
        }
        self.send(encode_session_start(1));
        if let Some((index, block)) = self.floor().filter(|(index, _)| self.next.0 < *index) {
            self.send(encode_replay_unavailable(index, block));
            return;
        }
        self.pump();
        self.send(encode_replay_done(self.head.0, self.head.1));
        while !self.stop.load(Ordering::Acquire) {
            self.tail();
        }
    }

    /// Wait one tail period, then send the new frames.
    fn tail(&mut self) {
        std::thread::sleep(TAIL_EVERY);
        self.pump();
    }

    fn floor(&self) -> Option<(u64, u64)> {
        let text = std::fs::read_to_string(self.dir.join(FLOOR)).ok()?;
        let (index, block) = text.split_once(' ')?;
        Some((index.parse().ok()?, block.parse().ok()?))
    }

    fn send(&self, frame: Vec<u8>) {
        // A closed egress means the engine ended; the session ends too.
        let _ = self.out.send(frame);
    }

    /// Send every complete entry past the last offset that the replay
    /// window holds.
    fn pump(&mut self) {
        self.read_new()
            .into_iter()
            .for_each(|entry| self.offer(entry));
    }

    fn offer(&mut self, entry: Framed) {
        let (next_index, next_block) = self.next;
        let kept = if entry.kind == 0 {
            self.head.0 = entry.key + 1;
            entry.key + OVERLAP >= next_index
        } else {
            self.head.1 = entry.key + 1;
            entry.key + 1 >= next_block
        };
        if kept {
            self.send(entry.frame);
        }
    }

    fn read_new(&mut self) -> Vec<Framed> {
        let mut file = std::fs::File::open(self.dir.join(LOG)).unwrap();
        file.seek(SeekFrom::Start(self.offset)).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        let entries: Vec<(usize, Framed)> =
            std::iter::successors(Some((0usize, None)), |(at, _)| {
                Self::parse(&bytes[*at..]).map(|(used, entry)| (at + used, Some(entry)))
            })
            .filter_map(|(at, entry)| entry.map(|e| (at, e)))
            .collect();
        let used = entries.last().map_or(0, |(at, _)| *at);
        self.offset += u64::try_from(used).unwrap();
        entries.into_iter().map(|(_, entry)| entry).collect()
    }

    /// One complete entry at the start of `bytes`, and its length.
    fn parse(bytes: &[u8]) -> Option<(usize, Framed)> {
        let header = bytes.get(..13)?;
        let key = u64::from_le_bytes(header[1..9].try_into().unwrap());
        let len = usize::try_from(u32::from_le_bytes(header[9..13].try_into().unwrap())).unwrap();
        let frame = bytes.get(13..13 + len)?.to_vec();
        Some((
            13 + len,
            Framed {
                kind: header[0],
                key,
                frame,
            },
        ))
    }
}
