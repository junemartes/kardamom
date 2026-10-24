//! [`CatchUp`]: the task that fills the place between the head of this
//! egress and the copies of the executors, from a peer egress.
//!
//! After a start, the copies chain only behind a frame that another source
//! delivers (see [`crate::merge`]). The task asks the owner where the
//! stream stands, reads the frames after that head from the range read of
//! a peer, and gives them to the owner in order. It reads again until the
//! copies join. It takes the next peer after a failure or an empty read,
//! so the instance itself in the peer list costs one empty read.
//!
//! A peer whose floor is above the head holds no frame before its floor,
//! and neither does any other peer that keeps the same blocks. So the
//! task gives the owner that floor as the new start, with the reference
//! mark of the peer. A peer that sends no mark gives `(0, 0)`.
//!
//! When no read brings a frame for the join timeout, the task tells the
//! owner to start the stream again after the next boundary of a copy.

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use tokio::sync::oneshot;

use crate::merge::{Head, Key};
use crate::owner::{Input, JoinState};
use crate::ref_mark::RefMark;

/// The bytes that one range read asks for.
const PAGE: u64 = 4 * 1024 * 1024;
/// The pause after a read that brought no frame.
const PAUSE: Duration = Duration::from_millis(500);

/// What one read of a peer gave.
enum Read {
    /// The frames after the head, in order. Empty when the peer has no
    /// frame after it.
    Frames(Vec<Vec<u8>>),
    /// The peer holds nothing that old: its floor and its reference mark.
    Floor(Head, RefMark),
}

/// The task that reads the peers until the copies join.
pub struct CatchUp {
    peers: Vec<String>,
    inputs: Sender<Input>,
    client: reqwest::Client,
    timeout: Duration,
    /// The last read that brought a frame, or the start.
    progress: Instant,
}

impl CatchUp {
    /// A task that reads `peers` (`http://host:port`), and gives up after
    /// `timeout` with no frame. `None` with no peer: the owner then joins
    /// at a boundary with no task.
    #[must_use]
    pub fn new(peers: Vec<String>, inputs: Sender<Input>, timeout: Duration) -> Option<Self> {
        (!peers.is_empty()).then(|| Self {
            peers,
            inputs,
            client: reqwest::Client::new(),
            timeout,
            progress: Instant::now(),
        })
    }

    /// Read the peers until the copies join, the timeout passes, or the
    /// owner is gone. Run it on a multi-thread runtime: a hand-off to the
    /// owner may wait for room on its queue.
    pub async fn run(mut self) {
        let mut peer = 0;
        while let ControlFlow::Continue(next) = self.step(peer).await {
            peer = next;
        }
    }

    /// One read of the peer at `peer`. `Continue` with the peer of the next
    /// read, `Break` when the task ends.
    async fn step(&mut self, peer: usize) -> ControlFlow<(), usize> {
        let state = self.state().await?;
        if state.joined {
            return ControlFlow::Break(());
        }
        if self.progress.elapsed() >= self.timeout {
            self.send(Input::GiveUp)?;
            return ControlFlow::Break(());
        }
        let base = &self.peers[peer % self.peers.len()];
        match self.read(base, state.head).await {
            Ok(Read::Frames(frames)) if !frames.is_empty() => {
                self.progress = Instant::now();
                self.send(Input::Caught(frames))?;
                ControlFlow::Continue(peer)
            }
            Ok(Read::Floor(floor, mark)) => {
                self.send(Input::Floor { floor, mark })?;
                ControlFlow::Continue(peer)
            }
            Ok(Read::Frames(_)) => self.next_peer(peer).await,
            Err(e) => {
                tracing::warn!(peer = %base, error = %e, "egress: a range read of a peer failed");
                self.next_peer(peer).await
            }
        }
    }

    async fn next_peer(&self, peer: usize) -> ControlFlow<(), usize> {
        tokio::time::sleep(PAUSE).await;
        // The index goes round the peers, so a wrap is its meaning.
        ControlFlow::Continue(peer.wrapping_add(1))
    }

    /// Where the stream stands. `Break` when the owner is gone.
    async fn state(&self) -> ControlFlow<(), JoinState> {
        let (reply, answer) = oneshot::channel();
        self.send(Input::State(reply))?;
        answer
            .await
            .map_or(ControlFlow::Break(()), ControlFlow::Continue)
    }

    /// Hand `input` to the owner. `Break` when the owner is gone.
    fn send(&self, input: Input) -> ControlFlow<()> {
        match tokio::task::block_in_place(|| self.inputs.send(input)) {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }

    /// The frames of the peer at `base` after `head`, or its floor.
    async fn read(&self, base: &str, head: Head) -> Result<Read, String> {
        let response = self
            .client
            .get(format!("{base}/v1/frames"))
            .query(&[
                ("from_index", head.next_index),
                ("from_block", head.next_block),
                ("limit", PAGE),
            ])
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if response.status() == reqwest::StatusCode::GONE {
            let headers = response.headers();
            return Ok(Read::Floor(Self::floor(headers)?, Self::mark(headers)));
        }
        let response = response.error_for_status().map_err(|e| e.to_string())?;
        let body = response.bytes().await.map_err(|e| e.to_string())?;
        Ok(Read::Frames(split(&body)))
    }

    /// The floor of a peer from its headers. A segment starts at genesis
    /// or after a boundary, so the last frame before the floor is the
    /// boundary of the block before it.
    fn floor(headers: &reqwest::header::HeaderMap) -> Result<Head, String> {
        let part = |name: &str| {
            Self::header(headers, name).ok_or_else(|| format!("the peer sent no {name}"))
        };
        let next_block = part("x-egress-floor-block")?;
        Ok(Head {
            next_index: part("x-egress-floor-index")?,
            next_block,
            last: next_block
                .checked_sub(1)
                .filter(|block| *block > 0)
                .map(Key::Boundary),
        })
    }

    /// The reference mark of a peer from its headers. A peer of an older
    /// release sends none: its mark counts as `(0, 0)`.
    fn mark(headers: &reqwest::header::HeaderMap) -> RefMark {
        match (
            Self::header(headers, "x-egress-ref-mark-index"),
            Self::header(headers, "x-egress-ref-mark-block"),
        ) {
            (Some(index), Some(block)) => RefMark { index, block },
            _ => RefMark::default(),
        }
    }

    /// The value of the header `name` as a `u64`.
    fn header(headers: &reqwest::header::HeaderMap, name: &str) -> Option<u64> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
    }
}

/// The frames of a body of `[len:u32][frame]` records.
fn split(mut body: &[u8]) -> Vec<Vec<u8>> {
    std::iter::from_fn(|| {
        let (len, rest) = body.split_first_chunk::<4>()?;
        let len = usize::try_from(u32::from_le_bytes(*len)).ok()?;
        let frame = rest.get(..len)?;
        body = &rest[len..];
        Some(frame.to_vec())
    })
    .collect()
}

#[cfg(test)]
#[path = "catchup_tests.rs"]
mod tests;
