//! [`EgressTransport`]: the canonical stream with its bytes, read from the
//! egress.
//!
//! A request reads the frames after its cursor from the range read of one
//! egress instance, page by page, until the cursor after a page reaches the
//! head of the store. Then the transport answers the request with
//! `REPLAY_DONE` at that head, and reads the live `egress_order` stream. The
//! live frames wait while a replay runs, so the reader gets the replay
//! first and the live frames after it. The reader drops a live frame that
//! the replay sent already.
//!
//! The egress is one source with no image to follow: the connection id
//! changes only when a range read fails, so the reader asks again.

use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use kardamom_cluster_adapter::wire::{
    EGRESS_KIND_REPLAY_DONE, EGRESS_KIND_REPLAY_UNAVAILABLE, encode_feed_answer,
};

use super::transport::{Poll, Request, Transport};

/// The bytes of one page of the range read.
const PAGE: u64 = 4 * 1024 * 1024;

/// One reader of the egress: its HTTP base, and the live stream.
pub struct EgressTransport {
    base: String,
    client: reqwest::Client,
    handle: tokio::runtime::Handle,
    live: Receiver<Vec<u8>>,
    /// The frames of the running replay, and then its answer.
    replay: Option<Receiver<Vec<u8>>>,
    connection: u64,
    /// A failed range read: the next poll counts a new connection.
    failed: Receiver<()>,
    fail: Sender<()>,
}

impl EgressTransport {
    /// A reader of the egress at `base` (`http://host:port`), with the
    /// frames of `egress_order` on `live`. The range reads run on `handle`.
    #[must_use]
    pub fn new(base: String, live: Receiver<Vec<u8>>, handle: tokio::runtime::Handle) -> Self {
        let (fail, failed) = crossbeam_channel::unbounded();
        Self {
            base,
            client: reqwest::Client::new(),
            handle,
            live,
            replay: None,
            connection: 1,
            failed,
            fail,
        }
    }
}

impl Transport for EgressTransport {
    fn connection(&mut self) -> Option<u64> {
        if self.failed.try_iter().count() > 0 {
            self.connection += 1;
        }
        Some(self.connection)
    }

    fn request(&mut self, request: Request) {
        let (frames, replay) = crossbeam_channel::unbounded();
        let page = Pages {
            base: self.base.clone(),
            client: self.client.clone(),
            request,
            frames,
            fail: self.fail.clone(),
        };
        self.handle.spawn(page.run());
        self.replay = Some(replay);
    }

    fn poll(&mut self, wait: Duration) -> Poll {
        let Some(replay) = &self.replay else {
            return match self.live.recv_timeout(wait) {
                Ok(frame) => Poll::Frame(frame),
                Err(RecvTimeoutError::Timeout) => Poll::Idle,
                Err(RecvTimeoutError::Disconnected) => Poll::Closed,
            };
        };
        match replay.recv_timeout(wait) {
            Ok(frame) => Poll::Frame(frame),
            Err(RecvTimeoutError::Timeout) => Poll::Idle,
            Err(RecvTimeoutError::Disconnected) => {
                self.replay = None;
                Poll::Idle
            }
        }
    }

    fn close(&mut self) {
        self.replay = None;
        self.live.try_iter().for_each(drop);
    }
}

/// The range reads of one request.
struct Pages {
    base: String,
    client: reqwest::Client,
    request: Request,
    frames: Sender<Vec<u8>>,
    fail: Sender<()>,
}

/// What one page says.
enum Page {
    /// The frames, the cursor after them, and the head of the store.
    Frames {
        body: bytes::Bytes,
        after: (u64, u64),
        head: (u64, u64),
    },
    /// The store holds nothing that old: its floor.
    Unavailable { floor: (u64, u64) },
}

impl Pages {
    /// Read the pages from the cursor of the request to the head, then send
    /// the answer. A failed read sends nothing more: the reader asks again.
    async fn run(self) {
        let mut cursor = (self.request.from_index, self.request.from_block);
        while let Some(next) = self.step(cursor).await {
            cursor = next;
        }
    }

    /// Read and send one page. Returns the cursor of the next page, or
    /// `None` when the answer ended or the read failed.
    async fn step(&self, cursor: (u64, u64)) -> Option<(u64, u64)> {
        match self.read(cursor).await {
            Ok(page) => self.send(page),
            Err(e) => {
                tracing::warn!(error = %e, base = %self.base, "a range read of the egress failed");
                let _ = self.fail.send(());
                None
            }
        }
    }

    /// Send the frames of `page`. Returns the cursor of the next page, or
    /// `None` when the page ended the answer.
    fn send(&self, page: Page) -> Option<(u64, u64)> {
        let request_id = self.request.request_id;
        let answer = match page {
            Page::Frames { body, after, head } => {
                split(&body).for_each(|frame| {
                    let _ = self.frames.send(frame.to_vec());
                });
                if after < head {
                    return Some(after);
                }
                encode_feed_answer(EGRESS_KIND_REPLAY_DONE, head.0, head.1, request_id)
            }
            Page::Unavailable { floor } => {
                encode_feed_answer(EGRESS_KIND_REPLAY_UNAVAILABLE, floor.0, floor.1, request_id)
            }
        };
        let _ = self.frames.send(answer);
        None
    }

    async fn read(&self, (index, block): (u64, u64)) -> Result<Page, String> {
        let response = self
            .client
            .get(format!("{}/v1/frames", self.base))
            .query(&[
                ("from_index", index),
                ("from_block", block),
                ("limit", PAGE),
            ])
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if response.status() == reqwest::StatusCode::GONE {
            return Ok(Page::Unavailable {
                floor: cursor_of(response.headers(), "floor")?,
            });
        }
        let response = response.error_for_status().map_err(|e| e.to_string())?;
        let after = cursor_of(response.headers(), "after")?;
        let head = cursor_of(response.headers(), "next")?;
        Ok(Page::Frames {
            body: response.bytes().await.map_err(|e| e.to_string())?,
            after,
            head,
        })
    }
}

/// The cursor in the headers `x-egress-<name>-index` and `-block`.
fn cursor_of(headers: &reqwest::header::HeaderMap, name: &str) -> Result<(u64, u64), String> {
    let part = |part: &str| {
        let key = format!("x-egress-{name}-{part}");
        headers
            .get(&key)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| format!("the egress sent no {key}"))
    };
    Ok((part("index")?, part("block")?))
}

/// The frames of a body of `[len:u32][frame]` records.
fn split(mut body: &[u8]) -> impl Iterator<Item = &[u8]> {
    std::iter::from_fn(move || {
        let (len, rest) = body.split_first_chunk::<4>()?;
        let len = usize::try_from(u32::from_le_bytes(*len)).ok()?;
        let frame = rest.get(..len)?;
        body = &rest[len..];
        Some(frame)
    })
}

#[cfg(test)]
#[path = "egress_tests.rs"]
mod tests;
