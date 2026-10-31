use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::Context;
use reqwest::header::HeaderMap;
use serde::Serialize;

use crate::harness::Harness;
use crate::poll::{self, Budget};

const PAGE_BYTES: usize = 1024 * 1024;
const HTTP_PORT: u16 = 8548;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(super) struct Cursor {
    index: u64,
    pub(super) block: u64,
}

impl Cursor {
    fn read(headers: &HeaderMap, prefix: &str) -> anyhow::Result<Self> {
        Ok(Self {
            index: Self::header(headers, &format!("x-egress-{prefix}-index"))?,
            block: Self::header(headers, &format!("x-egress-{prefix}-block"))?,
        })
    }

    fn header(headers: &HeaderMap, name: &str) -> anyhow::Result<u64> {
        headers
            .get(name)
            .with_context(|| format!("no {name}"))?
            .to_str()?
            .parse()
            .map_err(Into::into)
    }

    fn covers(self, other: Self) -> bool {
        self.index >= other.index && self.block >= other.block
    }
}

pub(super) struct Copies {
    http: reqwest::Client,
    first: String,
    rest: Vec<String>,
}

impl Copies {
    pub(super) async fn of(h: &Harness) -> anyhow::Result<Self> {
        let addresses = h
            .nomad
            .running("egress")
            .await?
            .iter()
            .map(|alloc| {
                let node = h.contract.node(&alloc.node_name)?;
                Ok(format!("http://{}:{HTTP_PORT}/v1/frames", node.ip))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut addresses = addresses.into_iter();
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
            first: addresses.next().context("no egress copy runs")?,
            rest: addresses.collect(),
        })
    }

    pub(super) fn len(&self) -> usize {
        // One first address and the remaining allocations fit in memory.
        self.rest.len() + 1
    }

    /// A cursor above every stored frame reads only the current head.
    pub(super) async fn head(&self) -> anyhow::Result<Cursor> {
        poll::until(Budget::secs(60, 1), |_| async {
            Ok::<_, anyhow::Error>(self.head_once().await.ok())
        })
        .await?
        .or_fail(|_| anyhow::anyhow!("prune-floor: the egress does not serve its head"))
        .map(|(head, _)| head)
    }

    async fn head_once(&self) -> anyhow::Result<Cursor> {
        Ok(self
            .page(
                &self.first,
                Cursor {
                    index: u64::MAX,
                    block: u64::MAX,
                },
                1,
            )
            .await?
            .head)
    }

    async fn page(&self, url: &str, cursor: Cursor, limit: usize) -> anyhow::Result<Page> {
        let response = self
            .http
            .get(url)
            .query(&[
                ("from_index", cursor.index.to_string()),
                ("from_block", cursor.block.to_string()),
                ("limit", limit.to_string()),
            ])
            .send()
            .await?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::OK,
            "prune-floor: {url} refuses retained cursor {cursor:?}: {}",
            response.status()
        );
        let after = Cursor::read(response.headers(), "after")?;
        let head = Cursor::read(response.headers(), "next")?;
        Ok(Page {
            after,
            head,
            bytes: response.bytes().await?.to_vec(),
        })
    }

    pub(super) async fn replay(
        &self,
        cursor: Cursor,
        minimum_distance: u64,
    ) -> anyhow::Result<Cursor> {
        let first = self.page(&self.first, cursor, PAGE_BYTES).await?;
        anyhow::ensure!(
            first
                .head
                .block
                .checked_sub(cursor.block)
                .is_some_and(|gap| gap > minimum_distance),
            "prune-floor: durable head {:?} is not past the protected window from {cursor:?}",
            first.head
        );
        let mut replay = Replay {
            copies: self,
            cursor,
            end: first.head,
        };
        replay.compare(&first).await?;
        replay.cursor = first.after;
        while replay.step().await?.is_continue() {}
        Ok(replay.end)
    }
}

struct Page {
    after: Cursor,
    head: Cursor,
    bytes: Vec<u8>,
}

impl Page {
    /// The store counts payload bytes, while HTTP prefixes each frame
    /// with its four-byte length. The same payload limit stops every
    /// copy at the same frame.
    fn payload_bytes(&self) -> anyhow::Result<usize> {
        let mut remaining = self.bytes.as_slice();
        let mut total = 0usize;
        while !remaining.is_empty() {
            (remaining, total) = Self::frame(remaining, total)?;
        }
        anyhow::ensure!(total > 0, "prune-floor: retained replay makes no progress");
        Ok(total)
    }

    fn frame(bytes: &[u8], total: usize) -> anyhow::Result<(&[u8], usize)> {
        let (len, body) = bytes
            .split_at_checked(4)
            .context("truncated frame length")?;
        let size = usize::try_from(u32::from_le_bytes(len.try_into()?))?;
        let (_, rest) = body
            .split_at_checked(size)
            .context("truncated frame body")?;
        Ok((
            rest,
            total
                .checked_add(size)
                .context("replay byte count overflows")?,
        ))
    }
}

struct Replay<'a> {
    copies: &'a Copies,
    cursor: Cursor,
    end: Cursor,
}

impl Replay<'_> {
    async fn step(&mut self) -> anyhow::Result<ControlFlow<()>> {
        if self.cursor.covers(self.end) {
            return Ok(ControlFlow::Break(()));
        }
        let page = self
            .copies
            .page(&self.copies.first, self.cursor, PAGE_BYTES)
            .await?;
        self.compare(&page).await?;
        anyhow::ensure!(
            page.after != self.cursor,
            "prune-floor: replay stalls at {:?}",
            self.cursor
        );
        self.cursor = page.after;
        Ok(ControlFlow::Continue(()))
    }

    async fn compare(&self, page: &Page) -> anyhow::Result<()> {
        let limit = page.payload_bytes()?;
        for url in &self.copies.rest {
            self.compare_copy(url, page, limit).await?;
        }
        Ok(())
    }

    async fn compare_copy(&self, url: &str, expected: &Page, limit: usize) -> anyhow::Result<()> {
        let (copy, _) = poll::until(Budget::secs(30, 1), |_| async {
            let page = self.copies.page(url, self.cursor, limit).await?;
            Ok::<_, anyhow::Error>(page.after.covers(expected.after).then_some(page))
        })
        .await?
        .or_fail(|_| anyhow::anyhow!("prune-floor: {url} does not reach {:?}", expected.after))?;
        anyhow::ensure!(
            copy.after == expected.after && copy.bytes == expected.bytes,
            "prune-floor: {url} disagrees with the retained canonical range at {:?}",
            self.cursor
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "replay_tests.rs"]
mod tests;
