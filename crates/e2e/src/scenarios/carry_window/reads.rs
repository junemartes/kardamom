//! The reads of the carry window scenarios: the admin endpoint of each
//! sealer member, the egress, and the gauges of the readers.

use std::net::SocketAddr;
use std::time::Duration;

use alloy_primitives::B256;
use anyhow::{Context, Result};

use crate::harness::l2::L2Client;
use crate::harness::metrics::{self, poll_until};
use crate::scenarios::metric_u64;

pub(super) const RETAINED_FRAMES: &str = "kardamom_sealer_retained_frames";
pub(super) const WINDOW_EXACT: &str = "kardamom_sealer_window_exact";
pub(super) const DECISION_VERSION: &str = "kardamom_sealer_decision_version";
pub(super) const REF_REFUSED: &str = "kardamom_sealer_ref_refused_total";
pub(super) const FULL_REF_BELOW_V5: &str = "kardamom_sealer_full_ref_below_v5_total";
pub(super) const FEED_STATE: &str = "kardamom_sealer_feed_state";
pub(super) const FEATURE_STATE: &str = "kardamom_sealer_feature_state";
pub(super) const SNAPSHOT_VERSION: &str = "kardamom_sealer_snapshot_version";
pub(super) const VALIDATOR_CURSOR: &str = "kardamom_validator_egress_cursor_index";
pub(super) const BATCHER_CURSOR: &str = "kardamom_batcher_egress_cursor_index";
pub(super) const BATCHER_POSTED: &str = "kardamom_batcher_last_posted_block";
pub(super) const SEQ_SEALER_VERSION: &str = "kardamom_sequencer_sealer_version";
pub(super) const SEQ_REF_REFUSED: &str = "kardamom_sequencer_ref_refused_total";
pub(super) const SEQ_TX_DROPS: &str = "kardamom_sequencer_tx_drops_total";
const REF_MAX_INDEX: &str = "kardamom_egress_ref_frame_max_index";
const REF_MAX_BLOCK: &str = "kardamom_egress_ref_frame_max_block";

/// `kardamom_sealer_feed_state` of a feed that serves its executor live.
pub(super) const FEED_LIVE: u64 = 3;
/// `kardamom_sealer_feature_state` of a feature that is on.
pub(super) const FEATURE_ON: u64 = 2;
/// `kardamom_sealer_feature_state` of a feature held and off.
pub(super) const FEATURE_HELD_OFF: u64 = 1;

/// How long a wait for a member, an egress or a reader may take.
const WAIT: Duration = Duration::from_mins(1);
const POLL: Duration = Duration::from_millis(250);
/// The longest wait on a frozen sealer, below the leader heartbeat timeout.
const SETTLE: Duration = Duration::from_secs(5);

/// One gauge or counter on one metrics page.
#[derive(Clone, Copy)]
pub(super) struct Gauge<'a> {
    addr: SocketAddr,
    name: &'a str,
    label: &'a str,
}

impl<'a> Gauge<'a> {
    /// `name` at `addr`, summed over its label sets.
    pub(super) fn of(addr: SocketAddr, name: &'a str) -> Self {
        Self {
            addr,
            name,
            label: "",
        }
    }

    /// Only the samples with `label`.
    pub(super) fn with(self, label: &'a str) -> Self {
        Self { label, ..self }
    }

    /// The value. A counter that nothing counted yet reads 0.
    ///
    /// # Errors
    /// Returns an error when the scrape fails, or when the value is not a
    /// whole number.
    pub(super) async fn read(&self) -> Result<u64> {
        let s = metrics::scrape(self.addr).await?;
        s.value_where(self.name, self.label)
            .map_or(Ok(0), metric_u64)
    }

    /// Wait until the value is above `floor`, and return it. A scrape that
    /// fails reads as not yet: the service may start again.
    ///
    /// # Errors
    /// Returns an error when the wait ends first.
    pub(super) async fn await_above(&self, floor: u64) -> Result<u64> {
        let what = format!("{} at {} above {floor}", self.name, self.addr);
        poll_until(&what, WAIT, POLL, || async {
            Ok(self.read().await.ok().filter(|v| *v > floor))
        })
        .await
    }
}

/// The admin endpoints of the sealer members.
pub(super) struct Members {
    admin: Vec<SocketAddr>,
}

impl Members {
    pub(super) fn new(admin: Vec<SocketAddr>) -> Self {
        Self { admin }
    }

    /// The admin endpoint of member `id`.
    pub(super) fn admin(&self, id: usize) -> SocketAddr {
        self.admin[id]
    }

    /// The value of `name` with `label` on each member, in member order.
    ///
    /// # Errors
    /// Returns an error when a member does not answer.
    pub(super) async fn values(&self, name: &str, label: &str) -> Result<Vec<u64>> {
        futures::future::try_join_all(
            self.admin
                .iter()
                .map(|addr| async move { Gauge::of(*addr, name).with(label).read().await }),
        )
        .await
    }

    /// Wait until `want` holds for the value of `name` on every member. A
    /// member that does not answer reads as not yet: it may start again.
    ///
    /// # Errors
    /// Returns an error when the wait ends first.
    pub(super) async fn await_all(
        &self,
        name: &str,
        label: &str,
        want: impl Fn(u64) -> bool,
    ) -> Result<Vec<u64>> {
        poll_until(
            &format!("{name}{{{label}}} on every member"),
            WAIT,
            POLL,
            || async {
                let values = self.values(name, label).await.ok();
                Ok(values.filter(|values| values.iter().all(|v| want(*v))))
            },
        )
        .await
    }

    /// The `/status` of each member, in member order.
    ///
    /// # Errors
    /// Returns an error when a member does not answer, or answers no JSON.
    pub(super) async fn statuses(&self) -> Result<Vec<serde_json::Value>> {
        futures::future::try_join_all(self.admin.iter().map(|addr| async move {
            let body = metrics::get(*addr, "/status").await?.body;
            serde_json::from_str(&body).with_context(|| format!("{addr} /status: {body}"))
        }))
        .await
    }

    /// A follower other than member 0, which feeds the executor.
    ///
    /// # Errors
    /// Returns an error when a member does not answer, or when no such
    /// follower exists.
    pub(super) async fn follower(&self) -> Result<usize> {
        self.statuses()
            .await?
            .iter()
            .enumerate()
            .skip(1)
            .find(|(_, s)| s["role"].as_str() == Some("FOLLOWER"))
            .map(|(id, _)| id)
            .context("no follower other than member 0")
    }

    /// Wait until every member reports `carryWindow` as `on` on `/status`.
    /// A member that does not answer reads as not yet.
    ///
    /// # Errors
    /// Returns an error when the wait ends first.
    pub(super) async fn await_carry_window(&self, on: bool) -> Result<()> {
        poll_until(
            &format!("carryWindow {on} on every member"),
            WAIT,
            POLL,
            || async {
                let all = self.statuses().await.is_ok_and(|statuses| {
                    statuses
                        .iter()
                        .all(|s| s["carryWindow"].as_bool() == Some(on))
                });
                Ok(all.then_some(()))
            },
        )
        .await
    }

    /// Every member keeps the exact window: the window gauge is 1, and the
    /// retained frames fall within `retention`. The prune at the end of the
    /// entry that turns the window on drains the frames, and the gauge
    /// follows each entry, so the wait is short.
    ///
    /// # Errors
    /// Returns an error when a member does not answer, or when a member
    /// does not keep the exact window within the wait.
    pub(super) async fn await_exact_window(&self, retention: u64) -> Result<()> {
        self.await_all(WINDOW_EXACT, "", |v| v == 1).await?;
        self.await_all(RETAINED_FRAMES, "", |v| v <= retention)
            .await
            .map(|_| ())
    }
}

/// The highest stored reference of the egress: R.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Reference {
    pub(super) index: u64,
    pub(super) block: u64,
}

/// The reads of the egress: its gauges, the head of its store, and its
/// status call.
pub(super) struct EgressView {
    metrics: SocketAddr,
    http: SocketAddr,
    rpc: L2Client,
}

impl EgressView {
    /// # Errors
    /// Returns an error when the client for `url` fails to build.
    pub(super) fn new(url: &str, metrics: SocketAddr) -> Result<Self> {
        let http = url
            .strip_prefix("http://")
            .and_then(|a| a.parse().ok())
            .with_context(|| format!("the egress URL {url}"))?;
        Ok(Self {
            metrics,
            http,
            rpc: L2Client::new(url, Duration::from_secs(5))?,
        })
    }

    /// The next block of the store: a range read after every frame
    /// returns no frame, and the head of the store in its headers.
    ///
    /// # Errors
    /// Returns an error when the read fails or has no head.
    async fn head_block(&self) -> Result<u64> {
        let path = format!("/v1/frames?from_index={0}&from_block={0}&limit=0", u64::MAX);
        let answer = metrics::get(self.http, &path).await?;
        answer
            .header("x-egress-next-block")
            .and_then(|v| v.parse().ok())
            .context("the range read of the egress names no head")
    }

    /// Wait until the head of the store is past `block` and keeps its
    /// value over one poll: on a frozen chain, the egress then holds every
    /// frame that the executor sent. The wait is short, so a frozen sealer
    /// starts no election.
    ///
    /// # Errors
    /// Returns an error when a read fails, or when the wait ends first.
    pub(super) async fn await_settled(&self, block: u64) -> Result<()> {
        let last = std::sync::atomic::AtomicU64::new(0);
        poll_until(
            &format!("the egress settles past block {block}"),
            SETTLE,
            Duration::from_millis(500),
            || async {
                let head = self.head_block().await?;
                let before = last.swap(head, std::sync::atomic::Ordering::Relaxed);
                Ok((head > block && head == before).then_some(()))
            },
        )
        .await
    }

    /// Wait until the store of the egress holds every block through
    /// `block`. The egress stores the frames after the executor runs them,
    /// so it can trail the chain.
    ///
    /// # Errors
    /// Returns an error when a read fails, or when the wait ends first.
    pub(super) async fn await_head_past(&self, block: u64) -> Result<()> {
        poll_until(
            &format!("the egress stores block {block}"),
            WAIT,
            POLL,
            || async { Ok((self.head_block().await? > block).then_some(())) },
        )
        .await
    }

    /// R, from the two gauges of the egress.
    ///
    /// # Errors
    /// Returns an error when the egress does not answer.
    pub(super) async fn reference(&self) -> Result<Reference> {
        Ok(Reference {
            index: Gauge::of(self.metrics, REF_MAX_INDEX).read().await?,
            block: Gauge::of(self.metrics, REF_MAX_BLOCK).read().await?,
        })
    }

    /// Wait until the egress shows R again after a restart: it sets the
    /// gauge when it opens its store, after its metrics page is up.
    ///
    /// # Errors
    /// Returns an error when the wait ends first.
    pub(super) async fn await_up(&self) -> Result<()> {
        poll_until("the egress opens its store", WAIT, POLL, || async {
            let scrape = metrics::scrape(self.metrics).await.ok();
            Ok(scrape.and_then(|s| s.value(REF_MAX_INDEX)).map(|_| ()))
        })
        .await
    }

    /// The `kardamom_getTransactionStatus` answer for `hash`.
    ///
    /// # Errors
    /// Returns an error when the call fails.
    pub(super) async fn status(&self, hash: B256) -> Result<serde_json::Value> {
        self.rpc
            .raw_call(
                "kardamom_getTransactionStatus",
                &[serde_json::Value::String(hash.to_string())],
            )
            .await
            .result
            .map_err(|e| anyhow::anyhow!("kardamom_getTransactionStatus: {e}"))
    }

    /// Wait until the status of `hash` is `want`, and return the answer. A
    /// call that fails reads as not yet: the owner thread of the egress
    /// answers between its writes.
    ///
    /// # Errors
    /// Returns an error when the wait ends first.
    pub(super) async fn await_status(&self, hash: B256, want: &str) -> Result<serde_json::Value> {
        poll_until(&format!("status {want} of {hash}"), WAIT, POLL, || async {
            let answer = self.status(hash).await.ok();
            Ok(answer.filter(|a| a["status"].as_str() == Some(want)))
        })
        .await
    }
}
