//! The pipeline metrics of the session thread. See
//! `kardamom_obs::pipeline`.
//!
//! - The loop of the session thread is busy while a duty cycle moves
//!   frames or offers, and idle while a cycle finds nothing and while it
//!   waits for input. A session that subscribes to the canonical stream
//!   reports as `cluster_client.stream_session`, every other session as
//!   `cluster_client.session`. A process can drive one session of each
//!   kind, and the two must not share one series.
//! - The step `cluster_client.offer` is one offer request: the wrap and the
//!   publish of its payload.
//! - The queue `cluster_client.requests` is the offer requests that wait.
//!   Only a session with an ingress seam sets it.
//! - The queue `cluster_client.egress_out` (or
//!   `cluster_client.stream_egress_out`) is the payloads that wait for the
//!   egress consumer.

use std::time::{Duration, Instant};

use kardamom_obs::pipeline::{LoopMeter, LoopState, Queue, Step};

use super::endpoints::now_ms;

/// The metric names of one kind of session.
struct Names {
    session: &'static str,
    egress_out: &'static str,
}

impl Names {
    const PUBLISHER: Names = Names {
        session: "cluster_client.session",
        egress_out: "cluster_client.egress_out",
    };
    const STREAM: Names = Names {
        session: "cluster_client.stream_session",
        egress_out: "cluster_client.stream_egress_out",
    };
}

/// The depths of the two queues of the session thread at the end of one
/// duty cycle. `None` is a seam whose other end is gone.
pub(super) struct Depths {
    pub(super) requests: Option<usize>,
    pub(super) egress_out: Option<usize>,
}

/// The pipeline metrics of one session thread.
pub(super) struct SessionMeters {
    cycle: LoopMeter,
    offer: Step,
    requests: Queue,
    egress_out: Queue,
    egress_age: metrics::Histogram,
    /// The oldest header timestamp of the app messages in the current
    /// drain, in cluster milliseconds.
    oldest_ms: Option<i64>,
}

impl SessionMeters {
    /// The meters of a session. `stream` is a session that subscribes to
    /// the canonical stream.
    pub(super) fn new(stream: bool) -> Self {
        let names = if stream {
            Names::STREAM
        } else {
            Names::PUBLISHER
        };
        Self {
            cycle: LoopMeter::new(names.session),
            offer: Step::new("cluster_client.offer"),
            requests: Queue::new("cluster_client.requests"),
            egress_out: Queue::new(names.egress_out),
            egress_age: metrics::histogram!(crate::metrics::EGRESS_AGE_SECONDS),
            oldest_ms: None,
        }
    }

    /// The duty part of a cycle ends: charge it busy when it moved
    /// something, and set the depths of the live seams.
    pub(super) fn duties_done(&mut self, worked: bool, depths: &Depths) {
        self.cycle.pass(worked);
        if let Some(n) = depths.requests {
            self.requests.set(n);
        }
        if let Some(n) = depths.egress_out {
            self.egress_out.set(n);
        }
    }

    /// The idle wait of a cycle ends.
    pub(super) fn waited(&mut self) {
        self.cycle.mark(LoopState::Idle);
    }

    /// The time of one offer request from `started` to now.
    pub(super) fn offered(&self, started: Instant) {
        self.offer.since(started);
    }

    /// Note the header timestamp of one app message of the current drain.
    pub(super) fn note_egress(&mut self, timestamp_ms: i64) {
        self.oldest_ms = Some(self.oldest_ms.map_or(timestamp_ms, |t| t.min(timestamp_ms)));
    }

    /// The drain ends: record the age of its oldest app message, if it
    /// had one.
    pub(super) fn drain_done(&mut self) {
        let Some(oldest) = self.oldest_ms.take() else {
            return;
        };
        self.egress_age.record(Self::age(now_ms(), oldest));
    }

    /// The age at `now_ms` of a message stamped at `timestamp_ms`, both in
    /// unix milliseconds. A clock of the leader ahead of this host gives a
    /// negative difference, and that reads as zero age.
    fn age(now_ms: u64, timestamp_ms: i64) -> Duration {
        let age_ms = i64::try_from(now_ms)
            .ok()
            .and_then(|now| now.checked_sub(timestamp_ms))
            .and_then(|ms| u64::try_from(ms).ok())
            .unwrap_or(0);
        Duration::from_millis(age_ms)
    }
}

#[cfg(test)]
#[path = "meters_tests.rs"]
mod tests;
