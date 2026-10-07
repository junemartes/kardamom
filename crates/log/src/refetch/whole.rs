//! The replay of every recording of one stream, each from its start.
//!
//! A record of the `l1_blocks` stream names its own place: its L1 block
//! number. A consumer that resumes at a block reads the whole stream and
//! keeps the records it needs, so it needs no archive position. Two
//! follower instances and their restarts each leave a recording; the
//! replay reads every one, and the consumer drops the copies.

use super::{ArchiveRefetcher, FoundRecording, ReplayPlan, replay_sub_uri};
use crate::codec::WireMessage;
use crate::error::LogError;

impl ArchiveRefetcher {
    /// Replay every recording of `stream_id` on the first archive of
    /// `endpoints` that answers, each from its start to its recorded
    /// position, and hand each record to `sink`. Returns how many records
    /// arrived. A replay drains for at most the drain cap, so a caller
    /// treats the result as a prefix and reads the rest elsewhere.
    ///
    /// # Errors
    ///
    /// Returns an error if no archive endpoint answers, the catalog does
    /// not list, or a replay does not start. The endpoint rotates first.
    pub fn fetch_whole<T: WireMessage>(
        &mut self,
        endpoints: &[String],
        stream_id: i32,
        mut sink: impl FnMut(T),
    ) -> Result<u64, LogError> {
        let recs = self.list_or_rotate(endpoints, stream_id)?;
        let delivered = recs.iter().try_fold(0u64, |delivered, rec| {
            Ok::<_, LogError>(
                delivered.saturating_add(self.replay_whole(endpoints, rec, &mut sink)?),
            )
        })?;
        tracing::info!(
            stream_id,
            recordings = recs.len(),
            delivered,
            "whole-stream replay drained"
        );
        Ok(delivered)
    }

    /// One recording, from its start to its recorded position.
    fn replay_whole<T: WireMessage>(
        &mut self,
        endpoints: &[String],
        rec: &FoundRecording,
        sink: &mut impl FnMut(T),
    ) -> Result<u64, LogError> {
        let limit = self.recorded_limit(endpoints, rec)?;
        let len = limit.replay_len(rec.start_position);
        if len <= 0 {
            return Ok(0);
        }
        let plan = ReplayPlan {
            from_raw: rec.start_position,
            len,
            endpoint: self.cfg.replay_endpoint.clone(),
            limit,
        };
        let replay_stream = self.replay_stream_id();
        let sub_uri = replay_sub_uri(&plan.endpoint, rec.session_id);
        let (sub_id, mut rx) = self
            .ensure_runtime()?
            .open_subscription_with_id::<T>(&sub_uri, replay_stream)?;
        let session = self.ensure_session(endpoints)?;
        let started = Self::start_bounded_replay(session, rec, replay_stream, &plan);
        let delivered = match started {
            Ok(_) => Self::drain(&mut rx, |(_, record)| sink(record)),
            Err(_) => 0,
        };
        self.close_replay_subscription(sub_id);
        if let Err(e) = started {
            tracing::warn!(error = %e, "whole-stream replay did not start; rotating endpoint");
            self.rotate();
            return Err(e);
        }
        Ok(delivered)
    }
}
