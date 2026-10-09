//! The bounded replay of an executor stream recording from a locator.
//!
//! A consumer of the executor stream asks an executor where its archive
//! holds canonical index `i`. The answer names the archive, the session
//! of the recording, and a raw position at or before the record. This
//! replays that recording from the position to its recorded limit and
//! decodes each fragment as an [`ExecTxRecord`]. The replay stays pinned
//! to the session, as a `tx_data` refetch does.

use kardamom_types::ExecTxRecord;
use tracing::{info, warn};

use super::recording::{Origin, Wanted};
use super::{ArchiveRefetcher, replay_sub_uri};
use crate::error::LogError;

/// One replay of the executor stream: the stream, the archive record id,
/// the session of the recording, and the raw start position.
#[derive(Clone, Copy, Debug)]
pub struct ExecReplay<'a> {
    pub stream_id: i32,
    pub archive_id: &'a str,
    pub session_id: i32,
    pub position: i64,
}

impl ArchiveRefetcher {
    /// Replay the `exec_txs` recording that `at` names, from its position
    /// to the recording's current recorded position, on the archive with
    /// the id `at.archive_id`. Deliver each record to `sink`. Return the
    /// delivered count.
    ///
    /// # Errors
    ///
    /// Returns an error if no endpoint of the archive is reachable, or if
    /// starting the bounded replay fails. Returns
    /// [`LogError::RangeAbsent`] when the archive holds no byte from the
    /// position.
    pub fn fetch_exec_records(
        &mut self,
        at: &ExecReplay<'_>,
        mut sink: impl FnMut(ExecTxRecord),
    ) -> Result<u64, LogError> {
        let endpoints = self.cfg.exec_txs_endpoints.of_archive(at.archive_id);
        let recs = self.list_or_rotate(&endpoints, at.stream_id)?;
        let wanted = Wanted {
            stream_id: at.stream_id,
            session_id: at.session_id,
            from: Origin::Raw(at.position),
        };
        let found = wanted.resolve(recs).map_err(|u| self.refuse(u))?;
        let Some(plan) = self.prepare_replay(&endpoints, &found, wanted)? else {
            return Ok(0);
        };
        let rec = &found.rec;
        // The subscription must exist before the replay starts; see the
        // matching comment in `fetch_tx_data`.
        let replay_stream = self.replay_stream_id();
        let sub_uri = replay_sub_uri(&plan.endpoint, rec.session_id);
        let (sub_id, mut rx) = self
            .ensure_runtime()?
            .open_subscription_with_id::<ExecTxRecord>(&sub_uri, replay_stream)?;
        let session = self.ensure_session(&endpoints)?;
        let replay_result = Self::start_bounded_replay(session, rec, replay_stream, &plan);
        let delivered = if replay_result.is_ok() {
            Self::drain(&mut rx, |(_, record)| sink(record))
        } else {
            0
        };
        self.close_replay_subscription(sub_id);
        if let Err(e) = replay_result {
            warn!(error = %e, "exec_txs replay start failed; rotating endpoint");
            self.rotate();
            return Err(e);
        }
        info!(
            archive_id = at.archive_id,
            session_id = at.session_id,
            from_raw = plan.from_raw,
            replay_len = plan.len,
            delivered,
            "exec_txs replay drained"
        );
        Ok(delivered)
    }
}
