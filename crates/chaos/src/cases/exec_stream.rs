//! The executor-stream check of the executor restart cases: the restarted
//! executor records a new session on its archive, and its recorded cursor
//! advances.

use crate::harness::Harness;
use crate::poll::{self, Budget};

/// The Aeron session id of the recorded `exec_txs` publication.
const SESSION_ID: &str = "kardamom_executor_exec_stream_session_id";

/// The recorded cursor of the executor stream.
const RECORDED_INDEX: &str = "kardamom_executor_exec_stream_recorded_index";

/// The recorded-publication session of each executor before the
/// injection, by executor index. `None` for an executor that exports no
/// session gauge.
pub(crate) struct ExecStreamSessions {
    before: Vec<Option<i64>>,
}

/// The restarted executor, its new session, and its first cursor value.
struct Restarted {
    index: usize,
    session: i64,
    recorded: i64,
}

impl ExecStreamSessions {
    /// Read the session gauge of every executor.
    pub(crate) async fn read(h: &Harness) -> Self {
        Self {
            before: Self::sample(h, SESSION_ID).await,
        }
    }

    /// One gauge of every executor, by executor index.
    async fn sample(h: &Harness, metric: &str) -> Vec<Option<i64>> {
        let mut values = Vec::with_capacity(h.probes.executors.len());
        for i in 0..h.probes.executors.len() {
            values.push(h.probes.exec_metric(i, metric).await);
        }
        values
    }

    /// After the restart, wait for an executor whose session differs from
    /// its session before the injection, then for its recorded cursor to
    /// pass its first value. The check does not apply, and passes with a
    /// log line, when no executor exported the session before the
    /// injection (an image with no executor stream), or when no executor's
    /// cursor moves in the window (a canonical order that does not move).
    ///
    /// # Errors
    ///
    /// Returns an error when no executor shows a new session, or when the
    /// cursor of the restarted executor stays flat while a peer's moves.
    pub(crate) async fn assert_restart_records(
        &self,
        h: &Harness,
        ctx: &str,
    ) -> anyhow::Result<()> {
        if self.before.iter().all(Option::is_none) {
            crate::log(format!(
                "{ctx}: no executor exports {SESSION_ID}; the exec stream check does not apply"
            ));
            return Ok(());
        }
        let restarted = self.wait_new_session(h, ctx).await?;
        crate::log(format!(
            "{ctx}: executor-{} records a new exec_txs session {} (was {:?}); recorded index {}",
            restarted.index, restarted.session, self.before[restarted.index], restarted.recorded
        ));
        Self::wait_cursor_moves(h, ctx, &restarted).await
    }

    async fn wait_new_session(&self, h: &Harness, ctx: &str) -> anyhow::Result<Restarted> {
        let outcome = poll::until(Budget::secs(180, 3), |_| async move {
            Ok(self.restarted(h).await)
        })
        .await?;
        let (restarted, _) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: no executor shows a new exec_txs session after {}s (before: {:?})",
                t.as_secs(),
                self.before
            )
        })?;
        Ok(restarted)
    }

    /// The first executor that shows a new session and a cursor value.
    async fn restarted(&self, h: &Harness) -> Option<Restarted> {
        let sessions = Self::sample(h, SESSION_ID).await;
        let index = sessions
            .iter()
            .zip(&self.before)
            .position(|(now, before)| now.is_some() && now != before)?;
        let recorded = h.probes.exec_metric(index, RECORDED_INDEX).await?;
        Some(Restarted {
            index,
            session: sessions[index]?,
            recorded,
        })
    }

    async fn wait_cursor_moves(
        h: &Harness,
        ctx: &str,
        restarted: &Restarted,
    ) -> anyhow::Result<()> {
        let peers_before = Self::sample(h, RECORDED_INDEX).await;
        let outcome = poll::until(Budget::secs(120, 3), |_| async move {
            Ok(h.probes
                .exec_metric(restarted.index, RECORDED_INDEX)
                .await
                .filter(|&now| now > restarted.recorded))
        })
        .await?;
        if let poll::Outcome::Ready { value, elapsed } = outcome {
            crate::log(format!(
                "{ctx}: executor-{} recorded cursor {} -> {value} after {}s",
                restarted.index,
                restarted.recorded,
                elapsed.as_secs()
            ));
            return Ok(());
        }
        let peers_after = Self::sample(h, RECORDED_INDEX).await;
        let peer_moved = peers_before
            .iter()
            .zip(&peers_after)
            .any(|(before, after)| after > before);
        if peer_moved {
            return Err(crate::chaos_fail!(
                "{ctx}: executor-{} recorded cursor stays at {} while a peer's moves ({peers_before:?} -> {peers_after:?})",
                restarted.index,
                restarted.recorded
            ));
        }
        crate::log(format!(
            "{ctx}: no executor's recorded cursor moved; the exec stream check does not apply"
        ));
        Ok(())
    }
}
