//! Per-shard `tx_data` archive-recorder threads, and the ready barrier.
//!
//! `kardamom_log::recorder::record_stream_until_stopped` runs the
//! recorder-thread body: connect a thread-confined archive session, start
//! recording, report the startup outcome, and hold until stop. The
//! da-watcher's `tx_deposits` recorder shares this function. This module owns
//! the per-shard fan-out and the barrier that `main` blocks on before it
//! serves RPC.
//!
//! The threads stay std threads: they hold Aeron archive sessions
//! (`!Send`). The seam to the async shell is tokio: the stop token inside
//! [`RecorderThreads`], and one `oneshot` per recorder for readiness.

use std::num::NonZeroU8;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_log::config::{AeronConfig, ChannelsConfig};
use kardamom_log::recorder::{RecorderKind, RecorderThreads, record_stream_until_stopped};
use tokio::sync::oneshot;

/// Readiness report of one recorder: its shard id and the recording id (or
/// the startup failure reason).
pub(crate) type RecorderReady = oneshot::Receiver<(u8, Result<i64, String>)>;

/// Spawns one archive recorder thread for each `tx_data` lane. Each thread
/// connects its own thread-confined archive session, starts recording its
/// shard's `tx_data` publication, reports its startup outcome on its
/// `oneshot`, and holds the recording alive until the returned
/// [`RecorderThreads`] drops. The `ArchivingMediaDriver` runs the
/// recording itself. The thread only keeps the session connected and
/// re-adopts an existing recording after a restart.
///
/// Returns the threads (dropping them stops and joins every recorder) and
/// one readiness receiver per lane, in lane order.
pub(crate) fn spawn_tx_data_recorders(
    aeron_dir: Option<&Path>,
    channels: &ChannelsConfig,
    aeron_cfg: &AeronConfig,
    lanes: NonZeroU8,
) -> Result<(RecorderThreads, Vec<RecorderReady>)> {
    let mut recorders = RecorderThreads::new();
    let ready = (0..lanes.get())
        .map(|sid| {
            let aeron_dir = aeron_dir.map(Path::to_path_buf);
            let channels = channels.clone();
            let aeron_cfg = aeron_cfg.clone();
            let (ready_tx, ready_rx) = oneshot::channel();
            recorders
                .spawn(format!("ingress-tx-data-recorder-{sid}"), move |stop| {
                    let outcome = record_stream_until_stopped(
                        aeron_dir.as_deref(),
                        &aeron_cfg,
                        &channels.tx_data_channel(sid),
                        channels.tx_data_stream_id(sid),
                        RecorderKind::TxData { sequencer_id: sid },
                        &stop,
                        |outcome| report_recorder_outcome(sid, outcome, ready_tx),
                    );
                    let _ = outcome
                        .inspect_err(|e| tracing::error!(shard = sid, error = %e, "tx_data recorder exited with error"));
                })
                .map(|()| ready_rx)
        })
        .collect::<std::io::Result<Vec<_>>>()
        .context("spawn tx_data recorder thread")?;
    Ok((recorders, ready))
}

/// Logs an active recording, then reports one shard's startup outcome
/// on its `oneshot`. This is the callback [`record_stream_until_stopped`]
/// calls once, from inside `spawn_tx_data_recorders`.
fn report_recorder_outcome(
    sid: u8,
    outcome: Result<i64, String>,
    ready_tx: oneshot::Sender<(u8, Result<i64, String>)>,
) {
    if let Ok(recording_id) = &outcome {
        tracing::info!(
            shard = sid,
            recording_id = *recording_id,
            "ingress: recording tx_data shard"
        );
    }
    let _ = ready_tx.send((sid, outcome));
}

/// Waits until every recorder thread reports readiness. Fails on the
/// first reported error, or on timeout. This is the barrier: publish
/// and RPC must not start before the recordings are active.
pub(crate) async fn wait_for_recorders(ready: Vec<RecorderReady>) -> Result<()> {
    // This budget is generous in total. The publications are already
    // open, so the recording normally starts within one catalog-poll
    // tick (about 500ms). The timeout only bounds a stuck or
    // unreachable archive.
    const RECORDER_READY_TIMEOUT: Duration = Duration::from_secs(60);
    let all = async {
        for rx in ready {
            report_ready(rx.await)?;
        }
        Ok(())
    };
    match tokio::time::timeout(RECORDER_READY_TIMEOUT, all).await {
        Ok(res) => res,
        Err(_) => anyhow::bail!(
            "timed out ({RECORDER_READY_TIMEOUT:?}) waiting for a tx_data recording to become active"
        ),
    }
}

/// One recorder's readiness report, for [`wait_for_recorders`]'s loop.
/// Logs on success. Fails on a reported startup error, or on the
/// recorder thread exiting before it reported readiness.
fn report_ready(
    result: Result<(u8, Result<i64, String>), oneshot::error::RecvError>,
) -> Result<()> {
    match result {
        Ok((sid, Ok(recording_id))) => {
            tracing::info!(
                shard = sid,
                recording_id,
                "tx_data recording confirmed active"
            );
            Ok(())
        }
        Ok((sid, Err(e))) => {
            anyhow::bail!("tx_data recorder for shard {sid} failed to start: {e}");
        }
        Err(_) => {
            anyhow::bail!("a tx_data recorder thread exited before reporting readiness");
        }
    }
}
