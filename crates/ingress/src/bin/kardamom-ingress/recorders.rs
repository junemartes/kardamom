//! Per-shard tx_data archive-recorder threads, and the ready barrier.
//!
//! `kardamom_log::recorder::record_stream_until_stopped` runs the
//! recorder-thread body: connect a thread-confined archive session, start
//! recording, report the startup outcome, and hold until stop. The
//! da-watcher's tx_deposits recorder shares this function. This module owns
//! the per-shard fan-out and the barrier that `main` blocks on before it
//! serves RPC.
//!
//! The threads stay std threads: they hold Aeron archive sessions
//! (`!Send`). The seam to the async shell is tokio: a
//! [`CancellationToken`] for stop, and one `oneshot` per recorder for
//! readiness.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use kardamom_log::config::{AeronConfig, ChannelsConfig};
use kardamom_log::recorder::{RecorderKind, record_stream_until_stopped};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Readiness report of one recorder: its shard id and the recording id (or
/// the startup failure reason).
pub type RecorderReady = oneshot::Receiver<(u8, Result<i64, String>)>;

/// Spawns one archive recorder thread for each tx_data shard. Each thread
/// connects its own thread-confined archive session, starts recording its
/// shard's tx_data publication, reports its startup outcome on its
/// `oneshot`, and holds the recording alive until `stop` is cancelled.
/// The ArchivingMediaDriver runs the recording itself. The thread only
/// keeps the session connected and re-adopts an existing recording after
/// a restart.
///
/// Returns the join handles (for teardown) and one readiness receiver per
/// shard, in shard order.
pub fn spawn_tx_data_recorders(
    aeron_dir: Option<PathBuf>,
    channels: ChannelsConfig,
    aeron_cfg: AeronConfig,
    shards: u8,
    stop: &CancellationToken,
) -> (Vec<std::thread::JoinHandle<()>>, Vec<RecorderReady>) {
    (0..shards)
        .map(|sid| {
            let aeron_dir = aeron_dir.clone();
            let channels = channels.clone();
            let aeron_cfg = aeron_cfg.clone();
            let stop = stop.clone();
            let (ready_tx, ready_rx) = oneshot::channel();
            let handle = std::thread::Builder::new()
                .name(format!("ingress-tx-data-recorder-{sid}"))
                .spawn(move || {
                    if let Err(e) = record_stream_until_stopped(
                        aeron_dir.as_deref(),
                        &aeron_cfg,
                        &channels.tx_data_channel(sid),
                        channels.tx_data_stream_id(sid),
                        RecorderKind::TxData { sequencer_id: sid },
                        &stop,
                        |outcome| {
                            if let Ok(recording_id) = &outcome {
                                tracing::info!(
                                    shard = sid,
                                    recording_id = *recording_id,
                                    "ingress: recording tx_data shard"
                                );
                            }
                            let _ = ready_tx.send((sid, outcome));
                        },
                    ) {
                        tracing::error!(shard = sid, error = %e, "tx_data recorder exited with error");
                    }
                })
                .expect("spawn tx_data recorder thread");
            (handle, ready_rx)
        })
        .unzip()
}

/// Waits until every recorder thread reports readiness. Fails on the
/// first reported error, or on timeout. This is the barrier: publish
/// and RPC must not start before the recordings are active.
pub async fn wait_for_recorders(ready: Vec<RecorderReady>) -> Result<()> {
    // This budget is generous in total. The publications are already
    // open, so the recording normally starts within one catalog-poll
    // tick (about 500ms). The timeout only bounds a stuck or
    // unreachable archive.
    const RECORDER_READY_TIMEOUT: Duration = Duration::from_secs(60);
    let all = async {
        for rx in ready {
            match rx.await {
                Ok((sid, Ok(recording_id))) => {
                    tracing::info!(
                        shard = sid,
                        recording_id,
                        "tx_data recording confirmed active"
                    );
                }
                Ok((sid, Err(e))) => {
                    anyhow::bail!("tx_data recorder for shard {sid} failed to start: {e}");
                }
                Err(_) => {
                    anyhow::bail!("a tx_data recorder thread exited before reporting readiness");
                }
            }
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
