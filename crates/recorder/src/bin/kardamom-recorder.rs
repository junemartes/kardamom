//! `kardamom-recorder`: the deployable recorder / quorum process (#38).
//!
//! Two modes, one per process (each is a single blocking loop on a
//! thread-confined Aeron client):
//!
//!   * **record** (default): connect the Archive, start (or adopt) the
//!     recording for this node's `--kind`, and run the fsync-watermark loop —
//!     polling the durable recording position and publishing this recorder's
//!     [`FsyncWatermark`]. Every recorder node runs one of these.
//!
//!   * **aggregate** (`--aggregate --no-record`): subscribe to all N
//!     per-recorder fsync watermarks, compute the Q-of-N quorum, and publish
//!     the [`QuorumWatermark`] that ingress `--ack-policy on-quorum` gates on.
//!     Exactly one instance runs cluster-wide; it is a liveness-only singleton
//!     (Q durable copies exist regardless — if it dies, acks stall until Nomad
//!     restarts it, but no data is at risk).
//!
//! The recording started with `auto_stop=false` outlives this process, so a
//! restart re-adopts it (see [`kardamom_log::recorder`]). Shutdown is a clean
//! stop of the loop on SIGINT/SIGTERM; the Archive keeps the recording.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::{QuorumPublisher, WatermarkPublisher};
use kardamom_log::recorder::{Recorder, connect_archive, connect_client, run_watermark_loop};
use kardamom_log::subscriber::WatermarkSubscriber;
use kardamom_log::watermark::{QuorumState, run_quorum_loop};

#[derive(Clone, Debug, clap::ValueEnum, PartialEq, Eq)]
enum KindArg {
    /// Record the canonical tx_ordering stream (the quorum-fsynced B channel).
    TxOrdering,
    /// Record a per-sequencer tx_data stream (single-host fsync; requires
    /// `--sequencer-id`).
    TxData,
}

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-recorder",
    version,
    about = "kardamom recorder / quorum-watermark process"
)]
struct Args {
    /// Optional `LogConfig` TOML supplying `[channels]` / `[aeron]` /
    /// `[quorum]`. Unset ⇒ built-in single-host IPC defaults.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// This recorder's stable quorum identity (unique across the N recorders).
    #[arg(long)]
    recorder_id: u8,
    /// Aeron Media Driver directory (`aeron.dir`). Unset ⇒ the C client's
    /// default lookup.
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Which stream to record (record mode only).
    #[arg(long, value_enum, default_value = "tx-ordering")]
    kind: KindArg,
    /// Sequencer id for `--kind tx-data`. Required iff `--kind tx-data`.
    #[arg(long)]
    sequencer_id: Option<u8>,
    /// Disable recording. Only valid together with `--aggregate` (the quorum
    /// singleton records nothing).
    #[arg(long)]
    no_record: bool,
    /// Run the quorum-watermark aggregation loop instead of recording.
    #[arg(long)]
    aggregate: bool,
    /// Watermark / quorum poll cadence in milliseconds.
    #[arg(long, default_value_t = 1)]
    poll_interval_ms: u64,
}

/// Resolved run mode after validating the flag combination.
#[derive(Debug, PartialEq, Eq)]
enum Mode {
    Record,
    Aggregate,
}

/// Validate the flag combination and resolve the run mode. Factored out so the
/// rules are unit-testable without constructing an Aeron client.
fn resolve_mode(
    aggregate: bool,
    no_record: bool,
    kind: &KindArg,
    sequencer_id: Option<u8>,
) -> Result<Mode> {
    if aggregate {
        if !no_record {
            bail!(
                "--aggregate must be combined with --no-record: one process runs one loop, and the quorum aggregator records nothing"
            );
        }
        return Ok(Mode::Aggregate);
    }
    if no_record {
        bail!(
            "--no-record is only valid with --aggregate (there would be nothing for this process to do)"
        );
    }
    if *kind == KindArg::TxData && sequencer_id.is_none() {
        bail!("--kind tx-data requires --sequencer-id");
    }
    Ok(Mode::Record)
}

fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let mode = resolve_mode(
        args.aggregate,
        args.no_record,
        &args.kind,
        args.sequencer_id,
    )?;
    let cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;

    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = stop.clone();
        ctrlc::set_handler(move || stop.store(true, Ordering::SeqCst))
            .context("install signal handler")?;
    }
    let should_stop = {
        let stop = stop.clone();
        move || stop.load(Ordering::SeqCst)
    };
    let poll = Duration::from_millis(args.poll_interval_ms);

    match mode {
        Mode::Record => run_record(&args, &cfg, poll, should_stop),
        Mode::Aggregate => run_aggregate(&args, &cfg, should_stop),
    }
}

/// Record mode: start/adopt the recording and publish this recorder's fsync
/// watermark until shutdown.
fn run_record(
    args: &Args,
    cfg: &LogConfig,
    poll: Duration,
    should_stop: impl FnMut() -> bool,
) -> Result<()> {
    // Archive control client + a separate publishing client, both joined to
    // the node-local Media Driver (rusteron splits the archive and client
    // Aeron types; see kardamom_log::recorder).
    let session =
        connect_archive(args.aeron_dir.as_deref(), &cfg.aeron).context("connect archive")?;
    let client = connect_client(args.aeron_dir.as_deref()).context("connect aeron client")?;

    let recorder = match args.kind {
        KindArg::TxOrdering => {
            tracing::info!(recorder_id = args.recorder_id, "recording tx_ordering (B)");
            Recorder::start_b(
                session.archive,
                &cfg.channels,
                args.recorder_id,
                cfg.aeron.archive_dir.clone(),
            )
            .context("start_b")?
        }
        KindArg::TxData => {
            let sid = args
                .sequencer_id
                .expect("validated: --kind tx-data implies --sequencer-id");
            tracing::info!(
                recorder_id = args.recorder_id,
                sequencer_id = sid,
                "recording tx_data (A)"
            );
            Recorder::start_a(
                session.archive,
                &cfg.channels,
                args.recorder_id,
                sid,
                cfg.aeron.archive_dir.clone(),
            )
            .context("start_a")?
        }
    };

    let publisher = WatermarkPublisher::open(&client, &cfg.channels, args.recorder_id)
        .context("open WatermarkPublisher")?;

    tracing::info!(
        recording_id = recorder.recording_id(),
        "kardamom-recorder running; publishing fsync watermark"
    );
    run_watermark_loop(&recorder, &publisher, poll, should_stop).context("watermark loop")?;
    tracing::info!("kardamom-recorder: shutdown");
    Ok(())
}

/// Aggregate mode: subscribe to all N fsync-watermark streams and publish the
/// Q-of-N quorum watermark until shutdown.
fn run_aggregate(args: &Args, cfg: &LogConfig, should_stop: impl FnMut() -> bool) -> Result<()> {
    let client = connect_client(args.aeron_dir.as_deref()).context("connect aeron client")?;
    let n = cfg.quorum.n;
    let mut subs: Vec<WatermarkSubscriber> = (0..n)
        .map(|rid| {
            WatermarkSubscriber::open(
                &client,
                &cfg.channels.fsync_watermark_channel(rid as u8),
                cfg.channels.fsync_watermark_stream_id,
            )
        })
        .collect::<std::result::Result<_, _>>()
        .context("open watermark subscribers")?;
    let publisher =
        QuorumPublisher::open(&client, &cfg.channels).context("open QuorumPublisher")?;
    let mut state = QuorumState::new(cfg.quorum.n, cfg.quorum.q);

    tracing::info!(
        n = cfg.quorum.n,
        q = cfg.quorum.q,
        "kardamom-recorder aggregating quorum watermark"
    );
    run_quorum_loop(&mut subs, &mut state, &publisher, should_stop);
    tracing::info!("kardamom-recorder: shutdown");
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_mode_is_default() {
        let m = resolve_mode(false, false, &KindArg::TxOrdering, None).unwrap();
        assert_eq!(m, Mode::Record);
    }

    #[test]
    fn aggregate_requires_no_record() {
        // --aggregate alone (record still implied) is rejected.
        assert!(resolve_mode(true, false, &KindArg::TxOrdering, None).is_err());
        // --aggregate --no-record is the valid quorum singleton.
        assert_eq!(
            resolve_mode(true, true, &KindArg::TxOrdering, None).unwrap(),
            Mode::Aggregate
        );
    }

    #[test]
    fn no_record_without_aggregate_is_rejected() {
        assert!(resolve_mode(false, true, &KindArg::TxOrdering, None).is_err());
    }

    #[test]
    fn tx_data_requires_sequencer_id() {
        assert!(resolve_mode(false, false, &KindArg::TxData, None).is_err());
        assert_eq!(
            resolve_mode(false, false, &KindArg::TxData, Some(0)).unwrap(),
            Mode::Record
        );
    }
}
