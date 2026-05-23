//! Configuration types for the log subsystem.
//!
//! Loaded from TOML at process start; passed to the supervisor and the
//! quorum aggregator. No global state.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Static identifier for this host's recorder. Must be unique across N recorders.
pub type RecorderId = u8;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogConfig {
    pub recorder_id: RecorderId,
    pub aeron: AeronConfig,
    pub channels: ChannelsConfig,
    pub fsync: FsyncConfig,
    pub quorum: QuorumConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AeronConfig {
    /// Directory the Media Driver uses for its shared-memory ring buffers.
    /// Must be on tmpfs for low latency. Default: `/dev/shm/aeron-kardamom`.
    pub aeron_dir: PathBuf,

    /// Directory the Archive uses for its segment files.
    pub archive_dir: PathBuf,

    /// Path to the Aeron Media Driver binary (jar or native). Spawned by supervisor.
    pub media_driver_cmd: Vec<String>,

    /// Path to the Aeron Archive runner. Spawned by supervisor.
    pub archive_cmd: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelsConfig {
    /// Channel B: canonical tx log. Recorded.
    pub b_channel: String,
    pub b_stream_id: i32,

    /// Channel C: receipts + block boundaries. Not recorded.
    pub c_channel: String,
    pub c_stream_id: i32,

    /// Receipt-cache channel: `CachedReceipt` messages for proxy/RPC consumers.
    /// Not recorded.
    pub receipt_cache_channel: String,
    pub receipt_cache_stream_id: i32,

    /// Per-recorder fsync watermark publication, parameterized by recorder_id.
    /// e.g. "aeron:ipc?alias=fsync-wm-{rid}".
    pub fsync_watermark_channel_template: String,
    pub fsync_watermark_stream_id: i32,

    /// Aggregated quorum watermark.
    pub quorum_watermark_channel: String,
    pub quorum_watermark_stream_id: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FsyncConfig {
    /// `O_DIRECT` mirror file path. Sidecar writes the recorder's bytes here
    /// and fsyncs this file.
    pub mirror_path: PathBuf,

    /// io_uring submission queue depth. 256 is a good default for sustained throughput.
    pub uring_entries: u32,

    /// How often (number of completed fsyncs) to publish a watermark.
    /// 1 = every fsync; 16 = every 16th. Higher = lower watermark CPU, higher tail latency.
    pub watermark_publish_every: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuorumConfig {
    /// Total recorders.
    pub n: usize,
    /// Required for quorum (Q ≤ N). Default Q=2 for N=3.
    pub q: usize,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            recorder_id: 0,
            aeron: AeronConfig {
                aeron_dir: PathBuf::from("/dev/shm/aeron-kardamom"),
                archive_dir: PathBuf::from("/var/lib/kardamom/archive"),
                media_driver_cmd: vec!["aeron-media-driver".into()],
                archive_cmd: vec!["aeron-archive".into()],
            },
            channels: ChannelsConfig {
                b_channel: "aeron:udp?endpoint=224.0.1.1:40001".into(),
                b_stream_id: 1001,
                c_channel: "aeron:udp?endpoint=224.0.1.1:40002".into(),
                c_stream_id: 1002,
                receipt_cache_channel: "aeron:udp?endpoint=224.0.1.1:40003".into(),
                receipt_cache_stream_id: 1003,
                fsync_watermark_channel_template: "aeron:udp?endpoint=224.0.1.1:4010{rid}".into(),
                fsync_watermark_stream_id: 1010,
                quorum_watermark_channel: "aeron:udp?endpoint=224.0.1.1:40020".into(),
                quorum_watermark_stream_id: 1020,
            },
            fsync: FsyncConfig {
                mirror_path: PathBuf::from("/var/lib/kardamom/mirror.bin"),
                uring_entries: 256,
                watermark_publish_every: 1,
            },
            quorum: QuorumConfig { n: 3, q: 2 },
        }
    }
}
