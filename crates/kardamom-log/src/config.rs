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

    /// Aeron Archive `fileSyncLevel` for segment data files.
    /// 0 = no fsync (page cache only), 1 = fdatasync per frame, 2 = fsync per frame.
    /// Default 1: per-frame fdatasync gives byte-durable recording positions on
    /// PLP NVMe, at the cost of a per-frame fdatasync round-trip.
    pub file_sync_level: u8,

    /// Aeron Archive `catalog.fileSyncLevel` for the recording catalog metadata file.
    /// Default 1: the catalog is tiny and updated infrequently, so fsync is cheap.
    pub catalog_file_sync_level: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelsConfig {
    /// Channel A[i]: per-sequencer **exclusive** publisher of full
    /// `TxEnvelope` bytes (D-Sh12). One stream per sequencer. URI template
    /// substitutes `{sid}` with the sequencer id (e.g.
    /// `"aeron:ipc?alias=a-{sid}"`); stream id is
    /// `a_stream_id_base + sequencer_id`.
    pub a_channel_template: String,
    pub a_stream_id_base: i32,

    /// Channel B: canonical orderer carrying tiny `ChannelBMessage`
    /// records (TxRef + sealer-emitted boundary markers). Recorded.
    pub b_channel: String,
    pub b_stream_id: i32,

    /// Channel C: receipts + block boundaries. Not recorded.
    pub c_channel: String,
    pub c_stream_id: i32,

    /// Receipt-cache channel: `CachedReceipt` messages for proxy/RPC consumers.
    /// Not recorded.
    pub receipt_cache_channel: String,
    pub receipt_cache_stream_id: i32,

    /// Channel-B per-recorder fsync watermark publication, parameterized by
    /// recorder_id. e.g. "aeron:ipc?alias=fsync-wm-b-{rid}".
    pub fsync_watermark_channel_template: String,
    pub fsync_watermark_stream_id: i32,

    /// Channel-A per-sequencer fsync watermark publication. D-Sh12: each
    /// channel A has its own fsync sidecar publishing
    /// `fsynced_position_a[i]` to its own watermark stream. URI template
    /// substitutes `{sid}` with the sequencer id. Stream id is
    /// `fsync_watermark_a_stream_id_base + sequencer_id`.
    pub fsync_watermark_a_channel_template: String,
    pub fsync_watermark_a_stream_id_base: i32,

    /// Aggregated quorum watermark (channel B).
    pub quorum_watermark_channel: String,
    pub quorum_watermark_stream_id: i32,
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
                file_sync_level: 1,
                catalog_file_sync_level: 1,
            },
            channels: ChannelsConfig {
                a_channel_template: "aeron:ipc?alias=a-{sid}".into(),
                a_stream_id_base: 2000,
                b_channel: "aeron:udp?endpoint=224.0.1.1:40001".into(),
                b_stream_id: 1001,
                c_channel: "aeron:udp?endpoint=224.0.1.1:40002".into(),
                c_stream_id: 1002,
                receipt_cache_channel: "aeron:udp?endpoint=224.0.1.1:40003".into(),
                receipt_cache_stream_id: 1003,
                fsync_watermark_channel_template: "aeron:udp?endpoint=224.0.1.1:4010{rid}".into(),
                fsync_watermark_stream_id: 1010,
                fsync_watermark_a_channel_template: "aeron:ipc?alias=fsync-wm-a-{sid}".into(),
                fsync_watermark_a_stream_id_base: 1030,
                quorum_watermark_channel: "aeron:udp?endpoint=224.0.1.1:40020".into(),
                quorum_watermark_stream_id: 1020,
            },
            quorum: QuorumConfig { n: 3, q: 2 },
        }
    }
}
