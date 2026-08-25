//! CLI surface of the executor binary (schema + docs). The runtime wiring
//! lives in `main.rs`; state recovery in `state.rs`; role adapters in
//! `wiring.rs`.

use std::path::PathBuf;

use clap::Parser;
use kardamom_engine::bin_support::StateDurabilityArg;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-executor",
    version,
    about = "kardamom executor process"
)]
pub(crate) struct Args {
    /// Path to the TOML config file (schema: `ExecutorConfig`).
    #[arg(long)]
    pub(crate) config: PathBuf,
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    /// Unset ⇒ built-in single-host IPC defaults (preserves local/e2e
    /// behaviour); multi-host deployments point this at the rendered UDP
    /// channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    pub(crate) log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    pub(crate) aeron_dir: Option<PathBuf>,
    /// This replica's index, used as the per-replica tx_receipts MDS endpoint
    /// selector (`channels.tx_receipts_endpoint(recorder_id)`). In the cluster
    /// this is wired from `${NOMAD_ALLOC_INDEX}` (the executor job is
    /// count-based with `distinct_hosts`), matching the co-located recorder's
    /// id. Only consulted when `tx_receipts_mds_enabled()`; the legacy shared
    /// single-channel path ignores it.
    #[arg(long, env = "KARDAMOM_RECORDER_ID", default_value_t = 0)]
    pub(crate) recorder_id: u32,
    /// Number of tx_data shards to subscribe to (defaults to 8 — matches
    /// the default `partition_count` in the sequencer).
    #[arg(long, default_value_t = 8)]
    pub(crate) shards: u8,
    /// Execute blocks through the Block-STM engine (block-at-a-time; the
    /// streaming P3 pipeline is the follow-up). Off = the streaming
    /// per-tx path, byte-for-byte as before. Output is byte-identical
    /// either way — receipts, deltas and published BALs are the same
    /// artifacts; the validator cross-check fail-stops on any drift.
    #[arg(long, env = "KARDAMOM_PARALLEL_EXECUTION", default_value_t = false)]
    pub(crate) parallel_execution: bool,
    /// Worker threads for --parallel-execution. 0 = auto
    /// (`min(available_parallelism, 8)`). Hard-capped at 40: the mdbx
    /// reader-slot budget (`MAX_READERS = 64`) reserves the rest.
    #[arg(long, env = "KARDAMOM_EXECUTION_WORKERS", default_value_t = 0)]
    pub(crate) execution_workers: usize,
    /// This node's cluster-egress endpoint `ip:port` (cluster mode). Overrides/sets
    /// the [cluster] egress_channel as `aeron:udp?endpoint=<ip:port>`. Injected per
    /// node by the Nomad job as ${meta.node_ip}:<cluster_egress_port>.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    pub(crate) cluster_egress_endpoint: Option<String>,
    /// L2 chain id (used for revm).
    #[arg(long, default_value_t = 1)]
    pub(crate) chain_id: u64,
    /// Deprecated / ignored: the executor's startup block is now read from the
    /// persisted state cursor (`last_committed_block`, 0 for a fresh genesis
    /// DB). Kept so existing invocations don't error; the value is not used.
    #[arg(long, default_value_t = 1)]
    pub(crate) initial_block: u64,
    /// Path to a genesis TOML (schema: `kardamom_types::Genesis`). The
    /// chain id is taken from this file (must match `--chain-id` if both
    /// are set), and every `[[alloc]]` entry seeds the in-memory state
    /// DB with the listed balance / nonce / code so revm has account
    /// state to debit on the first transaction from each sender.
    #[arg(long)]
    pub(crate) chain: Option<PathBuf>,
    /// Directory for the libmdbx state database. The Nomad executor job mounts
    /// a persistent volume here so chain state survives restarts.
    #[arg(
        long,
        env = "KARDAMOM_STATE_DIR",
        default_value = "/opt/kardamom/state"
    )]
    pub(crate) state_dir: PathBuf,
    /// State durability mode. `durable` fdatasyncs on every block commit (the
    /// production default); `safe-no-sync` skips the fsync (tests / ephemeral
    /// runs only — unsafe on real hosts).
    #[arg(long, value_enum, default_value_t = StateDurabilityArg::Durable)]
    pub(crate) state_durability: StateDurabilityArg,
    /// UDP endpoint (`host:port`) on this node where **refetched** tx_data /
    /// tx_deposits fragments land. A canonical ref whose envelope never
    /// arrived on the live multicast (image lapse, blackout, restart
    /// down-window) is recovered in-band: the reader replays the missing range
    /// from the remote durability archives (`tx_data_archive_endpoints` /
    /// `tx_deposits_archive_endpoints` in channels.toml) onto this endpoint.
    /// Unset ⇒ refetch disabled (single-host/IPC runs); a lost envelope is
    /// then fatal after the join timeout. (tx_ordering crash recovery is
    /// handled by the Aeron Cluster client's REPLAY_FROM, not this path.)
    #[arg(long, env = "KARDAMOM_REPLAY_DESTINATION")]
    pub(crate) replay_destination_endpoint: Option<String>,
    /// UDP endpoint (`host:port`) on this node for the refetch client's
    /// archive-control RESPONSES (the control connection to a remote archive
    /// is UDP in both directions). Required alongside
    /// `--replay-destination-endpoint` for refetch to engage.
    #[arg(long, env = "KARDAMOM_ARCHIVE_CONTROL_RESPONSE")]
    pub(crate) archive_control_response_endpoint: Option<String>,
    /// Directory for periodic state checkpoints (fast cold-start recovery). When
    /// set, a wiped/empty `state_dir` is restored from the newest checkpoint here
    /// before startup (replaying only the tail instead of re-syncing from
    /// genesis), and — if `checkpoint_interval_secs > 0` — new checkpoints are
    /// written here as the chain advances. A peer's checkpoint dir is a valid
    /// restore source (executor replicas are deterministic at the same block).
    #[arg(long, env = "KARDAMOM_CHECKPOINT_DIR")]
    pub(crate) checkpoint_dir: Option<PathBuf>,
    /// Interval, in seconds, between periodic state checkpoints. 0 disables
    /// checkpoint creation (restore-only). Ignored unless `checkpoint_dir` is set.
    #[arg(long, default_value_t = 0)]
    pub(crate) checkpoint_interval_secs: u64,
    /// Number of recent checkpoints to retain (older ones are pruned).
    #[arg(long, default_value_t = 3)]
    pub(crate) checkpoint_keep: u64,
    /// TCP address on which to serve this node's newest checkpoint to peer
    /// executors (`GET /checkpoint/latest`). Replicas are deterministic state
    /// machines, so any replica's checkpoint is a valid restore source for
    /// another. Requires `--checkpoint-dir`.
    #[arg(long, env = "KARDAMOM_CHECKPOINT_SERVE_ADDR")]
    pub(crate) checkpoint_serve_addr: Option<std::net::SocketAddr>,
    /// Comma-separated peer checkpoint servers (`host:port`) to fetch a
    /// checkpoint from when local state can't reach the chain: a fresh/wiped
    /// node whose genesis replay aged out of the cluster retention window, or
    /// a resuming node whose cursor did (`REPLAY_UNAVAILABLE`). Requires
    /// `--checkpoint-dir`.
    #[arg(long, env = "KARDAMOM_CHECKPOINT_PEERS", value_delimiter = ',')]
    pub(crate) checkpoint_peers: Vec<String>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9004")]
    pub(crate) metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    pub(crate) host_id: String,
}
