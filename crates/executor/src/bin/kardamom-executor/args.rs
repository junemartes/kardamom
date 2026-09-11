//! CLI surface of the executor binary (schema + docs). The runtime wiring
//! lives in `main.rs`; state recovery in `state.rs`; role adapters in
//! `wiring.rs`.

use std::num::{NonZeroU64, NonZeroUsize, ParseIntError};
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use kardamom_engine::bin_support::StateDurabilityArg;

/// Default `--checkpoint-keep`.
const DEFAULT_CHECKPOINT_KEEP: NonZeroU64 = NonZeroU64::new(3).unwrap();

/// `checkpoint_interval_secs`'s parsed value: `None` (from `"0"`) means
/// checkpointing is disabled. A newtype, not `Option<NonZeroU64>`
/// directly, because clap-derive's special-cased handling of an
/// `Option<T>` field expects `T` to come back from its value parser,
/// not `Option<T>`; wrapping it in a struct with its own `FromStr`
/// keeps clap's "one required arg" machinery off this field, and lets
/// the parser return the `None`-means-disabled value clap otherwise
/// reserves for "argument absent".
#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckpointInterval(pub(crate) Option<NonZeroU64>);

impl FromStr for CheckpointInterval {
    type Err = ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(|n| Self(NonZeroU64::new(n)))
    }
}

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
    /// Optional `LogConfig` TOML that supplies the Aeron `[channels]` config.
    /// If unset, the binary uses built-in single-host IPC defaults (this
    /// keeps local and end-to-end test behavior the same). Multi-host
    /// deployments point this at the rendered UDP channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    pub(crate) log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    pub(crate) aeron_dir: Option<PathBuf>,
    /// This replica's index. It selects the per-replica `tx_receipts` MDS
    /// endpoint (`channels.tx_receipts_endpoint(recorder_id)`). In the
    /// cluster, this comes from `${NOMAD_ALLOC_INDEX}` (the executor job is
    /// count-based, with `distinct_hosts`), and matches the co-located
    /// recorder's ID. The code uses it only when
    /// `tx_receipts_mds_enabled()` is true.
    #[arg(long, env = "KARDAMOM_RECORDER_ID", default_value_t = 0)]
    pub(crate) recorder_id: u32,
    /// Execute blocks through the Block-STM engine (block-at-a-time). When
    /// off, the binary uses the streaming per-tx path, with byte-for-byte
    /// identical output. Output is byte-identical either way: receipts,
    /// deltas, and published BALs are the same artifacts. The validator
    /// cross-check stops the process on any drift.
    #[arg(long, env = "KARDAMOM_PARALLEL_EXECUTION", default_value_t = false)]
    pub(crate) parallel_execution: bool,
    /// Worker threads for `--parallel-execution`. Unset means auto
    /// (`min(available_parallelism, 8)`). The hard cap is 40: the mdbx
    /// reader-slot budget (`MAX_READERS = 64`) reserves the rest.
    #[arg(long, env = "KARDAMOM_EXECUTION_WORKERS")]
    pub(crate) execution_workers: Option<NonZeroUsize>,
    /// This node's cluster-egress endpoint `ip:port` (cluster mode). It sets
    /// or overrides the `[cluster]` `egress_channel` as
    /// `aeron:udp?endpoint=<ip:port>`. The Nomad job injects it per node as
    /// `${meta.node_ip}:<cluster_egress_port>`.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    pub(crate) cluster_egress_endpoint: Option<String>,
    /// L2 chain id (used for revm).
    #[arg(long, default_value_t = 1)]
    pub(crate) chain_id: u64,
    /// Deprecated. Accepted and ignored: the executor reads its startup
    /// block from the persisted state cursor (`last_committed_block`, 0
    /// for a fresh genesis DB).
    #[arg(long, default_value_t = 1)]
    pub(crate) initial_block: u64,
    /// Path to a genesis TOML (schema: `kardamom_types::Genesis`). The chain
    /// ID comes from this file (it must match `--chain-id` if both are
    /// set). Each `[[alloc]]` entry seeds the in-memory state DB with the
    /// listed balance, nonce, and code, so revm has account state to debit
    /// on the first transaction from each sender.
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
    /// State durability mode. `durable` runs fdatasync on every block
    /// commit (the production default). `safe-no-sync` skips the fsync; use
    /// it only for tests or short-lived runs. It is unsafe on real hosts.
    #[arg(long, value_enum, default_value_t = StateDurabilityArg::Durable)]
    pub(crate) state_durability: StateDurabilityArg,
    /// UDP endpoint (`host:port`) on this node for refetched `tx_data` and
    /// `tx_deposits` fragments. A canonical reference whose envelope never
    /// arrived on the live multicast (image lapse, blackout, or a restart
    /// down-window) is recovered in-band: the reader replays the missing
    /// range from the remote durability archives
    /// (`tx_data_archive_endpoints` and `tx_deposits_archive_endpoints` in
    /// channels.toml) onto this endpoint. If unset, refetch is disabled
    /// (single-host and IPC runs), and a lost envelope is fatal after the
    /// join timeout. `tx_ordering` crash recovery uses the Aeron Cluster
    /// client's `REPLAY_FROM` instead of this path.
    #[arg(long, env = "KARDAMOM_REPLAY_DESTINATION")]
    pub(crate) replay_destination_endpoint: Option<String>,
    /// UDP endpoint (`host:port`) on this node for the refetch client's
    /// archive-control responses (the control connection to a remote
    /// archive is UDP in both directions). Required together with
    /// `--replay-destination-endpoint` for refetch to run.
    #[arg(long, env = "KARDAMOM_ARCHIVE_CONTROL_RESPONSE")]
    pub(crate) archive_control_response_endpoint: Option<String>,
    /// Directory for periodic state checkpoints (fast cold-start recovery).
    /// When set, a wiped or empty `state_dir` restores from the newest
    /// checkpoint here before startup, replaying only the tail instead of
    /// re-syncing from genesis. If `checkpoint_interval_secs > 0`, new
    /// checkpoints are written here as the chain advances. A peer's
    /// checkpoint dir is a valid restore source, because executor replicas
    /// are deterministic at the same block.
    #[arg(long, env = "KARDAMOM_CHECKPOINT_DIR")]
    pub(crate) checkpoint_dir: Option<PathBuf>,
    /// Interval, in seconds, between periodic state checkpoints. 0 disables
    /// checkpoint creation (restore-only). Ignored unless `checkpoint_dir` is set.
    #[arg(long, default_value = "0")]
    pub(crate) checkpoint_interval_secs: CheckpointInterval,
    /// Number of recent checkpoints to retain (older ones are pruned).
    /// Non-zero: keeping 0 checkpoints defeats the feature.
    #[arg(long, default_value_t = DEFAULT_CHECKPOINT_KEEP)]
    pub(crate) checkpoint_keep: NonZeroU64,
    /// TCP address that serves this node's newest checkpoint to peer
    /// executors (`GET /checkpoint/latest`). Replicas are deterministic
    /// state machines, so any replica's checkpoint is a valid restore
    /// source for another. Requires `--checkpoint-dir`.
    #[arg(long, env = "KARDAMOM_CHECKPOINT_SERVE_ADDR")]
    pub(crate) checkpoint_serve_addr: Option<std::net::SocketAddr>,
    /// Comma-separated peer checkpoint servers (`host:port`) to fetch a
    /// checkpoint from, when local state cannot reach the chain. This
    /// covers a fresh or wiped node whose genesis replay aged out of the
    /// cluster retention window, and a resuming node whose cursor did
    /// (`REPLAY_UNAVAILABLE`). Requires `--checkpoint-dir`.
    #[arg(long, env = "KARDAMOM_CHECKPOINT_PEERS", value_delimiter = ',')]
    pub(crate) checkpoint_peers: Vec<String>,
    /// Serve read-only account nonce queries (`eth_getTransactionCount`)
    /// on this address. The sequencers ask it for the committed nonce of a
    /// cold sender. Off when unset. See
    /// `docs/specs/dynamic-sequencer-sizing.md`, section 3.4.
    #[arg(long, env = "KARDAMOM_NONCE_QUERY_ADDR")]
    pub(crate) nonce_query_addr: Option<std::net::SocketAddr>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9004")]
    pub(crate) metrics_addr: std::net::SocketAddr,
    /// Host identifier. It is stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    pub(crate) host_id: String,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Args;

    /// `CheckpointInterval` gives clap one concrete type for this
    /// field. This test parses a real command line, so a clap
    /// definition-versus-access mismatch fails here.
    #[test]
    fn args_parse_with_only_the_required_flag() {
        Args::try_parse_from(["kardamom-executor", "--config", "executor.toml"])
            .expect("Args parses with just --config");
    }

    #[test]
    fn checkpoint_interval_secs_parses_zero_and_nonzero() {
        let a = Args::try_parse_from([
            "kardamom-executor",
            "--config",
            "executor.toml",
            "--checkpoint-interval-secs",
            "0",
        ])
        .expect("parses");
        assert!(a.checkpoint_interval_secs.0.is_none(), "0 means disabled");

        let a = Args::try_parse_from([
            "kardamom-executor",
            "--config",
            "executor.toml",
            "--checkpoint-interval-secs",
            "30",
        ])
        .expect("parses");
        assert_eq!(
            a.checkpoint_interval_secs.0.map(std::num::NonZeroU64::get),
            Some(30)
        );
    }
}
