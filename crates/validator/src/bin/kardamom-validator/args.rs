//! CLI + file-config surface of `kardamom-validator`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_engine::bin_support::StateDurabilityArg;
use kardamom_engine::reader::cluster::ClusterConfig;

/// Top-level config the `kardamom-validator` binary deserializes from
/// `--config`. Same `[cluster]` section shape as the executor's.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub struct ValidatorFileConfig {
    /// Aeron Cluster (Raft) sealer client config. tx_ordering ALWAYS comes from
    /// the cluster egress — there is no non-cluster path.
    pub cluster: ClusterConfig,
}

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-validator",
    version,
    about = "kardamom validator node"
)]
pub struct Args {
    /// Path to the TOML config file (presence-checked; tuning is via flags).
    #[arg(long)]
    pub config: PathBuf,
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    pub log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    pub aeron_dir: Option<PathBuf>,
    /// Number of tx_data shards to subscribe to.
    #[arg(long, default_value_t = 8)]
    pub shards: u8,
    /// Number of executor replicas whose tx_receipts endpoints to attach when
    /// tx_receipts MDS is enabled. Falls back to `channels.tx_receipts_executor_count`.
    #[arg(long, env = "KARDAMOM_EXECUTOR_COUNT")]
    pub executor_count: Option<u32>,
    /// L2 chain id (used for revm).
    #[arg(long, default_value_t = 1)]
    pub chain_id: u64,
    /// Path to a genesis TOML (schema: `kardamom_types::Genesis`).
    #[arg(long)]
    pub chain: Option<PathBuf>,
    /// Directory for the libmdbx state database (the validator keeps its own).
    #[arg(
        long,
        env = "KARDAMOM_STATE_DIR",
        default_value = "/opt/kardamom/validator-state"
    )]
    pub state_dir: PathBuf,
    /// State durability mode.
    #[arg(long, value_enum, default_value_t = StateDurabilityArg::Durable)]
    pub state_durability: StateDurabilityArg,
    /// Local checkpoint staging dir for the replay-unavailable fallback
    /// (#143): peer checkpoints are fetched here and adopted on the next
    /// start. The validator never CREATES checkpoints (its state is derived);
    /// this is an adoption-only dir.
    #[arg(long, env = "KARDAMOM_CHECKPOINT_DIR")]
    pub checkpoint_dir: Option<PathBuf>,
    /// Executor checkpoint-serve addresses (`host:port`, comma-separated) to
    /// fetch from when the cluster refuses replay (cursor below the retention
    /// floor). Blocks through an adopted checkpoint are UNVERIFIED by this
    /// validator — the same trust class as #78 catch-up; the trustless
    /// alternative is rebuild-from-L1 (kardamom-reconstruct).
    #[arg(long, env = "KARDAMOM_CHECKPOINT_PEERS", value_delimiter = ',')]
    pub checkpoint_peers: Vec<String>,
    /// Enable the state-trie shadow-check: every N blocks, recompute the world
    /// state root by full rebuild and fail-stop on mismatch with the incremental
    /// walker (a canary against trie bugs). Absent ⇒ incremental only; `1` ⇒
    /// every block. Costs a full rebuild on the sampled blocks.
    #[arg(long, env = "KARDAMOM_TRIE_SHADOW_CHECK")]
    pub trie_shadow_check: Option<u64>,
    /// UDP endpoint (`host:port`) on this node where **refetched** tx_data /
    /// tx_deposits fragments land (join-miss recovery from the remote
    /// durability archives; see the executor's flag of the same name). Unset
    /// ⇒ refetch disabled; a lost envelope is then fatal after the join
    /// timeout. (tx_ordering recovery is the cluster client's replay, not
    /// this path.)
    #[arg(long, env = "KARDAMOM_REPLAY_DESTINATION")]
    pub replay_destination_endpoint: Option<String>,
    /// UDP endpoint (`host:port`) on this node for the refetch client's
    /// archive-control RESPONSES. Required alongside
    /// `--replay-destination-endpoint` for refetch to engage.
    #[arg(long, env = "KARDAMOM_ARCHIVE_CONTROL_RESPONSE")]
    pub archive_control_response_endpoint: Option<String>,
    /// This node's cluster-egress endpoint `ip:port`. Overrides/sets the
    /// [cluster] egress_channel as `aeron:udp?endpoint=<ip:port>`. Injected per
    /// node by the Nomad job as ${meta.node_ip}:<cluster_egress_port>.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    pub cluster_egress_endpoint: Option<String>,
    /// Address for the Prometheus /metrics HTTP listener. (9007: 9006 is the
    /// ingress default — running both locally with defaults must not race for
    /// one socket; see docs/observability.md.)
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9007")]
    pub metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    pub host_id: String,

    // --- L1 output attestation (all three required to enable) -------------
    /// L1 JSON-RPC endpoint the attester posts withdrawal outputs to.
    #[arg(long, env = "KARDAMOM_L1_RPC_URL")]
    pub l1_rpc_url: Option<String>,
    /// Address of the deployed `WithdrawalOutputOracle` proxy.
    #[arg(long, env = "KARDAMOM_OUTPUT_ORACLE")]
    pub output_oracle: Option<alloy_primitives::Address>,
    /// Address of the deployed `ETHLockbox` proxy. With `--l1-rpc-url`, turns
    /// on EPOCH VERIFICATION: every epoch on the canonical stream is
    /// re-derived from L1 and a mismatch is a divergence (phase 1 of
    /// docs/agents/l1-origin-deposit-derivation-spec.md). Without it the
    /// validator still checks the origin SEQUENCE (rules 1-2, which need no
    /// L1) but cannot check an epoch's contents.
    #[arg(long, env = "KARDAMOM_LOCKBOX")]
    pub lockbox: Option<alloy_primitives::Address>,
    /// Attester private key: raw hex, or `env:VAR` to read it from the
    /// environment (the deployer's key convention). Must be the oracle's
    /// permissioned `attester`.
    #[arg(long, env = "KARDAMOM_ATTESTER_KEY")]
    pub attester_key: Option<String>,
    /// Post one L1 output per this many L2 blocks.
    /// Re-execute each block as seeded PARALLEL batches driven by the
    /// EIP-7928 BAL (spec: bal-attribution-parallel-validation). Falls back
    /// to sequential per block when claims are unavailable or the block
    /// contains deposits, so liveness never depends on the BAL.
    #[arg(long, env = "KARDAMOM_PARALLEL_VALIDATION", default_value_t = false)]
    pub parallel_validation: bool,
    /// Spool anchored prover inputs (one frame per block: witness + MPT
    /// proofs + records + BAL, plus the expected public outputs) to this
    /// directory — the zkVM prover's queue (spec 3c). Entirely off the hot
    /// path; blocks the spool cannot pin a pre-state snapshot for are
    /// dropped with a counter, never awaited. Requires the trie-aware
    /// writer (the default).
    #[arg(long, env = "KARDAMOM_PROVE_BATCHES")]
    pub prove_batches: Option<std::path::PathBuf>,
    /// Transactions per parallel batch (scheduling granularity — independent
    /// of the BAL's attribution granularity). Only meaningful at wire
    /// granularity K = 1: at K > 1 (the K=8 wire default) batches are
    /// chunk-aligned to the frame's K, and the worker count below is the
    /// real parallelism knob.
    #[arg(long, env = "KARDAMOM_VALIDATION_BATCH_SIZE", default_value_t = 8)]
    pub validation_batch_size: usize,
    /// Worker threads in the parallel-validation pool. 0 = auto
    /// (`min(available_parallelism, 8)`). Hard-capped at 40: the mdbx
    /// reader-slot budget (`MAX_READERS = 64`) reserves the rest for the
    /// exec thread, RPC, and compaction.
    #[arg(long, env = "KARDAMOM_VALIDATION_WORKERS", default_value_t = 0)]
    pub validation_workers: usize,

    #[arg(long, env = "KARDAMOM_ATTESTER_POST_INTERVAL", default_value_t = 1)]
    pub attester_post_interval: u64,
}

/// Resolve the attester key flag: raw hex, or `env:VAR` (the deployer's key
/// convention) read from the environment.
pub fn resolve_attester_key(key: &str) -> Result<String> {
    match key.strip_prefix("env:") {
        Some(var) => {
            std::env::var(var).with_context(|| format!("read attester key from env var {var}"))
        }
        None => Ok(key.to_string()),
    }
}
