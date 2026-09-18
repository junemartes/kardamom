//! Stack configuration: [`StackConfig`] and the L2 genesis choice.

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;

use super::services::IngressOptions;

/// The dev chain id (`deploy/cluster/config/genesis/dev.toml`).
pub const DEV_CHAIN_ID: NonZeroU64 = NonZeroU64::new(412_346).unwrap();

/// Default shard count: [`StackConfig::default`]. Matches the deployed
/// cluster's shard count.
const DEFAULT_SHARDS: NonZeroU32 = NonZeroU32::new(2).unwrap();

const DEFAULT_CLUSTER_TICK_MS: NonZeroU64 = NonZeroU64::new(250).unwrap();

/// Stack settings a scenario can tune. The defaults match the deployed
/// shape where it matters (shards=2, like the cluster), and use a
/// test-friendly value where it does not (250 ms boundary ticks).
#[allow(
    clippy::struct_excessive_bools,
    reason = "each bool below is an independent, orthogonal bring-up toggle (not a state \
               machine), and scenarios set only the ones they need with \
               ..StackConfig::default(); a flags enum would not reduce the call-site noise"
)]
pub struct StackConfig {
    pub shards: NonZeroU32,
    pub sealer_members: NonZeroUsize,
    pub cluster_tick_ms: NonZeroU64,
    pub ingress: IngressOptions,
    /// Also run `kardamom-validator` (used by the consistency and
    /// divergence-detection tests). Off by default: the nonce and RPC
    /// scenarios do not need it, and stacks stay lighter without it.
    pub validator: bool,
    /// Run the validator with `--parallel-validation` (the deployed
    /// cluster's mode: seeded BAL batches on the whole-block path). The
    /// consistency and two-stacks scenarios set it so the validator's
    /// verdict covers the whole-block arm, not just the streaming one.
    pub validator_parallel: bool,
    /// Run the validator with `--serve-feed` (the public-validator role):
    /// outbox extraction + the WS feed surfaces. The bound address lands
    /// in `<root>/validator-feed.addr`; see
    /// [`crate::harness::LocalStack::validator_feed_url`].
    pub validator_serve_feed: bool,
    /// The sealer's remote-origin allowlist
    /// (`-Dkardamom.cluster.remoteOrigins`): the peer chain ids whose
    /// kind-5 records the cluster seals. Empty disables interop, and every
    /// remote-origin record is rejected. The xchain scenarios set it.
    pub remote_origins: Vec<u64>,
    /// Trie shadow-check cadence for the validator. `Some(1)` checks every
    /// block, the semantics-suite default. The cluster runs with 8.
    pub trie_shadow_check: Option<NonZeroU64>,
    /// The L2 chain id. Defaults to [`DEV_CHAIN_ID`] (what every genesis TOML
    /// in the repo declares). A different id makes launch materialise a
    /// patched copy of the genesis into the stack root (same alloc + predeploy
    /// blobs, overridden `chain_id` line) — the two-stack interop scenario
    /// needs a distinct chain B without duplicating predeploy bytecode in a
    /// sibling TOML (the deployer's genesis drift guard stays pointed at the
    /// canonical files).
    pub chain_id: NonZeroU64,
    /// Which L2 genesis to run. Bridge scenarios need
    /// [`Genesis::DevWithdrawals`] for the `L2ToL1MessagePasser` predeploy.
    pub genesis: Genesis,
    /// Bring up anvil, the bridge contracts, the da-watcher, and, when a
    /// validator runs, its L1 output attester.
    pub l1: bool,
    /// Record `tx_data` into the shared Aeron archive (the ingress
    /// `--archive-durability` flag) and turn on the consumers' join-miss
    /// refetch. Crash recovery needs this: a restarted consumer replays
    /// canonical records from its persisted cursor, but the envelopes for
    /// those records were published while it was down, so only the
    /// archive still has them. This costs a recorder-startup barrier at
    /// bring-up, so it is opt-in.
    pub archive_durability: bool,
    /// Route the validator's L1 reads through the mock verified endpoint
    /// (`l1_verified`), the way production routes them through a light
    /// client. The da-watcher keeps talking to anvil directly. Only the
    /// verifier's view is interposed, so a fault isolates to verification
    /// and does not also corrupt the epochs being produced.
    pub verified_l1: bool,
}

/// The L2 genesis a stack boots from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Genesis {
    /// `deploy/cluster/config/genesis/dev.toml`: 18 prefunded dev accounts,
    /// with no withdrawal predeploy. This is what the deployed cluster
    /// runs.
    ClusterDev,
    /// `chains/dev-withdrawals.toml`: the `L2ToL1MessagePasser` predeploy
    /// at `0x42…16`, but only account #0 is prefunded.
    DevWithdrawals,
    /// `chains/dev-interop.toml` — the cross-chain `Outbox`/`Inbox`
    /// predeploys at `0x42…E0`/`0x42…E1`, only account #0 prefunded. The
    /// xchain scenario needs it.
    DevInterop,
}

impl Genesis {
    pub(super) fn path(self, repo: &std::path::Path) -> PathBuf {
        match self {
            Genesis::ClusterDev => repo.join("deploy/cluster/config/genesis/dev.toml"),
            Genesis::DevWithdrawals => repo.join("chains/dev-withdrawals.toml"),
            Genesis::DevInterop => repo.join("chains/dev-interop.toml"),
        }
    }
}

impl Default for StackConfig {
    fn default() -> Self {
        Self {
            shards: DEFAULT_SHARDS,
            sealer_members: NonZeroUsize::MIN,
            cluster_tick_ms: DEFAULT_CLUSTER_TICK_MS,
            ingress: IngressOptions::default(),
            validator: false,
            validator_parallel: false,
            validator_serve_feed: false,
            remote_origins: Vec::new(),
            trie_shadow_check: Some(NonZeroU64::MIN),
            chain_id: DEV_CHAIN_ID,
            genesis: Genesis::ClusterDev,
            l1: false,
            archive_durability: false,
            verified_l1: false,
        }
    }
}
