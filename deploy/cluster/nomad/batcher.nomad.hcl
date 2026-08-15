# kardamom-batcher — LIVE service (#39): tails the canonical ordering from the
# Aeron Cluster egress (joining tx_data, exactly like the validator's front
# end), packs KAR1 → zstd → blob batches as boundaries arrive, and posts each
# to the in-cluster anvil L1 (`KardamomL2Settlement.postBatch`, EIP-4844 blob
# txs), recording blob bytes in the DA store for `kardamom-reconstruct`.
#
# Durability: L1's `lastBatchIndex` + `BatchPosted` events are the record of
# what has been posted; the cursor file under /opt/kardamom/batcher is the
# ordering-stream position matching that record, written only after a
# confirmed post. A restart replays from the cursor and skips blocks L1
# already covers — see docs/agents/batcher-live-l1-spec.md.
#
# Placement: the AUX node, next to validator + da-watcher (outside the chaos
# suite's blast radius). Ports on the aux node: cluster egress 40231, refetch
# 40133/40143, metrics 9002 (validator holds 40230/40131/40141/9006).
#
# The settlement address is deployed by scripts/deploy.sh (kardamom-deploy
# against anvil) and injected at submit time:
#   nomad run -var 'settlement_address=0x<addr>' batcher.nomad.hcl
# The batcher EOA is anvil dev account #2 (pre-funded); its key below is the
# public anvil dev mnemonic key — real deployments must inject a real secret
# instead (this is the first key plumbing in deploy/cluster; flagged in the
# spec).
#
# NOTE: this job uses file() for its templates, so submit it from the
# deploy/cluster/ directory (scripts/deploy.sh does this).

variable "settlement_address" {
  type = string
  # PLACEHOLDER — replace via `-var settlement_address=0x...` at submit time.
  default = "0x0000000000000000000000000000000000000000"
}

variable "batcher_key" {
  type = string
  # anvil dev account #2 (public dev key, matches crates/e2e BATCHER_KEY).
  default = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a"
}

# Digest-pinned image (attested-identity P0.1): scripts/deploy.sh passes the
# repo@sha256:... reference captured at push time (deploy/cluster/
# images.digests). The empty default falls back to the mutable :dev tag in
# the task config — a dev affordance for manual `nomad job run` during
# debugging, NOT a production path.
variable "image_ref" {
  type        = string
  description = "Digest-pinned image reference (repo@sha256:...) from the deploy's push manifest. Empty = mutable :dev tag fallback (dev-only)."
  default     = ""
}

job "batcher" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "aux"
  }

  group "batcher" {
    count = 1

    # Restarts are the recovery loop: the cursor file + L1 reconcile make a
    # restart resume exactly where the last confirmed post left off. Unlike
    # the validator there is no divergence signal to preserve — keep
    # restarting (mode=delay); a wedged L1 or an aged-out cluster replay
    # fail-stops repeatedly and surfaces via the semantics shard's l1-batch
    # assertion instead.
    restart {
      attempts = 3
      interval = "1m"
      delay    = "5s"
      mode     = "delay"
    }

    reschedule {
      delay          = "10s"
      delay_function = "exponential"
      max_delay      = "1m"
      unlimited      = true
    }

    update {
      max_parallel     = 1
      health_check     = "task_states"
      min_healthy_time = "10s"
      healthy_deadline = "2m"
    }

    network {
      mode = "host"
    }

    task "batcher" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "192.168.56.10:5000/kardamom-batcher:dev"
        # force_pull kept for the mutable-:dev fallback path (see executor
        # job); redundant-but-harmless for the digest-pinned path.
        force_pull = true
        # Read-only rootfs (attested-identity P0.3): the batcher's writable
        # surfaces — cursor file + DA blob store + aeron dir — are all
        # explicit bind mounts below, plus Nomad's alloc/local/secrets
        # mounts. Validated by the cluster-e2e suite.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          # Cursor file + DA blob store live under the persistent mount.
          "/opt/kardamom/batcher:/opt/kardamom/batcher",
        ]
        args = [
          "--live",
          "--dry-run=false",
          "--config", "/local/batcher.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          # This node's cluster-egress (response) endpoint for the batcher's
          # OWN cluster client session — 40231, distinct from the validator's
          # 40230 on the same node.
          "--cluster-egress-endpoint", "${meta.node_ip}:40231",
          "--shards", "2",
          # Join-miss archive refetch (tx_data / tx_deposits), same contract
          # as the validator's flags; distinct ports on the shared aux node.
          "--replay-destination-endpoint", "${meta.node_ip}:40133",
          "--archive-control-response-endpoint", "${meta.node_ip}:40143",
          "--l1-rpc", "http://192.168.56.10:8546",
          "--settlement", "${var.settlement_address}",
          "--da-store", "/opt/kardamom/batcher/da",
          "--cursor-file", "/opt/kardamom/batcher/cursor.json",
          # Group a few blocks per batch: the sealer emits ~1 boundary/s even
          # when idle, and dense DA coverage means empty blocks are posted
          # too — grouping keeps idle L1 traffic at ~1 tx / 5 s.
          "--blocks-per-batch", "5",
          "--flush-ms", "3000",
        ]
      }

      env {
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9002"
        KARDAMOM_L1_KEY       = "${var.batcher_key}"
      }

      # Cluster LogConfig (UDP multicast channels), consumed via --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      # [cluster] ingress endpoints (same contract as the executor's config).
      template {
        destination = "local/batcher.toml"
        data        = file("config/executor.toml")
      }

      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
