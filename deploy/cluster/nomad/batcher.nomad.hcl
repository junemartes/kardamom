# kardamom-batcher is a live service. It tails the canonical ordering
# from the Aeron Cluster egress, joining tx_data exactly like the
# validator's front end. It packs KAR1 into zstd blob batches as
# boundaries arrive, and posts each to the in-cluster anvil L1
# (`KardamomL2Settlement.postBatch`, EIP-4844 blob txs), recording blob
# bytes in the DA store for `kardamom-reconstruct`.
#
# Durability: L1's `lastBatchIndex` and `BatchPosted` events are the
# record of what has posted. The cursor file under
# /opt/kardamom/batcher holds the ordering-stream position matching
# that record, written only after a confirmed post. A restart replays
# from the cursor, and skips blocks L1 already covers. See
# docs/agents/batcher-live-l1-spec.md.
#
# Placement: the aux node, next to the validator and da-watcher,
# outside the chaos suite's blast radius. Ports on the aux node:
# cluster egress 40231, refetch 40133/40143, metrics 9002 (the
# validator holds 40230/40131/40141/9006).
#
# scripts/deploy.sh deploys the settlement address, with
# kardamom-deploy against anvil, and injects it at submit time:
#   nomad run -var 'settlement_address=0x<addr>' batcher.nomad.hcl
# The batcher EOA is anvil dev account #2, pre-funded. Its key below is
# the public anvil dev mnemonic key. Real deployments must inject a
# real secret instead; this is the first key plumbing in
# deploy/cluster, and the spec flags it.
#
# This job uses file() for its templates, so submit it from the
# deploy/cluster/ directory. scripts/deploy.sh does this.

variable "settlement_address" {
  type = string
  # This is a placeholder. Replace it with `-var
  # settlement_address=0x...` at submit time.
  default = "0x0000000000000000000000000000000000000000"
}

variable "batcher_key" {
  type = string
  # anvil dev account #2. This public dev key matches crates/e2e
  # BATCHER_KEY.
  default = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a"
}

# Digest-pinned image. scripts/deploy.sh
# passes the repo:tag@sha256:... reference captured at push time
# (deploy/cluster/images.digests). The empty default falls back to the
# mutable :dev tag in the task config. That fallback is a dev
# affordance for manual `nomad job run` during debugging, not a
# production path.
variable "image_ref" {
  type        = string
  description = "Digest-pinned image reference (repo:tag@sha256:...) from the deploy's push manifest. Empty = mutable :dev tag fallback (dev-only)."
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

    # Restarts are the recovery loop. The cursor file and L1 reconcile
    # make a restart resume exactly where the last confirmed post left
    # off. Unlike the validator, there is no divergence signal to
    # preserve, so this keeps restarting (mode=delay). A wedged L1, or
    # an aged-out cluster replay, fail-stops repeatedly, and surfaces
    # through the semantics shard's l1-batch assertion instead.
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
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it. On the pinned path, the
        # 1.9.5 driver pulls the tag but resolves the image by digest,
        # so the pin holds.
        force_pull = true
        # Read-only rootfs. The batcher's
        # writable surfaces are the cursor file, the DA blob store,
        # and the aeron directory. All are explicit bind mounts below,
        # plus Nomad's alloc, local, and secrets mounts. cluster-e2e
        # validates this.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          # The cursor file and DA blob store live under the persistent
          # mount.
          "/opt/kardamom/batcher:/opt/kardamom/batcher",
        ]
        args = [
          "--live",
          "--dry-run=false",
          "--config", "/local/batcher.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          # This node's cluster-egress (response) endpoint, for the
          # batcher's own cluster client session: 40231, distinct
          # from the validator's 40230 on the same node.
          "--cluster-egress-endpoint", "${meta.node_ip}:40231",
          "--shards", "2",
          # Join-miss archive refetch (tx_data and tx_deposits). Same
          # contract as the validator's flags, with distinct ports on
          # the shared aux node.
          "--replay-destination-endpoint", "${meta.node_ip}:40133",
          "--archive-control-response-endpoint", "${meta.node_ip}:40143",
          "--l1-rpc", "http://192.168.56.10:8546",
          "--settlement", "${var.settlement_address}",
          "--da-store", "/opt/kardamom/batcher/da",
          "--cursor-file", "/opt/kardamom/batcher/cursor.json",
          # Group a few blocks per batch. The sealer emits about 1
          # boundary a second even when idle, and dense DA coverage
          # means empty blocks get posted too. Grouping keeps idle L1
          # traffic to about 1 tx every 5 seconds.
          "--blocks-per-batch", "5",
          "--flush-ms", "3000",
          # The L2 chain id. The records commitment digests each
          # remote-epoch message leaf, which commits to this id. Same
          # value as the executor and validator jobs.
          "--chain-id", "412346",
        ]
      }

      env {
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9002"
        KARDAMOM_L1_KEY       = "${var.batcher_key}"
      }

      # Cluster LogConfig (UDP multicast channels), read through
      # --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      # [cluster] ingress endpoints. Same contract as the executor's
      # config.
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
