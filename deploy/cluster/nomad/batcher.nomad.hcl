# kardamom-batcher is a live service. It tails the canonical ordering
# from the Aeron Cluster egress, joining tx_data exactly like the
# validator's front end. It packs KAR1 into zstd payloads as boundaries
# arrive, disperses each through the EigenDA proxy
# (nomad/da-proxy.nomad.hcl), and posts the certificate to L1
# (`KardamomL2Settlement.postBatch`); `kardamom-reconstruct` reads the
# payloads back by certificate.
#
# Durability: L1's `lastBatchIndex` and `BatchPosted` events are the
# record of what has posted. The cursor file under
# /opt/kardamom/batcher holds the ordering-stream position matching
# that record, written only after a confirmed post. A restart replays
# from the cursor, and skips blocks L1 already covers. See
# docs/agents/batcher-live-l1-spec.md.
#
# A restart past the sealer's retention rebuilds the gap: the query
# endpoints of the executors and the validator (--block-refs-source)
# serve each block's transaction references, and the tx_data archives
# serve the bytes through the join-miss refetch. Retention is a latency
# while one state database and one archive survive.
#
# Placement: the aux node, next to the validator and da-watcher,
# outside the chaos suite's blast radius. Ports on the aux node:
# cluster egress and refetch on Nomad dynamic ports, metrics 9002 (the
# validator holds 9006 and its own dynamic ports).
#
# ansible/deploy.yml deploys the settlement address, with
# kardamom-deploy against anvil, and injects it at submit time:
#   nomad run -var 'settlement_address=0x<addr>' batcher.nomad.hcl
# The batcher EOA is anvil dev account #2, pre-funded. Its key below is
# the public anvil dev mnemonic key. Real deployments must inject a
# real secret instead; this is the first key plumbing in
# deploy/cluster, and the spec flags it.
#
# This job uses file() for its templates, so submit it from the
# deploy/cluster/ directory. ansible/deploy.yml does this.

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

# Digest-pinned image. ansible/deploy.yml
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

# The Aeron stall tolerance, in milliseconds: how long an Aeron party
# waits through a stalled peer before it declares the peer dead. Aeron's
# default is 10000, and production keeps it: a longer value delays the
# detection of a dead process. CI raises it to ride out host stalls.
variable "aeron_stall_tolerance_ms" {
  type        = number
  description = "The driver timeout of the Aeron clients of the job, in milliseconds. Aeron's default is 10000."
  default     = 10000

  validation {
    condition     = var.aeron_stall_tolerance_ms >= 1000 && floor(var.aeron_stall_tolerance_ms) == var.aeron_stall_tolerance_ms
    error_message = "The Aeron stall tolerance must be a whole number of milliseconds, at least 1000."
  }
}

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

variable "executor_count" {
  type        = number
  description = "The executor node count (node_classes.executor.count). The void voter ids come from it."
  default     = 3
}

# The inbox indexer's API. With it, a batcher whose node is fresh resumes
# just past the last posted batch (public #455). Empty: no indexer, the
# job's replay-from-genesis behavior.
# The settlement's deployment block: where a BatchPosted scan starts. A
# public endpoint caps a log query's range; 0 suits anvil.
variable "settlement_deploy_block" {
  type        = string
  description = "The settlement's deployment block on L1. Empty: 0."
  default     = ""
}

variable "indexer_url" {
  type        = string
  description = "The inbox indexer's JSON-RPC endpoint (nomad/l1-indexer.nomad.hcl). Empty: none."
  default     = ""
}

# The EigenDA proxy (nomad/da-proxy.nomad.hcl): the batcher disperses
# every payload through it and posts the certificate on L1.
variable "da_proxy" {
  type        = string
  description = "The EigenDA proxy's URL. The default is the in-cluster proxy by its Consul service record."
  default     = "http://kardamom-da-proxy.service.consul:3100"
}

# The posting cadence. The sealer closes about one block a second even
# when idle, and every block is posted, so a real L1 pays one post per
# blocks_per_batch seconds: 5 is right for anvil, 300 for a testnet.
variable "blocks_per_batch" {
  type        = string
  description = "Blocks per post. The default suits the in-cluster anvil; a real L1 takes a larger group."
  default     = "5"
}

variable "flush_ms" {
  type        = string
  description = "Post a group that holds a transaction after this wait, in milliseconds."
  default     = "3000"
}

variable "idle_flush_ms" {
  type        = string
  description = "Post a group of empty blocks after this wait, in milliseconds. Empty: the same as flush_ms."
  default     = ""
}

variable "l1_rpc" {
  type        = string
  description = "The L1 JSON-RPC endpoints, comma-separated, best first. A request falls back to the next on an error or a rate limit. The default is the in-cluster anvil by its Consul service record."
  default     = "http://anvil.service.consul:8546"
}

# The query endpoints that serve block references: every executor's
# (group_vars/all.yml, ports.executor_nonce_query) and the validator's
# (ports.validator_query), by the records the jobs register.
variable "executor_query_port" {
  type    = number
  default = 9024
}

variable "validator_query_port" {
  type    = number
  default = 9025
}

job "batcher" {
  datacenters = [var.datacenter]
  type        = "service"

  # The nodes whose role set holds batcher (group_vars/all.yml, node_classes).
  constraint {
    attribute = "${meta.roles}"
    operator  = "set_contains"
    value     = "batcher"
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

    # In place: a singleton with a static port restarts on its node.
    # Healthy by its /ready check: the feed loop runs over the restored
    # spool, so nothing is lost across the restart.
    update {
      max_parallel      = 1
      health_check      = "checks"
      min_healthy_time  = "15s"
      healthy_deadline  = "5m"
      progress_deadline = "10m"
      auto_revert       = false
    }

    network {
      mode = "host"
      # The metrics port, as a Consul service: monitoring scrapes the
      # service, not a node name.
      port "metrics" {
        static = 9002
      }
      # The cluster egress (response) port, unique per allocation. A
      # fixed port sat in the node's ephemeral range, where the shared
      # media driver's port-0 discovery sockets could take it first.
      port "egress" {}
      # The join-miss refetch ports: replayed fragments and archive
      # control responses. Nomad picks them per allocation, below the
      # ephemeral range, so no other process on the node holds them.
      port "replay" {}
      port "archive_response" {}
    }

    task "batcher" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-batcher:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it. On the pinned path, the
        # 1.9.5 driver pulls the tag but resolves the image by digest,
        # so the pin holds.
        force_pull = true
        # Read-only rootfs. The batcher's
        # writable surfaces are the cursor file and the aeron
        # directory. All are explicit bind mounts below,
        # plus Nomad's alloc, local, and secrets mounts. cluster-e2e
        # validates this.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          # The cursor file lives under the persistent mount.
          "/opt/kardamom/batcher:/opt/kardamom/batcher",
        ]
        args = concat(
          [
            "--live",
            "--dry-run=false",
            "--config", "/local/batcher.toml",
            "--log-config", "/local/channels.toml",
            "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
            # This allocation's cluster-egress (response) endpoint, for
            # the batcher's own cluster client session: the node IP and a
            # Nomad dynamic port, so it never clashes with the validator's
            # on the same node.
            "--cluster-egress-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_egress}",
            # The void voter id: the second id after the executors'
            # (cluster.nomad.hcl builds the sealer's voter list the same way).
            "--void-voter-id", format("%d", var.executor_count + 1),
            # Join-miss archive refetch (tx_data and tx_deposits). Same
            # contract as the validator's flags, on this allocation's
            # dynamic ports.
            "--replay-destination-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_replay}",
            "--archive-control-response-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_archive_response}",
            "--l1-rpc", var.l1_rpc,
            "--settlement", "${var.settlement_address}",
            "--da-proxy", var.da_proxy,
            "--cursor-file", "/opt/kardamom/batcher/cursor.json",
            "--spool-dir", "/opt/kardamom/batcher/spool",
            # The block references: the executors' query endpoints by
            # node name, then the validator's by its service record.
            "--block-refs-source", join(",", concat(
              [for i in range(var.executor_count) : "http://executor-${i}.node.${var.datacenter}.consul:${var.executor_query_port}"],
              ["http://kardamom-validator-query.service.${var.datacenter}.consul:${var.validator_query_port}"],
            )),
            # Group a few blocks per batch. The sealer emits about 1
            # boundary a second even when idle, and dense DA coverage
            # means empty blocks get posted too. Grouping keeps idle L1
            # traffic to about 1 tx every 5 seconds on anvil; a real L1
            # takes a larger group (the workloads role, BATCHER_BLOCKS_PER_BATCH).
            "--blocks-per-batch", var.blocks_per_batch,
            "--flush-ms", var.flush_ms,
            # The L2 chain id. The records commitment digests each
            # remote-epoch message leaf, which commits to this id. Same
            # value as the executor and validator jobs.
            "--chain-id", "412346",
          ],
          var.indexer_url != "" ? ["--indexer-url", var.indexer_url] : [],
          var.idle_flush_ms != "" ? ["--idle-flush-ms", var.idle_flush_ms] : [],
          var.settlement_deploy_block != "" ? ["--settlement-deploy-block", var.settlement_deploy_block] : [],
        )
      }

      env {
        # The service identity on the metrics (host_id) and on the events
        # stream (instance). Each instance must have its own, or the
        # events of two instances merge into one state.
        KARDAMOM_HOST_ID = "batcher-${NOMAD_ALLOC_INDEX}"
        # The Aeron C client reads its driver timeout from this variable,
        # and the service code never overrides it.
        AERON_DRIVER_TIMEOUT  = var.aeron_stall_tolerance_ms
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9002"
        KARDAMOM_L1_KEY       = "${var.batcher_key}"
      }

      # Cluster LogConfig (UDP multicast channels), read through
      # --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
        # The template reads the archive records from Consul. A change
        # there re-renders the file; the process reads it once at start
        # and follows the catalog through discovery, so never restart.
        change_mode = "noop"
      }

      # [cluster] ingress endpoints. Same contract as the executor's
      # config.
      template {
        destination = "local/batcher.toml"
        data        = file("config/executor.toml")
      }

      service {
        name     = "kardamom-batcher"
        port     = "metrics"
        provider = "consul"
        tags     = ["metrics"]
        check {
          type     = "http"
          path     = "/ready"
          interval = "10s"
          timeout  = "2s"
        }
      }

      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
