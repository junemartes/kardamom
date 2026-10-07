# kardamom-l1-indexer is the L1 follower: the one service that reads L1
# data. It reads the finalized L1 once per finality step (the beacon
# schedule, when beacon_api is set), and publishes one record per
# finalized block on the l1_blocks Aeron stream, which the archive of its
# node records. It also archives the inbox: every batch the settlement
# contract posted, with its payload, and every finalized block's epoch
# record. It serves them over JSON-RPC on the private network, for the
# rebuild (kardamom-reconstruct), the validator, and the batcher's
# resume.
#
# Two instances run on two nodes. Both read and both publish; a consumer
# keeps the first record of each block number, so one instance down
# costs nothing.
#
# Why: EigenDA holds a payload for 14 days. A rebuild after that window
# needs a copy that was taken in time. The indexer is that copy. It is
# not a source of truth: a payload is stored under the certificate L1
# committed to, as the proxy served and checked it, and losing the
# archive costs a re-index from the start block, not the chain.
#
# Like the light client, this job deploys against a real network: it
# follows finality. The chaos-l1 shard also deploys it against the
# in-cluster anvil, through the L1 fault proxy, where anvil finalizes
# two blocks behind its head; that shard is the only CI run of it. Validate a change
# to it against a testnet too. The empty defaults exist for
# `just validate` only; the workloads role passes every value.

variable "l1_rpc" {
  type        = string
  description = "The L1 JSON-RPC endpoints the indexer follows, comma-separated. With two or more, a block, a log query or a hash is archived only when two agree."
  default     = ""
}

# The light client's endpoint, when one runs: its answer settles a read
# it serves, and a public endpoint that disagrees with it is the liar.
variable "l1_light_client_rpc" {
  type        = string
  description = "The L1 light client's endpoint (nomad/l1-light-client.nomad.hcl). Empty: none."
  default     = ""
}

variable "da_proxy" {
  type        = string
  description = "The EigenDA proxy's URL (nomad/da-proxy.nomad.hcl): where the payloads come from, checked against their certificates."
  default     = "http://kardamom-da-proxy.service.consul:3100"
}

variable "settlement_address" {
  type        = string
  description = "L1 address of KardamomL2Settlement, whose BatchPosted events name the batches."
  default     = ""
}

variable "lockbox_address" {
  type        = string
  description = "L1 address of the ETHLockbox proxy, whose logs the epoch records derive from."
  default     = ""
}

variable "start_block" {
  type        = string
  description = "The first L1 block to index on an empty archive: the block of the contract deploy. Empty: the finalized block at first start."
  default     = ""
}

variable "poll_interval_secs" {
  type        = string
  description = "One slot, in seconds: the read cadence while the finalized tip does not move or the follower is halted, and the whole cadence without beacon_api. Empty: the binary's default, 12."
  default     = ""
}

# The beacon API the follower reads the genesis time and the slot length
# from, once: with it, the follower reads L1 on the finality schedule.
# Empty (anvil, which has no beacon chain): every poll interval.
variable "beacon_api" {
  type        = string
  description = "A beacon API endpoint for the finality schedule. Empty: read every poll interval."
  default     = ""
}

variable "max_log_range" {
  type        = string
  description = "The most blocks one log query spans: the provider's cap. Empty: the binary's default, 10."
  default     = ""
}

# Two instances on two nodes whose role set holds indexer. A deployment
# with one such node runs one.
variable "follower_count" {
  type        = number
  description = "Follower instances, each on its own node."
  default     = 2
}

# The Aeron stall tolerance, in milliseconds (see the da-watcher job).
variable "aeron_stall_tolerance_ms" {
  type        = number
  description = "The driver timeout of the Aeron clients of the job, in milliseconds. Aeron's default is 10000."
  default     = 10000

  validation {
    condition     = var.aeron_stall_tolerance_ms >= 1000 && floor(var.aeron_stall_tolerance_ms) == var.aeron_stall_tolerance_ms
    error_message = "The Aeron stall tolerance must be a whole number of milliseconds, at least 1000."
  }
}

# Digest-pinned image, as in the other service jobs.
variable "image_ref" {
  type        = string
  description = "Digest-pinned image reference (repo:tag@sha256:...) from the deploy's push manifest. Empty = mutable :dev tag fallback (dev-only)."
  default     = ""
}

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

# The API port. Consumers read it by the Consul service
# kardamom-l1-indexer.
variable "rpc_port" {
  type    = number
  default = 8549
}

job "l1-indexer" {
  datacenters = [var.datacenter]
  type        = "service"

  # The nodes whose role set holds indexer; each disk holds an archive,
  # and each node's Aeron archive records l1_blocks.
  constraint {
    attribute = "${meta.roles}"
    operator  = "set_contains"
    value     = "indexer"
  }

  # One instance per node: two on one node share one fault.
  constraint {
    operator = "distinct_hosts"
    value    = "true"
  }

  group "l1-indexer" {
    count = var.follower_count

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
      auto_revert      = false
    }

    network {
      mode = "host"
      port "rpc" {
        static = var.rpc_port
      }
      # The metrics port, as a Consul service: monitoring scrapes the
      # service, not a node name.
      port "metrics" {
        static = 9009
      }
    }

    task "l1-indexer" {
      driver = "docker"

      config {
        image           = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-l1-indexer:dev"
        force_pull      = true
        readonly_rootfs = true
        network_mode    = "host"
        # The archive, on the node's disk (paths.l1_indexer_dir). A
        # volume snapshot of it is a backup; a lost one is re-indexed.
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/l1-indexer:/opt/kardamom/l1-indexer",
        ]
        args = concat(
          [
            "--l1-rpc", var.l1_rpc,
            "--da-proxy", var.da_proxy,
            "--settlement", var.settlement_address,
            "--lockbox", var.lockbox_address,
            "--data-dir", "/opt/kardamom/l1-indexer",
            "--listen", "0.0.0.0:${var.rpc_port}",
            "--log-config", "/local/channels.toml",
            "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
            # Record l1_blocks on the node's archive before the first
            # publish: a consumer replays the stream from it.
            "--archive-durability",
          ],
          var.start_block != "" ? ["--start-block", var.start_block] : [],
          var.poll_interval_secs != "" ? ["--poll-interval-secs", var.poll_interval_secs] : [],
          var.l1_light_client_rpc != "" ? ["--l1-light-client-rpc", var.l1_light_client_rpc] : [],
          var.beacon_api != "" ? ["--beacon-api", var.beacon_api] : [],
          var.max_log_range != "" ? ["--max-log-range", var.max_log_range] : [],
        )
      }

      env {
        # The service identity on the metrics (host_id) and on the events
        # stream (instance). Each instance must have its own, or the
        # events of two instances merge into one state.
        KARDAMOM_HOST_ID = "l1-indexer-${NOMAD_ALLOC_INDEX}"
        # The Aeron C client reads its driver timeout from this variable.
        AERON_DRIVER_TIMEOUT = var.aeron_stall_tolerance_ms
        # Bind the exporter on the node, not loopback, so the monitoring
        # job scrapes it off-node.
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9009"
      }

      # Cluster LogConfig (Aeron streams and discovery), read through
      # --log-config. The process reads it once and follows the catalog
      # through discovery, so a re-render never restarts it.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
        change_mode = "noop"
      }

      service {
        name     = "kardamom-l1-indexer"
        port     = "rpc"
        provider = "consul"
      }

      service {
        name     = "kardamom-l1-indexer-metrics"
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

      # One header batch and a few log queries per finality step, one
      # HTTP round trip per batch for the payload, and one write per
      # payload and per block. The archive grows with the chain; the
      # process does not.
      resources {
        cpu    = 300
        memory = 256
      }
    }
  }
}
