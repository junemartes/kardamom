# kardamom-l1-indexer follows the finalized L1 through the light client
# and archives the inbox: every batch the settlement contract posted,
# with its payload, and every finalized block's epoch record. It serves
# them over JSON-RPC on the private network, for the rebuild
# (kardamom-reconstruct), the validator, and the batcher's resume.
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
  description = "The L1 JSON-RPC endpoint the indexer follows: the light client (nomad/l1-light-client.nomad.hcl), so every block, log, and hash is verified before it is archived."
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
  description = "The poll cadence, in seconds. Empty: the binary's default, 12."
  default     = ""
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

  # The node whose role set holds indexer; its disk holds the archive.
  constraint {
    attribute = "${meta.roles}"
    operator  = "set_contains"
    value     = "indexer"
  }

  group "l1-indexer" {
    count = 1

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
          ],
          var.start_block != "" ? ["--start-block", var.start_block] : [],
          var.poll_interval_secs != "" ? ["--poll-interval-secs", var.poll_interval_secs] : [],
        )
      }

      env {
        # Bind the exporter on the node, not loopback, so the monitoring
        # job scrapes it off-node.
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9009"
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
      }

      # One HTTP round trip per block, one per batch for the payload,
      # and one write per payload. The archive grows with the chain; the
      # process does not.
      resources {
        cpu    = 300
        memory = 256
      }
    }
  }
}
