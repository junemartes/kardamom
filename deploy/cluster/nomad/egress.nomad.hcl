# kardamom-egress: the stage after the executors. It merges the
# exec_frames copies of every executor into one canonical stream, keeps
# the stream and the receipts on disk, and serves the readers after the
# executors:
#
#   - egress_order (stream 1009): the merged frames, live;
#   - egress_receipts (stream 1011): the receipts of every executor, each
#     once, with their account rows;
#   - HTTP on :8548: the range read GET /v1/frames for a reader that
#     resumes, GET /health, and eth_getTransactionReceipt on POST /.
#
# The executors publish exec_frames only with --exec-frames (the job
# variable exec_frames of executor.nomad.hcl). The readers switch to the
# egress one by one: the validator and the batcher (--egress), the
# sequencer (receipts_from_egress), the ingress
# (--receipt-query-endpoints) and the notifier (--receipts-from-egress).
# They reach the HTTP port through the Consul service kardamom-egress.
#
# Placement: independent instances, one for each host, at least two.
# With egress_nodes off (the default) they run on the executor nodes:
# distinct_hosts keeps two instances on two of the three executor nodes.
# The executor holds 9004, 9014 and 9024 there, and the state mirror
# 9007. With egress_nodes on they run on the nodes with egress in
# meta.roles, one on each: their store syncs then do not share a disk
# with the state DB of an executor (docs/specs/2026-10-25-client-events.md,
# 4.7). The aux node keeps the readers (the validator and the batcher), so
# one node loss never takes the egress and its readers together. Ports:
# HTTP 8548, metrics 9013.
#
# The merge chains the copy of an executor only behind a frame that
# another source delivers. After a start, an instance reads the frames
# between its head and the copies from the range read of a peer
# (--peers, crates/egress/src/catchup.rs). So an instance with an empty
# store fills it from a running peer. With no peer that holds the frames,
# it starts again after the next boundary of a copy.
#
# This job uses file() for its templates, so submit it from the
# deploy/cluster/ directory. ansible/deploy.yml does this.

# Digest-pinned image. ansible/deploy.yml passes the repo:tag@sha256:...
# reference captured at push time (deploy/cluster/images.digests). The
# empty default falls back to the mutable :dev tag in the task config.
# That fallback is a dev affordance for manual `nomad job run` during
# debugging, not a production path.
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
  description = "The executor replica count (--executor-count): sizes the tx_receipts fan-in."
  default     = 3
}

variable "egress_count" {
  type        = number
  description = "The egress instances, one for each executor node. At least 2."
  default     = 2

  validation {
    condition     = var.egress_count >= 2 && floor(var.egress_count) == var.egress_count
    error_message = "The egress count must be a whole number, at least 2."
  }
}

# The range read, /health and eth_getTransactionReceipt
# (group_vars/all.yml, ports.egress_http).
variable "http_port" {
  type    = number
  default = 8548
}

# The Prometheus exporter (group_vars/all.yml, ports.egress_metrics).
variable "metrics_port" {
  type    = number
  default = 9013
}

# The store and the merge. Each default is the default of the binary.
variable "segment_blocks" {
  type        = number
  description = "The blocks in one segment file of the store (--segment-blocks)."
  default     = 1000
}

variable "keep_blocks" {
  type        = number
  description = "The blocks that the store keeps below its head, whatever the L1 heads say (--keep-blocks). The default is 1 day at 2 s a block."
  default     = 43200
}

# The floor of the store. The egress keeps every block above the posted
# head, which it reads from the l1_blocks stream of the L1 follower. With
# an output oracle, it keeps every block above the lower of the posted
# head and the final head, which it reads from the oracle on L1. The
# egress appends each flag only when its variable is set, and refuses to
# start with one of the two alone.
variable "l1_rpc_url" {
  type        = string
  description = "The L1 JSON-RPC endpoint of the output oracle: the light client (--l1-rpc-url). Empty: no oracle."
  default     = ""
}

variable "withdrawal_oracle" {
  type        = string
  description = "The L1 address of WithdrawalOutputOracle (--withdrawal-oracle). Empty: the floor is the posted head."
  default     = ""
}

variable "sync_ms" {
  type        = number
  description = "The period of the disk sync, in milliseconds (--sync-ms)."
  default     = 1000
}

variable "max_waiting" {
  type        = number
  description = "The frames of the copies ahead of the head that may wait (--max-waiting)."
  default     = 1000000
}

variable "compared" {
  type        = number
  description = "The delivered frames whose digests the egress keeps to compare the copies (--compared)."
  default     = 1000000
}

variable "receipts" {
  type        = number
  description = "The newest receipts that the egress keeps for eth_getTransactionReceipt (--receipts)."
  default     = 1000000
}

# The egress on its own nodes: "on" or "off". Ansible deployment passes
# -var from KARDAMOM_EGRESS_NODES. Off keeps the egress on the executor
# nodes.
variable "egress_nodes" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.egress_nodes)
    error_message = "The egress_nodes value must be on or off."
  }
}

# The peers of each instance: the HTTP surface on each node where an
# instance can run. An instance reads the frames between its head and the
# copies of the executors from a peer after a start, so a moved instance
# with an empty store fills it from the instance that did not move yet. A
# node with no instance refuses the connection, and the instance itself
# costs one empty read. So with egress_nodes on, the list names the egress
# nodes and every executor node, for the move one instance at a time.
locals {
  executor_peers = [
    for i in range(var.executor_count) :
    "http://executor-${i}.node.${var.datacenter}.consul:${var.http_port}"
  ]
  egress_peers = [
    for i in range(var.egress_count) :
    "http://egress-${i}.node.${var.datacenter}.consul:${var.http_port}"
  ]
  peers = join(",", var.egress_nodes == "on" ? concat(local.egress_peers, local.executor_peers) : local.executor_peers)
  # The node constraint of each value of egress_nodes.
  placement = {
    off = { attribute = "$${meta.role}", operator = "=", value = "executor" }
    on  = { attribute = "$${meta.roles}", operator = "set_contains", value = "egress" }
  }[var.egress_nodes]
}

job "egress" {
  # The egress of this release belongs to the release whose readers run a
  # full reference that a sealer below decision version 5 relays as is
  # (relayed_full_ref). The deploy reads this meta before it sets a
  # sequencer version floor above 0. The egress of this release keeps the
  # highest stored reference of record type 0 in the side file ref-mark of
  # its store, so the mark survives a restart (ref_mark). The deploy reads
  # this meta before it trusts the mark: an older egress stores
  # references and does not raise the mark.
  meta {
    relayed_full_ref = "true"
    ref_mark         = "true"
  }

  datacenters = [var.datacenter]
  type        = "service"

  constraint {
    attribute = local.placement.attribute
    operator  = local.placement.operator
    value     = local.placement.value
  }

  group "egress" {
    count = var.egress_count

    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

    restart {
      # An Aeron client exits until the media driver of its node runs
      # again, as after a node restart. Ten attempts at the 5 s delay
      # cover that start; with three, three fast failures would leave the
      # rest of the interval without a try. The cost: a task that crashes
      # in a loop restarts about every 5 s, about three times the load on
      # its targets of three attempts. A recovery after a node restart that
      # is up to a minute faster outweighs that load.
      attempts = 10
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

    # One instance at a time, healthy by its /health check. The other
    # instance serves the readers meanwhile. No revert: an instance
    # carries its store, and the role's rollback is the path.
    update {
      max_parallel     = 1
      health_check     = "checks"
      min_healthy_time = "10s"
      healthy_deadline = "2m"
      auto_revert      = false
    }

    network {
      mode = "host"
      port "http" {
        static = var.http_port
      }
      port "metrics" {
        static = var.metrics_port
      }
    }

    task "egress" {
      driver = "docker"

      env {
        # The Aeron C client reads its driver timeout from this variable,
        # and the service code never overrides it.
        AERON_DRIVER_TIMEOUT = var.aeron_stall_tolerance_ms
      }

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-egress:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it.
        force_pull = true
        # Read-only rootfs. The egress writes only the bind-mounted aeron
        # directory and its own store directory.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/egress:/opt/kardamom/egress",
        ]
        args = concat([
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--dir", "/opt/kardamom/egress",
          "--bind", "0.0.0.0:${var.http_port}",
          "--segment-blocks", format("%d", var.segment_blocks),
          "--keep-blocks", format("%d", var.keep_blocks),
          "--sync-ms", format("%d", var.sync_ms),
          "--max-waiting", format("%d", var.max_waiting),
          "--compared", format("%d", var.compared),
          "--receipts", format("%d", var.receipts),
          "--executor-count", format("%d", var.executor_count),
          "--peers", local.peers,
          # The service identity on the metrics and in the discovery
          # records. Each instance must have its own.
          "--host-id", "egress-${NOMAD_ALLOC_INDEX}",
          "--metrics-addr", "0.0.0.0:${var.metrics_port}",
          ],
          var.l1_rpc_url == "" ? [] : ["--l1-rpc-url", var.l1_rpc_url],
          var.withdrawal_oracle == "" ? [] : ["--withdrawal-oracle", var.withdrawal_oracle],
        )
      }

      # Cluster LogConfig (Aeron streams and discovery), read through
      # --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
        # The template reads the archive records from Consul. A change
        # there re-renders the file; the process reads it once at start
        # and follows the catalog through discovery, so never restart.
        change_mode = "noop"
      }

      # The HTTP surface as a Consul service: the readers reach the
      # instances as kardamom-egress.service.consul.
      service {
        name     = "kardamom-egress"
        port     = "http"
        provider = "consul"

        check {
          type     = "http"
          path     = "/health"
          interval = "10s"
          timeout  = "2s"
        }
      }

      # The metrics port as a Consul service: monitoring scrapes the
      # service, not a node name.
      service {
        name     = "kardamom-egress-metrics"
        port     = "metrics"
        provider = "consul"
        tags     = ["metrics"]
      }

      # The receipt ring (--receipts) and the digests of the compared
      # frames (--compared) take most of the memory.
      resources {
        cpu    = 500
        memory = 1536
      }
    }
  }
}
