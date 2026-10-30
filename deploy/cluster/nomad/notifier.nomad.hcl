# kardamom-notifier: transaction status events for clients. Two
# instances behind the edge load balancer: one per ingress node, or with
# notifier_nodes on, one per node of role notifier. Each
# taps tx_status, tx_receipts and tx_errors itself (multi-destination
# cast: a new reader costs the publishers nothing), keeps a ring of the
# last minutes of events, and serves:
#
#   - the WebSocket feed kardamom_subscribeTxStatus(filter) on :8547.
#
# The notifier keeps no state on disk. The webhooks are removed
# (2026-10-25); offline delivery comes later through a task queue. So
# an instance needs no peers, and a change of the instance count moves
# nothing.
#
# The edge forwards the client route to kardamom-notifier.service.consul
# (:8547), with WebSocket upgrades and long idle timeouts.

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
  description = "The executor replica count: sizes the tx_receipts fan-in."
  default     = 3
}

# The ring: the last minutes of events a late subscriber can replay, and
# a hard cap on their count. An event costs about 300 bytes with its
# indexes, so one million events is about 300 MB; the cap, not the age,
# bounds the memory under load. With notifier_nodes on, the placement
# lowers the cap to fit the smaller memory reservation.
variable "ring_minutes" {
  type        = number
  description = "How many minutes of status events the ring keeps."
  default     = 10
}

variable "ring_max_events" {
  type        = number
  description = "The most status events the ring keeps, whatever their age."
  default     = 1000000
}

# The receipts from the egress, "on" or "off" (--receipts-from-egress).
# On, the notifier reads egress_receipts, each receipt once, in place of
# the tx_receipts stream of every executor. Ansible deployment passes
# -var from KARDAMOM_NOTIFIER_RECEIPTS_FROM_EGRESS.
variable "receipts_from_egress" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.receipts_from_egress)
    error_message = "The receipts_from_egress value must be on or off."
  }
}

# The events path, "on" or "off" (--events,
# docs/specs/2026-10-25-client-events.md). On, the notifier takes the
# stages from tx_events and the merged receipts of the egress, through its
# stage table, and keeps only the rejections of tx_status. It needs
# KARDAMOM_EGRESS=1. Ansible deployment passes -var from
# KARDAMOM_NOTIFIER_EVENTS.
variable "events" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.events)
    error_message = "The events value must be on or off."
  }
}

# With the events path on, the executed stage from the tx_receipts of every
# executor, "on" or "off" (--executed), for the evaluation of the stages.
# Ansible deployment passes -var from KARDAMOM_NOTIFIER_EXECUTED.
variable "executed" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.executed)
    error_message = "The executed value must be on or off."
  }
}

# The notifier on its own nodes, "on" or "off". Off, the notifier runs
# one instance per ingress node. On, it runs notifier_count instances, one
# per node whose role set holds notifier, with a smaller memory
# reservation. The aeron job takes the same switch for the idle cap of
# the driver on those nodes. Ansible deployment passes -var from
# KARDAMOM_NOTIFIER_NODES.
variable "notifier_nodes" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.notifier_nodes)
    error_message = "The notifier_nodes value must be on or off."
  }
}

# The instance count with notifier_nodes on: the count of the notifier
# nodes, which are notifier-0 to notifier-<count - 1>. The first phase
# has one notifier node. Ansible deployment passes -var from
# KARDAMOM_NOTIFIER_COUNT, and its preflight refuses a count above the
# count of the notifier nodes.
variable "notifier_count" {
  type        = number
  description = "The notifier instance count with notifier_nodes on, one per notifier node."
  default     = 1

  validation {
    condition     = var.notifier_count >= 1 && floor(var.notifier_count) == var.notifier_count
    error_message = "The notifier count must be a whole number, at least 1."
  }
}

# The placement of each value of notifier_nodes: every node of the class
# runs one instance (distinct_hosts).
#
# On a notifier node, the ring keeps at most 250,000 events: about 75 MB,
# and about 50 seconds at the staging load-test peak of 5,100 events a
# second (1,700 tx/s, three stages). With the process and the Aeron
# client, the task stays in its 512 MB.
locals {
  placement = {
    off = {
      attribute  = "${meta.role}"
      operator   = "="
      value      = "ingress"
      count      = 2
      memory     = 1024
      ring_max   = var.ring_max_events
    }
    on = {
      attribute  = "${meta.roles}"
      operator   = "set_contains"
      value      = "notifier"
      count      = var.notifier_count
      memory     = 512
      ring_max   = min(var.ring_max_events, 250000)
    }
  }[var.notifier_nodes]
}

job "notifier" {
  datacenters = [var.datacenter]
  type        = "service"

  # One instance per node of the placement: an ingress node, next to the
  # front door it extends, or a notifier node.
  constraint {
    attribute = local.placement.attribute
    operator  = local.placement.operator
    value     = local.placement.value
  }

  group "notifier" {
    count = local.placement.count

    constraint {
      distinct_hosts = true
    }

    restart {
      # An Aeron client exits until the media driver of its node runs
      # again, as after a node restart. Ten attempts at the 5 s delay
      # cover that start; three fast failures left the rest of the
      # interval without a try.
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

    update {
      max_parallel     = 1
      health_check     = "checks"
      min_healthy_time = "10s"
      healthy_deadline = "2m"
      auto_revert      = false
    }

    network {
      mode = "host"
      # The client port: the WebSocket feed.
      port "feed" {
        static = 8547
      }
      port "metrics" {
        static = 9008
      }
    }

    task "notifier" {
      driver = "docker"

      env {
        # The Aeron C client reads its driver timeout from this variable,
        # and the service code never overrides it.
        AERON_DRIVER_TIMEOUT = var.aeron_stall_tolerance_ms
      }

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-notifier:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it.
        force_pull = true
        # Read-only rootfs. The notifier writes only the bind-mounted
        # aeron directory.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = concat([
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--executor-count", "${var.executor_count}",
          "--bind", "0.0.0.0:8547",
          "--ring-minutes", "${var.ring_minutes}",
          "--ring-max-events", "${local.placement.ring_max}",
          "--instance-index", "${NOMAD_ALLOC_INDEX}",
          "--host-id", "notifier-${NOMAD_ALLOC_INDEX}",
          "--metrics-addr", "0.0.0.0:9008",
          ],
          var.receipts_from_egress == "on" ? ["--receipts-from-egress"] : [],
          # Off renders no flag, so the job stays as before.
          var.events == "on" ? ["--events", "on", "--executed", var.executed] : [],
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

      # The client surface as a Consul service: the edge reaches the
      # instances as kardamom-notifier.service.consul.
      service {
        name     = "kardamom-notifier"
        port     = "feed"
        provider = "consul"

        check {
          type     = "tcp"
          interval = "10s"
          timeout  = "2s"
        }
      }

      # The metrics port as a Consul service: monitoring scrapes the
      # service, not a node name.
      service {
        name     = "kardamom-notifier-metrics"
        port     = "metrics"
        provider = "consul"
        tags     = ["metrics"]
      }

      resources {
        cpu    = 500
        memory = local.placement.memory
      }
    }
  }
}
