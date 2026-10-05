# kardamom-notifier: transaction status events for clients. Two
# instances, one per ingress node, behind the edge load balancer. Each
# taps tx_status, tx_receipts and tx_errors itself (multi-destination
# cast: a new reader costs the publishers nothing), keeps a ring of the
# last minutes of events, and serves:
#
#   - the WebSocket feed kardamom_subscribeTxStatus(filter) on :8547;
#   - POST /webhooks on the same port, delivered at least once from a
#     per-subscription outbox under /opt/kardamom/notifier.
#
# Webhook subscriptions are sharded across the two instances by a
# rendezvous hash of the subscription id. A registration that lands on
# either instance is stored on both (the peers list), so the other
# instance can take a shard over when the instance count changes.
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

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

variable "ingress_count" {
  type        = number
  description = "The ingress node count: the notifier runs one instance per ingress node, and the peers list names them all."
  default     = 2
}

variable "executor_count" {
  type        = number
  description = "The executor replica count: sizes the tx_receipts fan-in."
  default     = 3
}

# The ring: the last minutes of events a late subscriber can replay, and
# a hard cap on their count. An event costs about 300 bytes with its
# indexes, so one million events is about 300 MB; the cap, not the age,
# bounds the memory under load.
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

# A fully delivered outbox longer than this is cut to zero.
variable "outbox_retain_bytes" {
  type        = number
  description = "The retention bound of a webhook outbox, in bytes."
  default     = 268435456
}

locals {
  peers = join(",", [for i in range(var.ingress_count) : "http://ingress-${i}.node.${var.datacenter}.consul:8547"])
}

job "notifier" {
  datacenters = [var.datacenter]
  type        = "service"

  # One instance per ingress node, next to the front door it extends.
  constraint {
    attribute = "${meta.role}"
    value     = "ingress"
  }

  group "notifier" {
    count = 2

    constraint {
      distinct_hosts = true
    }

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
      health_check     = "checks"
      min_healthy_time = "10s"
      healthy_deadline = "2m"
      auto_revert      = false
    }

    network {
      mode = "host"
      # The client port: the WebSocket feed and POST /webhooks.
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
        # The UDP block of the subscriber's discovery sockets.
        KARDAMOM_MDC_PORTS = "40370-40379"
      }

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-notifier:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it.
        force_pull = true
        # Read-only rootfs. The notifier writes only the bind-mounted
        # aeron directory and its own outbox directory.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/notifier:/opt/kardamom/notifier",
        ]
        args = [
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--executor-count", "${var.executor_count}",
          "--bind", "0.0.0.0:8547",
          "--dir", "/opt/kardamom/notifier",
          "--ring-minutes", "${var.ring_minutes}",
          "--ring-max-events", "${var.ring_max_events}",
          "--outbox-retain-bytes", "${var.outbox_retain_bytes}",
          "--instance-index", "${NOMAD_ALLOC_INDEX}",
          "--instance-count", "2",
          # Every instance, this one included: a registration forwarded
          # to itself is the same subscription again, a no-op.
          "--peers", local.peers,
          "--host-id", "notifier-${NOMAD_ALLOC_INDEX}",
          "--metrics-addr", "0.0.0.0:9008",
        ]
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
        memory = 1024
      }
    }
  }
}
