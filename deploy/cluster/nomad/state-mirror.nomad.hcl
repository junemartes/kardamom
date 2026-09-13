# kardamom-state-mirror projects every receipt batch's account rows and
# receipts into Redis. One mirror runs next to each executor, on the
# executor-class nodes: a rebuild reads the co-located executor's newest
# checkpoint from /opt/kardamom/checkpoints. See
# docs/specs/2026-09-13-redis-account-cache-design.md, sections 5.6 and 6.
#
# The mirror subscribes to tx_receipts and writes to Redis. It publishes
# nothing on the L2 streams. Three mirrors write the same values through
# the monotone write rule, so their order does not matter.
#
# This job uses file() for its templates, so submit it from the
# deploy/cluster/ directory. ansible/deploy.yml does this.

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

variable "executor_count" {
  type        = number
  description = "The executor node count (node_classes.executor.count): the number of mirrors and of tx_receipts publishers."
  default     = 3
}

job "state-mirror" {
  datacenters = [var.datacenter]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "executor"
  }

  group "mirror" {
    count = 3
    constraint {
      operator = "distinct_hosts"
      value    = "true"
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
      health_check     = "task_states"
      min_healthy_time = "10s"
      healthy_deadline = "2m"
      auto_revert      = false
    }

    network {
      mode = "host"
    }

    task "state-mirror" {
      driver = "docker"

      env {
        # The UDP port block this subscriber's discovery sockets use
        # on the executor node, distinct from the executor's own.
        KARDAMOM_MDC_PORTS = "40360-40369"
      }

      config {
        image           = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-state-mirror:dev"
        force_pull      = true
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          # The co-located executor's checkpoints, read only: the
          # rebuild restores the newest one into the mirror dir.
          "/opt/kardamom/checkpoints:/opt/kardamom/checkpoints:ro",
          # The local head file and the rebuild scratch copy.
          "/opt/kardamom/mirror:/opt/kardamom/mirror",
        ]
        args = [
          "--config", "/local/state-mirror.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          # The mirror id is the co-located executor's index.
          "--mirror-id", "${NOMAD_ALLOC_INDEX}",
          "--executor-count", "${var.executor_count}",
          "--checkpoints-dir", "/opt/kardamom/checkpoints",
          "--mirror-dir", "/opt/kardamom/mirror",
          "--host-id", "state-mirror-${NOMAD_ALLOC_INDEX}",
          # Bind the exporter on all interfaces; the chaos suite probes
          # it over the cluster bridge (ports.state_mirror_metrics).
          "--metrics-addr", "0.0.0.0:9007",
        ]
      }

      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
        change_mode = "noop"
      }

      # The [cache] section: the three sentinels by their node records.
      # The mirror asks them for the primary and asks again after a
      # failure.
      template {
        destination = "local/state-mirror.toml"
        data        = <<EOF
[cache]
sentinels = [
  "redis://aux-0.node.${var.datacenter}.consul:26379",
  "redis://ingress-0.node.${var.datacenter}.consul:26379",
  "redis://ingress-1.node.${var.datacenter}.consul:26379",
]
master_name = "kardamom"
EOF
      }

      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
