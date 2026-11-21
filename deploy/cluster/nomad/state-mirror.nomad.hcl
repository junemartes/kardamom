# kardamom-state-mirror projects every receipt batch's account rows and
# receipts into Redis. One mirror runs next to each executor, on the
# executor-class nodes: a rebuild reads the co-located executor's newest
# checkpoint from /opt/kardamom/checkpoints. With the checkpoint executor,
# a rebuild also fetches the newest checkpoint of the checkpoint executor
# into /opt/kardamom/mirror/peer-checkpoints, and restores the newest of
# the two. See docs/specs/2026-09-13-redis-account-cache-design.md,
# sections 5.6 and 6.
#
# The mirror subscribes to tx_receipts and writes to Redis. It publishes
# nothing on the L2 streams. Every mirror writes the same values through
# the monotone write rule, so their order does not matter.
#
# With the audit mirror, a fourth mirror runs on the node of role backup,
# beside the checkpoint executor. It does the daily audit rebuild from the
# local checkpoints of that executor. The hot mirrors on the executor or
# sealer nodes then run with the audit off. See
# docs/specs/2026-11-17-checkpoint-executor.md, section 4.
#
# This job uses file() for its templates, so submit it from the
# deploy/cluster/ directory. ansible/deploy.yml does this.

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

# The executors on the sealer nodes, "on" or "off" (executor.nomad.hcl).
# On, the executors run on the sealer nodes, so each mirror runs there
# too, next to its executor. The deploy passes it from
# KARDAMOM_EXEC_ON_SEALERS.
variable "exec_on_sealers" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.exec_on_sealers)
    error_message = "The exec_on_sealers value must be on or off."
  }
}

variable "executor_count" {
  type        = number
  description = "The executor node count (node_classes.executor.count): the number of mirrors and of tx_receipts publishers."
  default     = 3
}

# The checkpoint executor, "on" or "off" (executor.nomad.hcl). On, a
# rebuild fetches its newest checkpoint (--checkpoint-peers), because the
# hot executors in restore-only mode make none. The fetch refuses a
# checkpoint of another genesis, so the mirror then reads the genesis of
# the chain (--chain). The deploy passes it from
# KARDAMOM_CHECKPOINT_EXECUTOR.
variable "checkpoint_executor" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.checkpoint_executor)
    error_message = "The checkpoint_executor value must be on or off."
  }
}

# The fragments of the genesis, as on the executor (executor.nomad.hcl):
# the carry from genesis, the local canary and the priority fees. The
# mirror renders the genesis only with the checkpoint executor.
variable "genesis_carry" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.genesis_carry)
    error_message = "The genesis_carry value must be on or off."
  }
}
variable "canary_genesis" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.canary_genesis)
    error_message = "The canary_genesis value must be on or off."
  }
}
variable "priority_fees" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.priority_fees)
    error_message = "The priority_fees value must be on or off."
  }
}

# The audit mirror, "on" or "off". On, the group mirror-audit runs one
# more mirror on the node of role backup, and the hot mirrors run with
# the audit off (--audit-hours 0). It needs the checkpoint executor,
# which writes the checkpoints of that node. The deploy passes it from
# KARDAMOM_MIRROR_AUDIT_ON_BACKUP.
variable "audit_mirror" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.audit_mirror)
    error_message = "The audit_mirror value must be on or off."
  }
}

# Each group is hot or not. A hot mirror runs next to a hot executor. It
# applies the live batches, and its head is one of the heads that the
# ingress and the sequencer read: they read the ids 0 to
# executor_count - 1. The audit mirror (group mirror-audit, on the node
# of role backup) is not hot. It applies the same live batches and does
# the daily audit rebuild. Its mirror id 255 is outside the ids of the
# readers, so a late or missing head of the audit mirror never changes
# their freshness rule. Its host id is state-mirror-audit. It reads the
# checkpoints of the checkpoint executor on its own node, so it fetches
# none from a peer: in the restore-only mode, that executor makes every
# new checkpoint. During its audit it is not serving, so its deployment
# waits only for its task to run.
locals {
  checkpoint = var.checkpoint_executor == "on"
  audit      = var.audit_mirror == "on"
  role       = var.exec_on_sealers == "on" ? "sealer" : "executor"
  hot_groups = [{
    label     = "mirror"
    count     = 3
    hot       = true
    id        = "${NOMAD_ALLOC_INDEX}"
    mirror_id = "${NOMAD_ALLOC_INDEX}"
    service   = "kardamom-state-mirror"
    constraints = concat(
      [{ attribute = "", operator = "distinct_hosts", value = "true" }],
      local.audit ? [{ attribute = "${meta.role}", operator = "=", value = local.role }] : [],
    )
  }]
  audit_groups = local.audit ? [{
    label       = "mirror-audit"
    count       = 1
    hot         = false
    id          = "audit"
    mirror_id   = "255"
    service     = "kardamom-state-mirror-audit"
    constraints = [{ attribute = "${meta.role}", operator = "=", value = "backup" }]
  }] : []
  mirror_groups = concat(local.hot_groups, local.audit_groups)
}

job "state-mirror" {
  datacenters = [var.datacenter]
  type        = "service"

  # With the audit mirror, the role constraint moves into each group,
  # because the audit group runs on another role.
  dynamic "constraint" {
    for_each = local.audit ? [] : [local.role]
    content {
      attribute = "${meta.role}"
      value     = constraint.value
    }
  }

  dynamic "group" {
    for_each = local.mirror_groups
    labels   = [group.value.label]
    content {
      count = group.value.count

      dynamic "constraint" {
        for_each = group.value.constraints
        content {
          attribute = constraint.value.attribute
          operator  = constraint.value.operator
          value     = constraint.value.value
        }
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

      # One mirror at a time, healthy by its /ready check: it applied a
      # batch within the stale window, so it is attached to tx_receipts.
      # The audit mirror is healthy when its task runs. A deployment fails
      # as a whole when one group misses its deadline, so a slow audit
      # mirror must not hold a deployment of the hot mirrors.
      update {
        max_parallel      = 1
        health_check      = group.value.hot ? "checks" : "task_states"
        min_healthy_time  = "15s"
        healthy_deadline  = "5m"
        progress_deadline = "10m"
        auto_revert       = false
      }

      network {
        mode = "host"
        # The exporter, as a Consul service: the /ready check reads it.
        port "metrics" {
          static = 9007
        }
      }

      task "state-mirror" {
        driver = "docker"

        env {
          # The Aeron C client reads its driver timeout from this variable,
          # and the service code never overrides it.
          AERON_DRIVER_TIMEOUT = var.aeron_stall_tolerance_ms
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
          args = concat([
            "--config", "/local/state-mirror.toml",
            "--log-config", "/local/channels.toml",
            "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
            # The mirror id of a hot mirror is the co-located executor's
            # index. The id of the audit mirror is outside those indexes.
            "--mirror-id", group.value.mirror_id,
            "--executor-count", "${var.executor_count}",
            "--checkpoints-dir", "/opt/kardamom/checkpoints",
            "--mirror-dir", "/opt/kardamom/mirror",
            "--host-id", "state-mirror-${group.value.id}",
            # Bind the exporter on all interfaces; the chaos suite probes
            # it over the cluster bridge (ports.state_mirror_metrics).
            "--metrics-addr", "0.0.0.0:9007",
            ],
            # The checkpoint executor by its Consul service, as on the
            # executors (executor.nomad.hcl). The audit mirror runs beside
            # it and reads its checkpoints from the local directory.
            group.value.hot && local.checkpoint ? [
              "--checkpoint-peers", "kardamom-executor-checkpoint.service.${var.datacenter}.consul:9014",
              "--chain", "/local/genesis.toml",
            ] : [],
            # With the audit mirror, only the audit mirror does the audit.
            group.value.hot && local.audit ? ["--audit-hours", "0"] : [],
          )
        }

        template {
          destination = "local/channels.toml"
          data        = file("config/channels.toml.tpl")
          change_mode = "noop"
        }

        # The chain genesis, the same file as the executor renders. A fetch
        # refuses a peer checkpoint whose genesis digest differs.
        dynamic "template" {
          for_each = group.value.hot && local.checkpoint ? ["local/genesis.toml"] : []
          content {
            destination = template.value
            data = join("\n", concat(
              var.genesis_carry == "on" ? [file("config/genesis/carry.toml")] : [],
              [file("config/genesis/dev.toml")],
              var.canary_genesis == "on" ? [file("config/genesis/canary.toml")] : [],
              var.priority_fees == "on" ? [file("config/genesis/fees.toml")] : [],
            ))
          }
        }

        # The [cache] section: the three sentinels by their node records.
        # The mirror asks them for the primary and asks again after a
        # failure.
        # The sentinels by their Consul service record, which resolves to
        # the healthy sentinels; the mirror resolves it again after a
        # failure.
        template {
          destination = "local/state-mirror.toml"
          data        = <<EOF
[cache]
sentinels = [
  "redis://redis-sentinel.service.${var.datacenter}.consul:26379",
]
master_name = "kardamom"
EOF
        }

        # A hot mirror registers kardamom-state-mirror with its /ready
        # check. The audit mirror registers kardamom-state-mirror-audit
        # with a TCP check: it is not ready during its audit, and its
        # record resolves for the scrape while the process runs.
        service {
          name     = group.value.service
          port     = "metrics"
          provider = "consul"
          tags     = ["metrics"]
          check {
            type     = group.value.hot ? "http" : "tcp"
            path     = group.value.hot ? "/ready" : ""
            interval = "10s"
            timeout  = "2s"
          }
        }

        # A cold rebuild restores the newest executor checkpoint into
        # /opt/kardamom/mirror/rebuild. The dirty pages of that copy count
        # against the memory limit of the task, and a checkpoint of a loaded
        # chain is several GB (3.6 GB on staging). A Redis restart makes
        # every mirror rebuild cold at once, so the limit must hold a full
        # restore, or each rebuild is killed for OOM. The audit mirror
        # restores the same image in each audit, and it holds the live
        # batches that arrive during the audit in its queue.
        resources {
          cpu    = 500
          memory = 4096
        }
      }
    }
  }
}
