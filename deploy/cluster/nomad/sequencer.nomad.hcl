# Each lane has two racing replicas on distinct nodes. Stable group names and
# unchanged arguments keep losing lanes serving throughout a resize overlap.
variable "image_ref" {
  type    = string
  default = ""
}
variable "datacenter" {
  type    = string
  default = "dc1"
}
variable "executor_count" {
  type    = number
  default = 3
}
variable "tx_ttl_ms" {
  type    = number
  default = 30000
}
variable "executor_query_port" {
  type    = number
  default = 9024
}
variable "metrics_base" {
  type    = number
  default = 9001
}
variable "mdc_base" {
  type    = number
  default = 40340
}
variable "shard_table" {
  type        = list(number)
  description = "The target map's 256 lane assignments. Empty uses the two-lane development identity map."
  default     = []
  validation {
    condition     = length(var.shard_table) == 0 || (length(var.shard_table) == 256 && length([for lane in var.shard_table : lane if lane < 0 || lane >= 8 || floor(lane) != lane]) == 0)
    error_message = "The shard table must have 256 integer lanes in 0..7."
  }
}
variable "previous_shard_table" {
  type        = list(number)
  description = "The old map during overlap. Empty selects steady state."
  default     = []
  validation {
    condition     = length(var.previous_shard_table) == 0 || (length(var.previous_shard_table) == 256 && length([for lane in var.previous_shard_table : lane if lane < 0 || lane >= 8 || floor(lane) != lane]) == 0)
    error_message = "The previous shard table must have 256 integer lanes in 0..7."
  }
}

locals {
  target         = length(var.shard_table) == 0 ? [for slot in range(256) : slot % 2] : var.shard_table
  overlap        = length(var.previous_shard_table) > 0
  previous       = local.overlap ? var.previous_shard_table : local.target
  target_count   = max(local.target...) + 1
  previous_count = max(local.previous...) + 1
}

locals {
  sets = { for lane in distinct(concat(local.target, local.previous)) : format("%d", lane) => {
    old      = [for slot, owner in local.previous : slot if owner == lane]
    next     = [for slot, owner in local.target : slot if owner == lane]
    incoming = [for slot, owner in local.target : slot if owner == lane && local.previous[slot] != lane]
  } }
}

locals {
  lanes = { for lane, slots in local.sets : lane => {
    slots    = local.overlap && length(slots.incoming) == 0 ? slots.old : slots.next
    count    = local.overlap && length(slots.incoming) == 0 ? local.previous_count : local.target_count
    incoming = local.overlap ? slots.incoming : []
  } if local.overlap || length(slots.next) > 0 }
}

job "sequencer" {
  datacenters = [var.datacenter]
  type        = "service"
  constraint {
    attribute = "${meta.role}"
    value     = "sequencer"
  }
  dynamic "group" {
    for_each = local.lanes
    labels   = ["seq-${group.key}"]
    content {
      count = 2
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
        port "egress" {}
      }

      dynamic "task" {
        for_each = [group.key]
        labels   = ["sequencer-${task.value}"]
        content {
          driver = "docker"

          config {
            image           = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-sequencer:dev"
            force_pull      = true
            readonly_rootfs = true
            network_mode    = "host"
            volumes = [
              "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
            ]
            args = concat([
              "--config", "/local/sequencer.toml",
              "--log-config", "/local/channels.toml",
              "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
              "--partition-count", format("%d", group.value.count),
              "--partition-index", group.key,
              "--sequencer-id", group.key,
              "--lane", group.key,
              "--vslots", join(",", group.value.slots),
              "--tx-ttl-ms", format("%d", var.tx_ttl_ms),
              "--executor-query-endpoints", join(",", [for i in range(var.executor_count) : "http://executor-${i}.node.${var.datacenter}.consul:${var.executor_query_port}"]),
              "--cluster-egress-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_egress}",
              ], length(group.value.incoming) == 0 ? [] : [
              "--extra-lanes", join(",", sort(distinct([for slot in group.value.incoming : var.previous_shard_table[slot]]))),
              "--shadow-vslots", join(",", group.value.incoming),
            ])
          }

          env {
            KARDAMOM_METRICS_ADDR = "0.0.0.0:${var.metrics_base + 10 * parseint(group.key, 10)}"
            KARDAMOM_HOST_ID      = "node${meta.node_index}-seq-${group.key}"
            KARDAMOM_MDC_PORTS    = "${var.mdc_base + 10 * parseint(group.key, 10)}-${var.mdc_base + 10 * parseint(group.key, 10) + 9}"
          }

          template {
            destination = "local/channels.toml"
            data        = file("config/channels.toml.tpl")
            change_mode = "noop"
          }

          template {
            destination = "local/sequencer.toml"
            data        = file("config/sequencer.toml.tpl")
          }

          resources {
            cpu    = 750
            memory = 512
          }
        }
      }
    }
  }
}
