# Each lane has two racing replicas on distinct nodes. Stable group names and
# unchanged arguments keep losing lanes serving throughout a resize overlap.
variable "image_ref" {
  type    = string
  default = ""
}
# The Nomad node pool of the job. An elastic pool registers its nodes in
# a pool of its own (roles/nomad: node_pool); the workloads role passes
# the pool id (workloads_node_pools). The default pool holds the fixed
# servers.
variable "node_pool" {
  type    = string
  default = "default"
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

# The longest idle wait of every hot-path thread, in microseconds: the
# media driver and the archive, the sealer, and the poll loops of the Rust
# services. A message that reaches an idle thread waits up to this long.
# An idle thread at the cap wakes about once per cap.
variable "idle_cap_us" {
  type        = number
  description = "The longest idle wait of every hot-path thread, in microseconds (KARDAMOM_IDLE_CAP_US), from 1 to 1000000. Default 50."
  default     = 50

  validation {
    condition     = var.idle_cap_us >= 1 && var.idle_cap_us <= 1000000 && floor(var.idle_cap_us) == var.idle_cap_us
    error_message = "The idle cap must be a whole number of microseconds from 1 to 1000000."
  }
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
# Priority fees, "on" or "off" (`[fees] priority`, through KARDAMOM_PRIORITY_FEES). The deploy sets every job's
# fee setting from one value, PRIORITY_FEES, so the sequencer's tip, the
# sealer's ordering window, and the executor's fee schedule cannot
# disagree. Ansible deployment passes -var from PRIORITY_FEES.
variable "priority_fees" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.priority_fees)
    error_message = "The priority_fees value must be on or off."
  }
}
# The receipts from the egress, "on" or "off" (receipts_from_egress in
# the config). On, the sequencer reads the receipts and their account rows
# from egress_receipts, each once, in place of the tx_receipts stream of
# every executor. Ansible deployment passes -var from
# KARDAMOM_SEQUENCER_RECEIPTS_FROM_EGRESS.
variable "receipts_from_egress" {
  type    = string
  default = "off"
  validation {
    condition     = contains(["on", "off"], var.receipts_from_egress)
    error_message = "The receipts_from_egress value must be on or off."
  }
}
variable "metrics_base" {
  type    = number
  default = 9001
}
# Canary allocations per lane deployment: 0 or 1. A canary needs a spare
# sequencer-class node; a profile without one keeps 0.
variable "canary" {
  type    = number
  default = 0
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

# The ingress service inside each sequencer process (the `ingress`
# subcommand of kardamom-sequencer). Off by default: the ingress job is
# the front door. On, each sequencer allocation also serves JSON-RPC,
# registers the Consul service ingress-jsonrpc, and drains its parked
# submits before it stops.
variable "ingress" {
  type        = bool
  description = "Run the ingress service inside each sequencer process."
  default     = false
}

# The JSON-RPC port of the in-process ingress of lane 0. Lane n listens on
# this port plus n, so two lanes on one node never take the same port.
# The load balancer targets each sequencer node on each port of its lanes,
# or reads the ingress-jsonrpc service from Consul.
variable "ingress_rpc_port_base" {
  type    = number
  default = 8550

  validation {
    condition     = var.ingress_rpc_port_base >= 1024 && var.ingress_rpc_port_base <= 65528 && floor(var.ingress_rpc_port_base) == var.ingress_rpc_port_base
    error_message = "The ingress port base must be a whole number from 1024 to 65528."
  }
}

# The in-process ingress records its tx_data lanes on the archive of the
# node. Off by default: the sequencer nodes do not advertise a tx_data
# archive, and the ingress nodes record every tx_data publisher that the
# catalog lists, the in-process ones too.
variable "ingress_archive_durability" {
  type    = bool
  default = false
}

# How far past the newest boundary the in-process ingress stamps a
# transaction's inclusion deadline. It must equal the sealer's
# cluster_inclusion_horizon_blocks. ansible/contract.yml checks this.
variable "inclusion_horizon_blocks" {
  type    = string
  default = "64"
}

locals {
  executor_endpoints = join(",", [for i in range(var.executor_count) : "http://executor-${i}.node.${var.datacenter}.consul:${var.executor_query_port}"])
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
  # The sequencer of this release moves the nonce floor past a skip that
  # used its nonce (nonce_burn). It sends the activation record of
  # FEATURE_SEALER_V4 in front of its epoch, and the stream position of
  # each reference to a sealer on decision version 4 (sealer_v4). The
  # upgrade check (`just sealer-upgrade-check`) reads these metas.
  meta {
    nonce_burn = "true"
    sealer_v4  = "true"
  }

  datacenters = [var.datacenter]
  type        = "service"
  node_pool   = var.node_pool
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

      # One replica of a lane at a time, healthy by its /ready check:
      # the lane left its startup resync and the cluster egress is
      # attached. The twin keeps the lane publishing meanwhile. A
      # failed instance reverts the job: a sequencer holds no state.
      update {
        max_parallel      = 1
        canary            = var.canary
        auto_promote      = false
        auto_revert       = true
        health_check      = "checks"
        min_healthy_time  = "15s"
        healthy_deadline  = "3m"
        progress_deadline = "10m"
      }

      network {
        mode = "host"
        port "egress" {}
        # The exporter, one port per lane, so two lanes share a node.
        port "metrics" {
          static = var.metrics_base + 10 * parseint(group.key, 10)
        }
        # The JSON-RPC port of the in-process ingress, one port per lane.
        dynamic "port" {
          for_each = var.ingress ? ["jsonrpc"] : []
          labels   = [port.value]
          content {
            static = var.ingress_rpc_port_base + parseint(group.key, 10)
          }
        }
        # The cluster egress port of the in-process ingress: its egress tap
        # is a cluster session of its own, beside the session of the
        # sequencer on the egress port.
        dynamic "port" {
          for_each = var.ingress ? ["ingress_egress"] : []
          labels   = [port.value]
          content {}
        }
      }

      service {
        name     = "kardamom-sequencer"
        port     = "metrics"
        provider = "consul"
        tags     = ["metrics", "lane-${group.key}"]
        check {
          type     = "http"
          path     = "/ready"
          interval = "10s"
          timeout  = "2s"
        }
      }

      # The RPC front door of the in-process ingress. GET /health answers
      # 200 while the ingress serves, and 503 from the first moment of the
      # drain, so a stopping sequencer leaves the rotation before its
      # parked submits finish.
      dynamic "service" {
        for_each = var.ingress ? ["ingress-jsonrpc"] : []
        content {
          name     = service.value
          port     = "jsonrpc"
          provider = "consul"
          tags     = ["sequencer", "lane-${group.key}"]
          check {
            type     = "http"
            path     = "/health"
            method   = "GET"
            interval = "5s"
            timeout  = "2s"
          }
        }
      }

      dynamic "task" {
        for_each = [group.key]
        labels   = ["sequencer-${task.value}"]
        content {
          driver = "docker"

          # The drain of the in-process ingress: the parked submits finish
          # within the park bound (tx_ttl_ms), plus a margin. Without the
          # ingress, the Nomad default of 5 seconds.
          kill_timeout = format("%ds", var.ingress ? ceil(var.tx_ttl_ms / 1000) + 10 : 5)

          config {
            # The ingress holds one connection for each parked submit, up
            # to 8192. Without the ingress, the limits of the Docker daemon.
            ulimit          = var.ingress ? [{ nofile = "65536:65536" }] : []
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
              "--executor-query-endpoints", local.executor_endpoints,
              "--cluster-egress-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_egress}",
              ], length(group.value.incoming) == 0 ? [] : [
              "--extra-lanes", join(",", sort(distinct([for slot in group.value.incoming : var.previous_shard_table[slot]]))),
              "--shadow-vslots", join(",", group.value.incoming),
              ], var.ingress ? concat([
                # The subcommand comes last: the flags after it are the
                # flags of kardamom-ingress.
                "ingress",
                "--shards", format("%d", local.target_count),
                "--shard-map", "/local/shard-map.toml",
                "--jsonrpc-bind", "0.0.0.0:${NOMAD_PORT_jsonrpc}",
                "--pending-receipt-timeout-ms", format("%d", var.tx_ttl_ms),
                "--inclusion-horizon-blocks", var.inclusion_horizon_blocks,
                # The ingress id is the decimal (lane + 1) followed by the
                # allocation index: lane 0 has 10 and 11, lane 7 has 80 and
                # 81. Each id is unique and at most 255, and none is an id
                # of the ingress job (its allocation index, 0 or 1).
                "--ingress-id", "${parseint(group.key, 10) + 1}${NOMAD_ALLOC_INDEX}",
                "--rpc-max-connections", "8192",
                # The L2 chain id (group_vars chain_id), as the ingress job.
                "--chain-id", "412346",
                "--executor-query-endpoints", local.executor_endpoints,
                "--cluster-egress-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_ingress_egress}",
            ], var.ingress_archive_durability ? ["--archive-durability"] : []) : [])
          }

          env {
            # The Aeron C client reads its driver timeout from this variable,
            # and the service code never overrides it.
            AERON_DRIVER_TIMEOUT = var.aeron_stall_tolerance_ms
            # The idle cap of the hot-path poll loops (kardamom_log IdleCap).
            KARDAMOM_IDLE_CAP_US   = var.idle_cap_us
            KARDAMOM_METRICS_ADDR  = "0.0.0.0:${var.metrics_base + 10 * parseint(group.key, 10)}"
            KARDAMOM_HOST_ID       = "node${meta.node_index}-seq-${group.key}"
            KARDAMOM_PRIORITY_FEES = var.priority_fees == "on" ? "true" : "false"
          }

          template {
            destination = "local/channels.toml"
            data        = file("config/channels.toml.tpl")
            change_mode = "noop"
          }

          # The switch goes in front of the file: a top-level key after
          # the first table of the file would belong to that table.
          template {
            destination = "local/sequencer.toml"
            data = join("\n", concat(
              var.receipts_from_egress == "on" ? ["receipts_from_egress = true"] : [],
              [file("config/sequencer.toml.tpl")],
            ))
          }

          # The shard map of the in-process ingress, as the ingress job
          # renders it.
          dynamic "template" {
            for_each = var.ingress ? ["local/shard-map.toml"] : []
            content {
              destination = template.value
              data        = file("config/shard-map.toml")
            }
          }

          # The ingress job asks for 500 MHz and 512 MB.
          resources {
            cpu    = var.ingress ? 1250 : 750
            memory = var.ingress ? 1024 : 512
          }
        }
      }
    }
  }
}
