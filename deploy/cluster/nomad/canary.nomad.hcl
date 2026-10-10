# kardamom-canary: the transaction canary. One instance uses the chain
# as a user does, without pause, and reports each success and each
# failure as a metric (docs/specs/2026-10-07-tx-canary.md):
#
#   - transfer: a transfer between two ring accounts, through each
#     ingress instance in turn, timed by the notifier's status feed and
#     the receipt;
#   - read: the head of each ingress instance moves, and the receipt of
#     an old canary transaction still answers;
#   - contract: a write to the canary counter and a read that shows it.
#
# The ingress instances come from their Consul node records, one per
# ingress node, never the load balancer: a stopped ingress shows as the
# failures of its own endpoint. The status feed is the notifier's
# service record.
#
# The ring's mnemonic (KARDAMOM_CANARY_MNEMONIC) comes from the Nomad
# Variable nomad/jobs/canary, which the workloads role writes: the key
# never appears in the job. Without a real one, and with CANARY_LOCAL=1,
# the role writes the public anvil mnemonic with ring_offset 34, the
# accounts the dev genesis funds for the canary.
#
# Placement: the node whose role set holds monitoring (the aux node by
# default), outside the chaos suite's blast radius, beside the
# Prometheus that scrapes it.

# Digest-pinned image. ansible/deploy.yml passes the repo:tag@sha256:...
# reference captured at push time (deploy/cluster/images.digests). The
# empty default falls back to the mutable :dev tag in the task config,
# a dev affordance for a manual `nomad job run`.
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
  description = "The ingress node count: the canary probes each ingress instance."
  default     = 2
}

variable "ring_size" {
  type        = number
  description = "The number of ring accounts."
  default     = 4
}

variable "ring_offset" {
  type        = number
  description = "The derivation index of the first ring account."
  default     = 0
}

locals {
  ingress = join(",", [for i in range(var.ingress_count) : "ingress-${i}=http://ingress-${i}.node.${var.datacenter}.consul:8545"])
}

job "canary" {
  datacenters = [var.datacenter]
  type        = "service"

  # The nodes whose role set holds monitoring (group_vars/all.yml, node_classes).
  constraint {
    attribute = "${meta.roles}"
    operator  = "set_contains"
    value     = "monitoring"
  }

  group "canary" {
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
      health_check     = "checks"
      min_healthy_time = "10s"
      healthy_deadline = "2m"
      auto_revert      = false
    }

    network {
      mode = "host"
      port "metrics" {
        static = 9012
      }
    }

    task "canary" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-canary:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it.
        force_pull = true
        # Read-only rootfs. The canary writes only its data directory:
        # the nonce journal and the contract addresses.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/canary:/opt/kardamom/canary",
        ]
        args = [
          "--ingress", local.ingress,
          "--notifier-ws", "ws://kardamom-notifier.service.consul:8547",
          "--ring-size", "${var.ring_size}",
          "--ring-offset", "${var.ring_offset}",
          "--dir", "/opt/kardamom/canary",
          "--host-id", "canary-${NOMAD_ALLOC_INDEX}",
          "--metrics-addr", "0.0.0.0:9012",
        ]
      }

      # The ring's mnemonic (KARDAMOM_CANARY_MNEMONIC), from the job's
      # Nomad Variable, which the workloads role writes. It reaches the
      # task as environment, never as a job variable or an argument, so
      # a job read does not show it.
      template {
        destination = "secrets/canary.env"
        env         = true
        data        = <<-EOT
        {{- with nomadVar "nomad/jobs/canary" }}{{ range $k, $v := . }}
        {{ $k }}={{ $v.Value | toJSON }}{{ end }}{{ end }}
        EOT
      }

      # The metrics port as a Consul service: monitoring scrapes the
      # service, not a node name.
      service {
        name     = "kardamom-canary"
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
        cpu    = 200
        memory = 256
      }
    }
  }
}
