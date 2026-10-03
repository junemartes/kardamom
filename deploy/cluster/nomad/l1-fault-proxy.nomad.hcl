# kardamom-l1-fault-proxy: an L1 JSON-RPC proxy that lies on command, in
# front of the in-cluster anvil. The chaos-l1 shard deploys it and points
# the followers (the batcher, the da-watcher, the inbox indexer) at it;
# the harness then sets the faults through `POST /fault` on the same
# port. No other deployment runs it: the workloads role submits it only
# when KARDAMOM_L1_FAULT_PROXY is set.
#
# Placement: the node that runs the batcher, so the followers reach it
# over the same path they reach their peers. It is a Consul service,
# kardamom-l1-fault-proxy, and the followers name it by that record.

variable "upstream" {
  type        = string
  description = "The L1 JSON-RPC endpoint every call is forwarded to. Default: the in-cluster anvil by its Consul service record."
  default     = "http://anvil.service.consul:8546"
}

variable "port" {
  type        = number
  description = "The port of the JSON-RPC pipe and the control endpoint (ports.l1_fault_proxy in group_vars/all.yml)."
  default     = 8547
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

job "l1-fault-proxy" {
  datacenters = [var.datacenter]
  type        = "service"

  # The node whose role set holds batcher (group_vars/all.yml,
  # node_classes): the proxy sits where its first consumer runs.
  constraint {
    attribute = "${meta.roles}"
    operator  = "set_contains"
    value     = "batcher"
  }

  group "l1-fault-proxy" {
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

    network {
      mode = "host"
      port "rpc" {
        static = var.port
      }
    }

    service {
      name     = "kardamom-l1-fault-proxy"
      port     = "rpc"
      provider = "consul"
      check {
        type     = "http"
        path     = "/health"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "l1-fault-proxy" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-l1-fault-proxy:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it.
        force_pull = true
        # Read-only rootfs: the proxy writes nothing. cluster-e2e
        # validates this.
        readonly_rootfs = true
        network_mode    = "host"
        args = [
          "--upstream", var.upstream,
          "--listen", "0.0.0.0:${var.port}",
        ]
      }

      resources {
        cpu    = 200
        memory = 128
      }
    }
  }
}
