# kardamom-da-store: the file-backed stand-in for the EigenDA proxy, for
# a deployment without EigenDA (the local profile, the e2e suites), as
# anvil stands in for L1. It serves the proxy's API (POST /put, GET
# /get/<cert>, GET /health) under the same Consul service, so the
# batcher, the indexer and kardamom-reconstruct do not know which one
# answers. The payloads live under paths.da_store_dir on the node, so
# they survive a node outage the way EigenDA survives a proxy restart;
# the proxy's own in-memory store does not.
#
# On an EigenDA network the workloads role deploys nomad/da-proxy.nomad.hcl
# instead.

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

# The API port: the same as the proxy's, behind the same service.
variable "port" {
  type    = number
  default = 3100
}

job "da-store" {
  datacenters = [var.datacenter]
  type        = "service"

  # The batcher's node, as the proxy.
  constraint {
    attribute = "${meta.roles}"
    operator  = "set_contains"
    value     = "batcher"
  }

  group "da-store" {
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
      port "api" {
        static = var.port
      }
    }

    service {
      name     = "kardamom-da-proxy"
      port     = "api"
      provider = "consul"
      check {
        type     = "http"
        path     = "/health"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "da-store" {
      driver = "docker"

      config {
        image           = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-da-store:dev"
        force_pull      = true
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/da-store:/opt/kardamom/da-store",
        ]
        args = [
          "--dir", "/opt/kardamom/da-store",
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
