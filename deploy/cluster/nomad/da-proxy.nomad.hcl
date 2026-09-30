# The EigenDA proxy: the chain's data-availability layer, behind one HTTP
# API. The batcher disperses each batch's payload through it
# (POST /put) and posts the certificate it returns on L1; every reader
# (the validator, the indexer, kardamom-reconstruct) retrieves a payload
# by certificate (GET /get/<cert>). The proxy checks the certificate
# against EigenDA's verifier contract on both paths, and the bytes
# against the certificate's KZG commitment on the read path, so a reader
# trusts its proxy, not the disperser.
#
# The proxy runs on an EigenDA network only (var.eigenda_network). It
# fills the disperser and the contract addresses from the network name.
# The signer pays for dispersal from its PaymentVault deposit; the
# batcher's account is used. Without a network the workloads role
# deploys nomad/da-store.nomad.hcl, the file-backed stand-in, under the
# same Consul service.
#
# The proxy runs beside the batcher (the writer) and is one service for
# the readers. It holds no state a deployment relies on: EigenDA holds
# the bytes for 14 days and the indexer archives them.

variable "eigenda_network" {
  type        = string
  description = "EigenDA network (sepolia_testnet, mainnet). The empty default exists for `just validate` only."
  default     = ""
}

variable "eigenda_eth_rpc" {
  type        = string
  description = "The Ethereum RPC the proxy verifies certificates against (the L1 of the network)."
  default     = ""
}

variable "eigenda_signer_key" {
  type        = string
  description = "The hex private key that signs dispersals and pays from its PaymentVault deposit. Empty: the proxy is read-only."
  default     = ""
}

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

variable "port" {
  type    = number
  default = 3100
}

job "da-proxy" {
  datacenters = [var.datacenter]
  type        = "service"

  # The batcher's node: the writer's proxy. Readers reach it by its
  # Consul service.
  constraint {
    attribute = "${meta.roles}"
    operator  = "set_contains"
    value     = "batcher"
  }

  group "da-proxy" {
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
      # /health answers only once the SRS points are loaded and the API
      # listens; the deploy waits for it (roles/workloads, da_proxy.yml).
      check {
        type     = "http"
        path     = "/health"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "da-proxy" {
      driver = "docker"

      config {
        image        = "ghcr.io/layr-labs/eigenda-proxy:2.7.1"
        network_mode = "host"
        args         = ["--addr", "0.0.0.0", "--port", format("%d", var.port), "--apis.enabled", "standard"]
      }

      # The EigenDA V2 client.
      env {
        EIGENDA_PROXY_STORAGE_BACKENDS_TO_ENABLE        = "V2"
        EIGENDA_PROXY_STORAGE_DISPERSAL_BACKEND         = "V2"
        EIGENDA_PROXY_EIGENDA_V2_NETWORK                = var.eigenda_network
        EIGENDA_PROXY_EIGENDA_V2_ETH_RPC                = var.eigenda_eth_rpc
        EIGENDA_PROXY_EIGENDA_V2_SIGNER_PRIVATE_KEY_HEX = var.eigenda_signer_key
        EIGENDA_PROXY_EIGENDA_V2_MAX_BLOB_LENGTH        = "16MiB"
      }

      # The SRS points (32 MiB) and a few payloads in flight.
      resources {
        cpu    = 500
        memory = 768
      }
    }
  }
}
