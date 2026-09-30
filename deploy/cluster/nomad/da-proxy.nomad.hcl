# The EigenDA proxy: the chain's data-availability layer, behind one HTTP
# API. The batcher disperses each batch's payload through it
# (POST /put) and posts the certificate it returns on L1; every reader
# (the validator, the indexer, kardamom-reconstruct) retrieves a payload
# by certificate (GET /get/<cert>). The proxy checks the certificate
# against EigenDA's verifier contract on both paths, and the bytes
# against the certificate's KZG commitment on the read path, so a reader
# trusts its proxy, not the disperser.
#
# Two backends, one deployment switch (var.eigenda_network):
# - empty: the proxy's in-memory store. It answers the same API with no
#   network, for the local profile and the e2e suites; anvil has no
#   EigenDA. A payload lives until the store expires it. The proxy's
#   client config still wants a network name, so the store runs under
#   the testnet's name; it never contacts it.
# - a network name (sepolia_testnet, mainnet): EigenDA V2 through the
#   disperser of that network. The proxy fills the disperser and the
#   contract addresses from the name. The signer pays for dispersal
#   from its PaymentVault deposit; the batcher's account is used.
#
# The proxy runs beside the batcher (the writer) and is one service for
# the readers. It holds no state a deployment relies on: on a real
# network EigenDA holds the bytes for 14 days and the indexer archives
# them; the in-memory store is the local profile's only copy.

variable "eigenda_network" {
  type        = string
  description = "EigenDA network (sepolia_testnet, mainnet). Empty: the in-memory store, for a deployment without EigenDA."
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

# How long the in-memory store keeps a payload. Long enough for a full
# e2e run, and for a rebuild within it.
variable "memstore_expiration" {
  type    = string
  default = "24h"
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
        args = concat(
          ["--addr", "0.0.0.0", "--port", format("%d", var.port), "--apis.enabled", "standard"],
          var.eigenda_network == "" ? [
            "--memstore.enabled",
            "--memstore.expiration", var.memstore_expiration,
            "--eigenda.v2.network", "sepolia_testnet",
          ] : [],
        )
      }

      # The EigenDA V2 client, on a real network only. The in-memory
      # store needs none of these, so the map is empty then: an empty
      # value would still be a set variable.
      env = var.eigenda_network == "" ? {} : {
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
