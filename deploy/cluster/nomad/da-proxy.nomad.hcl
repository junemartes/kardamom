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
# batcher's account is used (its key comes from the Nomad Variable
# nomad/jobs/da-proxy, below). Without a network the workloads role
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

# The certificate verifier the proxy checks every certificate against:
# the network's EigenDACertVerifierRouter. The proxy does not fill it
# from the network name. The testnet default is the one the proxy's
# own example configuration ships (api/proxy/.env.example).
variable "eigenda_cert_verifier" {
  type        = string
  description = "EigenDACertVerifierRouter address of the network. Empty: the known address for sepolia_testnet."
  default     = ""
}

# How the signer pays: from an on-demand deposit in the PaymentVault
# (the default here; the operator makes the deposit before the first
# dispersal), or from a reservation EigenDA granted the account.
variable "eigenda_ledger_mode" {
  type        = string
  description = "The payment mode of the signer: on-demand-only, reservation-only, reservation-and-on-demand."
  default     = "on-demand-only"
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
        EIGENDA_PROXY_STORAGE_BACKENDS_TO_ENABLE    = "V2"
        EIGENDA_PROXY_STORAGE_DISPERSAL_BACKEND     = "V2"
        EIGENDA_PROXY_EIGENDA_V2_NETWORK            = var.eigenda_network
        EIGENDA_PROXY_EIGENDA_V2_MAX_BLOB_LENGTH    = "16MiB"
        EIGENDA_PROXY_EIGENDA_V2_CLIENT_LEDGER_MODE = var.eigenda_ledger_mode
        EIGENDA_PROXY_EIGENDA_V2_CERT_VERIFIER_ROUTER_OR_IMMUTABLE_VERIFIER_ADDR = (
          var.eigenda_cert_verifier != "" ? var.eigenda_cert_verifier :
          var.eigenda_network == "sepolia_testnet" ? "0x17ec4112c4BbD540E2c1fE0A49D264a280176F0D" : ""
        )
      }

      # The L1 the proxy verifies certificates against
      # (EIGENDA_PROXY_EIGENDA_V2_ETH_RPC) and the signer's key
      # (EIGENDA_PROXY_EIGENDA_V2_SIGNER_PRIVATE_KEY_HEX; without it the
      # proxy is read-only), from the job's Nomad Variable, which the
      # workloads role writes. They reach the task as environment, never
      # as a job variable or an argument, so a job read does not show
      # them.
      template {
        destination = "secrets/l1.env"
        env         = true
        data        = <<-EOT
        {{- with nomadVar "nomad/jobs/da-proxy" }}{{ range $k, $v := . }}
        {{ $k }}={{ $v.Value | toJSON }}{{ end }}{{ end }}
        EOT
      }

      # The SRS points (32 MiB) and a few payloads in flight.
      resources {
        cpu    = 500
        memory = 768
      }
    }
  }
}
