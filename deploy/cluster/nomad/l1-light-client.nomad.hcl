# helios is the Ethereum consensus light client, fronting L1 for the
# validator.
#
# Why: the validator re-derives every epoch from L1 (phase D of
# docs/agents/l1-origin-deposit-derivation-spec.md), but a plain RPC
# endpoint is blindly trusted. `RpcL1Source` returns whatever hash the
# provider says, and nothing anchors that view to anything the
# validator independently knows. An endpoint that fabricates a
# self-consistent history defeats the check, and one colluding with a
# dishonest sequencer would turn a detected forgery into an accepted
# one.
#
# Helios closes the correctness half. It follows finalized beacon
# headers, whose finality branch proves finalization per the beacon
# state, so it does not merely trust the sync committee's optimistic
# head. It verifies execution responses against the authenticated
# state and receipts roots in those headers. It speaks standard
# `eth_*` JSON-RPC, so the validator needs no code change: point
# `--l1-rpc-url` here instead of at the upstream RPC.
#
# What it does not close: a light client verifies data, but does not
# hold it. It still proxies to an untrusted execution RPC and a beacon
# API. A provider that withholds data (no logs for a block, or
# unreachable) cannot fool it, but can stop it from verifying, which
# degrades to validator_epochs_unverified_total and quietly drops
# coverage. Owning an L1 node would close that gap. Alert on that
# metric being non-zero.
#
# This job is not exercised by CI. Every kardamom test environment
# runs anvil as L1, and anvil is execution-only: no beacon chain, no
# sync committee, nothing for a consensus light client to sync from.
# So this job cannot come up in the local or cluster e2e, and deploys
# only against a real network. Validate changes to it manually against
# a testnet before relying on them.

variable "execution_rpc" {
  type        = string
  description = "UNTRUSTED upstream execution RPC helios proxies to. Untrusted by construction: helios verifies every response against a beacon-authenticated root, so this endpoint's honesty is not assumed — only its availability."
  default     = ""
}

variable "consensus_rpc" {
  type        = string
  description = "Beacon API endpoint helios pulls light-client updates from."
  default     = ""
}

variable "checkpoint" {
  type        = string
  description = "Weak-subjectivity checkpoint (a trusted beacon block root) helios syncs from. This IS a trust assumption — but a single auditable constant, sourced from several independent places or from our own node once, rather than continuous trust in a live endpoint. Rotate it when it ages out."
  default     = ""
}

variable "network" {
  type        = string
  description = "Ethereum network helios follows (mainnet, sepolia, holesky)."
  default     = "mainnet"
}

# Port the verified JSON-RPC is served on. The validator's
# --l1-rpc-url points here. Nothing else should, so a misconfiguration
# cannot silently route the verification path back to the untrusted
# upstream.
variable "rpc_port" {
  type    = number
  default = 8548
}

job "l1-light-client" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "aux"
  }

  group "l1-light-client" {
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
    }

    task "helios" {
      driver = "docker"

      config {
        image        = "ghcr.io/a16z/helios:latest"
        force_pull   = true
        network_mode = "host"
        args = [
          "ethereum",
          "--network", "${var.network}",
          "--execution-rpc", "${var.execution_rpc}",
          "--consensus-rpc", "${var.consensus_rpc}",
          "--checkpoint", "${var.checkpoint}",
          # Bind to the node IP, not localhost. The validator runs on a
          # different node, and must reach it.
          "--rpc-bind-ip", "${meta.node_ip}",
          "--rpc-port", "${var.rpc_port}",
        ]
      }

      # A light client is cheap. The steady-state cost is one
      # aggregate BLS verification per update, plus a brief spike
      # about every 27 hours when the sync committee period rolls
      # over. Bandwidth, not CPU, is what scales: verifying logs
      # against receiptsRoot pulls a block's full receipt set. This is
      # sized generously rather than tightly; it is a rounding error
      # next to the validator it sits beside.
      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
