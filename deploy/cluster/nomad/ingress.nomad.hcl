# kardamom-ingress — eth JSON-RPC proxy. Placed on ingress1 (192.168.56.32).
#
# Invocation:
#   kardamom-ingress --config <ingress.toml> --log-config <channels.toml> \
#       --aeron-dir <dir> --shards 2 --jsonrpc-bind 0.0.0.0:8545 \
#       --ack-policy on-quorum
#
# ingress.toml is presence-checked only; runtime tuning is via flags.
# channels.toml (issue #36) supplies the UDP multicast channels — including the
# quorum_watermark stream ingress subscribes to for the on-quorum ack gate,
# published by the quorum job (issue #38).
#
# tx_receipts MDS fan-in: channels.toml's tx_receipts_control_channel +
# tx_receipts_executor_count drive ingress to open one control-mode=manual
# subscription and attach each executor replica's per-replica endpoint (0..N),
# deduping the N identical receipt copies by tx hash. executor_count comes from
# the log config; override at runtime with --executor-count / KARDAMOM_EXECUTOR_COUNT.
# TODO(consul-watch): swap the static count for a Consul watch on an
# `executor-receipts` service so membership changes add/remove destinations live.
#
# Shares the node's Aeron media driver via the bind-mounted tmpfs aeron.dir.
# Host networking so :8545 binds on the ingress1 VM IP.
#
# NOTE: this job uses file() for its templates, so submit it from the
# deploy/cluster/ directory (scripts/deploy.sh does this).

variable "ack_policy" {
  type        = string
  description = "Ingress ack durability gate. Defaults to on-offer: a tx is acked once it is sequenced (offered), with no durable-watermark gate on the critical path. The strong durability boundary is L1 DA (via the batcher); a stronger pre-confirmation gate returns when the sealer becomes a raft-consensus turn-based system. Override with: nomad job run -var ack_policy=on-quorum ingress.nomad.hcl (requires the sealer's archive-at-sealer durable watermark)."
  default     = "on-offer"
}

job "ingress" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "ingress"
  }

  group "ingress" {
    count = 1

    network {
      mode = "host"
      port "jsonrpc" {
        static = 8545
      }
    }

    task "ingress" {
      driver = "docker"

      config {
        image        = "192.168.56.10:5000/kardamom-ingress:dev"
        # Always pull the freshly-built image: the mutable :dev tag would otherwise
        # let Nomad reuse a stale node-cached layer across rebuilds (caused a
        # crash-retry storm that stalled the deploy).
        force_pull    = true
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--config", "/local/ingress.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--shards", "2",
          "--jsonrpc-bind", "0.0.0.0:8545",
          "--ack-policy", "${var.ack_policy}",
        ]
      }

      # Presence-checked config (content lives in config/ingress.toml).
      template {
        destination = "local/ingress.toml"
        data        = file("config/ingress.toml")
      }

      # Cluster LogConfig (UDP multicast channels), single-sourced from
      # config/channels.toml.tpl and consumed via --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      resources {
        cpu    = 500
        memory = 512
      }

      service {
        name     = "ingress-jsonrpc"
        port     = "jsonrpc"
        provider = "consul"

        # The JSON-RPC server only answers POSTs, so a TCP connect check is
        # the right liveness signal here.
        check {
          type     = "tcp"
          interval = "10s"
          timeout  = "2s"
        }
      }
    }
  }
}
