# kardamom-ingress — eth JSON-RPC proxy. Placed on w1 (192.168.56.21).
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-ingress --config <ingress.toml> --aeron-dir <dir> --shards 2 \
#       --jsonrpc-bind 0.0.0.0:8545 --ack-policy on-offer
#
# ingress.toml is presence-checked only; runtime tuning is via flags.
# The channels.toml is rendered + mounted in anticipation of the future
# --log-config flag (see README "Required service changes"); ingress ignores it
# today and uses IPC defaults.
#
# Shares the node's Aeron media driver via the bind-mounted tmpfs aeron.dir.
# Host networking so :8545 binds on the w1 VM IP.
#
# NOTE: this job uses file() for its templates, so submit it from the
# deploy/cluster/ directory (scripts/deploy.sh does this).

variable "ack_policy" {
  type        = string
  description = "Ingress ack durability gate. Defaults to on-offer because the recorder/quorum role has no deployable process yet (issue #38; README 'Required service changes' item 3) — with on-quorum and no quorum-watermark publisher, ingress would never ack. Switch to on-quorum once #38 lands: nomad job run -var ack_policy=on-quorum ingress.nomad.hcl"
  default     = "on-offer"
}

job "ingress" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.kardamom_node}"
    value     = "w1"
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
        image        = "192.168.56.11:5000/kardamom-ingress:dev"
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--config", "/local/ingress.toml",
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

      # Provisioned for the forthcoming --log-config flag; not consumed yet.
      # Single-sourced from config/channels.toml.tpl.
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
