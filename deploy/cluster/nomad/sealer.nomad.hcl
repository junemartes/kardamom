# kardamom-sealer — emits block boundary markers and publishes the canonical
# tx_ordering stream (channel B). Placed on sealer1 (192.168.56.33).
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-sealer --config <sealer.toml> --aeron-dir <dir>
#
# sealer.toml is rendered from config/sealer.toml.tpl (schema:
# crates/sealer/src/config.rs). It carries the tx_ordering UDP endpoint
# (channel_b_uri) is a shared multicast group — the sealer is the publisher of tx_ordering, so
# the endpoint lives on this node (port 40001 = aeron_channel_base + 1).
#
# Shares the node Aeron media driver via the bind-mounted tmpfs aeron.dir.

job "sealer" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "sealer"
  }

  group "sealer" {
    count = 1

    network {
      mode = "host"
    }

    task "sealer" {
      driver = "docker"

      config {
        image        = "192.168.56.10:5000/kardamom-sealer:dev"
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--config", "/local/sealer.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
        ]
      }

      # Cluster LogConfig: the sealer's own channel_b_uri still wins for
      # tx_ordering, but the other channels (tx_receipts subscribe, etc.) come
      # from here. Consumed via --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      # Single-sourced from config/sealer.toml.tpl (submit from deploy/cluster/
      # — scripts/deploy.sh does this). channel_b_uri there is the tx_ordering
      # UDP endpoint on this (sealer) node (sealer1 192.168.56.33).
      template {
        destination = "local/sealer.toml"
        data        = file("config/sealer.toml.tpl")
      }

      resources {
        cpu    = 750
        memory = 512
      }
    }
  }
}
