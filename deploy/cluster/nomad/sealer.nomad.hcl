# kardamom-sealer — emits block boundary markers and publishes the canonical
# tx_ordering stream (channel B). Placed on w2 (192.168.56.22).
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-sealer --config <sealer.toml> --aeron-dir <dir>
#
# sealer.toml is rendered from config/sealer.toml.tpl (schema:
# crates/sealer/src/config.rs). It carries the tx_ordering UDP endpoint
# (channel_b_uri) on the w2 IP — the sealer is the publisher of tx_ordering, so
# the endpoint lives on this node (port 40001 = aeron_channel_base + 1).
#
# Shares the node Aeron media driver via the bind-mounted tmpfs aeron.dir.

job "sealer" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.kardamom_node}"
    value     = "w2"
  }

  group "sealer" {
    count = 1

    network {
      mode = "host"
    }

    task "sealer" {
      driver = "docker"

      config {
        image        = "192.168.56.11:5000/kardamom-sealer:dev"
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--config", "/local/sealer.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
        ]
      }

      # Mirrors config/sealer.toml.tpl. channel_b_uri is the tx_ordering UDP
      # endpoint on this (sealer) node, w2 192.168.56.22:40001.
      template {
        destination = "local/sealer.toml"
        data        = <<EOF
host_id = 1
channel_b_uri = "aeron:udp?endpoint=192.168.56.22:40001"
channel_b_tx_stream_id = 1
channel_b_boundary_stream_id = 1001
tick_interval_ms = 250
EOF
      }

      resources {
        cpu    = 1000
        memory = 1024
      }
    }
  }
}
