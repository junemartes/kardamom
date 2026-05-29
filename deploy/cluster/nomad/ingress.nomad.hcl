# kardamom-ingress — eth JSON-RPC proxy. Placed on w1 (192.168.56.21).
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-ingress --config <ingress.toml> --aeron-dir <dir> --shards 2 \
#       --jsonrpc-bind 0.0.0.0:8545 --ack-policy on-quorum
#
# ingress.toml is presence-checked only (empty); runtime tuning is via flags.
# The channels.toml is rendered + mounted in anticipation of the future
# --log-config flag (see README "Required service changes"); ingress ignores it
# today and uses IPC defaults.
#
# Shares the node's Aeron media driver via the bind-mounted tmpfs aeron.dir.
# Host networking so :8545 binds on the w1 VM IP.

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
          "--ack-policy", "on-quorum",
        ]
      }

      # Presence-checked empty config.
      template {
        destination = "local/ingress.toml"
        data        = <<EOF
EOF
      }

      # Provisioned for the forthcoming --log-config flag; not consumed yet.
      template {
        destination = "local/channels.toml"
        data        = <<EOF
# NOT YET CONSUMED — see deploy/cluster/config/channels.toml.tpl and README
# "Required service changes". Mounted in anticipation of --log-config.
tx_data_channel_template = "aeron:udp?control-mode=dynamic|control=0.0.0.0:40000|alias=a-{sid}"
tx_data_stream_id_base = 2000
tx_ordering_channel = "aeron:udp?endpoint=192.168.56.22:40001"
tx_ordering_stream_id = 1001
tx_receipts_channel = "aeron:udp?endpoint=192.168.56.21:40002"
tx_receipts_stream_id = 1002
tx_errors_channel = "aeron:udp?endpoint=192.168.56.21:40003"
tx_errors_stream_id = 1015
tx_deposits_channel = "aeron:udp?endpoint=192.168.56.22:40004"
tx_deposits_stream_id = 1016
fsync_watermark_channel_template = "aeron:udp?control-mode=dynamic|control=0.0.0.0:40010|alias=fsync-wm-{rid}"
fsync_watermark_stream_id = 1010
fsync_watermark_tx_data_channel_template = "aeron:udp?control-mode=dynamic|control=0.0.0.0:40030|alias=fsync-wm-a-{sid}"
fsync_watermark_tx_data_stream_id_base = 1030
quorum_watermark_channel = "aeron:udp?endpoint=192.168.56.22:40020"
quorum_watermark_stream_id = 1020
EOF
      }

      resources {
        cpu    = 1000
        memory = 1024
      }

      service {
        name     = "ingress-jsonrpc"
        port     = "jsonrpc"
        provider = "consul"
      }
    }
  }
}
