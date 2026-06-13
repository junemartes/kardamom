# kardamom-recorder --aggregate — the quorum-watermark aggregator (issue #38).
# A single cluster-wide instance subscribes to all N=3 per-recorder fsync
# watermarks, computes the Q-of-N (2-of-3) quorum position, and publishes the
# quorum watermark that ingress --ack-policy on-quorum gates on.
#
# Invocation:
#   kardamom-recorder --log-config <channels.toml> --aeron-dir <dir> \
#       --recorder-id 0 --aggregate --no-record
#
# It records nothing (--no-record); it is a LIVENESS-only singleton. The Q
# durable copies exist regardless of whether it is running — if it dies, acks
# stall until Nomad reschedules it, but no committed data is ever at risk.
# Pinned to a recorder node (r1) so it is co-located with a media driver and on
# the multicast segment. Submit from deploy/cluster/ (the template uses file()).

job "quorum" {
  datacenters = ["dc1"]
  type        = "service"

  # Pin to recorder r1 (any node on the multicast segment works; r1 is the
  # control node and always present).
  constraint {
    attribute = "${meta.kardamom_node}"
    value     = "r1"
  }

  group "quorum" {
    count = 1

    network {
      mode = "host"
    }

    task "quorum" {
      driver = "docker"

      config {
        image        = "192.168.56.11:5000/kardamom-recorder:dev"
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        # --recorder-id is required by the CLI but unused in aggregate mode
        # (the aggregator has no identity of its own); 0 is a placeholder.
        args = [
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--recorder-id", "0",
          "--aggregate",
          "--no-record",
        ]
      }

      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      resources {
        cpu    = 300
        memory = 256
      }
    }
  }
}
