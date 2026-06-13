# kardamom-recorder — records the canonical tx_ordering stream and publishes
# this node's fsync watermark (issue #38). Runs as a Nomad *system* job
# constrained to recorder nodes (${meta.role} == recorder), so exactly one
# instance lands on each of r1/r2/r3, each reading its own ${meta.recorder_id}.
#
# Invocation:
#   kardamom-recorder --log-config <channels.toml> --aeron-dir <dir> \
#       --recorder-id ${meta.recorder_id} --kind tx-ordering
#
# It connects the node-local Archive (control endpoint localhost:8010, from the
# [aeron] section of channels.toml) to start/adopt the tx_ordering recording,
# then polls the durable recording position and publishes a FsyncWatermark on
# the fsync_watermark multicast group. The quorum job aggregates all three into
# the quorum watermark that ingress --ack-policy on-quorum gates on.
#
# Shares the node's Aeron media driver + archive via the bind-mounted tmpfs
# aeron.dir and the persistent archive dir. Submit from deploy/cluster/ (the
# template uses file()).

job "recorder" {
  datacenters = ["dc1"]
  type        = "system"

  # Land on every recorder node (r1/r2/r3).
  constraint {
    attribute = "${meta.role}"
    value     = "recorder"
  }

  group "recorder" {
    network {
      mode = "host"
    }

    task "recorder" {
      driver = "docker"

      config {
        image        = "192.168.56.11:5000/kardamom-recorder:dev"
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/archive:/opt/kardamom/archive",
        ]
        args = [
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--recorder-id", "${meta.recorder_id}",
          "--kind", "tx-ordering",
        ]
      }

      # Shared cluster LogConfig (channels + quorum + archive control).
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
