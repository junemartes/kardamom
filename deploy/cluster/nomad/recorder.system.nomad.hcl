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
  type        = "service"

  # Collocate ONE recorder per executor node (role==executor), 1:1: each executor
  # is a state machine and its co-located recorder durably logs the canonical
  # order it applies. distinct_hosts puts exactly one on each of the 3 executor
  # nodes, so the recorder count tracks the executor count.
  constraint {
    attribute = "${meta.role}"
    value     = "executor"
  }

  group "recorder" {
    # Quorum N (must match quorum.n in config/channels.toml.tpl + the executor
    # count in group_vars). distinct_hosts → one per distinct executor node, so
    # losing one executor node loses at most one recorder.
    count = 3
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

    network {
      mode = "host"
    }

    task "recorder" {
      driver = "docker"

      config {
        image        = "192.168.56.10:5000/kardamom-recorder:dev"
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/archive:/opt/kardamom/archive",
        ]
        args = [
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--recorder-id", "${NOMAD_ALLOC_INDEX}",
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
