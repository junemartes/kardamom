# kardamom-sealer — emits block boundary markers and publishes the canonical
# tx_ordering stream (channel B) via Aeron MDC. Placed on the sealer node-class
# (192.168.56.51, constraint ${meta.role} == sealer).
# Also runs the ARCHIVE-AT-THE-SEALER durability sidecar (--archive-durability):
# it records its own tx_ordering MDC publication into the co-located Aeron
# Archive and publishes the recording's durable position as the single
# watermark ingress gates on (replacing the removed custom recorders + Q-of-N
# quorum aggregator).
#
# Invocation:
#   kardamom-sealer --config <sealer.toml> --log-config <channels.toml> \
#       --aeron-dir <dir> --archive-durability
#
# sealer.toml is rendered from config/sealer.toml.tpl (schema:
# crates/sealer/src/config.rs). channel_b_mdc_control there is this sealer's
# tx_ordering MDC control endpoint (192.168.56.51:40110), which must appear in
# tx_ordering_mdc_publishers in config/channels.toml.tpl.
#
# Shares the node Aeron media driver via the bind-mounted tmpfs aeron.dir and
# the persistent archive dir (segment files for the durability recording).

job "sealer" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "sealer"
  }

  group "sealer" {
    count = 1

    # Resilience (chaos tests): restart a crashed task on the same node (resumes
    # the archive recording from the persistent /opt/kardamom/archive dir);
    # reschedule onto a healthy node on node loss. Singleton on a single
    # sealer-role node, so node-failure recovers when the node returns.
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

    task "sealer" {
      driver = "docker"

      config {
        image        = "192.168.56.10:5000/kardamom-sealer:dev"
        # Always pull the freshly-built image: the mutable :dev tag would otherwise
        # let Nomad reuse a stale node-cached layer across rebuilds (caused a
        # crash-retry storm that stalled the deploy).
        force_pull    = true
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/archive:/opt/kardamom/archive",
        ]
        args = [
          "--config", "/local/sealer.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          # Archive-at-the-sealer durability: record this sealer's tx_ordering
          # MDC publication and publish its durable position as the watermark.
          "--archive-durability",
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
      # — scripts/deploy.sh does this). channel_b_mdc_control there is this
      # sealer's tx_ordering MDC control endpoint (192.168.56.51:40110).
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
