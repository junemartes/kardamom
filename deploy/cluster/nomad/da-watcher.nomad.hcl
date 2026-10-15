# kardamom-da-watcher takes the records of the l1_blocks stream, which
# the L1 follower (nomad/l1-indexer.nomad.hcl) publishes, and publishes
# the epoch of each, with its deposits, onto tx_deposits. It has no L1
# access. It runs on the aux node.
#
# A history read (a start, a gap) replays the l1_blocks recordings from
# the archives of the follower nodes onto this node's replay port, and
# reads what they do not hold from the follower's API
# (var.indexer_url).
#
# This shares the node's Aeron media driver, through the bind-mounted
# tmpfs aeron.dir.
#
# The watcher follows the sealer's commit through a boundary-only cluster
# session (config/da-watcher.toml, --config): the boundaries' L1 origin
# confirms the published epochs, the epochs that no boundary confirms are
# published again, and a start resumes after the sealer's origin.
#
# The L1 cursor file (the sealer's confirmed L1 origin, by number and
# hash) lives under /opt/kardamom/da-watcher, a host directory that the
# common role creates. A start with no boundary resumes after that block.

# The L1 follower's API, by its Consul service record: the backstop of
# a history read.
variable "indexer_url" {
  type        = string
  description = "The L1 follower's API (nomad/l1-indexer.nomad.hcl)."
  default     = "http://kardamom-l1-indexer.service.consul:8549"
}

# How long l1_blocks may carry no record before the watcher pauses with
# the follower as its root: three finality steps on Ethereum and
# Sepolia. Empty: the binary's default, 1152 s.
variable "l1_silence_secs" {
  type        = string
  description = "Seconds with no l1_blocks record before the watcher pauses on the follower. Empty: 1152."
  default     = ""
}

# Digest-pinned image. ansible/deploy.yml
# passes the repo:tag@sha256:... reference captured at push time
# (deploy/cluster/images.digests). The empty default falls back to the
# mutable :dev tag in the task config. That fallback is a dev
# affordance for manual `nomad job run` during debugging, not a
# production path.
variable "image_ref" {
  type        = string
  description = "Digest-pinned image reference (repo:tag@sha256:...) from the deploy's push manifest. Empty = mutable :dev tag fallback (dev-only)."
  default     = ""
}

# The Aeron stall tolerance, in milliseconds: how long an Aeron party
# waits through a stalled peer before it declares the peer dead. Aeron's
# default is 10000, and production keeps it: a longer value delays the
# detection of a dead process. CI raises it to ride out host stalls.
variable "aeron_stall_tolerance_ms" {
  type        = number
  description = "The driver timeout of the Aeron clients of the job, in milliseconds. Aeron's default is 10000."
  default     = 10000

  validation {
    condition     = var.aeron_stall_tolerance_ms >= 1000 && floor(var.aeron_stall_tolerance_ms) == var.aeron_stall_tolerance_ms
    error_message = "The Aeron stall tolerance must be a whole number of milliseconds, at least 1000."
  }
}

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

job "da-watcher" {
  datacenters = [var.datacenter]
  type        = "service"

  # The nodes whose role set holds da-watcher (group_vars/all.yml,
  # node_classes); their archive records tx_deposits.
  constraint {
    attribute = "${meta.roles}"
    operator  = "set_contains"
    value     = "da-watcher"
  }

  group "da-watcher" {
    count = 1

    # Resilience (chaos tests): restart a crashed task on the same
    # node, and reschedule onto a healthy node on node loss. This is a
    # singleton on a single aux-role node, so node-failure recovers
    # when the node returns.
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
      # The metrics port, as a Consul service: monitoring scrapes the
      # service, not a node name.
      port "metrics" {
        static = 9005
      }
      # The cluster-egress (response) endpoint of the boundary session.
      port "egress" {}
      # The history read of l1_blocks: replayed fragments and archive
      # control responses. Nomad picks them per allocation.
      port "replay" {}
      port "archive_response" {}
    }

    task "da-watcher" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-da-watcher:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it. On the pinned path, the
        # 1.9.5 driver pulls the tag but resolves the image by digest,
        # so the pin holds.
        force_pull = true
        # Read-only rootfs. The da-watcher
        # writes only to the bind-mounted aeron directory and its cursor
        # directory, plus Nomad's alloc, local, and secrets mounts.
        # cluster-e2e validates this.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          # The L1 cursor file lives under the persistent mount.
          "/opt/kardamom/da-watcher:/opt/kardamom/da-watcher",
        ]
        args = concat(
          [
            "--l1-blocks",
            "--indexer-url", var.indexer_url,
            "--replay-destination-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_replay}",
            "--archive-control-response-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_archive_response}",
            "--log-config", "/local/channels.toml",
            "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
            "--poll-interval-secs", "1",
            "--l1-cursor-file", "/opt/kardamom/da-watcher/l1-cursor",
            # Follow the sealer's boundaries. The egress channel is per
            # allocation (the node IP and the dynamic port are known only
            # at placement), so it is injected here.
            "--config", "/local/da-watcher.toml",
            "--cluster-egress-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_egress}",
            # Record tx_deposits to the archive, so a restarted
            # executor can replay deposit envelopes (Phase 2 crash
            # recovery).
            "--archive-durability",
          ],
          var.l1_silence_secs != "" ? ["--l1-silence-secs", var.l1_silence_secs] : [],
        )
      }

      env {
        # The service identity on the metrics (host_id) and on the events
        # stream (instance). Each instance must have its own, or the
        # events of two instances merge into one state.
        KARDAMOM_HOST_ID = "da-watcher-${NOMAD_ALLOC_INDEX}"
        # The Aeron C client reads its driver timeout from this variable,
        # and the service code never overrides it.
        AERON_DRIVER_TIMEOUT = var.aeron_stall_tolerance_ms
        # Bind the exporter on the node, not loopback, so the monitoring
        # job scrapes it off-node.
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9005"
      }

      # Cluster LogConfig (Aeron streams and discovery), read through
      # --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
        # The template reads the archive records from Consul. A change
        # there re-renders the file; the process reads it once at start
        # and follows the catalog through discovery, so never restart.
        change_mode = "noop"
      }

      # The [cluster] section of the boundary session.
      template {
        destination = "local/da-watcher.toml"
        data        = file("config/da-watcher.toml")
      }

      service {
        name     = "kardamom-da-watcher"
        port     = "metrics"
        provider = "consul"
        tags     = ["metrics"]
        check {
          type     = "http"
          path     = "/ready"
          interval = "10s"
          timeout  = "2s"
        }
      }

      resources {
        cpu    = 300
        memory = 256
      }
    }
  }
}
