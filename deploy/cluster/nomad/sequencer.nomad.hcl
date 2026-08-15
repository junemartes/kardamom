# kardamom-sequencer — partitions and orders L2 txs, P=2 racing replicas per
# shard. Two identical service groups ("seq-a", "seq-b") of count = 2 each run
# on the 2 sequencer-role nodes; both replicas of a shard subscribe to the same
# per-shard tx_data multicast stream and both offer refs to the Aeron Cluster,
# which dedups by canonical_id first-seen (the same mechanism that already
# absorbs the M duplicate DepositRefs). A replica crash therefore costs
# NOTHING: its twin never stopped. See
# docs/agents/replicated-sequencer-shards-spec.md.
#
# Placement is DETERMINISTIC, derived from node meta (not alloc index):
#   group seq-a: partition = ${meta.node_index}            → node-0: shard 0, node-1: shard 1
#   group seq-b: partition = ${meta.node_index} + offset 1 → node-0: shard 1, node-1: shard 0
# so the two replicas of any shard always land on DIFFERENT nodes and a node
# loss leaves every shard with one live replica.
#
# Colocated-replica port separation (host networking, two sequencer processes
# per node): seq-b moves its Prometheus listener to :9011 and its per-node
# cluster-egress (response) endpoint to :40211 (seq-a keeps the standard
# :9001 / :40210). Both metrics listeners bind 0.0.0.0 — the binary default
# is loopback, which is unreachable off-node in a host-network job and left
# the replicas unscrapable. NOTE for consumers: both replicas of a shard
# process the SAME tx stream, so per-shard tx totals exist once per replica —
# aggregate stream-derived counters with `max by (partition)`, never `sum`.
#
# Shares the node Aeron media driver via the bind-mounted tmpfs aeron.dir.

# Digest-pinned image (attested-identity P0.1): scripts/deploy.sh passes the
# repo:tag@sha256:... reference captured at push time (deploy/cluster/
# images.digests) — both racing replica groups run the same pinned bytes. The
# empty default falls back to the mutable :dev tag in the task configs — a dev
# affordance for manual `nomad job run` during debugging, NOT a production
# path.
variable "image_ref" {
  type        = string
  description = "Digest-pinned image reference (repo:tag@sha256:...) from the deploy's push manifest. Empty = mutable :dev tag fallback (dev-only)."
  default     = ""
}

job "sequencer" {
  datacenters = ["dc1"]
  type        = "service"

  # Sequencer-role nodes only.
  constraint {
    attribute = "${meta.role}"
    value     = "sequencer"
  }

  group "seq-a" {
    # One replica-a per distinct sequencer node; partition = node_index.
    count = 2
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

    # Resilience (chaos tests): restart a crashed task on the same node;
    # reschedule onto a healthy node on node loss. With 2 sequencer-role nodes
    # + distinct_hosts, a lost replica reschedules only when its node returns —
    # but its shard stays live throughout via the seq-b twin on the other node.
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

    task "sequencer-a" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "192.168.56.10:5000/kardamom-sequencer:dev"
        # force_pull kept for both paths (see the ingress job's comment):
        # the :dev fallback needs it; on the pinned path the 1.9.5 driver
        # pulls the tag but resolves the image by digest, so the pin holds.
        force_pull = true
        # Read-only rootfs (attested-identity P0.3): the sequencer writes only
        # to the bind-mounted aeron dir + Nomad's alloc/local/secrets mounts.
        # Validated by the cluster-e2e suite.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--config", "/local/sequencer.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--partition-count", "2",
          "--partition-index", "${meta.node_index}",
          # Cluster mode only: this node's cluster-egress (response) endpoint.
          # The cluster client's egress_channel is per-node (the node IP differs),
          # so it's injected here rather than baked into sequencer.toml.tpl.
          # Uniform port 40210 (cluster_egress_port); uniqueness comes from node_ip.
          "--cluster-egress-endpoint", "${meta.node_ip}:40210",
        ]
      }

      env {
        # Scrapable off-node (binary default is 127.0.0.1:9001); replica
        # group "a" stamped on every metric so the two racing replicas of a
        # shard are separable in Prometheus.
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9001"
        KARDAMOM_HOST_ID      = "node${meta.node_index}-seq-a"
      }

      # Cluster LogConfig (UDP multicast channels), consumed via --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      # Single-sourced from config/sequencer.toml.tpl (submit from
      # deploy/cluster/ — scripts/deploy.sh does this). partition_index /
      # sequencer_id in that file are placeholders overridden per-node by the
      # CLI flags above.
      template {
        destination = "local/sequencer.toml"
        data        = file("config/sequencer.toml.tpl")
      }

      resources {
        cpu    = 750
        memory = 512
      }
    }
  }

  group "seq-b" {
    # The racing twin: same node set, rotated shard (partition-offset 1), so
    # node N serves the shard its seq-a neighbour does NOT.
    count = 2
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

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

    task "sequencer-b" {
      driver = "docker"

      config {
        # Same digest-pin + fallback + readonly rationale as seq-a above.
        image           = var.image_ref != "" ? var.image_ref : "192.168.56.10:5000/kardamom-sequencer:dev"
        force_pull      = true
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--config", "/local/sequencer.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--partition-count", "2",
          "--partition-index", "${meta.node_index}",
          # Rotate onto the other shard (see header): replicas of a shard
          # never share a node.
          "--partition-offset", "1",
          # Distinct per-node cluster-egress endpoint: the seq-a process on
          # this node already binds :40210.
          "--cluster-egress-endpoint", "${meta.node_ip}:40211",
        ]
      }

      env {
        # The seq-a process on this node owns :9001; bind 0.0.0.0 so the
        # twin is scrapable off-node (loopback made seq-b invisible to
        # monitoring — exactly what would hide a zombie replica).
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9011"
        KARDAMOM_HOST_ID      = "node${meta.node_index}-seq-b"
      }

      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      template {
        destination = "local/sequencer.toml"
        data        = file("config/sequencer.toml.tpl")
      }

      resources {
        cpu    = 750
        memory = 512
      }
    }
  }
}
