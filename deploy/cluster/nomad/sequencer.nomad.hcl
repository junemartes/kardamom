# kardamom-sequencer partitions and orders L2 txs, with 2 racing
# replicas per shard. Two identical service groups ("seq-a", "seq-b"),
# each with count = 2, run on the 2 sequencer-role nodes. Both replicas
# of a shard subscribe to the same per-shard tx_data multicast stream,
# and both offer refs to the Aeron Cluster, which dedups by
# canonical_id first-seen. This is the same mechanism that already
# absorbs the M duplicate DepositRefs. So a replica crash costs
# nothing; its twin never stopped. See
# docs/agents/replicated-sequencer-shards-spec.md.
#
# Placement is deterministic, derived from node meta, not alloc index:
#   group seq-a: partition = ${meta.node_index}            → node-0: shard 0, node-1: shard 1
#   group seq-b: partition = ${meta.node_index} + offset 1 → node-0: shard 1, node-1: shard 0
# So the two replicas of any shard always land on different nodes, and
# a node loss leaves every shard with one live replica.
#
# Colocated-replica port separation. With host networking, two
# sequencer processes run per node. seq-b moves its Prometheus listener
# to :9011 and its per-node cluster-egress (response) endpoint to
# :40211; seq-a keeps the standard :9001 / :40210. Both metrics
# listeners bind 0.0.0.0. The binary default is loopback, which is
# unreachable off-node in a host-network job, and would leave the
# replicas unscrapable.
#
# Note for consumers: both replicas of a shard process the same tx
# stream, so per-shard tx totals exist once per replica. Aggregate
# stream-derived counters with `max by (partition)`, never `sum`.
#
# This shares the node's Aeron media driver, through the bind-mounted
# tmpfs aeron.dir.

# Digest-pinned image. scripts/deploy.sh
# passes the repo:tag@sha256:... reference captured at push time
# (deploy/cluster/images.digests). Both racing replica groups run the
# same pinned bytes. The empty default falls back to the mutable :dev
# tag in the task configs. That fallback is a dev affordance for manual
# `nomad job run` during debugging, not a production path.
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
    # Run one replica-a per distinct sequencer node. partition =
    # node_index.
    count = 2
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

    # Resilience (chaos tests): restart a crashed task on the same
    # node, and reschedule onto a healthy node on node loss. With 2
    # sequencer-role nodes and distinct_hosts, a lost replica
    # reschedules only when its node returns. Its shard stays live the
    # whole time, through the seq-b twin on the other node.
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
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it. On the pinned path, the
        # 1.9.5 driver pulls the tag but resolves the image by digest,
        # so the pin holds.
        force_pull = true
        # Read-only rootfs. The sequencer
        # writes only to the bind-mounted aeron directory, plus
        # Nomad's alloc, local, and secrets mounts. cluster-e2e
        # validates this.
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
          # Cluster mode only: this node's cluster-egress (response)
          # endpoint. The cluster client's egress_channel is per node,
          # since the node IP differs, so it is injected here instead
          # of baked into sequencer.toml.tpl. The port, 40210
          # (cluster_egress_port), stays uniform; uniqueness comes
          # from node_ip.
          "--cluster-egress-endpoint", "${meta.node_ip}:40210",
        ]
      }

      env {
        # This binds 0.0.0.0 so the process is scrapable off-node;
        # the binary default is 127.0.0.1:9001. Replica group "a" is
        # stamped on every metric, so the two racing replicas of a
        # shard are separable in Prometheus.
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9001"
        KARDAMOM_HOST_ID      = "node${meta.node_index}-seq-a"
      }

      # Cluster LogConfig (UDP multicast channels), read through
      # --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      # This comes from one source, config/sequencer.toml.tpl. Submit
      # from deploy/cluster/; scripts/deploy.sh does this.
      # partition_index and sequencer_id in that file are placeholders,
      # overridden per node by the CLI flags above.
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
    # This is the racing twin: the same node set, with a rotated
    # shard (partition-offset 1). Node N serves the shard its seq-a
    # neighbor does not.
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
        # Same digest-pin, fallback, and readonly reasoning as seq-a above.
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
          # Rotate onto the other shard; see the header. Replicas of a
          # shard never share a node.
          "--partition-offset", "1",
          # Use a distinct per-node cluster-egress endpoint. The seq-a
          # process on this node already binds :40210.
          "--cluster-egress-endpoint", "${meta.node_ip}:40211",
        ]
      }

      env {
        # The seq-a process on this node owns :9001. Bind 0.0.0.0 so
        # the twin is scrapable off-node. Loopback would make seq-b
        # invisible to monitoring, which could hide a zombie replica.
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
