# kardamom-executor replays the canonical order and applies state. It
# embeds the libmdbx StateWriter. It runs on its own node, exec1
# (192.168.56.31). It used to be co-located with sequencer #0 and
# ingress.
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-executor --config <executor.toml> --aeron-dir <dir> --shards 2 \
#       --chain-id 412346 --chain <genesis.toml>
#
# executor.toml is presence-checked only. The genesis renders from
# config/genesis/dev.toml, and passes through --chain. chain-id 412346
# comes from group_vars/all.yml. shards 2 equals partition_count.
#
# This mounts both the shared Aeron tmpfs aeron.dir and the persistent
# state_dir (/opt/kardamom/state), for the libmdbx StateWriter, so
# state survives restarts. On a restart against a non-empty state_dir,
# the executor runs Phase-2 crash recovery. It replays tx_data and
# tx_deposits from the Aeron Archive (replay-merge), and skip-counts
# past its durable cursor; tx_ordering re-reads from the Aeron Cluster
# egress. This needs the archive replay endpoint
# (--replay-destination-endpoint below), and the ingress and
# da-watcher recording tx_data and tx_deposits
# (--archive-durability on those jobs).
#
# This job uses file() for its templates, so submit it from the
# deploy/cluster/ directory. scripts/deploy.sh does this.

# Digest-pinned image. scripts/deploy.sh
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

job "executor" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "executor"
  }

  group "executor" {
    # Run one executor per executor-class node: 3-way redundant state
    # machines, each with a co-located recorder. distinct_hosts spreads
    # them across the nodes.
    count = 3
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

    # Resilience (chaos tests): restart a crashed task on the same
    # node, which resumes from persistent /opt/kardamom/state. On node
    # loss, reschedule onto a healthy node; a fresh node replays the
    # canonical order from genesis, which is fine for a redundant
    # state machine. With 3 executor-role nodes and distinct_hosts, a
    # lost replica reschedules to a spare node if one exists, or
    # degrades to 2.
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

    task "executor" {
      driver = "docker"

      # BAL granularity measurement (spec phase 1). This logs
      # per-block encoded sizes at each K, so the batch size can be
      # chosen from data. This stays unset in normal operation, and is
      # harmless (log-only) when set.
      env {
        # BAL attribution granularity. K=20 measured a 31% reduction
        # in frame bytes on contract workloads
        # (docs/agents/2026-08-01-bal-phase1-measurement and the DeFi
        # run), at zero parallelism cost under seeded execution.
        KARDAMOM_BAL_GRANULARITY = "20"
        # KARDAMOM_BAL_MEASURE stays unset in deployed profiles. The
        # K-ladder measure mode re-encodes every frame 4 times, on the
        # publisher thread. Under DeFi loads, that saturated the
        # publisher, filled the bounded exec-to-publisher handoff, and
        # back-pressured the exec thread into 10-15s stalls.
      }

      config {
        image = var.image_ref != "" ? var.image_ref : "192.168.56.10:5000/kardamom-executor:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it. On the pinned path, the
        # 1.9.5 driver pulls the tag but resolves the image by digest,
        # so the pin holds.
        force_pull = true
        # Read-only rootfs. The executor's
        # writable surfaces are state, checkpoints, and the aeron
        # directory. All are explicit bind mounts below, plus Nomad's
        # alloc, local, and secrets mounts. cluster-e2e validates
        # this.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/state:/opt/kardamom/state",
          # Periodic state checkpoints, for fast cold-start recovery.
          # This path stays distinct from state_dir, so a state-DB
          # wipe can recover from a checkpoint here, or from a peer
          # executor's, re-replicated in.
          "/opt/kardamom/checkpoints:/opt/kardamom/checkpoints",
        ]
        args = [
          "--config", "/local/executor.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          # This replica's index selects its per-replica tx_receipts
          # MDS endpoint (channels.toml tx_receipts_endpoint_base_port
          # plus index). The job is count-based with distinct_hosts,
          # so ${NOMAD_ALLOC_INDEX} stays stable at 0 through N, and
          # matches the co-located recorder's id.
          "--recorder-id", "${NOMAD_ALLOC_INDEX}",
          # Cluster mode only: this node's cluster-egress (response)
          # endpoint. The cluster client's egress_channel is per node,
          # since the node IP differs, so it is injected here instead
          # of baked into config/executor.toml. The port, 40210
          # (cluster_egress_port), stays uniform; uniqueness comes
          # from node_ip.
          "--cluster-egress-endpoint", "${meta.node_ip}:40210",
          "--shards", "2",
          "--chain-id", "412346",
          "--chain", "/local/genesis.toml",
          # Join-miss archive refetch (tx_data and tx_deposits). When
          # the live multicast misses a canonical ref's envelope
          # (down-window, image lapse, blackout), it replays in-band
          # from the durability archives listed in channels.toml
          # (ingress .31/.32; aux .61). Replayed fragments land on
          # 40130, and archive-control responses land on 40140, both
          # on this node's cluster NIC (${meta.node_ip}). One executor
          # runs per node (distinct_hosts), so there is no
          # cross-replica collision. tx_ordering recovery goes through
          # the Aeron Cluster client's REPLAY_FROM.
          "--replay-destination-endpoint", "${meta.node_ip}:40130",
          "--archive-control-response-endpoint", "${meta.node_ip}:40140",
          # Fast cold-start recovery: restore the newest checkpoint
          # into a wiped or empty state_dir before startup, replaying
          # only the tail, not from genesis. Write a checkpoint every
          # 20s as the chain advances.
          "--checkpoint-dir", "/opt/kardamom/checkpoints",
          "--checkpoint-interval-secs", "20",
          # Peer checkpoint exchange, a full-resync fallback. Each
          # replica serves its newest checkpoint on 9014, and can
          # fetch one from the others. A node whose replay cursor, or
          # genesis join, fell below the cluster's bounded retention
          # floor gets REPLAY_UNAVAILABLE, and can only be repaired
          # with peer state. Deterministic replicas make any peer's
          # checkpoint a valid restore source. Including self in the
          # peer list is harmless; its own checkpoint never satisfies
          # the floor.
          "--checkpoint-serve-addr", "${meta.node_ip}:9014",
          "--checkpoint-peers", "192.168.56.41:9014,192.168.56.42:9014,192.168.56.43:9014",
          # Bind the Prometheus exporter on all interfaces; the
          # default is loopback. The chaos suite probes it directly
          # over the cluster bridge (http://<node_ip>:9004/metrics),
          # which keeps dockerd out of the observation path. A docker
          # kill of a privileged sibling node can stall docker exec
          # runner-wide for minutes, which reads as "block 0 -> 0"
          # while executors were healthy. The bridge is the isolated
          # 192.168.56.0/24 test segment; loopback scrapes (docker exec
          # curl 127.0.0.1:9004) keep working too.
          "--metrics-addr", "0.0.0.0:9004",
        ]
      }

      # Cluster LogConfig (UDP multicast channels), read through
      # --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      # Presence-checked config. Content lives in config/executor.toml.
      template {
        destination = "local/executor.toml"
        data        = file("config/executor.toml")
      }

      # The chain genesis comes from one source, config/genesis/dev.toml.
      # It prefunds Anvil accounts #0 through #15 with 1000 ETH each,
      # plus the ERC-7955 factory predeploy.
      template {
        destination = "local/genesis.toml"
        data        = file("config/genesis/dev.toml")
      }

      resources {
        cpu    = 1000
        memory = 1536
      }
    }
  }
}
