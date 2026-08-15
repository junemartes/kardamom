# kardamom-executor — replays the canonical order and applies state (embeds the
# libmdbx StateWriter). Placed on its own node exec1 (192.168.56.31). (Was co-located with sequencer
# #0 and ingress.
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-executor --config <executor.toml> --aeron-dir <dir> --shards 2 \
#       --chain-id 412346 --chain <genesis.toml>
#
# executor.toml is presence-checked only. The genesis is rendered from
# config/genesis/dev.toml and passed via --chain. chain-id 412346 from
# group_vars/all.yml. shards 2 = partition_count.
#
# Mounts both the shared Aeron tmpfs aeron.dir AND the persistent state_dir
# (/opt/kardamom/state) for the libmdbx StateWriter so state survives restarts.
# On a restart against a non-empty state_dir the executor runs Phase-2 crash
# recovery: it replays tx_data + tx_deposits from the Aeron Archive (replay-merge)
# and skip-counts past its durable cursor (tx_ordering is re-read from the Aeron
# Cluster egress). That needs the archive replay endpoint
# (--replay-destination-endpoint below) and the ingress / da-watcher recording
# tx_data / tx_deposits (--archive-durability on those jobs).
#
# NOTE: this job uses file() for its templates, so submit it from the
# deploy/cluster/ directory (scripts/deploy.sh does this).

# Digest-pinned image (attested-identity P0.1): scripts/deploy.sh passes the
# repo:tag@sha256:... reference captured at push time (deploy/cluster/
# images.digests). The empty default falls back to the mutable :dev tag in
# the task config — a dev affordance for manual `nomad job run` during
# debugging, NOT a production path.
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
    # One executor per executor-class node (3-way redundant state machines, each
    # with a co-located recorder). distinct_hosts spreads them across the nodes.
    count = 3
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

    # Resilience (chaos tests): restart a crashed task on the same node (resumes
    # from persistent /opt/kardamom/state); reschedule onto a healthy node on
    # node loss (a fresh node replays the canonical order from genesis — fine for
    # a redundant state machine). With 3 executor-role nodes + distinct_hosts, a
    # lost replica reschedules to a spare node if one exists, else degrades to 2.
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

      # BAL granularity measurement (spec phase 1): logs per-block
      # encoded sizes at each K so the batch size can be chosen from data.
      # Unset in normal operation; harmless (log-only) when set.
      env {
        # BAL attribution granularity: K=20 measured -31% frame bytes on
        # contract workloads (docs/agents/2026-08-01-bal-phase1-measurement
        # + the DeFi run) at zero parallelism cost under seeded execution.
        KARDAMOM_BAL_GRANULARITY = "20"
        # NOTE: KARDAMOM_BAL_MEASURE stays UNSET in deployed profiles. The
        # K-ladder measure mode re-encodes every frame 4x ON THE PUBLISHER
        # THREAD; under DeFi loads that saturated the publisher, filled the
        # bounded exec->publisher handoff, and back-pressured the exec
        # thread into 10-15s stalls (the wandering ramp cliff).
      }

      config {
        image = var.image_ref != "" ? var.image_ref : "192.168.56.10:5000/kardamom-executor:dev"
        # force_pull kept for both paths (see the ingress job's comment):
        # the :dev fallback needs it; on the pinned path the 1.9.5 driver
        # pulls the tag but resolves the image by digest, so the pin holds.
        force_pull = true
        # Read-only rootfs (attested-identity P0.3): the executor's writable
        # surfaces — state, checkpoints, aeron dir — are all explicit bind
        # mounts below, plus Nomad's alloc/local/secrets mounts. Validated by
        # the cluster-e2e suite.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/state:/opt/kardamom/state",
          # Periodic state checkpoints (fast cold-start recovery). Kept on a
          # distinct path from state_dir so a state-DB wipe can be recovered from
          # a checkpoint here (or from a peer executor's, re-replicated in).
          "/opt/kardamom/checkpoints:/opt/kardamom/checkpoints",
        ]
        args = [
          "--config", "/local/executor.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          # This replica's index — selects its per-replica tx_receipts MDS
          # endpoint (channels.toml tx_receipts_endpoint_base_port + index).
          # The job is count-based with distinct_hosts, so ${NOMAD_ALLOC_INDEX}
          # is stable 0..N and matches the co-located recorder's id.
          "--recorder-id", "${NOMAD_ALLOC_INDEX}",
          # Cluster mode only: this node's cluster-egress (response) endpoint.
          # The cluster client's egress_channel is per-node (the node IP differs),
          # so it's injected here rather than baked into config/executor.toml.
          # Uniform port 40210 (cluster_egress_port); uniqueness comes from node_ip.
          "--cluster-egress-endpoint", "${meta.node_ip}:40210",
          "--shards", "2",
          "--chain-id", "412346",
          "--chain", "/local/genesis.toml",
          # Join-miss archive refetch (tx_data / tx_deposits): a canonical ref
          # whose envelope the live multicast missed (down-window, image lapse,
          # blackout) is replayed in-band from the durability archives listed in
          # channels.toml (ingress .31/.32; aux .61). Replayed fragments land on
          # 40130, archive-control responses on 40140, both on this node's
          # cluster NIC (${meta.node_ip}); one executor per node
          # (distinct_hosts) so no cross-replica collision. (tx_ordering
          # recovery is handled by the Aeron Cluster client's REPLAY_FROM.)
          "--replay-destination-endpoint", "${meta.node_ip}:40130",
          "--archive-control-response-endpoint", "${meta.node_ip}:40140",
          # Fast cold-start recovery: restore the newest checkpoint into a
          # wiped/empty state_dir before startup (replaying only the tail, not
          # from genesis), and write a checkpoint every 20s as the chain advances.
          "--checkpoint-dir", "/opt/kardamom/checkpoints",
          "--checkpoint-interval-secs", "20",
          # Peer checkpoint exchange (full-resync fallback): each replica serves
          # its newest checkpoint on 9014 and can fetch one from the others. A
          # node whose replay cursor (or genesis join) fell below the cluster's
          # bounded retention floor gets REPLAY_UNAVAILABLE and can only be
          # repaired with peer state — deterministic replicas make any peer's
          # checkpoint a valid restore source. Self is harmlessly included in
          # the peer list (its own checkpoint never satisfies the floor).
          "--checkpoint-serve-addr", "${meta.node_ip}:9014",
          "--checkpoint-peers", "192.168.56.41:9014,192.168.56.42:9014,192.168.56.43:9014",
          # Bind the Prometheus exporter on ALL interfaces (default is
          # loopback): the chaos suite probes it DIRECTLY over the cluster
          # bridge (http://<node_ip>:9004/metrics), keeping dockerd out of
          # the observation path — a docker kill of a privileged sibling node
          # can stall docker exec runner-wide for minutes, which read as
          # "block 0 -> 0" while executors were healthy (issue #76). The
          # bridge is the isolated 192.168.56.0/24 test segment; loopback
          # scrapes (docker exec curl 127.0.0.1:9004) keep working too.
          "--metrics-addr", "0.0.0.0:9004",
        ]
      }

      # Cluster LogConfig (UDP multicast channels), consumed via --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      # Presence-checked config (content lives in config/executor.toml).
      template {
        destination = "local/executor.toml"
        data        = file("config/executor.toml")
      }

      # Chain genesis, single-sourced from config/genesis/dev.toml. Prefunds
      # Anvil accounts #0..#15 with 1000 ETH each + the ERC-7955 factory
      # predeploy.
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
