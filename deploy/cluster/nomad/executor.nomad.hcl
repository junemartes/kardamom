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

      config {
        image        = "192.168.56.10:5000/kardamom-executor:dev"
        # Always pull the freshly-built image: the mutable :dev tag would otherwise
        # let Nomad reuse a stale node-cached layer across rebuilds (caused a
        # crash-retry storm that stalled the deploy).
        force_pull    = true
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/state:/opt/kardamom/state",
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
          # Crash-recovery archive replay-merge endpoint for tx_data / tx_deposits
          # (used only when the state DB is non-empty, i.e. on a restart). Bind on
          # this node's cluster NIC (${meta.node_ip}) at port 40130; one executor
          # per node (distinct_hosts) so no cross-replica collision. (tx_ordering
          # recovery is handled by the Aeron Cluster client, so there is no
          # separate live-destination endpoint anymore.)
          "--replay-destination-endpoint", "${meta.node_ip}:40130",
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
