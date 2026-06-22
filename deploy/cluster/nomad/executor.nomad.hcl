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
