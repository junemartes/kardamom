# kardamom-sequencer — partitions and orders L2 txs. Runs as a Nomad *system*
# job constrained to worker nodes (${meta.role} == sequencer), so exactly one
# instance lands on each of sq1 (sequencer_id 0) and sq2 (sequencer_id 1).
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs + the cluster contract):
#   kardamom-sequencer --config <sequencer.toml> --aeron-dir <dir>
# with per-node identity injected from Nomad node meta:
#   --partition-count 2
#   --partition-index ${meta.sequencer_id}
#   --sequencer-id    ${meta.sequencer_id}
# (CLI flags override config; see crates/sequencer/src/bin/kardamom-sequencer.rs.)
#
# Each worker reads its own ${meta.sequencer_id} (set by Ansible Nomad node
# meta), so the single job spec yields distinct sequencer identities per node.
# partition_count = 2 from group_vars/all.yml.
#
# Shares the node Aeron media driver via the bind-mounted tmpfs aeron.dir.

job "sequencer" {
  datacenters = ["dc1"]
  type        = "service"

  # One sequencer per dedicated sequencer node.
  constraint {
    attribute = "${meta.role}"
    value     = "sequencer"
  }

  group "sequencer" {
    # partition_count (group_vars/all.yml). distinct_hosts → one sequencer per
    # distinct sequencer node; partition-index = ${NOMAD_ALLOC_INDEX} (0,1).
    count = 2
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

    network {
      mode = "host"
    }

    task "sequencer" {
      driver = "docker"

      config {
        image        = "192.168.56.10:5000/kardamom-sequencer:dev"
        # Always pull the freshly-built image: the mutable :dev tag would otherwise
        # let Nomad reuse a stale node-cached layer across rebuilds (caused a
        # crash-retry storm that stalled the deploy).
        force_pull    = true
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--config", "/local/sequencer.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--partition-count", "2",
          "--partition-index", "${NOMAD_ALLOC_INDEX}",
          "--sequencer-id", "${NOMAD_ALLOC_INDEX}",
          # This node's tx_ordering MDC publisher control endpoint. Uniform port
          # 40110 on this node's IP; uniqueness comes from node_ip. Must match an
          # entry in tx_ordering_mdc_publishers in channels.toml.tpl.
          "--tx-ordering-mdc-control", "${meta.node_ip}:40110",
        ]
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
}
