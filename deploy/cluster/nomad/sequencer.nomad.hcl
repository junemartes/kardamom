# kardamom-sequencer — partitions and orders L2 txs. Runs as a Nomad *system*
# job constrained to worker nodes (${meta.role} == worker), so exactly one
# instance lands on each of w1 (sequencer_id 0) and w2 (sequencer_id 1).
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
  type        = "system"

  # Land on every worker node.
  constraint {
    attribute = "${meta.role}"
    value     = "worker"
  }

  group "sequencer" {
    network {
      mode = "host"
    }

    task "sequencer" {
      driver = "docker"

      config {
        image        = "192.168.56.11:5000/kardamom-sequencer:dev"
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--config", "/local/sequencer.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--partition-count", "2",
          "--partition-index", "${meta.sequencer_id}",
          "--sequencer-id", "${meta.sequencer_id}",
        ]
      }

      # Mirrors config/sequencer.toml.tpl. partition_index / sequencer_id here
      # are placeholders overridden per-node by the CLI flags above.
      template {
        destination = "local/sequencer.toml"
        data        = <<EOF
partition_count = 2
partition_index = 0
sequencer_id = 0
max_pending_per_sender = 16
backpressure_policy = "return_immediately"
EOF
      }

      resources {
        cpu    = 1000
        memory = 1024
      }
    }
  }
}
