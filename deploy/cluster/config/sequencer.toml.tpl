# kardamom-sequencer config template.
#
# Rendered into each sequencer alloc by nomad/sequencer.nomad.hcl. Schema:
# crates/sequencer/src/config.rs (SequencerConfig).
#
# The sequencer runs as a Nomad *system* job across both workers (w1, w2).
# This file supplies the static, node-independent defaults; the per-node
# identity (partition_index / sequencer_id) is injected at launch via CLI
# flags from Nomad node meta:
#     --partition-count 2
#     --partition-index ${meta.sequencer_id}
#     --sequencer-id    ${meta.sequencer_id}
# (kardamom-sequencer CLI flags override the config values; see
# crates/sequencer/src/bin/kardamom-sequencer.rs.) The partition_index /
# sequencer_id below are therefore placeholders that the CLI overrides.

# M = 2 sequencers (partition_count, from group_vars/all.yml partition_count).
partition_count = 2

# Overridden per-node by --partition-index ${meta.sequencer_id}.
partition_index = 0

# Overridden per-node by --sequencer-id ${meta.sequencer_id}.
sequencer_id = 0

# Per-sender pending bound and backpressure behaviour.
max_pending_per_sender = 16
backpressure_policy = "return_immediately"

[cluster]
ingress_endpoints = "0=192.168.56.51:40200,1=192.168.56.52:40200,2=192.168.56.53:40200"
initial_leader_member_id = 0
ingress_stream_id = 101
egress_stream_id = 102
keep_alive_interval_ms = 1000
# egress_channel is set per-node by --cluster-egress-endpoint (the node IP differs).
