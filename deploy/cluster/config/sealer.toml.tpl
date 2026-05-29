# kardamom-sealer config template.
#
# Rendered into the sealer alloc by the Nomad `template` stanza in
# nomad/sealer.nomad.hcl. Schema: crates/sealer/src/config.rs (SealerConfig).
#
# The sealer publishes the canonical tx_ordering stream (channel B). In the
# multi-host UDP topology that stream lives on the sealer node (w2,
# 192.168.56.22) — see config/channels.toml.tpl `tx_ordering_channel`. The
# stream ids below MUST agree with channels.toml.tpl's
# tx_ordering_stream_id / its boundary side-stream so subscribers (executor,
# sequencer) demultiplex correctly.

# host_id: stable identity for this sealer host. w2 is sequencer_id 1 in the
# cluster contract (group_vars/all.yml); we reuse that as the sealer host_id.
host_id = 1

# channel_b_uri: UDP unicast endpoint on the sealer node (w2). Port is
# aeron_channel_base + 1 (40001) — the tx_ordering offset defined in
# config/channels.toml.tpl.
channel_b_uri = "aeron:udp?endpoint=192.168.56.22:40001"

# Tx records on stream 1; sealer-emitted boundary markers on a distinct
# stream so subscribers can demultiplex by type on the same channel.
channel_b_tx_stream_id = 1
channel_b_boundary_stream_id = 1001

# Sealer tick cadence (block boundary emission interval).
tick_interval_ms = 250
