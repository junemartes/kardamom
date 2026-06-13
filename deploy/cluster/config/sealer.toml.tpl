# kardamom-sealer config template.
#
# Rendered into the sealer alloc by the Nomad `template` stanza in
# nomad/sealer.nomad.hcl. Schema: crates/sealer/src/config.rs (SealerConfig).
#
# The sealer publishes the canonical tx_ordering stream (channel B). The sealer
# overrides `channels.tx_ordering_channel` from its own config, so `channel_b_uri`
# below MUST be byte-identical to `tx_ordering_channel` in
# config/channels.toml.tpl (same multicast group/port/interface) and
# `channel_b_boundary_stream_id` MUST equal that file's `tx_ordering_stream_id`,
# so the sealer, the sequencers, the executor, and the 3 recorders all agree on
# one stream. (check-contract.py enforces the URI match.)

# host_id: stable identity for this sealer host. w2 is sequencer_id 1 in the
# cluster contract (group_vars/all.yml); we reuse that as the sealer host_id.
host_id = 1

# channel_b_uri: the multicast tx_ordering channel (matches channels.toml.tpl).
channel_b_uri = "aeron:udp?endpoint=239.192.56.11:40010|interface=192.168.56.0/24|ttl=1"

# Tx records on stream 1; sealer-emitted boundary markers on a distinct
# stream so subscribers can demultiplex by type on the same channel.
channel_b_tx_stream_id = 1
channel_b_boundary_stream_id = 1001

# Sealer tick cadence (block boundary emission interval).
tick_interval_ms = 250
