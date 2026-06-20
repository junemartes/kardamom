# kardamom-sealer config template.
#
# Rendered into the sealer alloc by the Nomad `template` stanza in
# nomad/sealer.nomad.hcl. Schema: crates/sealer/src/config.rs (SealerConfig).
#
# The sealer publishes the canonical tx_ordering stream (channel B) via Aeron
# MDC: it binds its OWN `control-mode=dynamic` publication to its node IP +
# control port (`channel_b_mdc_control`), which MUST appear in
# `tx_ordering_mdc_publishers` in config/channels.toml.tpl. The sealer also runs
# the archive-at-the-sealer durability sidecar (kardamom-sealer
# --archive-durability), recording that publication and publishing its durable
# position as the watermark ingress gates on. `channel_b_uri` is the single-host
# IPC fallback only (unused in the cluster because MDC is enabled in the
# channels config); it must still be a valid URI.

# host_id: stable identity for this sealer host. w2 is sequencer_id 1 in the
# cluster contract (group_vars/all.yml); we reuse that as the sealer host_id.
host_id = 1

# channel_b_uri: single-host IPC fallback. UNUSED when MDC is enabled in
# channels.toml (the cluster case), but must be a valid URI.
channel_b_uri = "aeron:ipc?alias=tx-ordering"

# channel_b_mdc_control: this sealer's tx_ordering MDC control endpoint
# (ip:port). MUST be one of the tx_ordering_mdc_publishers entries in
# config/channels.toml.tpl. The sealer runs on the sealer node-class
# (192.168.56.51); uniform MDC control port 40110 (unique by node IP).
channel_b_mdc_control = "192.168.56.51:40110"

# Tx records on stream 1; sealer-emitted boundary markers on a distinct
# stream so subscribers can demultiplex by type on the same channel.
# channel_b_boundary_stream_id MUST equal tx_ordering_stream_id in
# channels.toml.tpl so all publishers/subscribers agree on the boundary stream.
channel_b_tx_stream_id = 1
channel_b_boundary_stream_id = 1001

# Sealer tick cadence (block boundary emission interval). Slowed from 250ms to
# 2s for the container cluster-e2e: on an 8-core host running a whole cluster
# (3 EVM executors + their recorders + 8 Aeron drivers) the executors can't
# replay 4 empty blocks/sec under CPU contention and fall behind. At 1 block/2s
# there are ~8x fewer empty blocks to process, so the executors keep pace; the
# added block latency is irrelevant to the smoke. (A latency-tuned single-host
# deployment can lower this.)
tick_interval_ms = 2000
