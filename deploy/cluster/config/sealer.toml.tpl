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
# config/channels.toml.tpl. Sealer runs on w2 (192.168.56.22); 40112 is the
# sealer's control port (seq1 on the same host uses 40111).
channel_b_mdc_control = "192.168.56.22:40112"

# Tx records on stream 1; sealer-emitted boundary markers on a distinct
# stream so subscribers can demultiplex by type on the same channel.
# channel_b_boundary_stream_id MUST equal tx_ordering_stream_id in
# channels.toml.tpl so all publishers/subscribers agree on the boundary stream.
channel_b_tx_stream_id = 1
channel_b_boundary_stream_id = 1001

# Sealer tick cadence (block boundary emission interval).
tick_interval_ms = 250
