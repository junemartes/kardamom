# kardamom LogConfig — multi-host cluster (Aeron UDP multicast).
#
# CONSUMED via `--log-config` (issue #36): every pipeline service and the
# recorder/quorum process is launched with `--log-config /local/channels.toml`,
# which deserializes this whole file into a `kardamom_log::config::LogConfig`
# (schema: crates/log/src/config.rs). Any field omitted here inherits the
# built-in default; unknown keys are rejected.
#
# ── Why multicast ────────────────────────────────────────────────────────────
# The pipeline has channels with multiple publishers and/or multiple
# subscribers spread across hosts (tx_ordering: 2 sequencers + the sealer;
# fsync watermark: 3 recorders → the aggregator; the 3 recorder archives all
# subscribe tx_ordering). UDP unicast can't express multi-publisher/
# multi-subscriber, and MDC needs one control endpoint per publisher host.
# A single UDP **multicast** URI is valid on every node, so one shared file
# serves the whole cluster — this is also why there is no per-node rendering
# (closes issue #37). `{sid}`/`{rid}` survive only in the `alias` label;
# per-stream identity is the stream id, so one multicast group carries both
# sequencers / all three recorders.
#
# ⚠ HIGHEST-RISK AREA TO VALIDATE. Whether Aeron preserves the canonical
# tx_ordering order across multiple cross-host publishers over UDP multicast is
# exactly what the cluster e2e (.github/workflows/cluster-e2e.yml) exercises;
# the single-host IPC defaults (no --log-config) remain the known-good path for
# local/e2e. See deploy/cluster/README.md.
#
# Multicast group / port plan (odd groups 239.192.56.11‥25 on the cluster subnet;
# `interface` pins egress to the 192.168.56.0/24 host-only network, ttl=1 keeps
# traffic on-segment):

[aeron]
# Archive control rides aeron:ipc (the LogConfig default — restated here for
# visibility): each kardamom-recorder is co-located with its node's
# ArchivingMediaDriver and shares its aeron.dir, so it reaches the archive over
# the local IPC control channel. (UDP archive control would time out — the
# control response can't be reliably routed back to a co-located client; see
# crates/log/src/recorder.rs::connect_archive.)
archive_control_request_channel = "aeron:ipc"
archive_control_response_channel = "aeron:ipc"
# Where the recorder's segment files live (bind-mounted; paths.archive_dir).
archive_dir = "/opt/kardamom/archive"

[quorum]
# 3 recorders, tolerate 1 loss (Q-of-N fsync gate for ingress --ack-policy
# on-quorum). Mirrors cluster_nodes recorder count in group_vars/all.yml.
n = 3
q = 2

[channels]
# Aeron multicast DATA addresses must be ODD ("multicast data address must be
# odd" — the driver derives the even control address as data-1). So every
# endpoint below is odd and they are spaced by 2; the derived control addresses
# land on the disjoint even addresses and never collide. `interface` pins the
# join to the cluster NIC; `ttl=1` keeps traffic on-segment.
#
# --- TxData: per-sequencer exclusive publisher of full TxEnvelope bytes. ------
# One multicast group; stream id = base + sequencer_id distinguishes w1/w2.
tx_data_channel_template = "aeron:udp?endpoint=239.192.56.11:40000|interface=192.168.56.0/24|ttl=1|alias=a-{sid}"
tx_data_stream_id_base = 2000

# --- TxOrdering: canonical orderer, RECORDED. Multi-publisher (sequencers +
# sealer). The sealer's own channel_b_uri (config/sealer.toml.tpl) MUST match
# this group/port/stream so all publishers and the recorders agree.
tx_ordering_channel = "aeron:udp?endpoint=239.192.56.13:40010|interface=192.168.56.0/24|ttl=1"
tx_ordering_stream_id = 1001

# --- TxReceipts: receipts + block boundaries. Not recorded. ------------------
tx_receipts_channel = "aeron:udp?endpoint=239.192.56.15:40020|interface=192.168.56.0/24|ttl=1"
tx_receipts_stream_id = 1002

# --- TxErrors: sequencer-emitted rejection signals. RAM only. ----------------
# Stream id 1015 (not 1003) to avoid colliding with the receipts BlockBoundary
# side-stream (tx_receipts_stream_id + 1); see crates/log/src/config.rs.
tx_errors_channel = "aeron:udp?endpoint=239.192.56.17:40030|interface=192.168.56.0/24|ttl=1"
tx_errors_stream_id = 1015

# --- TxDeposits: DA watcher publishes Deposit envelopes; sequencers subscribe.
tx_deposits_channel = "aeron:udp?endpoint=239.192.56.19:40040|interface=192.168.56.0/24|ttl=1"
tx_deposits_stream_id = 1016

# --- fsync watermark (tx_ordering), per-recorder. `{rid}` in alias only;
# stream id is shared (1010) and recorder_id rides inside the FsyncWatermark
# payload, so one multicast group serves all 3 recorders → the aggregator.
fsync_watermark_channel_template = "aeron:udp?endpoint=239.192.56.21:40050|interface=192.168.56.0/24|ttl=1|alias=fsync-wm-{rid}"
fsync_watermark_stream_id = 1010

# --- fsync watermark (tx_data), per-sequencer. RAM only; single-host fsync. ---
fsync_watermark_tx_data_channel_template = "aeron:udp?endpoint=239.192.56.23:40060|interface=192.168.56.0/24|ttl=1|alias=fsync-wm-a-{sid}"
fsync_watermark_tx_data_stream_id_base = 1030

# --- Aggregated quorum watermark (tx_ordering). Published by the --aggregate
# recorder; subscribed by ingress for the on-quorum ack gate.
quorum_watermark_channel = "aeron:udp?endpoint=239.192.56.25:40070|interface=192.168.56.0/24|ttl=1"
quorum_watermark_stream_id = 1020
