# kardamom LogConfig — multi-host cluster (Aeron UDP multicast + tx_ordering MDC).
#
# CONSUMED via `--log-config` (issue #36): every pipeline service is launched
# with `--log-config /local/channels.toml`, which deserializes this whole file
# into a `kardamom_log::config::LogConfig` (schema: crates/log/src/config.rs).
# Any field omitted here inherits the built-in default; unknown keys are
# rejected.
#
# ── Transport model ──────────────────────────────────────────────────────────
# Most fan-out/fan-in channels still use UDP **multicast** (one shared URI valid
# on every node, no per-node rendering). The exception is **tx_ordering**, which
# now uses Aeron **MDC** (multi-destination-cast): each publisher (sealer + each
# sequencer) opens its OWN `control-mode=dynamic` publication bound to its node
# IP + a fixed control port; subscribers (executor, and the sealer's archive
# durability sidecar) attach to EVERY publisher's control endpoint and merge the
# images. This removes the shared tx_ordering multicast group whose
# subscriber-churn froze all executors' images (killing a recorder froze
# everything) — the bug this whole change fixes. The shared-multicast
# `tx_ordering_channel` below is kept only as the single-host IPC fallback; it is
# unused once `tx_ordering_mdc_control_template` is set.
#
# ⚠ HIGHEST-RISK AREA TO VALIDATE: that Aeron preserves the canonical
# tx_ordering order across the merged MDC images. The merge/dedup/boundary-
# alignment logic in the executor is unchanged (the per-image subscriber
# positions are identical to the shared-multicast case); the cluster e2e
# (.github/workflows/cluster-e2e.yml) exercises it. Single-host IPC defaults
# (no --log-config) remain the known-good local/e2e path. See README.md.
#
# Multicast group / port plan (groups 239.192.56.10‥14 on the cluster subnet;
# `interface` pins egress to the 192.168.56.0/24 host-only network, ttl=1 keeps
# traffic on-segment). tx_ordering MDC control ports: 40110 (seq0@w1),
# 40111 (seq1@w2), 40112 (sealer@w2); durable watermark on 239.192.56.17:40070.

[aeron]
# Archive control rides aeron:ipc (the LogConfig default — restated here for
# visibility): the sealer's durability sidecar is co-located with its node's
# ArchivingMediaDriver and shares its aeron.dir, so it reaches the archive over
# the local IPC control channel. (UDP archive control would time out — the
# control response can't be reliably routed back to a co-located client; see
# crates/log/src/recorder.rs::connect_archive.)
archive_control_request_channel = "aeron:ipc"
archive_control_response_channel = "aeron:ipc"
# Where the sealer-archive segment files live (bind-mounted; paths.archive_dir).
archive_dir = "/opt/kardamom/archive"

[quorum]
# VESTIGIAL after the move to archive-at-the-sealer durability. There is no
# longer a Q-of-N quorum aggregator; the single sealer archive's durable
# position is THE watermark. These values are retained only so an older
# channels.toml that still carries a [quorum] section parses (deny_unknown_fields
# would otherwise reject the section). n=q=1 documents "one durable copy".
n = 1
q = 1

[channels]
# --- TxData: per-sequencer exclusive publisher of full TxEnvelope bytes. ------
# One multicast group; stream id = base + sequencer_id distinguishes w1/w2.
tx_data_channel_template = "aeron:udp?endpoint=239.192.56.10:40000|interface=192.168.56.0/24|ttl=1|alias=a-{sid}"
tx_data_stream_id_base = 2000

# --- TxOrdering: canonical orderer, via MDC (see header). --------------------
# `tx_ordering_channel` is the single-host IPC fallback only — UNUSED in the
# cluster because tx_ordering_mdc_control_template below is set.
tx_ordering_channel = "aeron:ipc?alias=tx-ordering"
tx_ordering_stream_id = 1001
# MDC control template: `{ctl}` ← each publisher's ip:port control endpoint.
tx_ordering_mdc_control_template = "aeron:udp?control={ctl}|control-mode=dynamic|interface=192.168.56.0/24"
# Every tx_ordering publisher's control endpoint. Subscribers attach to all of
# them; each publisher selects its own (sealer via channel_b_mdc_control in
# sealer.toml; each sequencer via --tx-ordering-mdc-control / env in
# sequencer.nomad.hcl). Order: seq0@w1, seq1@w2, sealer@w2.
tx_ordering_mdc_publishers = [
  "192.168.56.21:40110",
  "192.168.56.22:40111",
  "192.168.56.22:40112",
]

# --- TxReceipts: receipts + block boundaries. Not recorded. ------------------
tx_receipts_channel = "aeron:udp?endpoint=239.192.56.12:40020|interface=192.168.56.0/24|ttl=1"
tx_receipts_stream_id = 1002

# --- TxErrors: sequencer-emitted rejection signals. RAM only. ----------------
# Stream id 1015 (not 1003) to avoid colliding with the receipts BlockBoundary
# side-stream (tx_receipts_stream_id + 1); see crates/log/src/config.rs.
tx_errors_channel = "aeron:udp?endpoint=239.192.56.13:40030|interface=192.168.56.0/24|ttl=1"
tx_errors_stream_id = 1015

# --- TxDeposits: DA watcher publishes Deposit envelopes; sequencers subscribe.
tx_deposits_channel = "aeron:udp?endpoint=239.192.56.14:40040|interface=192.168.56.0/24|ttl=1"
tx_deposits_stream_id = 1016

# --- fsync watermark (tx_data), per-sequencer. RAM only; single-host fsync. ---
# (The per-RECORDER tx_ordering fsync watermark + the Q-of-N aggregated quorum
# watermark channels were REMOVED with the custom recorders; the tx_data fsync
# sidecars are an independent, still-supported feature and stay.)
fsync_watermark_tx_data_channel_template = "aeron:udp?endpoint=239.192.56.16:40060|interface=192.168.56.0/24|ttl=1|alias=fsync-wm-a-{sid}"
fsync_watermark_tx_data_stream_id_base = 1030

# --- Durable watermark (tx_ordering). Repurposed from the old "quorum
# watermark": now the SINGLE archive-at-the-sealer durable position, published
# by `kardamom-sealer --archive-durability` and subscribed by ingress for the
# (unchanged) --ack-policy on-quorum ack gate.
quorum_watermark_channel = "aeron:udp?endpoint=239.192.56.17:40070|interface=192.168.56.0/24|ttl=1"
quorum_watermark_stream_id = 1020
