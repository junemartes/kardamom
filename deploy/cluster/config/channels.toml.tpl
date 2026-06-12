# kardamom ChannelsConfig (Aeron UDP) — multi-host cluster rendering.
#
# ============================ NOT YET CONSUMED ============================
# The kardamom service binaries do NOT yet read this file. Today they derive
# channels from `LogConfig::default().channels` (hardcoded `aeron:ipc?...`
# URIs) — see deploy/cluster/README.md "Required service changes" (item 1).
# This config is provisioned and mounted in anticipation of the forthcoming
# `--log-config <toml>` flag; until that flag lands the services ignore it and
# fall back to IPC defaults (single-host only). nomad/ingress.nomad.hcl renders
# THIS file into its alloc via file() so the wiring is reviewable and the flag,
# once added, has a config to point at.
# =========================================================================
#
# Schema: crates/log/src/config.rs (ChannelsConfig). Stream-id values mirror
# the IPC defaults in that file so they remain compatible with code paths that
# read defaults; only the transport (ipc -> udp) and endpoints change.
#
# Endpoint plan (derived from group_vars/all.yml: ports.aeron_channel_base =
# 40000, worker_ips, sealer_ip):
#   40000  tx_data base (per-sequencer; see note below)
#   40001  tx_ordering            -> sealer node w2 192.168.56.22 (publisher)
#   40002  tx_receipts
#   40003  tx_errors
#   40004  tx_deposits
#   40010  fsync_watermark (tx_ordering, per-recorder)
#   40020  quorum_watermark
#   40030  fsync_watermark base (tx_data, per-sequencer)
#
# NOTE on tx_data / per-sequencer templates ({sid}-substituted URIs): a single
# URI template cannot encode a distinct *IP* per sequencer (the template only
# substitutes `{sid}`, used today for stream-id math and the alias label). The
# two sequencers live on different hosts (sid 0 -> w1 192.168.56.21, sid 1 ->
# w2 192.168.56.22), so a per-IP unicast endpoint is not expressible in one
# template string. We therefore use a control-mode=dynamic / MDC-style endpoint
# keyed by port offset and let subscribers connect; the publisher binds on its
# own host. RISK/ASSUMPTION: this is the highest-uncertainty addressing choice
# here and is unverifiable without a running Aeron media driver. If the
# `--log-config` plumbing instead wants one endpoint per sequencer, this must
# become a per-node rendered template (one channels.toml per worker) rather
# than a shared one. See README "Required service changes".

# --- TxData: per-sequencer exclusive publisher of full TxEnvelope bytes ------
# `{sid}` -> sequencer id. Stream id = tx_data_stream_id_base + sequencer_id.
# Port = aeron_channel_base (40000). Uses MDC (control-mode=dynamic) so the
# single template covers both sequencer hosts; subscribers add their endpoint.
tx_data_channel_template = "aeron:udp?control-mode=dynamic|control=0.0.0.0:40000|alias=a-{sid}"
tx_data_stream_id_base = 2000

# --- TxOrdering: canonical orderer, RECORDED. Published by the sealer (w2). ---
tx_ordering_channel = "aeron:udp?endpoint=192.168.56.22:40001"
tx_ordering_stream_id = 1001

# --- TxReceipts: receipts + block boundaries. Not recorded. ------------------
tx_receipts_channel = "aeron:udp?endpoint=192.168.56.21:40002"
tx_receipts_stream_id = 1002

# --- TxErrors: sequencer-emitted rejection signals. RAM only. ----------------
# Stream id 1015 (not 1003) to avoid colliding with the receipts BlockBoundary
# side-stream (tx_receipts_stream_id + 1); see crates/log/src/config.rs.
tx_errors_channel = "aeron:udp?endpoint=192.168.56.21:40003"
tx_errors_stream_id = 1015

# --- TxDeposits: DA watcher publishes Deposit envelopes; sequencers subscribe.
# Published from the da_watcher node (w2 192.168.56.22). RAM only.
tx_deposits_channel = "aeron:udp?endpoint=192.168.56.22:40004"
tx_deposits_stream_id = 1016

# --- fsync watermark (tx_ordering), per-recorder. `{rid}` -> recorder id. -----
# MDC keyed on port 40010 so the one template serves all 3 recorders.
fsync_watermark_channel_template = "aeron:udp?control-mode=dynamic|control=0.0.0.0:40010|alias=fsync-wm-{rid}"
fsync_watermark_stream_id = 1010

# --- fsync watermark (tx_data), per-sequencer. `{sid}` -> sequencer id. -------
fsync_watermark_tx_data_channel_template = "aeron:udp?control-mode=dynamic|control=0.0.0.0:40030|alias=fsync-wm-a-{sid}"
fsync_watermark_tx_data_stream_id_base = 1030

# --- Aggregated quorum watermark (tx_ordering). ------------------------------
quorum_watermark_channel = "aeron:udp?endpoint=192.168.56.22:40020"
quorum_watermark_stream_id = 1020
