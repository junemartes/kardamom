# kardamom LogConfig — multi-host cluster (Aeron UDP multicast + tx_ordering MDC
# + tx_receipts MDS).
#
# CONSUMED via `--log-config` (issue #36): every pipeline service is launched
# with `--log-config /local/channels.toml`, which deserializes this whole file
# into a `kardamom_log::config::LogConfig` (schema: crates/log/src/config.rs).
# Any field omitted here inherits the built-in default; unknown keys are
# rejected.
#
# ── Transport model ──────────────────────────────────────────────────────────
# Most fan-out/fan-in channels use UDP **multicast** (one shared URI valid on
# every node, no per-node rendering). tx_ordering is point-to-point multi-
# destination instead, because a shared multicast group's subscriber-churn froze
# images (killing one recorder froze every executor):
#
#  • **tx_ordering** → Aeron **MDC** (multi-destination-cast). Each publisher
#    (sealer + each sequencer) opens its OWN `control-mode=dynamic` publication
#    bound to its node IP + a fixed control port; subscribers (executor + the
#    sealer's archive-durability sidecar) attach to EVERY publisher's control
#    endpoint and merge the images. The executor's canonical merge / dedup /
#    boundary alignment is unchanged (per-image subscriber positions match the
#    old shared-multicast case).
#
#  • **tx_receipts** → Aeron **multicast** (active/active 2a — see the TxReceipts
#    section below and docs/agents/resilient-ingress-spec.md D2). Receipts moved
#    OFF the per-ingress unicast MDS (which pinned all executors to ONE ingress
#    IP, so a second ingress replica got nothing) ONTO a shared multicast group:
#    every ingress replica joins and receives every receipt, each executor
#    publishes its copy to the group, and each ingress dedups the N copies by tx
#    hash locally. The freeze the MDS avoided is now guarded by the cluster-e2e
#    ingress-kill check rather than the topology.
#
# ⚠ HIGHEST-RISK AREA TO VALIDATE: that Aeron preserves the canonical
# tx_ordering order across the merged MDC images. The cluster e2e
# (.github/workflows/cluster-e2e.yml) exercises it. Single-host IPC defaults (no
# --log-config) remain the known-good local/e2e path. See README.md.
#
# Address plan: multicast DATA groups are ODD on 192.168.56.0/24 (the driver
# derives the even control address as data-1), spaced by 2 so derived control
# addresses never collide; `interface` pins egress, ttl=1 keeps traffic on-
# segment. tx_ordering MDC control ports (unicast on the publisher nodes):
# 40110 (seq0@.21), 40111 (seq1@.22), 40112 (sealer@.51). tx_receipts now rides
# the multicast group 239.192.56.15:40020 (data 1002 + boundary 1003), joined by
# every ingress replica — no per-ingress unicast endpoints.

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
# Remote durability archives for the join-miss refetch (crates/log/src/refetch.rs):
# a consumer whose live multicast missed an envelope replays the missing range
# from these archives instead of dying. tx_data is recorded by BOTH ingress
# nodes (multicast ⇒ each archive is a full mirror — either endpoint serves any
# range; consumers rotate on failure). tx_deposits is recorded by the
# da-watcher's node (aux). Ports = service_ports.aeron_archive_control (8010).
tx_data_archive_endpoints = ["192.168.56.31:8010", "192.168.56.32:8010"]
tx_deposits_archive_endpoints = ["192.168.56.61:8010"]

[quorum]
# VESTIGIAL after the move to archive-at-the-sealer durability. There is no
# longer a Q-of-N quorum aggregator; the single sealer archive's durable
# position is THE watermark. Retained only so a channels.toml carrying a
# [quorum] section still parses (deny_unknown_fields). n=q=1 = "one durable copy".
n = 1
q = 1

[channels]
# --- TxData: per-sequencer exclusive publisher of full TxEnvelope bytes. ------
# One multicast group; stream id = base + sequencer_id distinguishes the seqs.
tx_data_channel_template = "aeron:udp?endpoint=239.192.56.11:40000|interface=192.168.56.0/24|ttl=1|alias=a-{sid}"
tx_data_stream_id_base = 2000

# --- TxOrdering: canonical orderer, via MDC (see header). --------------------
# `tx_ordering_channel` is the single-host IPC fallback ONLY.
tx_ordering_channel = "aeron:ipc?alias=tx-ordering"
tx_ordering_stream_id = 1001

# --- TxReceipts: receipts + block boundaries. MULTICAST (active/active 2a). ----
# CHANGED for active/active ingress (docs/agents/resilient-ingress-spec.md, D2):
# receipts ride a shared **multicast** group, NOT the per-ingress unicast MDS
# fan-in. The MDS endpoints were pinned to ONE ingress IP — a second ingress
# replica received nothing. With multicast, every ingress replica simply joins
# `tx_receipts_channel` (data stream 1002 + boundary side-stream 1003) and
# receives every receipt; each executor replica publishes its copy to the same
# group, and each ingress dedups the N identical copies locally by tx hash
# (first-wins, `kardamom_ingress::seen_receipts`). Replica-count-agnostic on both
# sides — add ingress/executor replicas with no channel edits.
#
# MDS is DISABLED: `tx_receipts_control_channel` is empty, so ingress takes the
# single-channel path (`TxReceiptsSubscriberHandle::open` + the boundary sub) and
# executors publish via the non-MDS publisher. `tx_receipts_executor_count` is
# retained only as a hint for local dedup-window sizing; it no longer drives any
# endpoints.
#
# ⚠ FREEZE RISK (the reason MDS existed): a shared multicast group's
# subscriber-churn once froze images (killing one recorder froze every executor).
# Ingress churn (a replica crash/restart leaving+rejoining the group) could
# reintroduce that image-freeze for the surviving subscribers. The cluster-e2e
# (.github/workflows/cluster-e2e.yml → scripts/ci-cluster.sh ingress-churn check)
# exercises exactly this: kill one ingress while traffic flows and assert the
# survivor keeps receiving receipts.
#
# NOTE: the freeze that check first caught was NOT an Aeron image/transport
# issue — the multicast group preserves delivery across subscriber churn
# (verified on a real cluster: 0 loss, subscriber positions advancing). It was a
# deadlock in ingress's receipt cache: `ReceiptCache::evict_if_full` held a
# DashMap shard read-guard across `remove()`, wedging the tx_receipts watcher the
# moment the cache filled under the sustained receipt firehose — so an idle
# replica (serving no traffic but still receiving every receipt) froze. Fixed in
# crates/ingress/src/receipt_cache.rs; no channel/flow-control change needed.
tx_receipts_control_channel = ""
tx_receipts_endpoint_host = ""
tx_receipts_endpoint_base_port = 0
tx_receipts_endpoint_interface = ""
tx_receipts_executor_count = 3
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

# --- TxRemoteEpochs: interop watcher publishes RemoteEpochRecords (one per
# peer-chain origin block that carried cross-chain messages); sequencers
# subscribe and relay them onto tx_ordering. Its own group so a stalled peer
# pairing cannot delay L1 deposits.
tx_remote_epochs_channel = "aeron:udp?endpoint=239.192.56.27:40080|interface=192.168.56.0/24|ttl=1"
tx_remote_epochs_stream_id = 1017

# --- TxBal: per-block BAL (the executor's BlockDelta). The executor publishes
# one BlockDelta per sealed block; validators subscribe and cross-check their
# independent re-execution against it. Multicast (many validator subscribers).
# Own group .21:40050. Stream 1004 (free range between receipts and fsync).
tx_bal_channel = "aeron:udp?endpoint=239.192.56.21:40050|interface=192.168.56.0/24|ttl=1"
tx_bal_stream_id = 1004

# --- fsync watermark (tx_data), per-sequencer. RAM only; single-host fsync. ---
# (The per-RECORDER tx_ordering fsync watermark + the Q-of-N aggregated quorum
# watermark channels were REMOVED with the custom recorders; the tx_data fsync
# sidecars are an independent, still-supported feature and stay. Odd group .23.)
fsync_watermark_tx_data_channel_template = "aeron:udp?endpoint=239.192.56.23:40060|interface=192.168.56.0/24|ttl=1|alias=fsync-wm-a-{sid}"
fsync_watermark_tx_data_stream_id_base = 1030

# --- Durable watermark (tx_ordering). Repurposed from the old "quorum
# watermark": now the SINGLE archive-at-the-sealer durable position, published
# by `kardamom-sealer --archive-durability` and subscribed by ingress for the
# (unchanged) --ack-policy on-quorum ack gate. Own odd group .25.
quorum_watermark_channel = "aeron:udp?endpoint=239.192.56.25:40070|interface=192.168.56.0/24|ttl=1"
quorum_watermark_stream_id = 1020
