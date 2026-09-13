# kardamom LogConfig for the multi-host cluster.
#
# Every pipeline service is launched with `--log-config /local/channels.toml`.
# The file deserializes into `kardamom_log::config::LogConfig` (schema:
# crates/log/src/config/mod.rs). A field omitted here inherits the built-in
# default; an unknown key is rejected.
#
# Transport model: `[discovery]` is on, so every application stream is a
# dynamic Aeron MDC publication that registers its control endpoint with
# the local Consul agent, and every consumer joins the publishers the
# catalog lists through one multi-destination subscription per stream. The
# `[channels]` URIs below name the stream ids; their multicast groups apply
# only with `[discovery] enabled = false`, the single-transport fallback.
# See docs/aeron-discovery.md for the contract and the ports each job
# allocates.
#
# Every job renders this file as a Nomad template on its node. The
# placeholders (Nomad template `env` and `service` calls) read the node's
# meta and datacenter and the archive records in Consul, so one file serves
# the local profile and the production profile and names no fixed address.
# No comment may contain a placeholder: Nomad parses the whole file.
#
# The canonical order (tx_ordering) rides the Aeron Cluster (Raft) sealer,
# through the [cluster] section of each service config, not a channel here.
#
# Address plan of the fallback groups: multicast DATA groups are ODD
# (the driver derives the even control address as data-1), spaced by 2 so
# derived control addresses never collide; `interface` pins egress to the
# node's own address (a placeholder, like advertise_interface below), and
# ttl=1 keeps traffic on-segment.

[discovery]
# On: the dynamic MDC transport with Consul discovery. Off: the static
# multicast groups below.
enabled = true
# The local Consul agent (ansible/roles/consul; client_addr 0.0.0.0).
consul_http_addr = "http://127.0.0.1:8500"
# The discovery scope. cluster_id and datacenter come from the node:
# roles/nomad stamps the profile's cluster_id as node meta, and the Nomad
# datacenter is the profile's datacenter. chain_id mirrors
# group_vars/all.yml chain_id; scripts/check-contract.py checks the mirror.
cluster_id = "{{ env "meta.cluster_id" }}"
chain_id = 412346
datacenter = "{{ env "node.datacenter" }}"
# The address every control endpoint and receive endpoint binds: the
# node's private address, as roles/netinfo resolved it and roles/nomad
# stamped it. The /32 matches that one address and nothing else.
advertise_interface = "{{ env "meta.node_ip" }}/32"
# The Aeron flow control of every publication, as the `fc` URI parameter.
# Empty keeps the driver default, the same policy the multicast groups
# use: the fastest receiver paces the publisher, a lagging consumer
# recovers through the archive refetch, never by holding the stream.
flow_control = ""

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
# a consumer whose live subscription missed an envelope replays the missing
# range from these archives instead of dying. With `[discovery]` on, the
# refetch client reads the archive endpoints from the `kardamom-aeron-archive`
# records the aeron job registers (nomad/aeron.system.nomad.hcl), and these
# static lists are the fallback. Nomad renders them from the same records at
# task start: the archives tagged with the ingress role record tx_data (each
# ingress archive records every ingress publisher, so either endpoint serves
# any range; consumers rotate on failure), and the archive tagged with the
# aux role records tx_deposits. A change in the archive set re-renders the
# file without a restart (the jobs set change_mode noop); the running
# process follows the catalog through discovery instead.
tx_data_archive_endpoints = [{{ range service "ingress.kardamom-aeron-archive" }}"{{ .Address }}:{{ .Port }}", {{ end }}]
tx_deposits_archive_endpoints = [{{ range service "aux.kardamom-aeron-archive" }}"{{ .Address }}:{{ .Port }}", {{ end }}]

[channels]
# --- TxData: full TxEnvelope bytes, one stream per lane. ----------------------
# One multicast group; stream id = base + lane distinguishes the lanes. Both
# ingress replicas publish every lane; a consumer tells them apart by the
# publisher session id.
tx_data_channel_template = "aeron:udp?endpoint=239.192.56.11:40000|interface={{ env "meta.node_ip" }}/32|ttl=1|alias=a-{sid}"
tx_data_stream_id_base = 2000

# --- TxReceipts: receipts + block boundaries. Multicast. ----------------------
# Every ingress replica joins `tx_receipts_channel` (data stream 1002 +
# boundary side-stream 1003) and receives every receipt. Each executor replica
# publishes its copy to the same group, and each ingress dedups the N identical
# copies locally by tx hash (first-wins, `kardamom_ingress::seen_receipts`).
# Adding ingress or executor replicas needs no channel edit.
#
# MDS is off: `tx_receipts_control_channel` is empty, so ingress takes the
# single-channel path and executors publish through the non-MDS publisher.
# `tx_receipts_executor_count` only sizes the local dedup window.
#
# The cluster-e2e ingress-churn check kills one ingress while traffic flows
# and asserts that the survivor keeps receiving receipts.
tx_receipts_control_channel = ""
tx_receipts_endpoint_host = ""
# 0 means unset: receipts ride the multicast channel below, not MDS.
tx_receipts_endpoint_base_port = 0
tx_receipts_endpoint_interface = ""
tx_receipts_executor_count = 3
tx_receipts_channel = "aeron:udp?endpoint=239.192.56.15:40020|interface={{ env "meta.node_ip" }}/32|ttl=1"
tx_receipts_stream_id = 1002

# --- TxErrors: sequencer-emitted rejection signals. RAM only. ----------------
# Stream id 1015 (not 1003) to avoid colliding with the receipts BlockBoundary
# side-stream (tx_receipts_stream_id + 1); see crates/log/src/config.rs.
tx_errors_channel = "aeron:udp?endpoint=239.192.56.17:40030|interface={{ env "meta.node_ip" }}/32|ttl=1"
tx_errors_stream_id = 1015

# --- TxDeposits: DA watcher publishes Deposit envelopes; sequencers subscribe.
tx_deposits_channel = "aeron:udp?endpoint=239.192.56.19:40040|interface={{ env "meta.node_ip" }}/32|ttl=1"
tx_deposits_stream_id = 1016

# --- TxRemoteEpochs: interop watcher publishes RemoteEpochRecords (one per
# peer-chain origin block that carried cross-chain messages); sequencers
# subscribe and relay them onto tx_ordering. Its own group so a stalled peer
# pairing cannot delay L1 deposits.
tx_remote_epochs_channel = "aeron:udp?endpoint=239.192.56.27:40080|interface={{ env "meta.node_ip" }}/32|ttl=1"
tx_remote_epochs_stream_id = 1017

# --- TxBal: per-block BAL (the executor's BlockDelta). The executor publishes
# one BlockDelta per sealed block; validators subscribe and cross-check their
# independent re-execution against it. Multicast (many validator subscribers).
# Own group .21:40050. Stream 1004 (free range between receipts and fsync).
tx_bal_channel = "aeron:udp?endpoint=239.192.56.21:40050|interface={{ env "meta.node_ip" }}/32|ttl=1"
tx_bal_stream_id = 1004
