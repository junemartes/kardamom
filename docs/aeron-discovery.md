# Aeron MDC transport with Consul discovery

- Status: Implemented, discovery version 1.
- Specification: [aeron-mdc-consul-spec.md](agents/aeron-mdc-consul-spec.md).
- Code: `crates/log/src/discovery/`, `crates/log/src/config/discovery.rs`.

This document is the integration contract between the runtime and the
infrastructure. It names the configuration schema, the service records,
the environment each job supplies, and the port plan of the container
cluster.

## Transport

Every application stream is a dynamic Aeron MDC publication. A publisher
binds one control endpoint per publication on the advertised interface and
registers it. A consumer opens one multi-destination subscription per
stream and attaches one destination per publisher the catalog lists. The
publishers stay separate Aeron images, so a transaction keeps its
`(shard, session, position)` identity at the publisher, at every live
consumer, and in the recording refetch reads.

Consul is the discovery control plane only. Messages travel between Aeron
media drivers. Consul never relays a message, answers a per-message
lookup, or orders anything.

The streams and their publishers:

| Topic | Publisher | Consumers |
| --- | --- | --- |
| `tx_data` (one record per lane) | ingress | sequencer, executor, validator, batcher, ingress archives |
| `tx_receipts` | executor | ingress, sequencer, validator |
| `tx_receipt_boundaries` | executor | ingress |
| `tx_errors` | sequencer | ingress, notifier |
| `tx_status` | sequencer, ingress | notifier |
| `tx_deposits` | DA watcher | sequencer, DA watcher archive |
| `tx_remote_epochs` | DA watcher | sequencer |
| `tx_bal` | executor | validator |
| `events` | ingress, sequencer, executor, validator, batcher, DA watcher, state mirror | ingress, validator |
| `exec_txs` | executor | validator, batcher (none subscribes yet), the archive on the node of each executor |

The `exec_txs` stream carries the transactions that an executor joins, in canonical order: one `ExecTxRecord` for each `TxRef`.

- The stream id is 1005. The channel and the stream id are the keys `exec_txs_channel` and `exec_txs_stream_id` of `[channels]`.
- Each executor opens two publications on stream 1005. One publisher thread writes both, in the same order:
  - The recorded publication, an exclusive IPC publication on `aeron:ipc?alias=exec-txs`. The archive on the node of the executor records it. An IPC publication cannot run ahead of its slowest subscriber, so the recording loses no frame. An exclusive publication has its own session, so the executors that share one media driver never write into one session.
  - The live publication, a dynamic MDC publication with a `kardamom-mdc-publisher` record (topic `exec_txs`). It is lossy: one offer for each record, and a refused offer drops the record. A consumer repairs a gap from an archive.
- A static plane whose `exec_txs_channel` is IPC opens only the recorded publication. Its consumers read that one.
- A slow or absent archive refuses the recorded offer. The publisher offers the record again until the archive takes it. The reader then blocks, and this executor stalls. It never executes a record that its archive did not take.
- The validator and the batcher name the topic as their future source. No service subscribes to it yet.

The `events` stream carries the lifecycle state of each service (running, halted, paused, resumed).
See [failure-modes.md](failure-modes.md#halts-and-service-events).

- The stream id is 1019. The channel and the stream id are the keys `events_channel` and `events_stream_id` of `[channels]`.
- Delivery is best effort and RAM only. Nothing that changes the canonical order reads it.
- The publication uses a term length of 64 KiB, the Aeron minimum. The other topics use the driver default.
  A small term keeps the log buffers small, because each subscriber driver holds three terms for each publisher.
- The sealer and the L1 indexer are not on the stream. The ingress observes the sealer from its status frame.

The canonical order (`tx_ordering`) rides the Aeron Cluster. Discovery
resolves the cluster member ingress endpoints; it never changes the
voting set.

## Configuration: the `[discovery]` section of `LogConfig`

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Off keeps every stream on its static `[channels]` URI. |
| `consul_http_addr` | `http://127.0.0.1:8500` | The local Consul agent. |
| `consul_token_file` | none | A file holding the ACL token. Sent as `X-Consul-Token`, never logged. Unset, the token comes from the environment (below). |
| `cluster_id` | empty | Discovery scope. Required when enabled. |
| `chain_id` | `0` | Discovery scope. |
| `datacenter` | empty | Consul datacenter of the catalog queries. Empty means the agent's own. |
| `advertise_interface` | none | Interface name or IPv4 network in CIDR form. Required when enabled. Exactly one address must match. |
| `request_timeout_ms` | `5000` | One HTTP request's connect plus response budget. |
| `blocking_wait_ms` | `30000` | How long one blocking catalog query waits for a change. |
| `backoff_min_ms`, `backoff_max_ms` | `500`, `10000` | Retry backoff after a failed catalog read. |
| `removal_grace_ms` | `5000` | A missing publisher is detached only after this long. |
| `check_ttl_ms` | `10000` | The TTL of a publication's health check. |
| `deregister_after_ms` | `60000` | Consul deletes a registration critical for this long. |
| `flow_control` | empty | The `fc` URI parameter of every publication. Empty keeps the driver default. |

With `enabled = false`, no field is required. With `enabled = true`, an
empty `cluster_id`, a missing `advertise_interface`, or an address that
is absent or ambiguous on the host fails startup.

## Environment supplied by Nomad

| Variable | Meaning |
| --- | --- |
| `NOMAD_ALLOC_ID` | The instance id. Every service id of a process starts with it. Absent in a local run, where a process id and clock stamp replace it. |
| `CONSUL_HTTP_TOKEN`, then `CONSUL_TOKEN` | The ACL token, when `consul_token_file` is unset. `CONSUL_TOKEN` is the name Nomad sets on a task with a Consul workload identity. Absent, no token is sent. |

On the ACL profile the token needs `service:write` on
`kardamom-mdc-publisher` for the agent registration, `service:read` on
`kardamom-mdc-publisher`, `kardamom-aeron-archive` and
`kardamom-cluster-member`, and `node:read` for the health queries. How
the token reaches the task is the deployment profile's concern, not the
runtime's.

Receive ports are never configured: every destination binds an OS-chosen
port on the advertised interface.

## Service records

Service names are frozen. Every record carries the scope metadata
`discovery_version = "1"`, `cluster_id`, and `chain_id`, and every query
filters on it.

### `kardamom-mdc-publisher`, runtime-owned

One record per publication. The service id is
`<instance>:<topic>:<stream_id>`. The address and port are the
publication's control endpoint.

| Meta | Meaning |
| --- | --- |
| `topic` | One of the topics above. |
| `stream_id` | The Aeron stream id. |
| `publisher_id` | A label of the process, for logs. |
| `lane_id` | The physical lane of a `tx_data` publication. |
| `session_id` | The Aeron session id of the publication. The recorder matches recordings by it. The image header stays the authority for positions. |

The record is registered after the endpoint is bound, with a TTL check the
process passes at a third of `check_ttl_ms`. A graceful exit deregisters.
After a crash the check turns critical, consumers detach after their
grace, and Consul deletes the record after `deregister_after_ms`.

### `kardamom-aeron-archive`, Nomad-owned

One record per archive node, registered by the aeron system job. The
address and port are the archive control endpoint.

| Meta | Meaning |
| --- | --- |
| `archive_id` | Stable identity of the archive. |
| `topics` | Comma-separated topics this archive records. Empty means none. |

The refetch client reads the endpoints of a topic from these records at
every refetch. A passing record proves nothing about one recording:
refetch queries the catalog for the exact session and range, and fails
explicitly when the range is not there. The record outlives every
publisher, so retained recordings stay discoverable.

### `kardamom-cluster-member`, Nomad-owned

One record per Aeron Cluster member. The address and port are the
member's client ingress endpoint. Meta `member_id` is the fixed member
id. A cluster client resolves the member list from these records once at
startup and learns later changes from the cluster itself. Changing a
member's advertised endpoint stays the cluster's own reconfiguration
procedure.

## Reconciliation

- A read error keeps the last membership and retries with bounded backoff.
  An error is never an empty set.
- A successful empty read is a distinct state. Every attached publisher is
  detached after the removal grace.
- An index that moves backwards restarts the blocking query.
- Attachments are keyed by destination URI. A replacement incarnation on
  the same endpoint keeps its destination; the driver forms a new image
  with a new session id.
- Every driver call runs on a blocking thread, through a command-only
  handle on the Aeron runtime, so a discovery task never keeps the runtime
  alive past its last owner.

## Archives and readiness

An ingress runs one discovery-driven recorder thread. It records every
`tx_data` publisher the catalog lists, its own lanes and every other
ingress's, through the same dynamic MDC join a subscriber uses. Each
publisher is its own recording, keyed in the catalog by its control
endpoint and matched by its advertised session id. After a restart on the
same port, the earlier incarnation's finished recording is never adopted:
readiness waits for the new session's recording. The ingress serves only
once every one of its own lanes has a live recording. The DA watcher
records its own `tx_deposits` publication the same way.

Each executor records its own recorded `exec_txs` publication on the
archive of its node. The recorder adopts only the recording of the
session of this run, so a restarted executor never reads the position of
the recording of its earlier session. The executor joins nothing before
that recording is active. The recorder thread reads the recording
position every 20 ms. The publisher thread computes the recorded cursor
from it (`kardamom_executor_exec_stream_recorded_index`).

## Deployment profiles

`deploy/cluster/config/channels.toml.tpl` sets `enabled = true` and the
chain id, and reads the rest of the scope from the node. Every job
renders the file as a Nomad template on its node:

| Key | Placeholder | Source |
| --- | --- | --- |
| `cluster_id` | `{{ env "meta.cluster_id" }}` | The profile's `cluster_id`, stamped as node meta by `roles/nomad`. |
| `datacenter` | `{{ env "node.datacenter" }}` | The profile's `datacenter`, the Nomad agent's own. |
| `advertise_interface` | `{{ env "meta.node_ip" }}/32` | The node's private address, resolved by `roles/netinfo` and stamped as node meta. |

The local profile (explicit `node_ip` for each inventory host) and the
production profile (`node_ip` resolved from the vSwitch address or the
private interface) use one file. An elastic node that joins from the
image gets its scope from its own agent. The `kardamom-aeron-archive` and
`kardamom-cluster-member` records read `${meta.cluster_id}` the same way.

The file names no fixed address.

- The multicast fallback channels pin their `interface` to
  `{{ env "meta.node_ip" }}/32` too.
- The fallback archive lists render from the archive records. The
  `kardamom-aeron-archive` service carries the topic that the node
  records (`archive_topics`) as a tag.
- The template lists `tx_data.kardamom-aeron-archive` for `tx_data` and
  `tx_deposits.kardamom-aeron-archive` for `tx_deposits`. The selection
  follows the recording node in both profiles: the ingress nodes for
  `tx_data`, and the node with the `da-watcher` role for `tx_deposits`.
  The executor nodes carry the tag `exec_txs`. No consumer reads their
  archives yet.
- A node records at most one topic. A node that records nothing has an
  empty tag.
- Every job renders the file with `change_mode = "noop"`. A change in the
  archive set rewrites the file. The running process follows the catalog
  through discovery and does not restart.
- `ansible/contract.yml` checks the chain id mirror, the placeholders,
  the role tag, and the node meta that the Nomad agent template stamps.

No job configures a publication control port.

- Each publication names port 0 in its control endpoint. The media driver binds an OS-chosen port.
- The runtime reads the bound address from the driver (`aeron_publication_local_sockaddrs`). It waits up to 2 seconds for the bind.
- The publisher record carries the address that the driver bound.
- The driver holds the socket from the bind on. No other socket can take the port before the record is registered.

| Job | Publications |
| --- | --- |
| ingress | 8 `tx_data` lanes, `tx_status` |
| executor | receipts, boundaries, BAL, the live `exec_txs` publication |
| da-watcher | deposits, remote epochs |
| sequencer lane `n` | `tx_errors`, `tx_status` |

The aeron system job registers the archive record with the `archive_topics`
meta of the node:

- `tx_data` on a node with the `ingress` role.
- `tx_deposits` on a node with the `da-watcher` role.
- `exec_txs` on a node with the `executor` role. The archive keeps the stream of the executor on that node.
- Empty on every other node.

An archive record with an unknown topic fails to parse, and a refetch
client then skips the whole record. So a runtime that knows `exec_txs`
deploys before the executor nodes advertise it.

The `da-watcher` role is on its own node in the production profile. In the
local profile it is on the `aux` node.

The cluster job registers the member record with `member_id` equal to the
index of the sealer node.

## Switching a cluster to discovery

- A cluster does not mix multicast and MDC deployments.
- Every service reads the same `channels.toml`. `[discovery] enabled` switches the whole cluster at once.
- Recordings made on the multicast channels keep their catalog entries and their original channel URIs.
- A refetch of a range that was recorded on multicast resolves it by session id.

## Local runs

The IPC defaults keep working without Consul: `enabled = false` is the
default. The Docker-gated tests under `crates/log/tests/` run the
discovered transport on a real Aeron driver over the in-memory catalog,
and `consul_discovery.rs` runs the catalog contract against a real Consul
container.
