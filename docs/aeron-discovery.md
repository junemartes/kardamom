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

The migrated streams and their publishers:

| Topic | Publisher | Consumers |
| --- | --- | --- |
| `tx_data` (one record per lane) | ingress | sequencer, executor, validator, batcher, ingress archives |
| `tx_receipts` | executor | ingress, sequencer, validator |
| `tx_receipt_boundaries` | executor | ingress |
| `tx_errors` | sequencer | ingress |
| `tx_deposits` | DA watcher | sequencer, DA watcher archive |
| `tx_remote_epochs` | DA watcher | sequencer |
| `tx_bal` | executor | validator |

The canonical order (`tx_ordering`) rides the Aeron Cluster. Discovery
resolves the cluster member ingress endpoints; it never changes the
voting set.

## Configuration: the `[discovery]` section of `LogConfig`

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Off keeps every stream on its static `[channels]` URI. |
| `consul_http_addr` | `http://127.0.0.1:8500` | The local Consul agent. |
| `consul_token_file` | none | A file holding the ACL token. Sent as `X-Consul-Token`, never logged. |
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
| `KARDAMOM_MDC_PORTS` | The UDP port range `first-last` the process's publications bind. Absent, the OS picks a port per publication. Every port is bind-probed before it is advertised. |

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

## The container cluster

`deploy/cluster/config/channels.toml.tpl` sets `enabled = true`,
`cluster_id = "dev"`, the chain id, and
`advertise_interface = "192.168.56.0/24"`. `scripts/check-contract.py`
checks the mirrors.

| Job | `KARDAMOM_MDC_PORTS` | Publications |
| --- | --- | --- |
| ingress | `40300-40319` | 8 `tx_data` lanes |
| executor | `40320-40329` | receipts, boundaries, BAL |
| da-watcher | `40330-40339` | deposits, remote epochs |
| sequencer lane `n` | `40340+10n` to `40349+10n` | `tx_errors` |

The aeron system job registers the archive record with the node's
`archive_topics` meta: `tx_data` on the ingress nodes, `tx_deposits` on
the aux node, empty elsewhere. The cluster job registers the member
record with `member_id` equal to the sealer node's index.

## Cutover

No multicast-to-MDC mixed deployment is assumed compatible. The cutover
is coordinated: every service reads the same `channels.toml`, and
`[discovery] enabled` flips the whole cluster at once. Recordings made on
the multicast channels keep their catalog entries and their original
channel URIs; a refetch of a range recorded before the cutover resolves it
by session id as before.

## Local runs

The IPC defaults keep working without Consul: `enabled = false` is the
default. The Docker-gated tests under `crates/log/tests/` run the
discovered transport on a real Aeron driver over the in-memory catalog,
and `consul_discovery.rs` runs the catalog contract against a real Consul
container.
