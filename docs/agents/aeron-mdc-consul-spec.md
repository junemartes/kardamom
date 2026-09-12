# Aeron MDC with Consul discovery

- Status: Proposed; implementation belongs to the MDC session.
- Date: 2026-09-11
- Scope: Replace every active UDP multicast application channel with Aeron
  Multi-Destination-Cast (MDC), with membership discovered through Consul.
- Companion work: Infrastructure is developed separately against this contract
  and will be rebased onto the MDC implementation. This document adds no runtime code.

## Problem and decision

The multi-node configuration currently uses UDP multicast for transaction data,
receipts/boundaries, errors, deposits, remote epochs, BAL and some watermarks.
Those channels embed a multicast group and a fixed interface subnet. Archive
refetch and Aeron Cluster bootstrap also contain fixed server addresses.
This couples application deployment to a particular network and prevents new
nodes with arbitrary addresses from participating without configuration changes.

Use **dynamic MDC publications** (`control-mode=dynamic`) and Consul discovery
for every migrated application channel. Manual MDC publications and
publisher-maintained subscriber endpoint lists are out of scope and must not
be substituted for this design.
Each consumer discovers every relevant publisher and subscribes directly to its
control endpoint. Multiple publishers remain separate Aeron images; where useful,
use a multi-destination subscription (MDS) to merge those images into the existing
consumer handle. MDS is a subscription mechanism, not a different wire transport.

MDC sends a unicast copy to each destination. This removes the requirement for a
multicast-capable fabric, while retaining Aeron's publication, session and flow
control model. It increases publisher packet-send work and NIC bandwidth with
fanout; it is not a claim of equivalent latency or unlimited scale. Measure the
tradeoff on the intended hardware before setting production capacity limits.
See [Aeron MDC](https://aeron.io/docs/aeron/multi-destination-cast/) and
[multiple destinations](https://github.com/aeron-io/aeron/wiki/Multiple-Destinations).

Consul is the discovery control plane. Messages travel directly between Aeron
media drivers. Consul is never a message relay, per-message lookup, or ordering
authority. Do not introduce shell discovery loops or a proxy publication.

## Correctness contract

1. Preserve wire schemas, stream IDs, shard routing, transaction execution rules,
   receipt/boundary deduplication and Aeron Cluster's canonical ordering rules.
   Relative arrival times across independent publishers can change; identical
   wall-clock ordering across transports is not promised.
2. Keep one publication per logical publisher stream and fan out from that
   publication. A transaction's `(shard, session_id, position)` identity must
   match at the publisher, each live consumer and the recording used for refetch.
   Do not open an independent publication per receiver or re-offer data through
   a forwarding process that replaces the source session/positions.
3. Preserve ordering within each Aeron image. Neither multicast nor MDC supplies
   a global order across publishers. The Aeron Cluster remains the ordering
   authority; discovery enumeration order cannot influence application ordering.
4. Every consumer that previously joined a multicast group must receive all
   relevant publisher images. DNS round-robin or choosing a single healthy
   instance is insufficient for replicated ingress/executor publishers.
5. Preserve each consumer's current recovery semantics. In particular, do not
   add historical replay to racing sequencers indiscriminately: replaying refs
   beyond the orderer's dedup window can violate correctness. Follow the
   existing sequencer resync/rejoin design for live participation.
6. A newly discovered connection is not historical replay. Gaps requiring
   recovery must use the existing archive/checkpoint mechanisms and their
   bounds. Do not report readiness or silently skip required data on discovery
   errors, missing archives or exhausted recovery windows.

Relevant code includes `crates/log/src/aeron_live/handles/tx_data.rs`,
`crates/log/src/recorder.rs`, `crates/log/src/refetch.rs`,
`crates/ingress/src/bin/kardamom-ingress/recorders.rs` and
[the racing sequencer specification](replicated-sequencer-shards-spec.md).

## Channel inventory

Audit live call sites rather than trusting the current configuration comments;
several describe retired sidecars or the old standalone ordering path.

| Logical stream | Required migration |
| --- | --- |
| `tx_data` | Per-publisher, per-shard MDC; all required sequencer/executor/batcher consumers and archives attach to all relevant publishers. |
| `tx_receipts` | MDC from every executor replica to every ingress replica; preserve first-wins receipt deduplication. |
| Receipt block boundaries | Same publisher membership as receipts; retain the separate boundary stream ID and its processing rules. |
| `tx_errors` | All relevant error publishers discovered; preserve sender routing and rejection semantics. |
| `tx_deposits` | Live MDC plus discoverable archive refetch; retain origin/canonical identity. |
| `tx_remote_epochs` | MDC with the existing origin filtering and ordering path. |
| `tx_bal` | MDC from relevant executor publishers to validators; preserve attribution and verification. |
| `fsync_watermark_tx_data` | Migrate active producers/consumers, with the same shard/recorder identity and durability meaning. |
| `quorum_watermark` and old `tx_ordering` channel | Audit reachability. Delete retired multicast settings; if an active path remains, migrate it without changing durability meaning. |

Keep same-host IPC. Aeron Cluster client/member traffic already uses unicast;
this migration must not replace Raft, change membership automatically, or bring
back the retired standalone ordering MDC implementation. Dynamic discovery of
Cluster member endpoints is covered below; it is distinct from changing the
Raft membership set.

## Runtime discovery contract, version 1

The implementation must provide a common discovery abstraction in the transport
layer, rather than adding bespoke Consul watchers to each application. It must
support an in-memory test backend and explicit IPC mode for local unit tests.
Infrastructure must not render peer IP lists into application configuration.

### Configuration supplied by infrastructure

Expose one application discovery configuration with these semantics; exact Rust
field names can follow repository conventions, but document the final schema:

- `enabled`: opt-in during development; mandatory in the cloud deployment profile.
- `consul_http_addr`: the local Consul agent, normally `http://127.0.0.1:8500`.
  Loopback is an intentional local-agent endpoint, not a remote node address.
- `consul_token_file`: optional credential file, never logged or placed in tags.
- `cluster_id`, `chain_id`, `datacenter`: explicit discovery isolation boundaries.
- `advertise_interface`: the private network interface (or an equivalent
  unambiguous interface selector); resolve the current address at startup.
- Allocated UDP port range and instance identity supplied by Nomad. Publication
  control ports and receive ports must be bindable and reachable before use.
- Bounded query/connect timeouts, retry backoff and configurable flow control.

The runtime must reject ambiguous or absent private interfaces, incompatible
catalog versions and malformed endpoints. A resolved IP in a live Consul record
is expected; a source-controlled peer IP or ordinal-to-IP formula is not.
Do not assume DNS changes update an already-open Aeron channel.

### Service records

Freeze these service names for infrastructure integration:

| Service | Owner and meaning |
| --- | --- |
| `kardamom-mdc-publisher` | Runtime-owned publication control endpoints. |
| `kardamom-aeron-archive` | Archive endpoints and actual recording coverage. |
| `kardamom-cluster-member` | Fixed Aeron Cluster member identities and their current endpoints. |

Publisher records use a unique service ID per allocation/process incarnation,
logical topic, lane and publication. The service address and port are the
actual private control endpoint. Required metadata are string-valued:
`discovery_version=1`, `cluster_id`, `chain_id`, `topic`, `stream_id`,
`publisher_id`, and `lane_id` for transaction-data lanes. The lane ID is the
physical transport lane in the shard map, not a virtual-slot or node ordinal. Advertise session identity
when available, but use received Aeron image metadata as the authority for
transaction positions. Receiver reconcilers must distinguish replacement
incarnations even when an endpoint is reused.

Archive records advertise the archive control endpoint and coverage metadata
for supported topics/shards, plus stable archive identity. A passing service
check does not prove that a requested recording exists: refetch must query the
catalog for the exact session/position/range and verify availability. Retained
recordings remain discoverable after their publisher has left. Do not tie
archive retention to publisher health or deregistration.

Cluster member records include `member_id` and the named ingress/consensus/log/
catchup/archive ports. The stable membership IDs and expected membership size
are configuration. Discovering a new server must not mutate the voting set.
Initial client connections must resolve member ingress endpoints; the exact
member-advertised addresses sent by the protocol may remain literal runtime IPs.
Changing a member's advertised endpoint requires the existing supported Cluster
restart/reconfiguration procedure, not a catalog-only edit.

### Registration and reconciliation

Bind the endpoint before registering it. Service checks describe local endpoint
liveness, not dependency readiness: publishers must be discoverable before all
subscribers are connected, or startup can deadlock. Use allocation-scoped
registration with bounded health expiry and deregistration on graceful exit;
clean up stale records after crashes. Ensure that each record has one owner
(runtime or Nomad), never two competing registration mechanisms.

Consumers perform an initial health query and then blocking Consul queries,
filtered by cluster, chain, topic, shard and supported discovery version. Compute
idempotent add/remove diffs; preserve unchanged publications, subscriptions,
images and positions. Perform Aeron mutations through the existing driver-thread
command mechanism. Repeated updates must not leak sockets, handles or recordings.
Aeron transport liveness and Consul catalog health are separate observations.

On Consul timeout, authorization error, leader loss or malformed response, retain
the last known membership and retry with bounded backoff. An error is not an empty
membership set. A successful empty result is a distinct state: reconcile with a
bounded removal grace, expose degraded readiness as appropriate, and apply the
stream's existing loss/recovery policy. Handle blocking-query index resets.
A Consul outage must not tear down healthy existing data-plane connections.

Dynamic MDC means receivers join the publisher's advertised control endpoint.
This does not mean that Aeron discovers publishers itself. Use Consul for that
step. Subscription destination APIs may be used behind an MDS abstraction to join
multiple dynamic publishers or to integrate archives. This permission is only
for subscriptions: every publisher remains in dynamic MDC mode. Document and
test the resulting URI and image behavior for the repository's Aeron version.

## Archives, readiness and scale-in

The deployed ingress archives currently intend to mirror the shared transaction
streams. Preserve required coverage across concurrent ingress publishers.
Recording a local spy alone is insufficient to mirror remote publications.
Remote MDC recordings require receiving endpoints/destinations and source session
tracking; explicitly validate source location, recording-channel lookup and
catalog matching after channel URI changes. A partial archive must never be
advertised as a complete mirror.

Preserve the existing recording-before-serving barrier. Separate endpoint
registration, stream connectivity, recording readiness and application readiness.
Avoid circular requirements between publisher health and subscriber discovery.
The application must continue its established backpressure/failure behavior when
required consumers or recordings are unavailable.

Graceful drain stops admission, lets the existing accepted-work policy complete,
withdraws readiness, and closes/deregisters endpoints in a defined order. Archive
retention and access for outstanding references must outlive the publisher where
required. Autoscaler scale-in must not delete the last usable copy of required
recordings. Discovery alone is not a durability or safe-draining protocol.

Specify flow control for every migrated stream and document why it matches the
existing correctness contract. MDC and multicast both support receiver-based
flow control. Do not accidentally introduce slowest-receiver coupling everywhere
or allow required durable consumers to fall behind without a recovery path.
See [Aeron flow control](https://github.com/aeron-io/aeron/wiki/Flow-and-Congestion-Control).

## Infrastructure boundary

Terraform owns networks, firewall rules, instances/pool definitions and capacity
bounds. Ansible installs and configures local Consul/Nomad/Aeron agents. Nomad
runs allocations; the runtime owns dynamic application discovery. Nomad Autoscaler
owns runtime scaling decisions through a supported infrastructure target.

Consul needs a bootstrap mechanism independent of Consul itself: provider
metadata or infrastructure DNS records. Infrastructure may allocate private IPs
and publish DNS, but applications receive service identities/interface selectors.
Do not replace hardcoded IPs with a generated static peer list in each job.

The infrastructure session may implement bootstrap, private network selection,
Nomad server discovery, node classes/pools and Terraform resources while this
migration proceeds. It must not implement the MDC transport in parallel. Keep
discovery-aware jobs behind an explicit integration gate until the MDC binaries
and final configuration schema exist. Do not claim an end-to-end deployment
passes by substituting old multicast binaries.

Sequencer node elasticity is a separate problem from transport discovery.
Current main already implements explicit lanes and virtual-slot ownership; see
[dynamic sequencer sizing](../specs/dynamic-sequencer-sizing.md). Preserve that
resize protocol, two racing replicas per lane and independent placement.
Increasing a node pool does not authorize changing the shard map or deriving
lane IDs from ephemeral machine ordinals. Infrastructure must verify lane
coverage and drain rules before enabling automatic sequencer scale-in.

## Implementation sequence

1. Audit active channel call sites; remove misleading retired configuration.
2. Implement and test the shared discovery schema, registration and reconciliation.
3. Split publisher/subscriber channel construction and migrate one simple stream.
4. Migrate multi-publisher receipts/boundaries and transaction data, including
   archives, position identity and readiness barriers.
5. Migrate remaining active streams and archive/member endpoint discovery.
6. Update Nomad configuration consumers and publish the final infrastructure
   integration schema. Rebase the infrastructure branch onto this implementation.
7. Run transport and application fault tests with UDP multicast unavailable.

## Acceptance evidence

- Static check: no active deployment channel uses an IPv4 multicast destination
  or an embedded remote peer IP/subnet. Distinguish local bind addresses and
  runtime-resolved endpoints from source-controlled service addresses.
- Two concurrent publishers of the same shard: every required consumer receives
  both images, with identical source session/position identities and unchanged
  wire records. Receipt/boundary duplicates still deduplicate correctly.
- Sender/receiver ports can change on restart. Replacing a node with a different
  IP requires no configuration edits and no restart of healthy consumers.
- Join/leave churn under sustained traffic causes no freeze or loss outside the
  existing explicit recovery/failure contract. Cover multiple publishers and
  receivers, not just one-to-many fanout.
- Slow, stalled and disconnected receivers exercise each configured flow-control
  policy. Check backpressure, bounded memory and recovery behavior.
- Consul unavailable, permission denied, successful empty result, duplicate
  updates, catalog index reset and endpoint reuse are distinct tested cases.
- Each required archive can recover data from every expected publisher, including
  after publisher exit and consumer restart. Missing/expired recording ranges
  fail explicitly rather than selecting an unrelated recording or skipping data.
- Repeat existing smoke, racing sequencer, executor recovery, validator and
  ingress churn scenarios. Correctness gates include canonical ordering,
  nonce handling, receipt delivery, BAL verification and zero divergence.
- Benchmark 2/4/8 receivers per stream at fixed message sizes/rates. Report sender
  CPU, NIC utilization, retransmits, sustainable throughput and p50/p99/p99.9
  latency. Record hardware, Aeron version, MTU and flow-control settings.
- Existing IPC tests continue to work without Consul. No multicast-to-MDC mixed
  deployment is assumed compatible: initially use a coordinated cutover with
  explicit handling of old archive channel/catalog identifiers and retained data.
