# Resilient (Active/Active Replicated) Ingress — Spec

Status: implemented · Owner: ingress · Touches: `types`, `log`, `sequencer`,
`executor`, `ingress`, `e2e`, `deploy/cluster`

## Implementation status

- **D1 (session-discriminated join, 1a):** done. `TxRef.tx_data_session_id` +
  `TxDataLoc`; Aeron `frame.session_id()` threaded log→sequencer→executor; join
  buffer re-keyed `(shard, session, BPosition)`. Landed behavior-preserving.
  Proven by `executor::reader` unit tests `join_buffer_distinguishes_colliding_positions`
  and `reader_joins_two_sessions_at_same_position`; whole pre-existing
  executor/sequencer suites still pass.
- **D3/D4 (shared-nothing ingress + `ingress_id` correlation_id):** done.
  `IngressConfig.ingress_id` / `--ingress-id`; `pack_correlation_id`. Proven by
  `proxy::tests::correlation_id_*` and the in-process K-replica harness
  `crates/ingress/tests/replicated_cluster_test.rs` (routing agreement,
  correlation_id uniqueness, receipt fan-out, failover-from-cache-no-republish,
  concurrent-publishers-one-shard).
- **D2 (multicast receipts, 2a):** done as a config switch — `channels.toml.tpl`
  routes `tx_receipts` over the multicast group with MDS disabled; the ingress +
  executor non-MDS code paths already support it; the proxy's `SeenReceipts`
  dedups the N executor copies. Config contract guarded by
  `cluster_channels_tpl_receipts_are_multicast_not_mds`.
- **Cluster (active/active deploy + freeze guard):** done. `ingress.count = 2`
  (per-node driver, distinct_hosts, `--ingress-id ${NOMAD_ALLOC_INDEX}`); the
  cluster-e2e `ci-cluster.sh` ingress-churn step kills ingress-0 and re-smokes
  against ingress-1 — this is the multicast-receipts **freeze reproduction**
  (a frozen group ⇒ ingress-1 gets no receipt ⇒ smoke times out). Runs in
  `.github/workflows/cluster-e2e.yml`, not local `cargo test`.

## Goal

Run **N ingress replicas active/active**: every replica terminates client
connections, recovers the sender (signature check), and concurrently routes the
validated `TxEnvelope` to the correct sender-sharded sequencer via its per-shard
tx_data publisher. Any replica can accept any client's tx at any time; a replica
failure costs only its in-flight clients, who retry against a surviving replica.
Replicas share **nothing** — no leader, no sticky sessions, no cross-replica RPC.

To make active/active correct, two pipeline invariants must hold under
*concurrent publishers per shard*:

- **I-A (join integrity):** the executor resolves every `TxRef` to the exact
  envelope its sequencer pointed at, even when K ingresses publish to one shard.
- **I-B (receipt fan-out):** every ingress replica receives every receipt, so a
  tx parked at the replica that accepted it is released, and any replica can
  answer a retry from its cache.

## Non-Goals

1. Dynamic membership / discovery — replica count is static config (mirrors the
   existing `tx_receipts_executor_count` TODO(consul-watch)).
2. Load balancer / client-retry logic — external; we guarantee idempotent
   *server* behavior on retry.
3. Cross-shard / global tx ordering changes — canonical ordering (invariant I1)
   is unchanged; only the envelope **join key** gains a publisher discriminator.

## Design

### D1. Session-discriminated tx_data join (fixes I-A) — "1a"

**Problem.** `tx_data_position` is a `BPosition { term_id, term_offset }`
(`crates/types/src/position.rs`) and the executor join buffer keys on
`(shard_id, BPosition)` (`crates/executor/src/reader.rs`). Aeron positions are
**per session (per publisher)**; two ingress publishers on one tx_data stream
have independent term spaces, so their fragments can share a `(term_id,
term_offset)` → colliding join keys → wrong-envelope joins. With one publisher
this never happens (one session ⇒ unique positions, and sequencer + executor see
identical positions for a fragment).

**Fix.** Add the Aeron **session id** as the publisher discriminator. The
fragment header already exposes it: `frame.session_id() -> i32` on the same
`AeronHeaderValuesFrame` `header_pos` reads (`rusteron-client` 0.1.163).

- New wire field: `TxRef.tx_data_session_id: i32` (canonical record on
  tx_ordering; +4 B). `BPosition` is **unchanged** (117 call sites; session is
  meaningless for canonical/watermark positions).
- The tx_data read path surfaces session alongside position. Introduce
  `TxDataLoc { session_id: i32, position: BPosition }`; `log`'s `DeliverFn` and
  the `TxDataSubscriber` / executor `TxDataSubscription` yield it.
- Sequencer stamps `tx_data_session_id` into the `TxRef` it republishes.
- Executor join buffer is keyed `(shard_id, session_id, BPosition)`; the
  tx_ordering reader looks up `(shard_id, txref.tx_data_session_id,
  txref.tx_data_position)`.

This is **behavior-preserving for one publisher** (single session ⇒ identical
keys), so it lands and verifies against existing executor/sequencer tests before
any replication exists.

Per-sender **nonce ordering** across publishers needs no change: the sequencer's
nonce state machine already buffers future nonces until the gap fills
(`crates/sequencer/src/state.rs`), so interleaved arrival from K ingresses is
tolerated.

### D2. Multicast receipts + local dedup (fixes I-B) — "2a"

**Problem.** `tx_receipts` is a unicast MDS fan-in pinned to one ingress IP
(`tx_receipts_endpoint_host`, `channels.toml.tpl`): executors send receipts to
that single host. A second ingress receives nothing.

**Fix.** Route receipts over a **multicast group**; every ingress joins and
receives all copies; each ingress dedups the N executor-replica copies locally
via the existing `SeenReceipts` first-wins set (`crates/ingress/src/seen_receipts`).
Replica-count-agnostic, no per-ingress endpoint plumbing. The boundary
side-stream rides the same model.

**Known risk (must be tested, not assumed).** The team moved receipts *off*
multicast onto MDS because *"a shared multicast group's subscriber-churn froze
images (killing one recorder froze every executor)"* (channels.toml header).
Ingress churn (crash/restart joining/leaving the group) could reintroduce that
image-freeze. So D2 ships **with a freeze-reproduction test** (real-Aeron,
docker-e2e) that churns a subscriber and asserts the survivors keep receiving;
if it freezes, iterate the Aeron config (image liveness / `no_unavailable_image`
handling / channel params) until it does not. The other ingress-inbound streams
(`tx_errors` `.17`, `quorum_watermark` `.25`, `fsync_watermark` `.23`) are
already multicast and replica-agnostic — only `tx_receipts` changes.

### D3. Replica identity + globally-unique correlation_id

`correlation_id` is an opaque pass-through (stamped into `TxRef`/batcher frame,
logged) and is a per-process counter today → collides across replicas. Add
`ingress_id: u16` to `IngressConfig` (`--ingress-id` / `KARDAMOM_INGRESS_ID`)
and namespace: `correlation_id = (ingress_id as u64) << 48 | (seq & 0xFFFF_FFFF_FFFF)`.

### D4. Shared-nothing replicas + client retry

Each replica is a complete independent `IngressProxy` (own rate limiter,
verifier, pending map, cache, seen-receipts, correlation seq). Routing is a pure
function of `(sender, M)` so all replicas agree with zero coordination. A receipt
(fanned out per D2) releases the parked client at the accepting replica and is a
cache-populating no-op elsewhere. On client retry to another replica: already
executed ⇒ served from that replica's cache; not yet ⇒ idempotent re-publish,
deduped by the sequencer nonce gate (+ executor tx_hash dedup). At-least-once
publish, exactly-once execution, idempotent client response.

## Interfaces

- `TxRef { tx_hash, shard_id, tx_data_position, tx_data_session_id }` (+`i32`).
- `TxDataLoc { session_id: i32, position: BPosition }`; surfaced by `log`
  `DeliverFn`, `sequencer::TxDataSubscriber`, `executor::TxDataSubscription`.
- `JoinBuffer` keyed `(u8 shard, i32 session, BPosition)`.
- `IngressConfig { …, ingress_id: u16 }`; `kardamom-ingress --ingress-id`.
- `IngressProxy::next_correlation_id(&self) -> u64`.
- `channels.toml`: `tx_receipts` → multicast group (+ boundary side-stream);
  `tx_receipts_executor_count` retained only for local dedup sizing.

## Ethereum Spec References

- Sender recovery unchanged: secp256k1 `ecrecover` over the signing hash
  (EIP-155 legacy / EIP-2718 typed envelopes).
- Per-sender nonce monotonicity is what makes at-least-once publish under retry
  idempotent at the sequencer.

## Testing Strategy

- **Unit (deterministic):** session-id packing/plumbing; join-buffer
  session-keying; correlation_id namespacing; multicast-receipts config parse.
- **Integration (deterministic, in-process):** executor join under **two
  sessions with colliding `(term_id, term_offset)`** resolves each `TxRef` to
  the right envelope (the core I-A proof); K-replica in-process ingress harness
  (concurrent publishers carrying distinct session ids; routing agreement;
  correlation_id uniqueness; receipt release at the accepting replica; failover
  retry from another replica's cache; idempotent cold re-publish).
- **Real-Aeron (docker-e2e, `#[ignore]`):** multicast-receipts
  **freeze-reproduction** (churn a subscriber, assert survivors keep receiving);
  iterate to green.
- **Faithful cluster (cluster-e2e CI):** `ingress.count = 2`, per-node drivers,
  UDP multicast; **active/active** round-trip through both ingresses concurrently
  + an **ingress-kill redundancy check** mirroring the existing recorder-kill
  check.

Determinism: in-process tests use one shared bus and synthesize session
ids/positions explicitly (incl. the deliberate cross-session collision); no
fixed sleeps for correctness (bounded poll-until-state); real-Aeron freeze/
cluster tests are `#[ignore]`/CI-gated.

## Alternatives Considered

- **1b — per-ingress tx_data stream ids** (one publisher per stream, merge on
  read). Rejected vs 1a: same `TxRef`-wire blast radius but multiplies streams
  and adds per-shard fan-in topology; 1a keeps one stream per shard.
- **2b — per-ingress unicast MDS endpoints.** Rejected vs 2a: avoids the
  multicast-freeze risk but needs N_exec × N_ingress endpoints + membership;
  2a + local dedup is simpler and the freeze risk is converted into an explicit
  reproduction test.
- **Active/standby + floating VIP.** Rejected: the production front door is
  active/active ("scalable", `ingress.count 1+`); standby wastes a replica and
  doesn't exercise concurrent publishers.
