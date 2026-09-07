# Kardamom ↔ Kardamom interop: outbox messaging — design spec

Status: DRAFT v4 for discussion — 2026-08-10
Scope: in-protocol asynchronous cross-chain messaging between Kardamom chains, with
callbacks. Native value transfer has a designed path (burn → mint, §13) but ships
only after messaging is proven (§15).

## 1. Motivation

Kardamom chains should talk to each other without external relayers or third-party
bridges. The deposit pipeline already solves the hard sub-problems for the L1→L2
direction: deterministic derivation of externally-sourced transactions, no-skip
origin advancement through the sealer, fail-stop verification in the validator, and
a dedicated fee-free tx type. Interop generalizes that machinery so that *another
Kardamom chain* can be an origin, and adds a return channel (callbacks).

Design tenets, in order:
1. **Deterministic**: every replica of the destination chain derives the same
   cross-chain txs at the same canonical-stream position, or halts (fail-stop, like
   `EpochFault`).
2. **Verifiable**: the destination never has to trust the origin's feed — per
   pair it either re-derives the origin itself from an authenticated ordering
   source, or follows an agreeing quorum of the origin's validators (§10).
3. **Reuses existing seams**: outbox = `L2ToL1MessagePasser` generalized; watcher =
   `da_watcher` with a second source adapter; injection = `KIND_ORIGIN_RECORD`
   generalized; execution = `execute_deposit_tx` generalized; feed = the ingress
   `subscribeReceipts` jsonrpsee pattern on the validator.
4. **Scales in the number of chains**: per-destination filtering at the feed; remote
   origins advance as stream markers, not per-boundary stamps (§7).

## 2. Prior art in this repo (what we build on)

| Mechanism | Where | Reused as |
|---|---|---|
| Deposit derivation shared rule | `crates/types/src/epoch.rs` (`derive_epoch`, `source_hash` domains, `alias_l1_address`) | `derive_remote_epoch`, new source-hash domain, remote aliasing |
| Origin advance through sealer | `cluster-adapter/src/wire.rs` (`KIND_ORIGIN_RECORD`, `RT_EPOCH`, `epoch_slots`), `CanonicalSealerState.onOriginRecord` → `OriginAdvance` | new remote-origin record type, per-peer origin map |
| Canonical-stream epoch frames | `types/src/tx_ordering.rs` (`TxOrderingMessage::Epoch`), engine `reader.rs` dedup + `EpochObserver` seam | `TxOrderingMessage::RemoteEpoch`, `RemoteEpochObserver` |
| Fee-free system tx execution | `exec-core/src/executor.rs::execute_deposit_tx` (mint pre-credit, nonce-check off, receipt `tx_hash = source_hash`, `TX_TYPE_DEPOSIT = 0x7E`) | `execute_xchain_tx`, `TX_TYPE_XCHAIN = 0x7D`, mint side of value transfer |
| Message-passer contract shape | `contracts/src/L2/L2ToL1MessagePasser.sol` (monotone nonce, `sentMessages`, event + recomputed leaf) | `Outbox` predeploy |
| Leaf/tree/proof + output posting | `types/src/withdrawals.rs` (`withdrawal_leaf`, `withdrawals_root`, `output_root`), validator `attester.rs` (`OutputPoster`, posts to `WithdrawalOutputOracle` on L1) | **untouched** — attestation to L1 serves verifiers that cannot re-execute (L1). Kardamom peers re-execute, so interop needs none of it (§10) |
| Epoch verification vs L1 | validator `epoch_verify.rs` (`EpochVerifier`, inline sequence + queued content check, `EpochFault` fail-stop) | `RemoteEpochVerifier` on the destination |
| BAL per-tx attribution | `types/src/delta.rs` (`BalFrame`), `validator/src/parallel.rs::ClaimIndex` (address → slot → post-value per access index) | cross-check that claimed outbox writes really happened |
| Validator re-execution + fail-stop | `crates/validator` (`ValidatorWriterQueue`, `ValidatorReceiptSink`, `Divergence` halt latch, `parallel.rs` BAL-seeded re-execution) | **peer-validation mode** — B runs the same component against chain A (§10) |
| WS subscription surface | `ingress/src/json_rpc.rs` (`kardamom_subscribeReceipts`, filters, `Lagged` marker) | validator `kardamom_subscribeOutbox` |
| WS-triggered watching | `docs/agents/push-model-spec.md` Push-2 (on `feat/push-1c`) | remote-chain source adapter transport |
| L1 light client | PR #171 (`feat/l1-light-client`, Helios job + origin-hash chaining) | the interop node's shared L1 view, for DA-mode peer validation |
| Rebuild-from-DA | recovery series PR #80 (`crates/batcher`: `kardamom-reconstruct`, `reexec.rs` — re-derive chain state from L1 blobs) | sovereign DA-mode validation of a peer chain (§10) |

## 3. Architecture overview

```
CHAIN A (origin)                                CHAIN B (destination)
────────────────                                ─────────────────────
app tx → Outbox.sendMessage{value}(dest=B,…)
  │  (normal tx, paid in A-gas; value burned)
  ▼
executor A: BAL claims for Outbox slots
+ receipt with MessageSent log
  │ tx_bal / tx_receipts
  ▼
egress node of A (sequencer-owned validator — §5; or B's
own validator-of-A under posture A, see below)
  ├─ verifies block (BAL/receipt cross-check)
  ├─ extracts messages (log/calldata + BAL cross-check)
  ├─ relays the validator set's (chain_id, block, state_root)
  │   attestations; signs served segments
  └─ WS: kardamom_subscribeOutbox / subscribeAttestations
        │ (dest-filtered, resumable, signed)
        ▼
                                     interop watcher B (da_watcher adapter)
                                       ├─ subscribes (dest=B, cursor)
                                       ├─ verifies per the pair's §10 posture
                                       │   (own validator, or agreeing quorum)
                                       └─ derive_remote_epoch → publish
                                              │
                                              ▼
                                     sequencer B → cluster record (remote origin)
                                              ▼
                                     sealer B: per-peer origin advance, forced
                                     boundary, relay marker + messages
                                              ▼
                                     canonical stream: RemoteEpoch frame
                                              ▼
                                     executor B: execute_xchain_tx (0x7D)
                                       ├─ mint value (if any) to aliased sender
                                       └─ Inbox.deliver(...) → inner call
                                            └─ callback? → Outbox B.sendMessage(A,…)
                                                    (same pipeline, reversed)
```

The "validator of A" box is whichever §10 posture the pair is configured for:
**B's own** instance running against chain A (fed by A's live streams or
re-derived from A's L1 DA), or ≥Q of A's operator-run validators whose feeds
must agree. In the first case B executes messages it derived itself and any
attestations are a cross-check on the *ordering* it was served; in the second,
agreement across independent validators is the evidence.

## 4. The `Outbox` predeploy (every Kardamom chain)

A generalization of `L2ToL1MessagePasser`, at a reserved predeploy address (e.g.
`0x4200…00E0`, allocated in `chains/*.toml` genesis like the message passer).

```solidity
function sendMessage(
    uint64 destChainId,
    address target,
    uint256 gasLimit,        // execution budget on dest, capped (§12)
    bytes calldata data,
    Callback calldata cb     // optional; zeroed = none (§9)
) external payable returns (uint64 seq);
// msg.value = inbound fee (§12) + cross-chain value to burn and re-mint (§13)
```

- **Per-destination dense sequence**: `nonce[destChainId]++`. Dense per-pair
  sequencing is what lets the destination enforce the no-skip rule locally (the
  interop analogue of "derive one epoch per L1 block including empty ones"). A
  global nonce would force every destination to track a sparse set.
- **Storage holds commitments only, and the layout is diff-extraction-friendly**:
  `sentMessages[msgHash] = true` plus the per-dest counters, the `burned[dest]`
  supply ledger (§13) and the `owed[dest]` fee ledger (§12). Only
  `sentMessages` must be append-only and never overwritten within a block —
  that is what makes BAL claims for the Outbox map 1:1 onto sends; the
  counters and ledgers are ordinary accumulators the extractor ignores.
- **Payloads ride the data path, not state.** The message body is *never* stored:
  - v1: the extractor reads it from the `MessageSent` event / the sending tx's
    calldata (both already in the validator's hands via `ReceiptBuffer` and the
    witness), recomputes `msgHash`, and rejects drift — the
    `decode_message_passed` discipline;
  - at scale: payloads can move to **blobs** — message bodies ride DA and the
    feed carries `(blob_ref, offset, len)` pointers instead of inline bytes, with
    the same commitment check on the receiving side. The commitment-in-storage
    design makes the transport swappable without touching the trust story.
  - Either way the BAL cross-check is unchanged: the claimed `sentMessages` slot
    write (post-value, via `ClaimIndex`) must exist for every extracted message.
- **Message leaf** (in `crates/types/src/xchain.rs`, the single shared copy):
  `msg_leaf = keccak(abi.encode(LEAF_DOMAIN_XCHAIN, originChainId, destChainId,
  seq, sender, target, value, gasLimit, keccak(data), cbHash))`. Origin **and**
  destination chain ids inside the leaf make replay across pairs impossible;
  `value` inside the leaf puts the burn amount under the commitment (§13).

## 5. Extraction and the egress node (origin side)

All external serving is done by the **validator tier** (revised 2026-08-16;
see `egress-node-spec.md` v2). Validators are internet-facing by design —
they re-execute everything, attest state roots, serve state RPC, and serve
this feed. The operator's own instance with the **egress role config** is the
distribution origin: the only internet-connected node with direct access to
the internal Aeron planes, it recomputes state roots and streams outbox
content + roots outward in signed per-block segments through a CDN, from
which public validators (and peers' sovereign watchers) consume. The core —
sealers, sequencers, executors, and the ingress node, which accepts only an
allowlisted set of validator peers — never touches the internet. Three
properties motivate the shape:

- **Isolation**: the sealer cluster and executors are never externally
  reachable; the egress node is the only interop-facing surface, so the
  cutoff/quarantine machinery has one throat to choke and the Raft cluster's
  attack surface does not grow with the peer count.
- **CDN shape**: one origin tier that can fan out through replicas/relays —
  consumers verify the egress signature (key in the peer registry), so relays
  need no trust and scale is a caching problem, not a trust problem.
- **Post-verification vantage**: it re-executes and BAL/receipt-cross-checks
  before serving, holds the needed inputs (`ReceiptBuffer`,
  `BalBuffer`/`ClaimIndex`), and keeps the executor hot path untouched; the
  executor emits one firehose (`tx_bal`, unchanged) and the egress node fans
  out only what each subscriber asked for.

New `crates/validator/src/interop.rs`:
- `collect_outbox_messages(block)` — decode `MessageSent` logs from the Outbox
  address out of the block's receipts (the `receipt_withdrawal_leaves` pattern),
  recompute leaves, and cross-check each against the BAL claim for the
  `sentMessages` slot (via `ClaimIndex`: address → slot → post-value at the tx's
  access index). Mismatch = validator divergence halt, same posture as
  `write_set_eq` failures.
- **Attestation**: the validator signs `(chain_id, block_number, state_root)`
  per block and serves it on the attestation feed (§10). No finality stamp
  rides the messages — there is only one safety level (§10), so a message is
  either derived or it is not.
- Append accepted messages to a per-destination feed store (retained N blocks,
  N configurable; deeper backfill is a v2 concern — the data is in DA).
- **Feed surface**: a jsonrpsee WS server on the validator (its first — today it
  only exposes Prometheus metrics; the server shape is lifted from
  `ingress/src/json_rpc.rs`).
  `kardamom_subscribeOutbox(destChainId, cursor: {seq})` streaming:
  - `Message { originChainId, originBlockNumber, originBlockHash, seq, sender,
    target, value, gasLimit, data, cbSpec }`
  - `Lagged { skipped }` — subscriber fell behind; recover by re-subscribing from
    its cursor (the `subscribeReceipts` recovery discipline).
  and `kardamom_subscribeAttestations(cursor: {blockNumber})` streaming
  `Attestation { chainId, blockNumber, stateRoot, validatorId, signature }`.

The feed is **not** a trust boundary: under §10 a destination re-derives the
peer itself and uses the feed only as transport and as the attestation
carrier. That is why no inclusion proof accompanies a message — B already
computed it. A destination running in the dev/test feed-trust mode (the mock
in `da_watcher/src/interop/mock.rs`) is the one exception, and is not a
production posture.

## 6. The interop watcher (destination side)

`crates/da_watcher` is refactored around source adapters rather than forked:

- Existing: `L1Source` → deposits (unchanged behavior).
- New: `RemoteChainSource` — a WS subscription (Push-2 transport discipline: WS
  as trigger/stream, HTTP poll as reconnect fallback) whose *target* is set by
  the per-peer trust mode (§10):

  ```
  RemoteChainSource {
    chain_id, outbox,
    verify: OwnValidator {                       // posture A (§10)
              local_validator,
              ordering: FromDa                   // self-sufficient
                      | SignedStream { signer }  // verify the KEY, not the link
                      | CrossCheck { validator_keys, threshold },
            }
          | Quorum {                             // posture B (§10)
              endpoints, validator_keys, threshold,
              on_unavailable: Stall | Proceed,
            }
          | TrustFeed { endpoints }              // dev/test only — see mock.rs
  }
  ```
  `signer`, `validator_keys`, and `threshold` are read from the peer registry
  (§12), not from local config — endpoints are transport hints with no
  authority. The type makes the unsafe combination unrepresentable: an
  own-validator fed by an unauthenticated live stream with no cross-check is
  the one posture §10 rules out, so `ordering` has no such variant.
- Shared rule in `kardamom-types` (`xchain.rs`), mirroring `derive_epoch`:
  `derive_remote_epoch(self_chain_id, origin_chain_id, expected_first_seq,
  messages) -> Result<RemoteEpochRecord, XChainError>` — orders by `seq`,
  rejects gaps, regressions, duplicates, foreign destinations, and empty
  batches. `canonical_id = keccak(origin_chain_id ‖ anchor_hash ‖ first_seq ‖
  last_seq)` for cluster dedup, exactly like `EpochRecord`.
- **Batches are one per origin block** — all messages to us from exactly one
  origin block, never a partial or spanning range. Deterministic boundaries are
  what make racing relayers and restarts derive byte-identical records so
  `canonical_id` dedup collapses them; arbitrary boundaries would produce
  different ids for overlapping ranges and defeat it. (Cost: the last origin
  block stays open until a later one arrives — §16.)
- `remote_source_hash(origin_chain_id, seq)` under a **new domain 2**
  in the `source_hash` scheme (`epoch.rs`: domain 0 = user deposit, 1 = reserved
  system) — collision-free tx identity across all origins.
- **Sender aliasing**: `alias_remote_address(origin_chain_id, sender)` — the
  existing constant-offset trick does not survive multiple origins (two chains'
  contracts at the same address would collide). Proposal: `address(keccak256(
  ALIAS_DOMAIN ‖ origin_chain_id ‖ sender)[12..])`, defined once in `xchain.rs`.
  Trade-off vs. OP-style reversible offsets noted in §16.
- **Advance policy**: unlike L1 epochs (one per L1 block, empty or not), remote
  origins advance **only when messages exist**. Empty advances per peer per block
  would multiply stream noise by the peer count for no verification benefit —
  the no-skip rule is enforced on the dense per-pair `seq`, not on origin blocks.
  (Optional low-frequency heartbeat anchors for liveness monitoring: §16.)

The watcher only publishes messages that have cleared the trust gate (§10).

## 7. Injection: sealer and canonical stream (destination side)

- Wire: a **distinct ingress kind**, `KIND_REMOTE_ORIGIN_RECORD = 5`, carrying
  `RT_REMOTE_EPOCH = 3` and `remote_epoch_slots()` (marker + one slot per
  message), parallel to `epoch_slots`. Frame:
  `[kind=5][canonical_id:32][origin_chain_id:u64][anchor_number:u64][slot_count:u32][RT_REMOTE_EPOCH][rkyv…]`.
  A distinct kind rather than a new record type under kind 4 for the reason
  kind 4 exists at all: the sealer branches on the frame tag and never opens
  the payload, so it cannot see a record-type discriminator that lives inside
  it — and a remote origin needs a *pair* of u64s in the header (which chain,
  and its position) where an L1 epoch needs one.
- Java sealer: `CanonicalSealerState.onRemoteOriginRecord(...)` with a per-peer
  origin map, dedup by `canonical_id` (which already mixes in the origin chain
  id, so one shared window covers every peer), and the same forced-boundary
  contract as deposits (`RemoteOriginAdvance`: boundary offered before relayed,
  else messages land in the previous block's tail). **The map is replicated
  state and must be in the snapshot** — a member restored without it would
  accept an anchor its peers reject and diverge the state machine.
- **Boundaries do not stamp remote origins.** `l1_origin` rides every
  `BlockBoundary` because there is exactly one L1; per-peer origin stamps would
  grow boundaries by the peer set. Remote origin positions are recoverable from
  the stream itself (the `RemoteEpoch` markers), which replay already preserves.
- Canonical stream: new `TxOrderingMessage::RemoteEpoch(RemoteEpochRecord)` (new
  1-byte tag in `tx_ordering.rs`, messages by value like deposits — no side-stream
  join to lose). Engine `reader.rs`: dedup via `DedupWindow` on `canonical_id`,
  emit marker + one `ReaderToExec::XChainMsg` each; a `RemoteEpochObserver` seam
  mirroring `EpochObserver` — executor wires nothing, validator B wires
  `RemoteEpochVerifier`.

## 8. Execution (destination side)

- `TX_TYPE_XCHAIN: u8 = 0x7D` in `types/src/receipt.rs` (0x7E is deposits).
- `exec-core::execute_xchain_tx`, modeled on `execute_deposit_tx`: fee-free
  (`gas_price = 0`), nonce check disabled, mint pre-credit of `value` to the
  aliased sender when nonzero (§13), receipt `tx_hash = remote_source_hash`, BAL
  claims recorded via `record_writeset_into_bal` unchanged.
- The derived tx does not call `target` directly; it calls the **`Inbox`
  predeploy**: `Inbox.deliver(originChainId, seq, sender, target, value, gasLimit,
  data, cb)`, which:
  1. records delivery (`delivered[originChainId][seq] = status` and, for value,
    `minted[originChainId] += value`) — app-queryable records; note exactly-once
    does **not** depend on them (determinism + dedup + fail-stop verification
    already guarantee it, as with deposits — this is observability, callback
    bookkeeping, and the supply invariant's destination half),
  2. makes the inner call to `target` with the aliased sender and `gasLimit`
    budget; an inner revert marks `status = failed` but the delivery tx itself
    succeeds (the deposit posture: the pre-committed effect survives inner
    reverts — the minted value stays with the aliased sender),
  3. if a callback was requested, enqueues the response through B's own Outbox
    (§9) — atomically with delivery, in the same tx, so it is BAL-visible and
    extracted like any other message.

**Receiver authentication.** A receiver must check both conditions:
`msg.sender == INBOX`, and the `(originChainId, sender)` pair returned by
`Inbox.xDomainSender()`. The pair alone is not enough. It stays set for the
whole inner call, so every contract the delivery target calls during a
delivery sees the same `xDomainSender()`. A contract that checks only
`xDomainSender()` trusts a call the target made on its own behalf (a confused
deputy).

## 9. Callbacks

A callback is **just a message** flowing the other way — no new pipeline.

- `Callback { target: address /* on origin */, gasLimit: uint64, context: bytes32 }`
  (fixed-size; zeroed = none).
- Only the fully zeroed struct is "none". A callback with `target = 0` and a
  nonzero `context` (or `gasLimit`) is "some": the response goes to address
  zero on the origin and uses one seq. Both `XChain.isNone` and the Rust
  `Option<Callback>` decode treat it the same way. A sender that wants no
  response must zero all three fields.
- On delivery completion (success *or* failure — failure must call back, or the
  origin app waits forever), B's Inbox sends via B's Outbox:
  `kind = Callback, inReplyTo = (originChainId, seq), payload = { status,
  returnDataHash, context }` — return data by hash, bounded; apps that need the
  full return payload emit it themselves from `target`.
- **Depth is capped at one**: the Outbox rejects a callback spec on a message of
  kind Callback. No ping-pong loops by construction.
- **Prepaid at send time.** The response executes on the *origin* chain, so
  the origin charges for its gas budget when the message is sent and holds it
  against the callback landing (§12). Together with quota accounting against
  the originating pair, this is what stops mandatory failure callbacks from
  being free amplification.

## 10. Verification — what B must believe about A

**There is no finality gating, because there is nothing to gate on.** Two
independent properties collapse the question:

- **Inputs**: the chain only derives from L1-*finalized* blocks (the DA watcher
  tails `finalized_block_number`, `L1SourceError::NotFinalized`,
  `EpochFault::BlockBeyondFinality`; `epoch.rs` states it directly — an L1
  number maps to one hash forever, which is why no L1-reorg handling exists
  anywhere in the tree).
- **Ordering**: the canonical stream is Raft-committed. There is no fork
  choice, no unsafe head, and no block-replacement path in the codebase.

So a transaction that executes is final, full stop. OP's safety ladder
(unsafe → cross-unsafe → safe → finalized) is an artifact of an architecture
whose own blocks have several safety levels and can be replaced; Kardamom has
one level, so the ladder has nothing to describe. What remains is not *when*
B may believe a message, but *whether it is really A's chain talking*. This
section is about authentication only.

### Two facts to establish, often conflated

1. **Ordering authenticity** — is this the log A's sealer cluster actually
   committed?
2. **Execution correctness** — does executing that log yield this state and
   these messages?

Running your own validator of A settles (2) completely and (1) **not at all**:
it faithfully re-executes whatever stream it is fed, so an origin that serves B
a fabricated ordering gets back a perfectly self-consistent wrong answer. Self
-verification feels total but is conditional on where the ordering came from.
That conditionality is the whole design question here.

Above both postures, pairs operated within a single operational trust domain
may run an **instant tier**: messages derive with no verification ceremony,
and safety is provided operationally (intrusion detection, coordinated pause,
and bounded recovery) rather than cryptographically. That tier's mechanics
are deliberately documented separately, in the operator's infrastructure
documentation; nothing below changes for pairs that cross operator
boundaries.

### Two postures — either/or, chosen per pair

**Posture A — B runs its own validator of the peer.** B re-executes A and
derives the messages first-hand, so execution correctness is B's own and needs
no attester. Ordering authenticity then has to come from the stream source,
and there are three ways to get it:

- **From A's L1 DA** — *self-sufficient*. The ordering is whatever A's batcher
  posted to L1, authenticated by L1 itself; nothing else is required. Latency
  is batcher cadence. This is the strongest simple posture and the natural
  bootstrap.
- **From a signed live stream** — the ordering carries a signature from an
  identity registered for that chain in the peer registry (§12), so B verifies
  the *key*, not the connection. Lowest latency, and the transport becomes
  irrelevant: anyone may relay it. This is OP's design for their pre-L1 path —
  unsafe blocks gossiped over P2P are signed by an Unsafe Block Signer whose
  address lives in the `SystemConfig` contract on L1, with a grace period
  during which both the old and the new signer are accepted after a rotation.
  Trusting an endpoint URL instead would mean trusting DNS, TLS, and whoever
  runs the relay; trusting a registered key means none of that matters.
  **The signer is the origin's egress node (§5)** — the sealer itself stays
  unexposed and unsigned. Residual to be explicit about: the egress node is a
  single operator-owned relay attesting what *it* observed, weaker than the
  consensus attesting for itself — which is why a value-carrying pair pairs
  the signed stream with the attestation-quorum cross-check, and why
  threshold signing by the sealer members remains a future strengthening.
- **From an unauthenticated live stream, cross-checked by a quorum** — the quorum
  below, used only to catch a fabricated ordering. This is the "both" case,
  and it is the right combination when B wants live latency without trusting
  the operator serving the stream.

**Posture B — B follows a quorum of the peer's validators.** B does not
re-execute. It subscribes to ≥Q *independent* validators of A and requires
their message batches — and their attested `(chain_id, block_number,
state_root)` — to agree. Agreement across Q independent executions establishes
both facts at once, because a state root is a deterministic function of the
ordering: matching roots mean the attesters saw the same ordered prefix and
executed it identically. Trust is Q-of-N non-collusion. Cheapest to operate,
with no peer node to run.

The choice is per pair and belongs in config, not in the protocol: a
high-value pair on a live link runs A with a signed stream or a quorum
cross-check; a low-stakes pair can run B alone; a pair with no live transport
runs A over DA and needs nothing else. What must never happen is posture A
over an *unauthenticated* live stream with no cross-check — self-consistent
and wrong.

**Choosing between them — the cost shape.** Posture A verifies the *whole peer
chain* and then extracts messages, so its cost scales with that peer's
throughput and is independent of how much you actually talk to it. Posture B
costs the same regardless of the peer's size. So posture A against a
high-throughput peer you exchange a handful of messages a day with is a poor
trade, and that is exactly where posture B earns its place; posture A pays for
itself on pairs carrying value or high message volume. This is also the axis
that separates us and OP from LayerZero, whose DVNs attest *messages* rather
than deriving chains — which is why they can span a hundred heterogeneous
chains and we deliberately cannot.

**A general rule this design keeps running into**: because there are no
reorgs (above), every mechanism the other stacks apply *after the fact*
becomes something we must prevent *before* the fact. OP tolerates sequencer
equivocation on their unsafe path because L1 derivation later replaces the
offending block; having refused replacement, we have to make equivocation
impossible up front — hence signed ordering or a quorum, never "we will
notice and unwind."

### Consequence: no Merkle proofs in either posture

Posture A re-derives the messages, so B already holds them and the attestation
is a scalar equality check on a state root. Posture B compares whole message
batches across independent feeds, so agreement itself is the evidence. Neither
needs an inclusion proof against a commitment — which deletes an entire
subsystem from earlier drafts of this spec:

- no `outbox_root` and no third output component,
- no `OUTPUT_VERSION = 1` migration of the withdrawal oracle,
- no `AnchorProof`, and no side-tree-vs-MPT flavor decision,
- no L1 anchoring on the interop path at all.

OP arrives at the same place for the same reason: their supernode re-derives
peers rather than verifying proofs about them. Inclusion proofs only return
for a *light-client* consumer that declines to run a validator of its peer —
explicitly out of scope for v1, and the one future case that would justify
reintroducing an outbox commitment.

**What L1 attestation is still for.** The withdrawal path (L2→L1) keeps the
attester and `WithdrawalOutputOracle` exactly as they are: L1 cannot
re-execute a Kardamom chain, so a foreign verifier needs an attested output.
The principle worth stating once — *attestation serves verifiers that cannot
re-execute; Kardamom peers can, so they do not need it.*

### The attestation

New, small machinery: each validator signs `(chain_id, block_number,
state_root)` and publishes it on an attestation feed alongside the outbox
feed. Validators today verify and fail-stop but sign nothing, so this adds a
key, a signature, and a gossip surface — but no new verification logic.

`state_root` alone is sufficient: it commits to the Outbox predeploy's storage
(`sentMessages` and the per-destination nonces), so agreement on the state
root at block N is agreement on the complete set of messages sent through N.
There is no need to attest an outbox-specific commitment.

### Deployment implications (non-obvious, decide before P1)

- **"Quorum" here is the validator attestation quorum, NOT the sealer Raft
  quorum.** The two are different sets with different jobs: the sealer cluster
  decides *order*; the validator set confirms *execution agreement*. The word
  is overloaded across this codebase — keep the distinction explicit in code
  and metrics names.
- **Apps inherit the chain's posture and cannot override it.** Derivation is a
  chain-level property, so the posture chosen for a pair silently sets the
  security of every application using it — the same as OP, and the opposite of
  LayerZero, where each OApp configures its own verifiers (flexible, but a
  compromised app owner can install a malicious DVN set). Per-pair postures
  should therefore be published, so app developers can reason about what they
  are inheriting.
- **A validator SET is required only by the postures that use a quorum.**
  Today's deployments run a single validator (`validator.nomad.hcl`, one job),
  and a 1-of-1 quorum attests nothing, so any consumer on posture B or on the
  cross-check needs the origin to run N ≥ 3. Note the corollary, which is a
  genuinely useful property: **an origin whose consumers all run posture A over
  DA has to do nothing at all** — no validator set, no attestation keys, no
  feed. It just posts to L1 as it already does.
- **Node shape: one interop node, not one process per peer.** Following OP's
  `op-supernode` — which hosts the consensus layer of every chain in the
  dependency set in a single process, with cross-chain checks above the
  per-chain layer and a shared L1/beacon client — B should run *one* interop
  node hosting every peer's derivation and verification, sharing the L1 view
  (#171 Helios), the DA reader, and the ops surface. Cost then scales with
  total peer throughput rather than with a per-pair process constant, which is
  what makes running your own validator of every peer affordable at all.

### Transport modes for B's validator-of-A

- **Live mode** — consumes A's canonical stream + `tx_bal` directly. Lowest
  latency and the intended default, but those channels are Aeron IPC-scoped
  today, so this needs networked channel configs or a relay (§16). This is now
  a *required* piece of work, not an upgrade path.
- **DA mode** — re-derives A purely from A's L1 blobs via the recovery
  series' `kardamom-reconstruct`/`reexec` machinery. No dependency on A's live
  infrastructure at all; latency ≈ batcher cadence + L1 finality. The natural
  bootstrap and the natural fallback when a peer's live transport is
  unavailable.

### Failure semantics

- **B's own validator disagrees with the quorum** (posture A + cross-check), or
  **the quorum's feeds disagree with each other** (posture B) → halt *the
  pair*, alarm loudly. Either B has a bug or A's validators have diverged or
  colluded; both need a human. Pair-scoped, matching the watcher's fault
  domain — the destination chain keeps running for every other peer.
- **A record already on B's canonical stream fails verification** → chain-level
  fail-stop, as with `EpochFault`. Once a record is canonical, disagreeing
  about it is a consensus fault, not a pair problem. This asymmetry is
  deliberate.
- **Fewer than Q attestations available** → B stalls that pair. Per-pair policy
  knob `on_quorum_unavailable = stall | proceed_on_own_view`; the latter keeps
  content verification and drops only the equivocation check, trading security
  for liveness. Default `stall`.

### Residual accepted risk

Total loss of a majority of A's durable storage could rewind A below what
reached L1 DA, orphaning already-delivered messages on B. This is a
disaster-recovery event, not a reorg: it requires simultaneous catastrophic
loss across a Raft majority, and it breaks many other invariants at the same
time. Defending against it on the hot path would cost the entire latency
budget (it is what the earlier L1-anchored draft of this section was buying).
**Accepted explicitly, recorded rather than mitigated.**

### Later: ZK

A proof that "block N of A produced state root R" replaces *both* mechanisms
at once — B stops re-executing A and stops needing attesters. That is the
zk series' endgame (#173's stateless driver → 3b/3c), and it is the only
change that would let B verify a peer it does not follow.

## 11. Ordering and exactly-once

- **Per-pair FIFO**: dense `seq` per `(origin, dest)`, no-skip enforced at three
  layers (watcher derivation, sealer per-peer dedup/advance, verifier inline
  check). No cross-pair or cross-origin ordering is guaranteed — document this
  for app developers.
- **Exactly-once** falls out of determinism, exactly as for deposits: same
  `RemoteEpochRecord` at the same stream position on every replica, reader
  `DedupWindow` on `canonical_id`, and fail-stop divergence if any replica
  disagrees. The Inbox's `delivered` map is bookkeeping, not the mechanism.

## 12. The peer registry, quotas, and fees

### The peer registry

One record per peer, and it is the highest-leverage configuration in the
system: **adding a peer grants it the ability to inject transactions into this
chain.**

```
peer(chain_id) = {
  outbox, genesis_hash,          // identity
  posture,                       // §10: OwnValidator{ordering} | Quorum{…}
  ordering_signer,               // SignedStream: key + rotation grace period
  validator_keys, threshold,     // Quorum posture, or the cross-check
  feed_endpoints,                // TRANSPORT HINTS ONLY — never a trust input
  quota,                         // per-block message + gas caps (below)
  inbound_price, fee_recipient,  // §12 fees (default price 0)
}
```

`feed_endpoints` deliberately carries no authority: under §10 authenticity
comes from a signature, from DA, or from quorum agreement, never from having
connected to the "right" URL. Anyone may serve the feed.

**Governance.** A config file an operator can edit unilaterally is the wrong
home for tx-injection rights. v1: chain config with a documented owner and
change process; v2: an L1 registry contract, the analogue of OP's
governance-managed dependency set (their membership changes go through a
Protocol Upgrade vote against a contract on L1). Asymmetric application:

- changes that **tighten** — removing a peer, raising a threshold, cutting a
  quota — apply immediately;
- changes that **loosen or reprice** — adding a peer, raising a price,
  rotating an ordering signer — need a notice period, so running watchers and
  in-flight senders are never surprised. This is the same reasoning as the
  signer-rotation grace period in §10.

### Quotas — the security mechanism

- Sending pays **origin-chain gas** (`Outbox.sendMessage` is a normal tx) — the
  deposit shape, where L1 gas prices deposit ingress.
- Destination-side execution is fee-free at the protocol level (0x7D, like
  0x7E), so it must be quota-gated: per-origin-chain per-block message count
  and total-gas caps, plus the per-message `gasLimit` ceiling the Outbox
  enforces at send time (reject early, on the paid side). Quota overflow
  **defers** derivation — messages wait, and the dense seq preserves order —
  it never drops.
- **Callbacks count against the origin's quota.** Mandatory failure callbacks
  (§9) are an amplification vector: N messages aimed at a reverting target
  force N protocol-generated return messages. Charge them to the pair that
  caused them, not to fresh local traffic.
- What this buys and costs versus the alternatives: OP interop and LayerZero
  both have a relayer/executor pay destination gas, so their spam defense *is*
  the gas market. Deriving messages into the chain removes the
  relayer-liveness assumption entirely; quotas are the price of that.

### Inbound fees — accounting, not defense

Origin-chain gas pays the origin for its own work and nothing else, which is
fine while one operator runs every chain in a constellation and a real gap the
moment they are independent: there is otherwise no way to charge a peer for
the work its messages cause.

The fix does not need a cross-chain fee at all. The sender pays **on the
origin chain, in origin currency, to an address the destination designates**:

- `sendMessage` requires `msg.value >= inbound_price × (gasLimit +
  cb.gasLimit)` and credits `owed[destChainId] += fee`, which the recipient
  **withdraws** (pull, never push — paying a registry-controlled address
  inline inside `sendMessage` is a reentrancy surface for no benefit).
- **Both legs are prepaid.** The callback executes on the *origin* chain, so
  the origin charges for it at send time and holds it against the callback
  landing. This closes the amplification vector above at its source: aiming a
  flood at a reverting target now costs the sender both legs up front instead
  of conscripting the origin into unpaid return traffic.
- **Charged on the budget, not on consumption** — no refunds and no
  cross-chain settlement; the sender's incentive is simply to pick a tight
  `gasLimit`.
- The fee is **not** part of the message leaf and **not** the `value` field
  (§13), so the commitment, the cross-language vectors, and the burn/mint
  conservation invariant are all untouched by it.
- `inbound_price` defaults to **0**: a single-operator constellation should not
  pay overhead to charge itself.
- `fee_recipient` is a destination-designated **treasury** address, not a
  sequencer key — inbound work is done by executors, validators, and the
  sealer collectively, and binding revenue to one role invites a split
  argument later.

This shape is available to us only because the destination chain itself
executes: with no relayer there is nothing to quote and no third party to
compensate. LayerZero needs an Executor role plus quote/refund machinery for
the same job; OP has no mechanism at all, leaving relaying as unpaid
protocol-external work.

**Fees do not replace quotas, and must not be allowed to feel like they do.**
A hostile peer mints its own native token at zero cost, so a fee denominated
in the *origin's* currency is worthless against precisely the chain you would
most want to defend against. Quotas remain the security mechanism; fees are
accounting between cooperating operators. What fees add is a **graduated
lever** the design otherwise lacks — price a noisy-but-legitimate peer up
instead of choosing between tolerating it and halting the pair.

Two consequences worth stating once. Fee revenue gives a destination operator
a reason to accept a pair it might otherwise decline; that incentive must not
bleed into posture selection (§10), which is a security decision. And the
balance accrues *on the origin chain*, so repatriating it uses the bridge or
L1 — a mild circularity, not a problem.

## 13. Native value transfer (burn → mint)

Value rides the same messages; supply across the chain set is conserved by
burn-before-mint:

- `sendMessage` is payable. A nonzero `msg.value` is **burned on the origin** as
  part of the send: the Outbox forwards it to a system burn primitive — a
  precompile at a reserved address (or an executor-special-cased transfer) that
  destroys the balance and increments the Outbox's per-destination
  `burned[dest]` ledger (BAL-visible, auditable). A plain send-to-dead-address is
  not enough: supply accounting should be first-class and provable.
- `value` sits inside the message leaf (§4), so the burn amount is covered by the
  commitment, the BAL cross-check, and B's own re-execution of the origin.
- On the destination, `execute_xchain_tx` applies the deposit mint semantics:
  pre-credit `value` to the aliased sender **before** the inner Inbox call.
  An inner revert does not roll back the mint (`execute_deposit_tx`'s posture) —
  the funds stay with the aliased sender, and the failure callback (§9) tells the
  origin app so it can react.
- **Value narrows the §10 choice.** A value-carrying pair may run posture A
  over DA (self-sufficient), or posture A with a quorum cross-check, or posture
  B at a higher threshold Q than message-only pairs — never `SignedStream`
  alone (one key deciding what was minted) and never `TrustFeed`.
  `on_unavailable` must be `stall` for these pairs. It is also the one place
  §10's accepted residual risk has real teeth: a catastrophic A-side rewind
  below its posted DA shows up on B as minted supply with no burn behind it.
- Per-pair conservation invariant, checkable by anyone with both chains' state:
  `A.Outbox.burned[B] == B.Inbox.minted[A] + value-in-flight`. The chaos suite
  should assert it across the origin-recovery drill. **The inbound fee (§12) is
  outside this accounting** — it is origin-local revenue, never burned and
  never minted — so `sendMessage` must split `msg.value` into fee and transfer
  amounts before either ledger moves. Conflating them would break the
  invariant in the direction that looks like inflation.
- Ships in P4 (§15), after messaging is proven. The burn-primitive mechanism
  (precompile vs executor special-case) is open (§16).

## 14. Work map (crates/components touched)

| Component | Change |
|---|---|
| `crates/types` | new `xchain.rs` (message + leaf incl. `value`, `derive_remote_epoch`, `remote_source_hash` domain 2, `alias_remote_address`, `RemoteEpochRecord`); `receipt.rs` `TX_TYPE_XCHAIN = 0x7D`; `tx_ordering.rs` `RemoteEpoch` variant |
| `contracts` | `Outbox.sol` (payable; burn ledger, `owed[dest]` fee ledger + withdraw), `Inbox.sol` (delivered/minted ledgers) + tests; genesis alloc entries in `chains/*.toml` |
| peer registry | v1: chain config with a named owner and a documented change process (tighten now / loosen after notice); v2: an L1 registry contract holding identity, posture, ordering signer, validator keys, quota, price and fee recipient (§12) |
| `crates/exec-core` | `execute_xchain_tx` (mint pre-credit), burn primitive (P4), cfg pinning test additions |
| `crates/engine` | reader `RemoteEpoch` handling + `RemoteEpochObserver` seam |
| `crates/validator` | `interop.rs` (extraction + BAL cross-check + feed store), jsonrpsee WS server (outbox + attestation subscriptions), per-block attestation signing (key + gossip), `RemoteEpochVerifier` (destination role), and **peer-validation mode**: run the validator against a peer chain (live streams or its DA via the reconstruct path) and serve a local interop feed |
| interop node | the multi-peer host (§10): one process running every peer's derivation + verification with a shared L1 view, DA reader, and ops surface — rather than one validator process per pair |
| `crates/da_watcher` | source-adapter refactor; `RemoteChainSource` (WS + poll fallback); attestation-quorum check against the local validator's state root |
| `crates/cluster-adapter` | `RT_REMOTE_EPOCH` + `remote_epoch_slots` |
| Java sealer | `onRemoteOriginRecord`, per-peer origin map, `RemoteOriginAdvance` |
| `crates/sequencer` | relay path for remote-epoch records (mirror of the deposit relay) |
| `crates/deployer` | predeploy wiring (no oracle change — the withdrawal output format is untouched) |
| `crates/e2e` | **two-chain Target-L harness** — spawn two full local stacks with distinct chain ids; scenarios: send/deliver, callback round-trip, gap injection (must halt), quorum-disagreement (pair halts, chain survives), quorum-unavailable (stalls, then resumes), quota overflow; P4: burn/mint conservation |
| chaos suite | peer-feed kill (messages stall, none lost), peer-validator-set partition (quorum unavailable → pair stalls), equivocating-peer drill (own validator disagrees with quorum → pair halts, alarm), conservation invariant across the origin-recovery drill |

## 15. Phasing

- **P0 — spec + skeleton**: this doc reviewed; `xchain.rs` types + contracts +
  destination execution + watcher/mock feed + two-chain e2e harness (harness
  work is the long pole and de-risks everything). *Landed: slices 1–3 on
  `feat/interop-xchain-p1`.*
- **P1 — one-way A→B, end to end**: sequencer relay + sealer per-peer origin
  advance, cursor persistence, validator extraction + outbox feed, and the
  **peer-validation mode in DA mode** (re-derive the peer from its L1 blobs via
  the reconstruct path) — the destination verifies the origin itself from day
  one rather than trusting a feed. Ships behind a peer-allowlist config. No
  callbacks, no value.
- **P2 — attestation quorum**: validator signing keys, the attestation feed and
  subscription, the quorum check in the watcher, `on_quorum_unavailable`
  policy, and the disagreement/partition chaos drills. Completes the §10
  posture. Callbacks land alongside (mechanically small — the same pipeline in
  reverse).
- **P3 — latency + hardening**: signed-stream ordering and live-mode peer
  validation (§16 Q3 + the stream-transport answer) to cut DA-cadence latency
  to block time; the multi-peer interop node; quotas; inbound fees and the
  prepaid callback leg; feed backfill from DA; blob transport for payloads;
  the L1 registry contract; chain-id unification.
- **P4 — value transfer**: burn/mint per §13, higher quorum thresholds for
  value-carrying pairs, conservation invariant in the chaos suite.
- **Later — ZK**: proofs replace both re-execution and attesters (§10), and are
  the only path to verifying a peer you do not follow.

## 16. Open questions

1. **Alias scheme**: hash-based (irreversible, collision-free across chains) vs
   OP-style reversible offset (inspectable but needs a per-chain offset
   registry). Implemented hash-based; revisit only if tooling demands
   reversibility.
2. **Validator set size and threshold policy** (§10): for pairs whose posture
   uses a quorum, what N does the origin run, what Q does the consumer require,
   and does a value-carrying pair demand a higher Q? Today's deployments run a
   single validator, so this is a prerequisite for those pairs, not a tuning
   knob — while posture A over DA needs none of it.
   **Decided 2026-08-16**: N, Q, and per-pair overrides are operator
   configuration — environment variables on the validator/watcher, never
   protocol constants. Recommended defaults N=3, Q=2; the registry records
   each pair's effective Q (value-carrying pairs higher).
3. **Sealer-signed canonical stream** (§10) — *the highest-value open item*.
   If the Aeron Cluster signed its committed stream, ordering authenticity
   becomes self-evident: posture A over a live link needs neither a trusted
   endpoint nor a quorum, and the matrix collapses to "run your own validator,
   verify the signature" at block-time latency with no added trust. OP has the
   worked precedent — a signer address published in an L1 contract, rotated
   with a dual-accept grace period — so the design is mostly transferable. Two
   variants to cost: a **single cluster key** (simple, matches OP) or
   **per-member signatures with a threshold** (strictly stronger than OP's
   single signer, and it mirrors the Raft quorum that actually produced the
   order). The cost is signing machinery in the Java sealer plus key
   management. Decide this before building the cross-check path, which exists
   largely to compensate for its absence.
   **Superseded 2026-08-16** by the egress-node decision (§5): the
   sequencer-owned egress validator signs the stream it serves; the sealer
   stays unexposed and unsigned. Threshold sealer signing remains a future
   strengthening if single-relay signer trust ever becomes the binding
   constraint.
4. **Inbound-price setting** (§12): who sets `inbound_price` — the destination
   (it is their revenue) with a notice period, or a pair agreement recorded at
   registration? And is a flat price per gas unit enough, or does a congested
   destination need to price dynamically (which would need a published curve
   senders can read before sending)?
   **Decided 2026-08-16**: each chain sets its own parameters — the
   destination owns `inbound_price` (and quota values) as operator env/registry
   config, subject to §12's notice period for repricing. The same rule covers
   the other parameter-shaped opens (feed retention, heartbeat cadence):
   per-chain operator configuration, never protocol constants.
5. **Fee denomination for independent operators** (§12): an origin-denominated
   fee is only worth what the origin's token is worth. For constellations with
   a shared token this is moot; for genuinely independent operators, is
   origin-currency revenue acceptable, or does the pair need settlement in
   something both value (which would pull L1 back into the loop)?
6. **Attestation cadence, transport, and keys** (§10): sign every block or
   every K; dedicated subscription vs riding the outbox feed; and whether the
   signing key can reuse the attester's existing key material or needs its own
   identity with independent rotation.
   **Decided 2026-08-16 (transport)**: attestations ride the egress node's
   feed server — a sibling subscription on the same WS endpoint, no dedicated
   infrastructure. Cadence stays per-block until measurement says otherwise;
   key identity/rotation still open.
7. **Cursor persistence** (found building slice 3): the watcher's per-pair
   resume position must come from the destination chain's own record — the
   `RemoteEpoch` markers on the canonical stream — not from watcher-local
   bookkeeping, which would fault on the first batch after a restart. Confirm
   the read path.
8. **DA representation of `RemoteEpoch` records** (found building slice 2): the
    **Decided 2026-08-16**: RemoteEpoch records are posted into the
    destination's own DA batches — every chain self-reconstructible with no
    dependency on any peer being alive. No chain is in production, so the
    batch-format change lands without migration machinery. Implementation:
    a batcher slice on the interop branch.
   batcher currently refuses them. Since messages travel by value, posting the
   records into the destination's own DA batches makes the destination
   self-reconstructible with no dependency on the origin — that is the design
   answer; the open part is encoding and size budget. Blocking for any
   interop-active chain that relies on DA recovery.
9. **Tail-block latency** (found building slice 3): per-origin-block batching
   means the last block stays open until a later one arrives, so a batch is
   only known complete when the next block starts. With finality gating gone
   (§10) this is now the latency floor; closing it wants an explicit
   end-of-block signal in the feed rather than inference.
10. **Live-mode stream transport** (§10, required for P3): the peer's canonical
    **Decided 2026-08-16**: WebSockets, served by the egress node (§5) —
    peers are expected to sit in other regions, and Aeron never leaves the
    sealed box. Networked Aeron and relay-service options are dropped.
   stream and `tx_bal` are Aeron IPC-scoped — networked Aeron channel configs
   vs a dedicated relay service for cross-operator consumption.

11. **Burn primitive** (§13): precompile at a reserved address (cleaner for
   tooling and inspection) vs executor-special-cased transfer (no new
   precompile surface). P4 decision.
12. **Blob transport for payloads** (§4): `(blob_ref, offset, len)` pointer
    format and retention interplay with DA — needed before high-volume pairs.
13. **Heartbeat / liveness marker**: should an idle peer advance a marker so a
    consumer can distinguish "no messages" from "feed dead"? (Ops alternative:
    metrics on the watcher's connection state and
    `REMOTE_WATCHER_TICK_TOTAL{outcome}`.)
14. **Return-data in callbacks**: hash-only (current) vs bounded bytes.
    Hash-only is safer; revisit with app feedback.
15. **Multi-destination broadcast** (same payload to N chains): N sends for
    now; a broadcast kind would complicate per-pair seq — defer until a real
    use case.
16. **Chain-id sources**: unify `Genesis.chain_id` and the ingress `chain_id`
    config before peers depend on them being equal.
17. **Feed retention N** and backfill SLOs — needs sizing against expected
    message rates before P1 ships.

## 17. Relation to OP Stack interop and LayerZero

Recorded because several decisions above were made by comparison, and the next
reader will ask.

**The framing.** This design is *not* the analogue of OP's interop; it is the
analogue of OP's **deposit** path, generalized so the origin is a peer rollup
instead of L1. OP interop and LayerZero both treat a cross-chain message as an
ordinary destination transaction that someone submits and pays for, whose
validity is checked against external evidence. We treat it as a transaction
the chain manufactures for itself. Everything else follows from that.

| | Kardamom | OP Superchain interop | LayerZero V2 |
|---|---|---|---|
| Message enters destination | protocol derivation (0x7D tx) | a relayer's tx calling `CrossL2Inbox` | an Executor calling `lzReceive` |
| Destination gas paid by | nobody (fee-free + quotas §12) | the relayer | source, via quoted fee |
| What is verified | the whole peer chain (posture A) or a quorum's agreement (posture B) | the whole peer chain (supernode re-derivation) | only the message (DVN attestation) |
| Ordering authenticity from | DA, a signed stream, or quorum agreement | L1 derivation (safe) / L1-registered signer key (unsafe) | not reconstructed at all |
| Origin history changes | impossible (§10) | block replacement / reorg | app-chosen confirmations |
| Membership | per-pair opt-in, per-pair posture | full mesh, governance vote | per-app config |
| Blast radius of a bad peer | one pair (fail-stop) | cluster-wide ("weakest link") | one app |
| Ordering | dense per-pair FIFO, gap = fault | none | per-pathway nonce, ordered *or* unordered |
| Callbacks | first-class, incl. failure, prepaid | none | none (`lzCompose` ≠ return trip) |
| Value | burn→mint + conservation invariant | burn/mint (ERC-7802) | burn/mint or lock/unlock (OFT) |

**Taken from OP.** Signed ordering with the signer identity published on-chain
and rotated behind a grace period (§10, §16 Q3) — their Unsafe Block Signer,
which is a better answer than trusting a stream endpoint. The one-process
multi-peer node shape (§10), which is what makes "run your own validator of
every peer" affordable. And their framing of cross-chain verification as
re-derivation rather than proof-checking, which is why §10 needs no Merkle
proofs.

**Deliberately not taken.** The safety ladder: it describes a chain whose own
blocks have several safety levels, and ours has one (§10). Block replacement:
having refused reorgs we must *prevent* equivocation rather than resolve it.
And the full-mesh dependency set: per-pair membership keeps the blast radius
of a compromised peer to one pair instead of the cluster, which is the single
biggest structural advantage this design has over theirs.

**What LayerZero clarifies.** Verify-the-message versus verify-the-chain is
the axis that governs scale: attesting a payload hash costs the same whether
the source chain is tiny or enormous, which is why they span a hundred
heterogeneous chains and we deliberately do not. It is also the reason posture
B exists here — for a high-throughput peer you barely talk to, re-deriving the
whole chain is a bad trade. Their per-app security configuration is the
opposite of ours in both directions: finer blast-radius isolation, worse
consistency, and a compromised app owner can install a malicious verifier set.
Their Executor and quote/refund machinery is exactly what we avoid by having
no relayer (§12).

**Where this design is distinctive.** Protocol-level callbacks including
failure delivery, prepaid on the origin. Derived injection, which removes the
relayer-liveness assumption both alternatives carry. A per-pair fault domain.
And a per-pair supply conservation invariant for value that neither
alternative specifies.

## Known gaps (audit 2026-09-03)

An audit of the interop protocol on 2026-09-03 found the items below open on
`main`. Each line gives the audit id and the PR that addresses it, or
"pending" when no PR exists yet.

- C1 — #260
- C2 — #260
- C3 — pending
- C4 — pending
- H1 — #255
- H2 — pending
- H3 — pending
- H4 — pending
- H5 — pending
- H6 — pending
- H7 — #259
- H8 — #258
- H9 — pending
- M1 — #258
- M2 — #260
- M3 — #260
- M4 — pending
- M5 — pending
- M6 — pending
- M7 — #258
- M8 — #259
- M9 — #258
- M10 — #258
