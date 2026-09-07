# Validator Network & the Egress Role — design spec

Status: DRAFT v2 — 2026-08-16 (v1's separate-binary design superseded by
operator decision: the egress node IS a validator, distinguished by config)
Companion: `interop-outbox-messaging-spec.md` (§5, §10).

## 1. The topology: sentries in both directions

```
                    OPERATOR CORE (never internet-connected)
        sealers ─ sequencers ─ executors ─ ingress node
              │  (Aeron planes)                ▲
              ▼                                │ allowlisted validator
   ┌─────────────────────────┐                 │ connections only (via CDN)
   │ EGRESS VALIDATOR        │                 │
   │ (operator-run, role     │─── signed ──► CDN ──► PUBLIC VALIDATOR TIER
   │  config: egress=on)     │    segments            (internet-facing)
   └─────────────────────────┘                        ├─ re-execute + verify
                                                      ├─ attest state roots
                                                      ├─ serve RPC (state,
                                                      │   receipts, eth_call)
                                                      ├─ serve outbox feed
                                                      └─ accept user txs →
                                                          forward to ingress
                                                              ▲
                                                            users, peer
                                                            chains' watchers
```

- **One binary.** There is no `kardamom-egress`; every node runs
  `kardamom-validator`. Roles are configuration: which subscriptions it
  serves, which keys it holds, what it connects to. Any validator can be
  promoted or demoted by config change — one artifact to build, sign, and
  (eventually) measure.
- **The public validator tier is the chain's face.** Validators are
  internet-exposed *by design*: they re-execute everything, so they are the
  natural servers of state RPC — `eth_call`, balances, nonces, receipts —
  the queries the ingress node's minimal namespace has always deferred. They
  provide the trust story ("the sequencer executed correctly") because each
  independently recomputes and attests state roots. They also *accept user
  transactions* and forward them to the core.
- **The egress validator** is the operator's own instance of the same
  binary. It is special in exactly three ways: it is maintained by the
  sequencer operator; it is the only internet-connected component with
  direct access to the internal Aeron planes; and it is the origin of the
  distribution tree — it recomputes state roots and streams outbox content
  + state roots outward, in signed per-block segments, into the CDN.
- **The ingress node is NOT publicly reachable.** It accepts connections
  only from a select, allowlisted set of validator nodes (via the CDN
  path). The DDoS and abuse surface for tx intake moves to the horizontally
  scalable validator tier; the core's intake has a short, named peer list.

## 2. What flows where

| Stream | Origin | Path | Consumers |
|---|---|---|---|
| Signed segments (canonical records, outbox messages, state roots) | egress validator | → CDN → validators | public validators (verify + re-serve), peer chains' sovereign watchers |
| Attestations `(chain_id, block, state_root, sig)` | every public validator, over its own key | validator RPC / feed | peer chains' quorum checks, snapshot certification |
| State RPC + outbox feed subscriptions | each public validator, from its own recomputed state | validator WS/HTTP | users, apps, peer watchers |
| User transactions | users → any public validator | validator → CDN → ingress allowlist | the core |

Consequences worth stating:

- **Quorum posture endpoints are just public validators.** A peer running
  the interop Quorum posture subscribes to ≥Q public validators' feeds and
  requires agreement — no dedicated infrastructure; the tier *is* the
  quorum. The egress validator's signature stays transport authenticity
  (the `ordering_signer` of the peer registry); it is deliberately not a
  quorum vote.
- **Relays stay trustless.** CDN nodes and any cache re-serve signed
  segments; consumers verify keys, not connections. Public validators
  additionally *verify content* by re-execution before re-serving derived
  surfaces — a validator that diverges from the stream it received
  fail-stops loudly rather than serving garbage.
- **Serving and verifying share a process now** (config-role model). That
  is the right failure coupling: a validator whose verification halts must
  stop serving — the tier's redundancy (many public validators) is what
  keeps the chain's face available, not process separation inside one node.

## 3. Keys per role

| Key | Held by | Meaning |
|---|---|---|
| attestation key | every public validator (its own) | one quorum vote: "I executed this and got this root" |
| `ordering_signer` segment key | egress validator only | transport authenticity of the distribution origin |

Role config determines which keys a node loads; both live behind
Vault/HSM custody per the hardening plan. Compromise asymmetry is
preserved from v1: owning the egress instance yields zero quorum votes;
owning validators short of Q yields detectable disagreement.

## 4. Interop mapping (spec §5/§10 unchanged in substance)

- The outbox feed a peer's watcher consumes is served by public validators
  (Quorum/feed postures) or re-derived by the peer's own validator-of-us
  fed from the CDN's signed segments (posture A, SignedStream — the
  segment signature answers ordering authenticity).
- The mock feed from the e2e suite remains protocol-faithful: it simulates
  a public validator's outbox subscription. E1 of the build plan becomes:
  the validator binary gains the serving surfaces (outbox feed + state-root
  stream + segment intake), gated by config.

## 5. Build phases (revised)

- **E1 — validator serving surfaces**: outbox extraction + feed store +
  WS server (`kardamom_subscribeOutbox`, `kardamom_subscribeAttestations`)
  in `kardamom-validator`, enabled by config; egress role adds segment
  signing + the internal-plane source. Acceptance: the S12/S13 scenarios
  pass with a real config-role validator replacing the mock feed.
  E1 serves attestations **unsigned** (`signature` absent). An unsigned
  attestation carries no authority: anyone who can reach the socket can
  produce one. A consumer must treat it as unusable for quorum,
  cross-check, or any other trust decision until E2 lands.
- **E2 — attestation quorum wiring** (interop P2): per-validator signing
  keys, `tx_attestations` intake on the egress role, quorum checks in
  peer watchers.
- **E3 — segment distribution + validator state RPC**: signed segment
  stream consumed by non-operator validators (CDN-ready), state RPC
  namespace (`eth_call`, balances, receipts) served from validator state,
  tx-forwarding path validator → ingress allowlist.

## 6. Open questions

1. Validator state RPC scope for E3 — full `eth_*` compatibility or the
   pragmatic subset apps need first (call/balance/nonce/receipts/logs)?
2. Ingress allowlist mechanics: static peer list in config vs registry-
   driven; and whether forwarding validators need their own identity keys
   for the ingress connection (SPIFFE mTLS per the hardening plan is the
   natural answer once SPIRE lands).
3. CDN concretely: plain HTTPS fan-out of segment objects vs WS relay
   chain; segment object format should be cache-friendly either way
   (content-addressed by `(chain_id, block)`).
