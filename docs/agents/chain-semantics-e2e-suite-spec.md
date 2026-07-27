# Chain-Semantics E2E Test Suite — Spec

- **Date:** 2026-07-25
- **Status:** **DELIVERED.** All five requested semantics have end-to-end coverage on both targets.
  - PR-1 #90 — Target-L harness + S3/S4/S5 + the `chain-semantics-e2e` workflow, with the #81 pending-registry fix it found.
  - PR-2/PR-3 #96 — S6/S7, `kardamom_state::integrity`, S9a/c, S1/S2 bridge round-trip, S9b crash recovery, plus the validator SIGTERM and withdrawal-attestation fixes.
  - PR-5 #100 — S8 DA parity, plus `docs/agents/l1-origin-deposit-derivation-spec.md`.
  - PR-4 #105 — Target C: the `semantics` shard in `cluster-e2e.yml`.
- **Where it runs:** `just test-e2e-local` and the per-PR `chain-semantics-e2e` job (Target L, ~45 s for 12 scenarios); the `semantics` shard of `cluster-e2e.yml` (Target C, ~19 min on a fresh 12-node cluster).
- **Goal (definition of done):** an e2e suite proving the rollup's **chain semantics** end-to-end — bridge round-trips against a real anvil L1, nonce-ordering guarantees over real JSON-RPC, validator↔executor state parity, batcher/DA↔validator root parity, and state-DB integrity — with each scenario written once and run on **two targets**: a fast deterministic single-host harness (`crates/e2e/tests/`, green in a dedicated CI job well under the cluster-e2e budget, plus a revived `just test-e2e-local`) and the **real CI cluster** (a new `semantics` shard in `cluster-e2e.yml` on the standard `ci-cluster.sh` DinD bring-up).

## Background / current state

The **chaos suite** (`deploy/cluster/scripts/chaos.sh`, `cluster-e2e.yml`, 5 DinD shards) answers "does the pipeline survive faults under load?". Nothing today answers "does the chain **mean** the right thing?" end-to-end:

- The functional pipeline e2e (`crates/e2e/tests/{full_pipeline_e2e,multiprocess_e2e}.rs` — including `anvil_pipeline_e2e_l1_deposit_and_l2_round_trip`) was **deleted in the #67 cluster-only migration** because it spawned the removed standalone Rust sealer. `justfile:246` (`test-e2e-local`) still points at those deleted targets — the local e2e entrypoint is broken. The `e2e` crate skeleton and all its dev-deps (anvil bindings, jsonrpsee, deployer/da-watcher/executor/ingress/sequencer/state) survived.
- Crate-level anvil tests are healthy but siloed: `deployer/tests/deploy_e2e.rs`, `batcher/tests/{anvil_e2e,reconstruct_l1_e2e,section6_conformance}.rs`, `validator/tests/withdrawal_e2e.rs`. None of them run the live pipeline.
- All nonce-ordering tests feed `Sequencer::run_once` directly with scripted refs; **no test drives out-of-order nonces over real JSON-RPC**, and no test asserts the client-visible behavior of a never-filled gap.
- `docs/failure-modes.md` records: *"Validator divergence injection — fail-stop is unit-tested, but no e2e case feeds the validator a corrupted receipt/BAL stream."*
- `kardamom-reconstruct --expect-root` exists as a root-parity gate but is wired into nothing.

### Coverage gap map (requested feature → today → gap)

| Requested semantic | Covered today | Missing |
|---|---|---|
| L1→L2→L1 bridge round-trip on anvil | Foundry `WithdrawalFlow.t.sol` (contracts only); `withdrawal_e2e.rs` (attester vs Rust root, finalize path `#[ignore]`d); deleted #67 deposit e2e | Whole-pipeline round-trip: deposit → live L2 credit → spend → withdraw → attest → finalize, with real da-watcher/sequencer/executor/validator |
| Unordered nonces accepted, gaps never processed, RPC never hangs | sequencer unit/proptests (`state_proptest.rs`, `sequencer_step.rs`, #87 wedge guards) | Same guarantees observed **through** `eth_sendRawTransaction`; bounded-latency ("no hang") assertions on every endpoint; gap-timeout error-code contract |
| Validator state == sequencer/executor state | live BAL/receipt cross-check in `validator/src/lib.rs`; `ci-cluster.sh:573` metrics verdict | Offline **deep table-level compare** of the two mdbx DBs; negative test proving detection fires (corrupt BAL/receipt injection) |
| Batcher state == validator state | `reconstruct_l1_e2e.rs` (self-contained oracle, not the live pipeline) | Live-pipeline DA parity: what the batcher posts to L1, re-executed, equals the validator's root |
| DB view correct, never corrupted | `state` crate unit tests (recovery, MVCC, shadow-check, genesis digest) | Cross-scenario invariant sweep; crash-mid-load → recover → still bit-consistent; corruption detectors proven non-vacuous |

## Design overview

**Target-agnostic scenario drivers, two execution targets.** Each scenario is written once as a driver library in `crates/e2e/src/scenarios/` that talks only to externally observable seams — ingress JSON-RPC, L1 RPC + contract addresses, per-service `/metrics`, and (where available) state dirs for offline checks. The drivers then run against:

- **Target L (local)** — a fast single-host harness: real binaries as child processes, real anvil L1, real Java Aeron Cluster sealer (1 member), one shared media driver. Seconds of bring-up; runs per-PR; supports the full assertion set including offline mdbx deep-compares, SIGKILL crash cases, and stream-level fault injection. This is the cluster-only successor of the deleted multiprocess e2e.
- **Target C (cluster)** — **the real e2e setup the rest of CI uses**: the 12-node DinD cluster brought up by `deploy/cluster/scripts/ci-cluster.sh` (the same path as every `cluster-e2e.yml` shard), with the 3-member Raft sealer, active/active ingress, UDP multicast side-streams, Nomad supervision, and the in-cluster anvil at `192.168.56.10:8546`. A new `semantics` shard runs the same drivers against it after the standard bring-up + smoke gate. This catches what a single-host IPC harness structurally cannot: real quorum ordering, lossy multicast BAL delivery, cross-node timing.

Target L exists because semantics bugs should fail in seconds on every PR; Target C exists because the semantics must hold on the topology we actually ship. Same driver code, different fixture — divergence between the two targets is itself signal.

```
                       ┌────────────────────────────── test process ──────────────────────────────┐
                       │  L2Client (jsonrpsee + local signers)   MetricsClient   StateReader(mdbx) │
                       └───────┬──────────────────────────────────────┬────────────────┬──────────┘
                               │ eth_*                                │ /metrics       │ offline
  anvil (L1)                   ▼                                      │                ▼
  ┌──────────────┐   ┌─────────────────┐   tx_data (ipc)   ┌──────────┴─────┐   state dirs (tmp)
  │ ETHLockbox   │   │ kardamom-ingress │ ───────────────▶ │ kardamom-      │   executor-state/
  │ OutputOracle │   └─────────────────┘                   │ sequencer ×M   │   validator-state/
  │ L2Settlement │        ▲    ▲                           └───────┬────────┘
  └──────────────┘        │    │ tx_receipts / tx_errors           │ cluster ingress (udp loopback)
     ▲      ▲             │    │ (ipc, shared media driver)        ▼
     │      │   deposits  │    │                        ┌────────────────────┐
     │      └──────────── │ ───┼─────── tx_deposits ────│ ClusterNode (JVM)  │ 1-member Raft
     │  kardamom-da-watcher    │                        │ (own driver+archive)│
     │                         │                        └───────┬────────────┘
     │  attester (in validator)│           cluster egress (udp) ▼
     └───────────────┬─────────┴──────────────┬───────────────────────────┐
                     │  kardamom-validator    │  kardamom-executor        │
                     │  (trie ShadowCheck 1)  │  (TrieMode::Off)          │
                     └────────────────────────┴───────────────────────────┘
```

Topology facts the harness builds on (verified against `dc801df`):

- Every service binary accepts `--aeron-dir`, `--config <channels.toml>`, `--metrics-addr`, `--cluster-egress-endpoint` — they can all share **one host-native `ArchivingMediaDriver`** (the `just aeron-driver-up` pattern, but per-test dirs) with `aeron:ipc?alias=<session>-<chan>` channels (`crates/e2e/src/pipeline.rs::channel_uri_for` already exists for exactly this).
- The Java sealer is an all-in-one member (`cluster/sealer-service/.../ClusterNode.java`: media driver + archive + consensus + service in one JVM) configured entirely by `-Dkardamom.cluster.members=id,ingress,consensus,log,catchup,archive[|…]` and `-Dkardamom.cluster.memberId`. Rust side needs only `[cluster] ingress_endpoints = "0=127.0.0.1:<port>"` + `--cluster-egress-endpoint 127.0.0.1:<port>`. **Default: 1 member** — Raft quorum of 1; semantics need canonical ordering, not fault tolerance (that's chaos's job). The July flakiness campaign already ran bare-JVM ClusterNodes locally, so no container is needed.
- L1 is plain `alloy_node_bindings::Anvil` with `--slots-in-an-epoch 1` (so `finalized` advances — the da-watcher polls the finalized tag; this was the deleted test's hard-won lesson) and the ERC-7955 predeploy + `kardamom-deployer` flow that four existing tests already use.
- L2 genesis: `chains/dev-withdrawals.toml` (the only alloc with the `L2ToL1MessagePasser` predeploy at `0x42…16`).

### Harness components (`crates/e2e/src/harness/`)

| Module | Responsibility |
|---|---|
| `l1.rs` | Spawn anvil (`--slots-in-an-epoch 1`, optional `block_time`); ERC-7955 predeploy; deploy factory + ETHLockbox + WithdrawalOutputOracle + KardamomL2Settlement via `kardamom-deployer` lib (oracle address predicted, lockbox wired in one batch — the `withdrawal_e2e.rs:86` pattern); **short finalization window** (e.g. 30 s); helpers `warp_past_window()`, `mine(n)` |
| `aeron.rs` | Spawn `io.aeron.archive.ArchivingMediaDriver` (aeron-all jar, version from the justfile constant) with per-test tmp `aeron-dir`/archive dirs and small (4 MiB) term buffers; readiness = `cnc.dat` + `archive.catalog` exist |
| `sealer.rs` | Spawn N∈{1,3} `ClusterNode` JVMs from `kardamom-cluster-node.jar` (located via `KARDAMOM_CLUSTER_JAR` env or `cluster/sealer-service/service/build/libs/`; CI builds it with `./gradlew :service:shadowJar`); readiness = `"cluster node up"` on stdout |
| `services.rs` | Spawn `kardamom-{ingress,sequencer×M,executor,validator,da-watcher}` with per-test state dirs, generated `channels.toml` (ipc aliases), auto-assigned metrics ports, stdout/stderr teed to per-test log files; `kill_hard(service)`, `shutdown_graceful()`; kill-on-drop. Binaries resolved from `CARGO_TARGET_DIR` (CI/`just` build them first — the deleted multiprocess test's mechanism) |
| `l2.rs` | jsonrpsee HTTP client + `ANVIL_MNEMONIC` signers (`crates/bench/src/{mnemonic,signers}.rs` reused); a **raw-submit** path that signs at arbitrary nonces for deliberate misordering; per-call latency capture |
| `metrics.rs` | Typed scraper over the per-service exporters (`kardamom_executor_block_number`, `validator_divergence_total`, `kardamom_ingress_queue_depth`, …) with `poll_until(deadline, pred)` — **bounded polls only, no fixed sleeps** (repo convention) |
| `statecheck.rs` | Offline mdbx reader: open a state dir via `StateEnvBuilder` + `StateSnapshot`, run the invariant sweep and cross-DB deep-compare (below) |

Determinism rules: unique tmp dirs + ipc aliases + ports per test (parallel-safe); every scenario ends with *drain → quiesce (executor block gauge stable) → assertions*; wall-clock bounds only as deadlines, never as synchronization.

### New product-side changes the suite needs (all small)

1. **`--pending-receipt-timeout-ms` on `kardamom-ingress`** — the 30 s default (`ingress/src/config.rs:69`) has no CLI/TOML override today; the gap-timeout scenarios need ~3–5 s to keep the suite fast, and the knob is operationally useful anyway.
2. **`kardamom_state::integrity` module** (+ thin `kardamom-statecheck` bin): the invariant sweep of S9 packaged for reuse (chaos and ops can adopt it later). Checks per DB: schema version; genesis digest; `headers` dense `0..=last_committed_block`; every `receipts` entry decodable; `tx_hash_index` ↔ `receipts` bijective; meta cursors consistent; for trie-enabled DBs `trie::rebuild_root == meta[state_root]`.
3. **Harness-side canonical-block collector** for S8: subscribe to cluster egress + tx_data in the test, assemble ordered `BlockFrame`s, and drive `batcher::{pack_blocks, l1::post_batch}` as libraries. Rationale: post-#67 the batcher *binary* still reads pre-cluster archive segment files (`--channel-b-segment`; `batcher.nomad.hcl` runs dry-run with placeholder paths — live posting was never rewired). Feeding the library from the live canonical stream is the honest test **now**; rewiring the batcher bin to cluster replay is flagged as a product follow-up, not smuggled into this suite.

## Scenario catalog

Feature-gated `full-pipeline-e2e` + `#[ignore]`, mirroring the existing convention. Each scenario is one `#[tokio::test]` on a fresh stack.

**S1 — `bridge_deposit_round_trip`** (anvil + full stack + da-watcher, `--poll-interval-secs 1`)
`depositETH(to=fresh L2 account F, 50 ETH)` on L1 → mine past finalized → assert: L2 receipt with `tx_hash == source_hash(l1_block_hash, log_index)`, `status=true`, `effective_gas_price=0`; then **F spends the deposited funds** (transfer to G) — behavioral proof the mint landed and is usable, since ingress has no `eth_getBalance`; offline `StateReader` confirms F/G balances and the `from`-aliasing rule. Mixed workload alongside (transfers + a small contract deploy from F) per the original test's "chain stays healthy under mixed load".

**S2 — `bridge_withdrawal_round_trip`** (same stack as S1; validator runs the attester: `--l1-rpc-url --output-oracle --attester-key`, `KARDAMOM_ATTESTER_POST_INTERVAL=1`)
F calls `initiateWithdrawal(target=L1 address T)` with value V → assert `MessagePassed` log in the L2 receipt → `poll_until` `OutputProposed` on the oracle with `output_root == keccak(0x00 ‖ state_root ‖ withdrawals_root)` recomputed test-side → `warp_past_window()` → test rebuilds the leaf set from its own L2 receipts (`kardamom_types::withdrawals::{withdrawal_leaf, withdrawal_proof}` — nothing in prod builds user proofs; that's a documented gap this suite pins) → `finalizeWithdrawal` → assert T's L1 balance += V, replay attempt reverts, `WithdrawalFinalized` emitted. Mitigates the known `withdrawal_e2e.rs` post-warp flake by polling `eth_getTransactionReceipt` manually instead of alloy's watcher.

**S3 — `nonces_unordered_all_land`**
K=8 senders × N=64 nonces each, **shuffled per sender** (seeded RNG), submitted concurrently over real JSON-RPC → every submit eventually returns its tx hash; per-sender executed order is dense ascending from 0 (via receipts + offline nonce check); `kardamom_sequencer_tx_dropped_past_total` and `pending_evictions_total` do not grow; drain completes.

**S4 — `nonce_gap_is_never_processed`** (ingress `--pending-receipt-timeout-ms 4000`, client timeout 8 s)
Sender submits nonces {0,1,2,4,5} — (a) 0–2 get receipts; 4,5 park; **other senders keep landing txs during the park** (gap isolation — the #87 no-wedge property observed end-to-end); (b) at ~4 s, 4 and 5 fail with `-32000` timeout, *not* a hang, *not* success; (c) executor applied-tx counter and offline state show nonces 4,5 were never executed and sender nonce == 3; (d) **late fill**: resubmit 3, then 4,5 → all three land, chain drains. Also the disorder variant: submit {5,3,1,0,2,4} in that wire order → all land (buffered reorder within capacity).

**S5 — `rpc_endpoints_never_hang`**
Under sustained background load (reuse `kardamom-load`'s plan/engine as a lib, modest tps), fire the adversarial matrix at every endpoint and assert **every call returns within its contract bound** with the right code: malformed RLP → `-32602` immediate; bad signature → `-32602`; past nonce → `-32602` (duplicate) within grace+ε; resubmit-of-landed → immediate success from receipt cache; unknown-hash `eth_getTransactionReceipt` → `null` fast; `eth_chainId`/`eth_blockNumber` p99 bounded throughout; `eth_getBalance`/`eth_getTransactionCount` → clean `-32603` error (documented deferral, not a hang); with `--rpc-max-connections 8`, the 9th concurrent submit is **refused at connect, promptly** — transport error, no indefinite park. Plus the **abort-cleanup regression test** (born as the #81 leak canary): abort M in-flight submits (drop the connections mid-park) and assert `kardamom_ingress_queue_depth` returns to baseline after the timeout horizon. It reproduced the #81 pending-registry leak end-to-end, drove the fix (PR #91, the Weak-indexed registry), and now runs unconditionally.

**S6 — `validator_matches_executor`** (validator with `--trie-shadow-check 1`)
Mixed workload (transfers, deploys, deposits) → drain + quiesce → live asserts: `validator_divergence_total == 0`, `validator_blocks_verified_total > 0`, `validator_bal_missing_total == 0` (all-IPC single host — no lossy hop), `validator_committed_block == kardamom_executor_block_number`, shadow checks `> 0` with `mismatch_total == 0`; no `"halted on divergence"` in the validator log → graceful shutdown → **offline deep-compare** of executor vs validator mdbx: byte-identical `accounts`, `storage`, `code`, `headers`, `receipts`, `tx_hash_index`, equal meta cursors (validator additionally has trie tables + `state_root`; executor is `TrieMode::Off` by design). This is the strongest form of "validator state always matches" — stream-level checks catch execution divergence, the table diff catches persistence divergence.

**S7 — `divergence_detection_is_not_vacuous`** (negative control for S6)
The test process publishes onto the real streams: (a) a corrupted `BlockDelta` on `tx_bal` for a committed block → validator must log `"validator divergence detected — halting"` and exit 2 within a bound; (b) fresh stack, corrupted `Receipt` (wrong `write_set_hash`) on `tx_receipts` at a valid `tx_idx` → same fail-stop. Closes the `docs/failure-modes.md` "divergence injection" gap. (Deliberately *semantics*, not chaos: it proves the guarantee "if states disagreed, we would know".)

**S8 — `da_parity_batcher_matches_validator`** (deposit-free workload — deposits are absent from the DA payload, a documented product gap this scenario makes impossible to forget)
Workload → drain → harness collector assembles ordered blocks → `pack_blocks` → `post_batch` as **real EIP-4844 blob txs** to anvil's settlement contract (KZG sidecars; `reconstruct_l1_e2e` proved anvil accepts them) → assert `BatchPosted` CAS sequence → run the real `kardamom-reconstruct` binary with `--expect-root <validator's meta[state_root] at head>` against a fresh state dir → exit 0. Proves: L1-posted data alone re-derives exactly the state the validator attests.

**S9 — `db_integrity_and_crash_consistency`**
(a) The `integrity` sweep runs on executor + validator DBs at the end of **every** scenario above (shared teardown hook). (b) Dedicated crash case: SIGKILL the executor mid-load → restart → `read_recovery_point` resumes without genesis re-sync → drain → sweep green + S6 deep-compare green. (c) Detector non-vacuity: restart executor against a mutated genesis TOML → refuses with `GenesisMismatch`; flip a byte in a `receipts` value on a *copy* of the DB → sweep reports `RkyvDecode`/`BadEncoding` (proving the sweep would catch real rot).

## Target C — running the drivers on the real CI cluster

Mechanics, reusing what `chaos.sh`/`ci-cluster.sh` already established:

- **Entry point:** a `kardamom-semantics` driver binary (thin CLI over the scenario libraries, like `kardamom-load` is over `load/`) invoked by `ci-cluster.sh` after the smoke gate, parameterized by `RPC_URL=192.168.56.31:8545`, `L1_RPC=…:8546`, `CHAIN_ID=412346`, contract addresses, and the metrics topology. Scenario selection via `SEMANTICS_CASES`, mirroring `CHAOS_CASES`.
- **Metrics:** scraped via `docker exec kardamom-<node> curl 127.0.0.1:<port>` exactly as `chaos.sh` and `kardamom-load --metrics-via-docker` do (executor `:9004`, validator `:9006` on aux).
- **Offline state checks (S6 deep-compare, S9 sweep):** quiesce load → `nomad alloc stop` the executor and validator allocs → `docker cp` their state dirs off the nodes → run the `statecheck` compare on the runner → restart allocs. Copying a stopped writer's mdbx is safe; this also doubles as a restart-recovery exercise on the real topology.
- **Account budget:** each scenario owns dedicated funded accounts starting at nonce 0, extending the existing ledger (`ci-cluster.sh:476` — smoke #0, load #1–6, chaos #7–15, churn #16–17; semantics takes a new contiguous block, sized in `genesis/dev.toml`).
- **Expected Target-C deltas, encoded in the drivers not skipped silently:** `validator_bal_missing_total` is `== 0` on Target L but only *bounded* on Target C (lossy multicast is by design); S7 stream injection and S9's SIGKILL are Target-L-only (Target C's fault surface belongs to chaos); S4's timing asserts use the deployed 30 s `pending_receipt_timeout` unless the ingress job adopts the new flag.

**Cluster wiring the semantics shard needs (today's CI never exercises the bridge):**

1. `ci-cluster.sh` currently deploys **no L1 contracts** — `deploy.sh` warns and hands the da-watcher a placeholder lockbox address (`deploy.sh:125-129`). Add a deploy phase after anvil is up: run `kardamom-deploy` (ensure-factory + lockbox/oracle/settlement, short finalization window) against `…:8546` from the orchestrator, export the addresses into the da-watcher/validator job vars. This makes CI's da-watcher watch a *real* lockbox for the first time.
2. `deploy/cluster/config/genesis/dev.toml` lacks the `L2ToL1MessagePasser` predeploy (only `chains/dev-withdrawals.toml` has it) — add it so S2 can run on Target C.
3. `validator.nomad.hcl` has no attester wiring — add optional `--l1-rpc-url/--output-oracle/--attester-key` vars (attester key = a dedicated anvil dev key, same convention as `withdrawal_e2e.rs`).

S8 on Target C stays deferred until batcher live posting is rewired for the cluster topology (the bin still expects pre-#67 archive segment files); the spec's harness-side collector is a Target-L construct.

## CI + local integration

- **Target L — new workflow `chain-semantics-e2e.yml`** (parallel to `docker-e2e.yml`, opt-out label `skip-chain-semantics`): ubuntu-latest; deps = cmake/uuid/libbsd/ssl (rusteron build), temurin JDK 17 + `./gradlew :service:shadowJar`, foundry `v1.7.1` (anvil + forge for the deployer build script), aeron-all jar (justfile-pinned version); `cargo build -p … --bins` then `cargo test -p e2e --features full-pipeline-e2e -- --ignored`. Budget: bring-up is seconds per scenario (vs ~30 min per DinD shard); whole suite target **< 20 min**. Shard into two jobs (bridge+DA / nonce+consistency) only if the single job crowds the budget.
- **Target C — a sixth `semantics` shard in the `cluster-e2e.yml` matrix**, alongside `load`/`chaos-*`: standard `ci-cluster.sh` bring-up + smoke gate, then `RUN_SEMANTICS=1 SEMANTICS_CASES="bridge nonce rpc consistency integrity"` runs the `kardamom-semantics` driver. Same 75-minute shard budget and failure-dump conventions as the existing shards.
- **`just test-e2e-local`**: repoint the currently-dangling recipe at the Target-L suite (it already handles the Java shim / JDK detection); add a `just cluster-jar` helper for the shadowJar. Target C runs locally via the existing `local-cluster.sh` path with the same env knobs.
- Chaos stays the resilience gate; this suite is the semantics gate. They share `ci-cluster.sh`, the metrics-probe conventions, and the funded-account ledger, but assert different things.

## Phasing / PR breakdown

1. **PR-1 — drivers + Target-L harness + nonce/RPC semantics (S3, S4, S5):** no L1 contracts needed (anvil not even required — da-watcher/attester off); includes the ingress timeout flag, the justfile revival, and the `chain-semantics-e2e.yml` workflow. Proves the driver/harness split early on the least plumbing.
2. **PR-2 — consistency + injection (S6, S7) + integrity sweep (S9a/c):** adds `kardamom_state::integrity` + `statecheck`.
3. **PR-3 — bridge round-trip (S1, S2) on Target L:** anvil + deployer wiring in the harness; attester enabled.
4. **PR-4 — Target C:** `kardamom-semantics` CLI, the `semantics` shard in `cluster-e2e.yml`, and the cluster wiring (contract-deploy phase in `ci-cluster.sh`, message-passer predeploy in the cluster genesis, attester vars in `validator.nomad.hcl`). Runs S1–S6 + S9a against the real CI cluster.
5. **PR-5 — DA parity (S8) + crash case (S9b) on Target L:** harness canonical collector; wires `kardamom-reconstruct --expect-root` into CI for the first time.

Each PR lands with its scenarios green in the relevant workflow, gated on the full check set per repo convention. PR-4 is validated with a full local-cluster run before merge (per the standing local-validation directive).

## Findings (bugs the suite surfaced)

1. **#81 pending-registry leak** — reproduced by S5's queue-depth probe, fixed in #90 (Weak-indexed registry, waiter-owned entries, `Drop` as the single removal site). The probe now runs unconditionally as a regression test.
2. **`eth_chainId` served the compiled-in default `1`** everywhere, including the deployed 412346 cluster — there was no chain-id flag at all. Added in #90 and wired into `ingress.nomad.hcl`.
3. **The validator did not exit on SIGTERM** — found by S6's graceful-shutdown phase, **fixed in PR-2**. It survived 90 s+ of a single SIGTERM even with a healthy pipeline and a live sealer, while the executor — same `select!`/`drop(rt)`/`drop(cluster_guard)`/`join` shape — exited immediately, so Nomad SIGKILLed the validator on every stop/deploy. Root cause: an **ownership cycle**. `TxReceiptsSubscriberHandle` carries an `AeronRuntime` clone (for MDS destination churn), and the validator moved the whole handle into its receipts pump task; the runtime shuts down only when its last clone drops, that shutdown is what makes `recv()` return `None`, and the pump held the clone preventing it — so `drop(rt)` in `main` was a no-op, the engine's tx_data subscriptions never closed, and `join.await` never returned. Fix: `TxReceiptsSubscriberHandle::into_receiver()`, used by the validator and pre-emptively by the ingress (same shape, masked only because its `main` returns without joining). S6's shutdown now asserts a clean exit within 20 s, which is the regression test.
4. **Withdrawals could never be attested — the L2→L1 bridge was inert** (found by S2, **fixed in PR-3**). The attester collected leaves from the committed `BlockDelta`, but the engine finalizes every delta with an empty receipts vec (receipts travel on tx_receipts), so `collect_withdrawal_leaves` always returned nothing: every output posted `leaves=0`, and no withdrawal could be proven or finalized on L1. Its unit tests passed because they feed a delta *with* receipts — a shape the live pipeline never produces, which is exactly the class of bug only a full-stack test catches. Fixed by `AttestingReceiptSink` (tees leaves off the receipt stream, flushes per block boundary). Follow-on, still open: the persisted `receipts`/`tx_hash_index` tables are empty for the same reason, so the documented state-DB receipt read path cannot work — see `docs/failure-modes.md`.
5. **Crash recovery is impossible without the durability archive** (found by S9b). A restarted executor replays canonical records from its persisted cursor, but the tx_data envelopes for those records were published while it was down — a fresh Aeron subscription cannot see them, so the join-miss refetch is the only source, and its resume cursor (`last_fsynced_b_position`) only advances when a recorder publishes the fsync watermark. Without `--archive-durability` on the ingress plus the executor's two refetch endpoints, a restart over-replays and aborts loudly ("archive refetch exhausted") — the #84/#89 fail-loudly design working as intended. The harness gained `StackConfig::archive_durability` (opt-in: it adds a recorder-startup barrier) and S9b runs with it, so the suite now exercises #89's recovery machinery at Target L, not only through DinD chaos.
6. **`ETHLockbox` is an escrow, not a mint.** A withdrawal only finalizes if the bridge holds ETH from a prior deposit — S2 deposits first, which is what makes it a true L1→L2→L1 round-trip rather than two half-tests.
7. **Every state DB stores a `meta[state_root]`, including executor DBs** — `seed_genesis` writes it unconditionally, so a plain-writer (`TrieMode::Off`) DB carries the *genesis* root frozen at block 0 while its chain advances. Not a bug, but it invalidates the natural reading of "executor DBs have no root" (as the earlier survey concluded); S6 pins the real asymmetry instead (the two roots must differ), and `integrity::sweep` documents that its rebuild check validates the genesis-era trie on executor DBs.

## Open questions

1. ~~**S5 leak canary**~~ — resolved: shipped env-gated in PR-1, reproduced the leak, the fix landed as PR #91 (Weak-indexed pending registry), and the gate was removed — it now runs unconditionally.
2. **Sequencer cold-floor (F02.1)** — a sequencer restarted mid-run hydrates every sender at nonce 0 (`EmptyStateDatabase`) and wedges established senders. Worth a pinned `#[ignore]` scenario here, or leave it to the recovery roadmap where the fix will land? **Still open.**
3. ~~**Target C shard cost**~~ — the shard runs on every PR like the others. It is the cheapest of the six (~19 min vs 27–40 min) because it runs no load and no chaos. Revisit if PR latency becomes the binding constraint; the gate is one line (`RUN_SEMANTICS`).
4. ~~**Cluster genesis change**~~ — deferred with the bridge cases. Target C ships without any deployed-job, genesis or contract-deploy change, so it has no blast radius on the other five shards; S1/S2 on Target C need that wiring and will carry the genesis-digest change (fresh clusters only) when they land.

(Resolved by the two-target design: the local sealer stays 1-member — Target C exercises the real 3-member quorum.)

## What Target C covers today, and what it does not

Runs on the cluster: `nonce-unordered`, `nonce-gap`, `rpc-liveness`, `consistency` — via `kardamom-semantics`, a thin CLI over the same drivers, invoked by a `RUN_SEMANTICS=1` block in `ci-cluster.sh`.

Deliberately Target-L only:

- **S1/S2 (bridge)** — need the contract-deploy phase, the message-passer predeploy and attester vars in the *shared* bring-up path (see open question 4).
- **S7 (divergence injection)** and **S9b (crash recovery)** — drive process signals and raw Aeron publications from the test process, which is not reachable from outside the cluster. Fault injection on the cluster is chaos's domain.
- **S8 (DA parity)** — blocked on the batcher's live posting being rewired for the cluster topology; its binary still expects pre-#67 archive segment files.
- **S6's offline phase** and the **queue-depth probe** — the former needs the state dirs (`docker cp` from stopped allocs), the latter needs ingress metrics, which bind loopback-only in the deployed job.

A simplification worth recording: executor, sequencer and validator all bind `/metrics` on `0.0.0.0`, so the runner scrapes them directly and the `SocketAddr`-based `Target` needed no changes. The docker-exec transport this spec originally assumed was never necessary.
