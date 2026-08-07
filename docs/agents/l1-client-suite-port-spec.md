# L1-Client Test-Suite Port — EEST Conformance + Cluster-CI Enrichment — Spec

- **Date:** 2026-08-04
- **Status:** PROPOSED.
- **Goal (definition of done):** kardamom's CI adopts the test-suite layers and harness patterns that L1 Ethereum execution clients (reth, geth, nethermind, besu, erigon) have converged on, wherever they map onto our architecture. Concretely: (1) the EEST state-test corpus (O(10⁴) EVM cases) runs in-process against `kardamom-engine` on every PR, pinned to **the latest Ethereum hardfork only**, and every gas/execution-environment parameter the engine currently leaves at a silent revm default is set (or test-pinned) deliberately; (2) the RPC surface, the cluster-adapter wire framing, and the fresh-join paths of every role get conformance suites modeled on hive's `rpc-compat`, `engine`, and `sync` simulators; (3) the cluster suite's bash verdict plumbing is replaced by a continuous invariant checker (assertoor pattern), killing the vacuity class from the 2026-08-03 chaos-coverage audit; (4) a fuzz workload (spamoor pattern) and a consume-matrix root-convergence gate wire our three independent implementations of the same semantics — executor, validator, `kardamom-reconstruct` — together as differential oracles.
- **Non-goals:** adopting hive itself (our Nomad/DinD harness already plays that role and knows our topology); `blockchain_test`/`blockchain_test_engine` fixture formats (we have no header chain, block RLP, or Engine API); devp2p (no p2p); kurtosis multi-client devnets (single-implementation chain — the validator *is* our second client); historical forks or fork-transition testing (policy below).

## Background: what L1 clients actually run, and where we stand

Survey of reth/geth/nethermind/besu/erigon CI plus the shared harnesses (verified Aug 2026; sources at the end). The ecosystem splits testing into two layers:

1. **Semantic conformance** — fixture-driven, in-process, minutes. `execution-spec-tests` (EEST — merged into `ethereum/execution-specs` Nov 2025, "the Weld"): Python fillers → static JSON fixtures → delivered through *multiple mechanisms* against the same semantics (`consume direct` via a client CLI, RLP import at startup, live `engine_newPayload`, batched EngineX, two-node sync) — deliberately hitting different client code paths. Fixtures ship as release tarballs (`tests@vX.Y.Z`, X = mainnet fork number). Every client consumes them: geth `evm statetest`/`TestExecutionSpecState`, nethermind `nethtest`, reth `testing/ef-tests`.
2. **System integration** — containerized, sharded, expensive. hive (docker orchestrator; simulators: `engine`, `rpc-compat` with `.io` request/response vectors, `sync`, `devp2p`, `eels/consume-*`), kurtosis `ethereum-package` + assertoor (devnets + YAML invariant playbooks: finality, block proposals, tx inclusion, reorg detection), spamoor (25+ tx load/fuzz scenarios), erigon's `qa-*` long-running sync jobs, reth's *nightly* hive with `expected_failures.yaml` diffing.

**Kardamom is inverted relative to every one of these clients.** Our system-integration layer (7 DinD shards, 24 chaos cases, DeFi load, validator-as-oracle, per-case convergence asserts) is stronger than most clients' in-repo CI. Our semantic-conformance layer is **three test cases**: `crates/executor/tests/diff_reference.rs`'s v0 corpus is transfers + one `SSTORE` + one revert, with a literal `TODO(S4 v1): import a mainnet-style tx corpus`. We execute with stock revm against mainnet semantics — the canonical corpus for exactly that engine exists, is free, and runs in minutes.

### Portability map

| L1 suite / harness | Layer | Portable? | Kardamom move |
|---|---|---|---|
| EEST `state_test` fixtures | single-tx EVM state transition | **Yes, directly** — we run revm mainnet | W1: in-process `consume direct` runner against `kardamom-engine` |
| EEST `blockchain_test` / `_engine` / hive `consensus` | block RLP import, Engine API delivery | No — no header chain, no engine API | skip |
| hive `rpc-compat` (`.io` vectors) | RPC surface | **Yes (pattern)** | W2: golden vectors for the v0 subset, both targets |
| hive `engine` suite | consensus-interface contract, valid+invalid payloads | Analog — our "engine API" is the cluster-adapter wire (`wire.rs` kinds 0–4 / 1–5) | W8: shared Java↔Rust wire-conformance fixtures + adversarial frames |
| hive `sync`, kurtosis-sync-test | fresh node syncs from peers | **Yes (pattern)** — analog is late-join/catch-up | W6: `chaos-join` shard for the roles with zero join coverage |
| devp2p suites | wire protocol | No p2p; Aeron archive already chaos-covered | skip |
| assertoor playbooks | continuous invariants on a live net | **Yes (pattern)** | W4: `kardamom-checker` replacing bash verdicts |
| spamoor / tx-fuzz | tx-space fuzzing under load | **Yes (pattern)** | W7: `--workload fuzz` with validator/reconstruct as oracles |
| reth `e2e-test-utils` testsuite Actions, besu acceptance DSL | scenario-code cluster e2e with injectable actions | Medium | W11 (later): unify fault injection across Target L / Target C |
| erigon `qa-*`, reth nightly hive | long-running, off-PR-path | **Yes (pattern)** | W10: `cluster-nightly.yml` |
| hive reporting: `expected_failures.yaml`, JSON results, hiveview | CI ergonomics | **Yes** | W1 (xfail list), W9 (verdict JSON + expected-failures for shards) |

### Existing gaps this plan closes (from `docs/reviews/2026-08-03-chaos-coverage-audit.md` and `docs/failure-modes.md`)

- Audit A-2/A-3/A-4/A-5: sequencer / batcher / da-watcher / ingress-`tx_data` have **zero** empty-state-join or restart chaos coverage → W6.
- Audit B-5: no deliberate freeze past the 90 s cluster session timeout (plus the wrong "15s" comment at `chaos.sh:1614`) → W6.
- Audit C-2: `kardamom-statecheck` and `kardamom-reconstruct` are built by `cluster-e2e.yml` but invoked by no cluster script → W5.
- Audit D-1/D-2/D-7/D-8 (vacuity: scrape-failure-as-zero, `|| true` metric reads, the semantics shard's weak verdict, must-deliver downgraded to a warning) → W4.
- Audit D-11 (blast radius fixed at shard 0 / replica A / node 0) → W10.
- `diff_reference.rs` `TODO(S4 v1)` mainnet-style corpus → W1.
- Java sealer tests (~2,250 lines: `SealerClusterFailoverTest`, `SealerFanoutTest`, `SealerReplayTest`, `SnapshotRestoreTest`, `CanonicalSealerStateTest`, `ClusterNodeTest`) are executed by **no workflow** — both Gradle-touching workflows run only `:service:shadowJar` → W3.
- `SealerClusteredService.java:42` `TODO(envelope)`: Rust/Java byte framing hand-kept in lockstep with no shared schema → W8.

## Hardfork policy: latest fork only

Kardamom has no fork schedule and no fork-transition machinery: the chain is born at genesis on current-mainnet EVM semantics and stays there. Conformance therefore pins **exactly one fork** — no multi-fork CI matrix, no historical-fork fixtures, no transition tests.

- **The pin is explicit, in one place.** Today `ExecEnv::cfg_env()` (`crates/engine/src/block_env.rs`) builds `CfgEnv::default()` with only `chain_id` set, silently riding revm's default `SpecId` (currently `OSAKA` in revm 38). That is a latent drift risk: a revm upgrade can change execution semantics with no diff in our tree. W1 makes the pin explicit: `SPEC_ID: SpecId = SpecId::OSAKA` as a named constant in the engine crate, set on `CfgEnv`, referenced by the conformance runner. The pin extends beyond `SpecId` to **every gas/env parameter** — W1b inventories the fields we currently leave at silent defaults and makes each one a recorded decision.
- **Fixture pin follows the fork pin.** One pinned release tag of the `ethereum/execution-specs` fixture tarball (current train: `tests@v20.x` — "Osaka + BPO1 + BPO2"; BPO forks are blob-parameter-only and change no EVM semantics, so Osaka is the execution pin). The runner selects only `post` entries keyed by the pinned fork name and skips everything else.
- **Fork upgrade = one PR**: bump the revm `SpecId` pin, bump the fixture tag, regenerate the expected-failures list, green the suite. Optionally (W10) a non-blocking nightly job consumes the *next* fork's devnet fixture line (`tests-<devnet>@vN`) as early warning.

## Work items

### W1 — EEST state-test conformance runner (`consume direct` analog)

**What:** `crates/engine/tests/eest_state.rs` — an integration test (`#[ignore]`, env-gated on `KARDAMOM_EEST_FIXTURES=<dir>`) that walks the `state_tests/` tree of the pinned fixture tarball and, for each fixture × pinned-fork post entry:

1. Materialize `pre` into the in-memory DB the engine already abstracts over (`CacheDB<SnapshotDb<_>>`).
2. Build block/tx env **from the fixture's `env` and `transaction`**, not from `ExecEnv` — production choices (`basefee = 0`, `prevrandao = 0`, 30M gas limit) must not leak in; this tests the engine's revm integration, not boundary derivation. `Database::block_hash` gets a stub per the t8n convention; fixture families that need real ancestor hashes go on the exclusion list.
3. Execute through the same `ExecScope` path the executor and validator share.
4. Assert: post-state root (materialize the post alloc, compute the MPT root with the same `alloy_trie` primitives `kardamom_state::trie` uses — a pure `alloc_root()` helper, no mdbx) and the logs hash (`keccak(rlp(logs))`) against `post.hash` / `post.logs`.

**Expected failures, reth-style:** `crates/engine/tests/eest_expected_failures.yaml` — `{test_id, reason}` entries for legitimate v0 divergences (selfdestruct is explicitly out of scope, `crates/engine/src/delta.rs:12`; anything else revm-config-shaped we discover). The runner fails on unexpected *failures* **and** unexpected *passes*, so stale entries surface — this list is also our precise, versioned statement of "where kardamom's EVM deviates from mainnet", which we currently have nowhere.

**CI:** new `eest-conformance` job in `ci.yml`: download the pinned tarball (actions/cache keyed by release tag), `cargo test -p kardamom-engine --release --test eest_state -- --ignored`. Budget ≤ 10 min (geth/nethermind run comparable sets in minutes). Local entry: `just eest` (fetches to `~/.cache/kardamom/eest/<tag>`).

**v1 (nightly, optional):** a `--persistence` mode running a sampled subset through a real temp-mdbx `StateDatabase` + writer + incremental trie with shadow check — thousands of adversarial state shapes against the trie code, which today only sees workload-shaped state.

Subsumes the `diff_reference.rs` TODO. The 3-case differential stays (it cross-checks our executor loop, not just revm); the corpus question is answered by EEST.

### W1b — explicit gas/env parameter pinning (no more silent revm defaults)

`ExecEnv::block_env()` ends in `..Default::default()` and `cfg_env()` sets only `chain_id` — every other execution parameter rides whatever revm 38 defaults to. Audit of what that means **today** (revm-context 16.0.1 / revm-primitives 23.0.0):

| Field | Silent default | Live consequence | Decision to record |
|---|---|---|---|
| `BlockEnv.beneficiary` | `Address::ZERO` | With `basefee = 0`, the **entire** `gas_price × gas_used` of every legacy tx is a priority fee credited to the zero address — fee ETH accumulates in `address(0)`. Nobody chose this economic behavior | Keep as documented burn-to-zero, or introduce a fee-vault address; either way set explicitly |
| `BlockEnv.difficulty` | `0` | Fine post-merge (`DIFFICULTY` returns prevrandao) | Set `0` explicitly with a comment |
| `BlockEnv.blob_excess_gas_and_price` | `Some(new(0, BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE))` | A **Prague** constant under an Osaka pin. Numerically benign only because excess=0 → blob gasprice 1 regardless of fraction; `BLOBBASEFEE` returns 1 | Set explicitly per the blob policy below |
| `BlockEnv.slot_num` | `0` | EIP-7843 — Amsterdam (next fork), inert at Osaka | Add to the fork-bump checklist |
| `CfgEnv.tx_gas_limit_cap` | `None` → spec default; for OSAKA that is EIP-7825's **16,777,216 (2^24)** (`revm_primitives::eip7825::TX_GAS_LIMIT_CAP`) | **Already enforced in production**: any tx with gas limit > 2^24 is invalid → total-derivation turns it into a `status=false` skip receipt, while our block gas limit is 30M. An unchosen, undocumented, silently-applied cap | Adopt the cap explicitly (mainnet parity), **and** reject it at ingress with a clear error instead of a burned skip receipt; add an RPC vector (W2) and confirm EEST's 7825 cases pass (W1) |
| `CfgEnv.max_blobs_per_tx` | `None` → per the revm doc, "the check for max blobs will be **skipped**" | A type-3 tx that reached the engine would skip blob-count validation and charge blob gas at gasprice 1 — and we have no blob sidecar transport at all | Blob policy: ingress rejects type-3 envelopes outright (`-32602`-class, RPC vector in W2); engine sets `max_blobs_per_tx = Some(0)` as defense in depth (a type-3 that somehow reaches the canonical stream becomes a deterministic invalid-skip) |
| `CfgEnv.blob_base_fee_update_fraction` | `None` → per-fork constant | Only reachable via `BLOBBASEFEE` given the policy above | Set explicitly alongside the blob policy |
| `CfgEnv.limit_contract_code_size` / `limit_contract_initcode_size` | `None` → spec-derived (EIP-170 24,576 / EIP-3860 49,152 at Osaka) | Correct mainnet parity | Leave spec-derived; **pin via test** (below) |
| `CfgEnv.gas_params` (per-opcode cost table) | Derived from `SpecId` | An opcode-repricing fork or revm bump changes costs with no diff in our tree | Leave spec-derived; pin a golden hash of the 256-entry table via test |
| `memory_limit`, `disable_*` flags, `tx_chain_id_check` | Feature-gated defaults | Sane | Assert in the pinning test |

**Mechanics:** `BlockEnv` is a plain struct (not `#[non_exhaustive]`) — `block_env()` drops `..Default::default()` and constructs **every field literally**, so a revm upgrade that adds a field becomes a compile error, i.e. a forced decision. `CfgEnv` *is* `#[non_exhaustive]`, so it stays field-by-field assignment plus a `cfg_pinning` unit test asserting every effective value — including the spec-derived ones (code-size limits, tx cap, gas-table hash). Same pattern the repo already uses to pin Grafana dashboards (`crates/obs/tests/dashboards.rs`). The W1 EEST runner is the semantic backstop: fixtures set these fields per-test, and any divergence between our pinned values and mainnet behavior surfaces as an expected-failures entry that has to be justified in review.

Ships in PR-1 with W1 — the EEST runner is what proves the pinned values are the mainnet ones.

### W2 — RPC golden vectors (hive `rpc-compat` analog)

**What:** `crates/e2e/vectors/rpc/*.io` — hive-style request/response exchanges with wildcard matchers for volatile fields (hashes, timings). Because our surface is tiny, hand-rolled, and deliberately non-standard, golden vectors are the cheapest way to freeze the contract:

- `eth_chainId`, `eth_blockNumber` shapes; `eth_sendRawTransaction` happy path.
- Error contract: malformed RLP → `-32602`; bad signature → `-32602`; gap timeout → `-32000`; `eth_getBalance`/`eth_getTransactionCount` → `-32603` with the exact "deferred to S6 state writer" message; unknown-hash receipt → `null`.
- W1b policy surface: gas limit above the EIP-7825 cap → prompt ingress rejection (not a burned skip receipt); type-3 (4844) envelope → rejected.
- Receipt JSON shape including `blockHash: null` (the v0 no-state-commitment choice) and the deposit-type receipt envelope.
- `kardamom_sendRawTransactionAsync` ack shape; `kardamom_subscribeReceipts` event variants (`Receipt`/`TxError`/`Lagged`).

**Runner:** a driver in `crates/e2e/src/scenarios/rpc_vectors.rs` (same target-agnostic pattern as the existing suite) → a Target-L test and a new `rpc-vectors` case in `kardamom-semantics` (Target C). When the S6 state writer lands `eth_getBalance`/`eth_getTransactionCount`, their vectors replace the error vectors in the same corpus.

### W3 — run the Java sealer tests in CI

Add `./gradlew :service:test --no-daemon` as a job in `ci.yml` (JDK 17 setup already exists in two workflows to copy from). ~6 test classes covering failover, replay, snapshot restore, fanout, and the contiguity guard run today only on developer machines. Few minutes; zero design.

### W4 — `kardamom-checker`: continuous invariant checker (assertoor analog)

**What:** a small Rust binary (new `crates/checker` or a bin in `crates/bench`) that runs for the *entire* shard duration — started after smoke, verdicted at teardown — scraping all endpoints on a 5 s interval and evaluating invariants continuously instead of at end-of-run:

- **Hard invariants** (any confirmed nonzero read fails the shard, ever): `validator_divergence_total`, `kardamom_state_trie_shadow_mismatch_total`, "halted on divergence" would-be conditions.
- **Convergence invariants**: every executor individually scrapeable and within `EXEC_CONVERGE_LAG` of the fleet head (the existing `assert_executors_converged` logic, evaluated continuously).
- **Liveness invariants** with SLO windows: executor head advances; validator lag bounded (load shards); `lastBatchIndex` advances and `BatchPosted` indices stay dense (via the in-cluster anvil, absorbing the Target-C `l1-batch` checks).
- **Scrape failure is failure**, not zero: each endpoint gets a sustained-unavailability budget; transient gaps during injection are tolerated via chaos.sh signalling its injection windows to the checker (a window file or local HTTP endpoint), outside which sustained blackout fails the shard.

**Verdict:** a `checker-verdict.json` consumed by `ci-cluster.sh` in place of the ~150 lines of bash/curl metric plumbing (`val_metric`, the per-shard validator verdict). This *structurally* removes audit findings D-1 (stalled-assert vacuous on scrape blackout), D-2 (`|| true` metric reads), D-7 (semantics shard's weak verdict — the checker runs identically on every shard), and D-8 (must-deliver warning downgrade becomes a checker failure).

### W5 — consume-matrix root convergence (EEST multi-delivery insight, applied to our semantics)

EEST's core trick is delivering the same semantics through every consumption path a client has. Ours are: live egress execution (executor), independent re-execution (validator), and rebuild-from-L1 (`kardamom-reconstruct`). Today no cluster run ever cross-checks them at the state level (audit C-2).

**What:** a post-run step on the `semantics` shard (no new shard; reuses the workload the semantics cases already generated):

1. Gracefully stop one executor and the validator (`nomad alloc stop`; clean SIGTERM shutdown is already proven by the suite).
2. Copy both state dirs out of the DinD nodes (`docker cp` via the node's inner dockerd; CI-run DBs are small).
3. `kardamom-statecheck` sweep on each + `--compare` executor↔validator (byte-level table parity).
4. `kardamom-reconstruct --l1-rpc <in-cluster anvil> --settlement <proxy> --da-store <copied from batcher node> --expect-root <validator's root, computed by statecheck>` — exit 0, plus the S8-style non-vacuity control: a wrong `--expect-root` must be rejected.

This is also the first time DA-blob rebuildability is proven **on the real cluster topology** rather than Target L.

### W6 — `chaos-join` shard (hive `sync` analog)

New shard exercising fresh-join/restart for every role that has none. Case budget (≤ 9 funded accounts) — six cases:

| Case | Mechanism | Assertions |
|---|---|---|
| `sequencer-empty-join` | kill + restart replica B of shard 0 under load **pinned to shard 0** | replica converges to publishing for established senders (receipt-floor hydration actually completes — audit A-2 notes a restarted replica seeds floors at 0 and may never recover a quiet sender; `assert_replica_healthy` today only counts metric lines); `resync_entered_total` advances |
| `batcher-restart` | hard-kill mid-posting streak | resumes from the L1-as-truth cursor; no CAS double-post revert in anvil logs; `BatchPosted` indices stay dense (checker) |
| `batcher-cold-join` | kill + wipe any local batcher scratch | re-derives cursor purely from L1 `lastBatchIndex`; skips covered blocks; posting resumes |
| `da-watcher-restart` | hard-kill during a deposit-bearing epoch stream | epochs resume; no duplicate deposit lands (downstream `source_hash` dedup holds — observed via executor progress + zero validator divergence); at-least-once within run confirmed |
| `ingress-txdata-latejoin` | restart ingress-0 under load, then route new submissions to it | late-join on `tx_data` (#31, referenced nowhere in the repo today): receipts for pre-restart txs still retrievable from the peer; post-restart submissions on the newborn land |
| `session-timeout-freeze` | SIGSTOP an executor for **>90 s but within the retention window** (verified freeze, as `validator-lapse` does) | session re-establishment + gapless replay, *without* entering the retention-overrun checkpoint path; closes audit B-5. Also fix the wrong "15s session timeout" comment at `chaos.sh:1614` (configured value: 90 s, `ClusterNode.java:175`) |

Cost: one new `cluster-e2e.yml` matrix entry (~30 min bring-up + ~20 min cases; within the 75 min budget).

### W7 — fuzz workload (spamoor analog)

**What:** `--workload fuzz --seed <n>` in `kardamom-load`, structured like spamoor's scenario plugins over the funded-wallet pool: random calldata against echo/gas-burner contracts, deploys of bounded random-but-valid bytecode + immediate calls, storage-bloat writers, typed-envelope mix (0x00/0x01/0x02/0x03), value/gas edge cases, nonce-chaos senders (out-of-order bursts within gap limits). Fully deterministic per seed; the seed is printed in the verdict for replay.

**The oracles make this differential fuzzing, not smoke:** every fuzz tx flows through executor *and* validator (BAL/write-set/receipt cross-check, live), and the W5 post-run gate adds byte-level DB parity + L1 rebuild over the fuzzed state. Three independent consumers of the same canonical stream must agree — that is strictly stronger than what any single-implementation fuzzer gets.

**Where:** a third stage in the `load` shard (short, per-PR) and a long-duration variant in W10's nightly. Widening the state space also raises the value of every existing chaos case that runs after load.

### W8 — wire-conformance fixtures (hive `engine` analog)

The cluster-adapter framing (`crates/cluster-adapter/src/wire.rs`: ingress kinds 0–4, egress kinds 1–5, record types, `epoch_slots` arithmetic) is our consensus-interface contract, hand-mirrored in Java (`SealerClusteredService.java:42` `TODO(envelope)`).

**What:** `cluster/wire-fixtures/` — golden frame files + a manifest (frame bytes, expected parse, expected disposition), consumed by **both** sides:

- Rust: a `cluster-adapter` test decoding/re-encoding every golden frame byte-identically.
- Java: a sealer-service test feeding each frame through the service's parse path, asserting verbatim relay for valid frames and the defined disposition for adversarial ones.
- Adversarial corpus: truncated header, unknown ingress kind, `slot_count` inconsistent with the deposit count (sealer relays — it is schema-agnostic; the Rust consumer test asserts the documented fail-stop), zero-length payload, oversized `canonical_id`.

Any framing change now fails one side's CI until both are regenerated from the same fixtures — the drift risk the TODO warns about is closed structurally.

### W9 — shard verdict JSON + expected-failures (hive reporting pattern)

Each shard writes a `verdict.json` artifact (cases run, pass/fail, durations, SLO measurements, checker summary). A repo-level `deploy/cluster/expected-failures.yaml` lets a known-bad case be marked *expected* with a reason and an issue link — reported distinctly, never silently green. First customer: `archive-tx-data-wipe`'s too-tight restart SLO (`docs/failure-modes.md` — while that shard is red, real regressions behind it are invisible; today the only options are "red and ignored" or "delete the case").

### W10 — `cluster-nightly.yml` (erigon `qa-*` / reth nightly-hive pattern)

Nightly cron workflow for everything too expensive or too flaky-hunting for per-PR:

- Full chaos roster in one run (all 24+ cases across sequential chains, `chaos-iter.sh`-style — also fixes its stale case list, audit D-10).
- **Blast-radius rotation** (audit D-11): parameterize victim selection (`ingress-1`, `seq-b`, shard 1, an executor other than 0, a control-plane node) and rotate by date.
- Long soak: `LOAD_DURATION_S` 30–60 min + long-duration fuzz (W7).
- W1's `--persistence` trie mode over the full fixture set.
- Non-blocking early-warning job: consume the next fork's devnet fixture line (`tests-<devnet>@vN`).
- Failure files an issue with the verdict JSONs attached.

### W11 (later) — unified fault injection across targets

The audit's C-1: S7 (corrupt-BAL divergence) and S9b-class cases are Target-L-only because they need process signals and raw Aeron publications. reth's testsuite `Action` DSL and besu's condition DSL suggest the shape: put fault injection behind the same target-agnostic seam the scenario drivers already use (`harness/inject.rs` for Target L; a docker-exec injector for Target C), so divergence-detection and crash-recovery semantics run where they matter most — the real cluster. Deliberately last: it is harness surgery, and W4–W7 deliver more coverage per unit of work.

### Out of scope, recorded for later

A kardamom-native filler→fixture pipeline (EEST's authoring model applied to *our* semantics: declarative deposit/epoch/derivation scenario corpus, replayable through Target L, Target C, and reconstruct — precedent: NethermindEth/execution-spec-tests-gnosis retargeting EEST at a non-mainnet chain). Worth a spec of its own once W1–W8 have landed and the S10 cases feel cramped as code.

## Suggested PR sequencing

| PR | Contents | Risk |
|---|---|---|
| PR-1 | W1 + W1b: `SpecId` pin + explicit gas/env parameter pinning + EEST runner + xfail list + `eest-conformance` job + `just eest` | Low mechanically, but W1b surfaces two product decisions (fee sink, tx-gas cap at ingress); biggest single coverage win |
| PR-2 | W3 (sealer tests in CI) + W2 (RPC vectors) | Trivial + low |
| PR-3 | W4: `kardamom-checker` + `ci-cluster.sh` verdict swap | Medium — touches every shard's pass criteria; land behind a `CHECKER=1` env first, flip default once green a week |
| PR-4 | W5: consume-matrix post-run on the semantics shard | Low-medium (docker cp plumbing) |
| PR-5 | W6: `chaos-join` shard | Medium — new cases always need SLO tuning |
| PR-6 | W7: fuzz workload (load shard stage) | Low-medium |
| PR-7 | W8: wire fixtures both sides | Low |
| PR-8 | W9 + W10: verdict JSON, expected-failures, nightly | Low |

## CI cost accounting

| Item | Where | Added wall time |
|---|---|---|
| W1 | `ci.yml` new job (parallel) | ≤ 10 min, cached fixtures |
| W2 | Target-L job + semantics shard | seconds |
| W3 | `ci.yml` new job | ~3–5 min |
| W4 | all cluster shards | ~0 (replaces bash; runs concurrently) |
| W5 | semantics shard post-run | ~3–5 min |
| W6 | new shard | ~50 min (parallel matrix entry) |
| W7 | load shard stage | ~3 min per-PR; long form nightly |
| W8 | `ci.yml` test + gradle test | seconds |
| W10 | nightly only | off the PR path |

## Open questions

1. **W4 injection-window signalling:** file-based handshake in the shared scripts dir vs. a checker HTTP endpoint chaos.sh POSTs to. File is simpler and survives checker restarts; leaning file.
2. **W5 root oracle:** the validator exposes `validator_state_root_block` (a block number) but not the root itself (roots don't fit gauges). Plan: `kardamom-statecheck` computes the root from the copied validator dir and hands it to `--expect-root`. Alternative: a one-shot `kardamom-statecheck --print-root`.
3. **W1 BLOCKHASH stub:** confirm how the pinned fixture release encodes ancestor hashes for state tests (t8n `blockHashes` convention); if any Osaka state-test family genuinely requires them, decide stub vs. exclusion-list.
4. **W6 `ingress-txdata-latejoin` observability:** asserting the newborn actually *serves* pre-restart receipts requires routing a receipt query to a specific ingress — fixed per-instance addresses make this easy (`192.168.56.31` vs `.32`), but confirm the receipt cache rebuild path (#31) is expected to cover pre-restart txs at all, or whether the case should assert the documented degraded behavior instead.
5. **Fixture tarball size/caching:** the all-forks mainnet tarball is large but we only read the pinned fork's `state_tests`; if download time matters, check whether per-format sub-tarballs are published for the `tests@v20.x` line.
6. **W1b fee sink:** burn-to-zero-address (status quo, just documented) vs. a fee-vault predeploy. Burn is simplest and matches `basefee = 0` v0 economics; a vault is a product feature, not a test concern — leaning burn, revisit when fees become real.
7. **W1b type-3 envelopes:** confirm what ingress does with a type-3 (4844) `eth_sendRawTransaction` today — the RPC receipt mapper knows the 0x03 envelope, which suggests they may currently be accepted and executed under the skipped blob-count check. If so, the ingress rejection in W1b is a (desirable) behavior change and needs its own line in the PR description.

## Sources

- Weld announcement/completion: steel.ethereum.foundation/blog/2025-09-11_weld-announcement, /2025-11-04_weld_final; docs: steel.ethereum.foundation/docs/execution-specs/ (fixture formats, `consume` mechanisms, release scheme).
- hive: github.com/ethereum/hive (docs/{overview,clients,simulators}.md); simulators: `ethereum/{engine,rpc-compat,sync,consensus,graphql,eels/*}`, `devp2p`, `smoke/*`.
- reth: `.github/workflows/hive.yml` (nightly, fork × simulator matrix), `.github/scripts/hive/expected_failures.yaml` (~9/55/135 entries), `testing/ef-tests`, `crates/e2e-test-utils/src/testsuite`.
- geth: `tests/` (EEST via `TestExecutionSpecState`, fixtures pinned in `build/ci.go`), `cmd/evm` (`statetest`/`blocktest`/`t8n`), `cmd/devp2p/internal/*`, `eth/catalyst` SimulatedBeacon.
- nethermind: `hive-tests.yml` (~10 shards per master push), `nethtest`, `test-assertoor.yml`, `Ethereum.Test.Base` + `src/tests` submodule.
- besu `acceptance-tests` (Java cluster DSL); erigon `qa-*` workflows (long-running network QA).
- ethpandaops: `ethereum-package`, `assertoor`(+`assertoor-test` playbooks), `spamoor`, `kurtosis-assertoor-github-action`, `kurtosis-sync-test`.
- Non-mainnet EEST retarget precedent: github.com/NethermindEth/execution-spec-tests-gnosis.
