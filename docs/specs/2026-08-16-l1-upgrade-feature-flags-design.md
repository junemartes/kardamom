# L1-Governed Upgrades: Feature Flags via the X-Chain Messaging Box — Design

**Date:** 2026-08-16 (revised 2026-08-17: authority + permanence resolved;
health-check replaces the throwaway `hello` mock)
**Status:** Implemented — 4 PRs on `feat/l1-upgrade-feature-flags`; S13a/b/c
pass against a live local stack (real anvil L1, Aeron, Java Raft sealer,
executor + validator).
**Scope (milestone 1):** An L1-initiated upgrade path: a privileged L1 transaction
to the messaging box (`ETHLockbox`) schedules a feature flag in a new L2
`KardamomChainState` predeploy, with an optional activation timestamp; the
protocol (executor **and** validator, through the shared engine) reads the flag
at every block boundary. The first feature — a per-block **health beacon** —
proves the full flow end-to-end and stays as a permanent liveness signal.

---

## 1. Motivation

Kardamom has **no upgrade mechanism today**. The EVM spec is a compile-time
constant (`SPEC_ID = SpecId::OSAKA`, `crates/exec-core/src/block_env.rs:31`)
with the documented policy "there is no fork schedule; a fork upgrade is a
single PR that bumps this constant" — i.e. every behavior change is a
coordinated binary swap with no on-chain activation point. That works for a
dev-net; it does not work once independent operators run validators, because
nothing forces all nodes to switch behavior *at the same block*.

Because every Kardamom node already derives L2 blocks from **finalized L1**
(the DA watcher → epoch → canonical stream path), we can inherit L1's security
and ordering for upgrades instead of inventing client-side hard-fork
coordination: a multisig-controlled L1 transaction is the upgrade trigger, its
position in the canonical stream is Raft-ordered like everything else, and the
activation timestamp gives operators a rollout window. Nodes that never learned
about a feature **fail-stop at its activation point** instead of silently
diverging (see §8) — the same fail-safe posture the validator already takes.

## 2. Goals / Non-goals

**Goals (milestone 1)**
- A new privileged L1 entry point on the messaging box: the **upgrade
  transaction** (`ETHLockbox.initiateUpgrade`), sender-gated to the existing
  L1 authority (the factory owner — a Safe or EOA).
- The DA watcher picks it up through the **existing** epoch-derivation path and
  it executes deterministically inside every Kardamom instance as a **system
  deposit** (the reserved `source_hash` domain 1 / `is_system_transaction`
  slots, unused today, get their intended use).
- A new L2 predeploy, **`KardamomChainState`** at
  `0x4200000000000000000000000000000000000017`, holding
  `feature_id → activation_timestamp` in storage.
- Activation timestamp semantics: absent (`0`) ⇒ active immediately (from the
  very block that contains the upgrade tx); otherwise active from the first
  block whose header timestamp reaches it.
- The first real feature — **health check (id 1)**: once active, the engine
  records a **health beacon** (count, block number, block timestamp, packed into
  one storage word) in the chain-state predeploy **every block** and logs a
  heartbeat line. The beacon is consensus state — it rides in the block's
  `BlockDelta`, which the validator compares against the executor's published
  one and which feeds the state root — so the validator independently proves
  activation parity. It is also a genuinely useful liveness signal, kept
  permanently rather than removed after the test.
- E2E scenarios exercising the full flow (immediate + scheduled + negative
  controls) in the chain-semantics suite.

**Non-goals (later milestones)**
- Generic upgrade payloads (arbitrary calldata / target from L1) — v1 carries
  exactly `(feature_id, activation_timestamp)`.
- Feature deactivation / rollback semantics (v1 is schedule-only; a far-future
  reschedule is the escape hatch until then).
- Driving `SPEC_ID` / hardfork selection from a flag (the natural next
  consumer, but it multiplies the EEST/cfg-pinning surface; out of scope).
- Cluster (Target C) wiring and the rebuild-from-L1 interleaving of deposits
  (pre-existing phase-E gap; see §9).
- A dedicated upgrade-admin role separate from the factory owner (see Open
  Questions).

## 3. Key decisions

| # | Decision | Choice | Why |
|---|----------|--------|-----|
| 1 | Transport L1→L2 | **System deposit inside the existing `EpochRecord`** | Reuses the entire deposit machinery: finalized-only cursor, `derive_epoch`, `canonical_id` dedup, sealer relay, slot accounting (`1 + deposits.len()`), batcher exclusion from DA. The Java sealer and the wire format are untouched. |
| 2 | "New L1 transaction type" | A new **lockbox function + event**, not a literal EIP-2718 L1 tx type | We cannot add tx types to Ethereum L1; the logical "upgrade transaction" is `initiateUpgrade` → `UpgradeInitiated`, a second event on the same watched address. On L2 it *is* a distinct kind: domain-1 `source_hash`, `is_system_transaction = true`. |
| 3 | L1 authority | **Factory owner** via `Ownable2Step(FACTORY).owner()` — *resolved 2026-08-17* | The factory owner is the existing root of trust ("Safe or EOA"), already impersonated in dev (`DEV_OWNER`). No new storage, no initializer bump (`initializeV3`), no key fixtures. Point factory ownership at a Safe and the multisig requirement is met with no multisig code in kardamom. A dedicated `upgradeAdmin` role stays a later additive change. |
| 4 | Flag store | New predeploy `KardamomChainState` at `0x42…17`, code-only genesis alloc | Follows the `L2ToL1MessagePasser` (`0x42…16`) precedent exactly: pinned runtime bytecode in the chain TOML + a deployer pin test. No seeded storage needed (genesis `AllocEntry` has no storage field — and we don't need one: all flags start unset). |
| 5 | L2 write authority | Fixed system sender `SYSTEM_UPGRADER`, checked by the contract | Only derivation can mint a deposit from this address (see §7 security argument); users can't call `setFeature`. |
| 6 | Where the engine reads flags | **`on_boundary`** (block close), through the `snapshot ∘ parent ∘ delta` layering | `on_boundary` is engine-shared code — executor, validator (streaming *and* parallel, which funnels through it), so one hook covers all roles. Layered read is mandatory: the snapshot alone misses the current block's own `setFeature` write and up to K=4 unsettled parent blocks. |
| 7 | Activation predicate | `active(f, N) ⇔ activation(f) ≠ 0 ∧ activation(f) ≤ header_timestamp(N)` | `header_timestamp(N)` = boundary N's `l2_timestamp`, canonical stream data, identical on every replica. Immediate (`0` on L1) stores `block.timestamp` at execution (= boundary N−1's stamp, see §6), which is strictly less than boundary N's — so the upgrade's own block already beats. |
| 8 | First feature | **Health check**: heartbeat log **+ a packed health-beacon word per block** | The log satisfies "a message every block"; the storage write makes activation **consensus-observable** — it lands in the `BlockDelta`, which the validator compares via `write_set_eq` and which feeds the state root — so the cross-check proves both roles activated at the same block. Unlike a throwaway `hello` counter it is worth keeping (§5.4.1). It is deliberately NOT written into the EIP-7928 BAL; see §5.4. |
| 9 | Beacon shape | **One packed word** (count ‖ block ‖ timestamp), not three slots | One write-set entry per block instead of three; the triple is atomic by construction; an external monitor reads the whole health record in a single `eth_getStorageAt`. |
| 10 | Timestamp unit | **Milliseconds** | `l2_timestamp` is epoch-ms everywhere (sealer stamps `leaderClockMillis` floored to the 250 ms tick; the EVM `TIMESTAMP` opcode yields ms on this chain). `activationTimestamp` on L1 is therefore also **ms**. This is a footgun; it is pinned in the ABI docs, the contract natspec, and the e2e test. |
| 11 | Receipt shape | Reuse `TX_TYPE_DEPOSIT` (`0x7E`) | Same as OP: system txs are a deposit subspecies. Receipt `tx_hash` = domain-1 `source_hash`; consumers distinguish system txs by sender/`is_system_transaction`, not by a new type byte. |

## 4. Flow overview

```
 multisig (factory owner)
    │  initiateUpgrade(featureId, activationTs)          [L1 tx]
    ▼
 ETHLockbox ──emit──► UpgradeInitiated(nonce, featureId, activationTs)
    │                                        (same address the watcher already filters)
    ▼  finalized L1 block
 kardamom-da-watcher: RpcL1Source (filter now matches 2 topic0s)
    ▼
 derive_epoch (kardamom-types, shared with validator's EpochVerifier)
    │   upgrade log → Deposit { source_hash: domain-1, from: SYSTEM_UPGRADER,
    │                           to: CHAIN_STATE, mint: 0, is_system_transaction: true,
    │                           input: setFeature(featureId, activationTs) }
    ▼
 EpochRecord → tx_deposits → sequencers → sealer (opaque relay, unchanged)
    ▼
 canonical stream → engine reader → exec thread
    │   execute_deposit_tx → KardamomChainState.setFeature writes
    │   _activation[featureId] = ts (or block.timestamp if 0)
    ▼
 every subsequent on_boundary (executor + validator + replay):
    if active(HEALTH_CHECK, boundary.l2_timestamp):
        delta.storage[CHAIN_STATE][HEALTH_BEACON_SLOT] =
            pack(count+1, block_number, l2_timestamp)      (+ BAL)
        tracing::info!("health beacon: block N, beat #C")
```

## 5. Detailed design

### 5.1 L1: `ETHLockbox` — the upgrade transaction

`contracts/src/L1/ETHLockbox.sol` gains:

```solidity
/// @notice Monotonic upgrade counter (observability only; dedup is by L1 log position).
uint64 public upgradeNonce;

error NotUpgradeAuthority();

/// @param activationTimestamp L2 activation time in **epoch-milliseconds**
///        (L2 block.timestamp is ms on this chain). 0 = activate immediately.
event UpgradeInitiated(
    uint64  indexed upgradeNonce,
    uint256 indexed featureId,
    uint64  activationTimestamp
);

function initiateUpgrade(uint256 featureId, uint64 activationTimestamp) external {
    if (msg.sender != Ownable2Step(FACTORY).owner()) revert NotUpgradeAuthority();
    unchecked { upgradeNonce += 1; }
    emit UpgradeInitiated(upgradeNonce, featureId, activationTimestamp);
}
```

- Selector `initiateUpgrade(uint256,uint64)` = `0xbb8fbf56`.
- topic0 `keccak256("UpgradeInitiated(uint64,uint256,uint64)")` =
  `0xa19e752c58c419a78355e3ccf19c72d68dbe95a78fd63737a8bc880d909ab84f`.
- `FACTORY` is the constant `KardamomUUPSBase` already carries; `owner()` is a
  view call on the factory proxy. Ownership rotation (Ownable2Step) rotates the
  upgrade authority with it — one root of trust.
- New storage (`upgradeNonce`) is **appended** after existing lockbox slots; no
  initializer change is needed (zero-init is correct), so `ContractId`
  metadata, `init_signature()`, and the deploy flow are untouched. The impl
  bytecode changes, which is fine: `ETH_LOCKBOX_CREATION` is regenerated by the
  deployer build script automatically, and on the **current** toolchain
  (legacy pipeline, no `via_ir`) lockbox edits do **not** shift the pinned
  `FACTORY` address — only `KardamomFactoryV1`/`KardamomUUPSBase`/toolchain
  changes do. Live chains ship it through the existing factory UUPS upgrade
  path — the upgrade mechanism bootstraps itself over the contract-upgrade
  mechanism, which is pleasing.

  > **Coordination note (PR #221):** the open `feat/zk-optimistic-mode` PR
  > turns on `via_ir = true`, under which *any* contract edit can shift *all*
  > bytecode. Whichever of that PR and our contracts PR lands second must
  > regenerate the pinned artifacts (the `FACTORY` constant via
  > `print-factory-address` + `factory_address_sync`, the message-passer hex in
  > `chains/dev-withdrawals.toml`, the hardcoded `FACTORY` in
  > `WithdrawalOutputOracle.t.sol`/`ETHLockbox.t.sol`) — and once this design
  > lands, the **`KardamomChainState` predeploy hex becomes a fourth pinned
  > artifact** on that regeneration list.

Foundry tests (`contracts/test/L1/ETHLockbox.t.sol`): authority accepted /
stranger reverted / nonce increments / event fields exact / works through the
proxy with the real factory owner.

### 5.2 Types & derivation (`kardamom-types`, `kardamom-da-watcher`)

**New module `crates/types/src/upgrades.rs`:**

```rust
/// The KardamomChainState predeploy.
pub const CHAIN_STATE: Address = address!("4200000000000000000000000000000000000017");

/// Synthetic L2 sender for system upgrade deposits.
/// = last 20 bytes of keccak256("kardamom.upgrades.system-sender.v1")
/// keccak = 0x93f80dbddc7be0135c7ab3cb454156dab0518b9244cc7ff1b0ffff6c7e031b6d
pub const SYSTEM_UPGRADER: Address = address!("454156dab0518b9244cc7ff1b0ffff6c7e031b6d");

/// Gas limit stamped on every system upgrade deposit (setFeature costs ~50k).
pub const UPGRADE_TX_GAS_LIMIT: u64 = 1_000_000;

/// abi.encodeCall(KardamomChainState.setFeature, (feature_id, activation_ts))
/// = 0x8afdb854 ++ pad32(feature_id) ++ pad32(activation_ts)
/// Hand-rolled (68 bytes) to stay no_std; the selector is pinned against the
/// forge artifact by a deployer test.
pub fn encode_set_feature(feature_id: U256, activation_ts: u64) -> Bytes;
```

**`crates/types/src/epoch.rs`:**

- `source_hash_system(l1_block_hash, l1_log_index) -> B256` — identical
  construction to `source_hash` but with `domain = 1` (the slot the code
  already documents as "reserved = L1-attributes / system tx"). Anchored test
  vector alongside the existing domain-0 one.
- `UpgradeLog { block_number, block_hash, log_index, feature_id: U256, activation_ts: u64 }`.
- `enum LockboxLog { Deposit(DepositLog), Upgrade(UpgradeLog) }` with
  `block_hash()` / `block_number()` / `log_index()` accessors.
- `derive_epoch` takes `&[LockboxLog]`; unchanged rules (foreign-log rejection,
  `log_index` sort, duplicate rejection — deposits and upgrades interleave in
  L1 log order). An upgrade log maps to:

```rust
Deposit {
    source_hash: source_hash_system(log.block_hash, log.log_index),
    from: SYSTEM_UPGRADER,                       // fixed, NOT aliased
    to: Some(CHAIN_STATE),
    mint: 0,
    value: U256::ZERO,
    gas_limit: UPGRADE_TX_GAS_LIMIT,
    is_system_transaction: true,                 // first real use
    input: encode_set_feature(log.feature_id, log.activation_ts),
}
```

`EpochRecord`, its rkyv layout, `canonical_id`, and `epoch_slots` are all
**unchanged** — the upgrade rides in `deposits` as one slot. Nothing in the
sealer, cluster adapter, slot accounting, or batcher changes (the batcher
already excludes all deposits from DA blobs).

**`kardamom-da-watcher`:**

- `rpc_source.rs`: add the `UpgradeInitiated` mirror to the `sol!` block;
  widen the filter to
  `.event_signature(vec![DepositInitiated::SIGNATURE_HASH, UpgradeInitiated::SIGNATURE_HASH])`;
  decode by topic0 into `LockboxLog`.
- `L1Source::deposit_logs` becomes `lockbox_logs(...) -> Vec<LockboxLog>`
  (trait + `MockL1Source` + call sites). This is the **one seam** that keeps
  producer and verifier honest together: the validator's `EpochVerifier` uses
  the same `RpcL1Source` (blanket impl) and re-runs the same `derive_epoch`,
  so it verifies upgrade content with zero verifier-specific code. Had we
  widened only the watcher, every upgrade would halt every validator with a
  false `DepositsMismatch`.

**Execution needs no changes.** `execute_deposit_tx` already handles
`mint = 0` (a zero pre-credit), stamps the receipt with
`tx_hash = source_hash` and `TX_TYPE_DEPOSIT`, and runs the call with
`gas_price = 0` / `disable_nonce_check`. The first-seen dedup on `source_hash`
absorbs watcher retries exactly as for user deposits.

### 5.3 L2: the `KardamomChainState` predeploy

New contract `contracts/src/L2/KardamomChainState.sol`, deployed **only** as a
genesis predeploy (like `L2ToL1MessagePasser` — no constructor state, `nonce 1`):

```solidity
contract KardamomChainState {
    /// Only the derivation pipeline can produce a deposit from this sender.
    address public constant SYSTEM_UPGRADER = 0x454156dAb0518B9244CC7Ff1b0FfFf6c7E031B6D;

    error NotSystemUpgrader();

    /// activationTimestamp in epoch-**milliseconds** (this chain's block.timestamp unit).
    event FeatureScheduled(uint256 indexed featureId, uint256 activationTimestamp);

    /// slot 0: featureId => activation timestamp (ms). 0 = never scheduled.
    mapping(uint256 => uint256) internal _activation;
    /// slot 1: the health beacon, rewritten once per block by the protocol while
    /// the health-check feature is active. Packed low-to-high:
    ///   [0..64)   beat count
    ///   [64..128) block number
    ///   [128..192) block timestamp (ms)
    /// Written by the ENGINE directly at block close, never through the EVM —
    /// there is no transaction to carry it. Read-only from Solidity.
    uint256 public healthBeacon;

    function setFeature(uint256 featureId, uint64 activationTimestamp) external {
        if (msg.sender != SYSTEM_UPGRADER) revert NotSystemUpgrader();
        uint256 ts = activationTimestamp == 0 ? block.timestamp : uint256(activationTimestamp);
        _activation[featureId] = ts;
        emit FeatureScheduled(featureId, ts);
    }

    function activationOf(uint256 featureId) external view returns (uint256) {
        return _activation[featureId];
    }

    function isActive(uint256 featureId) external view returns (bool) {
        uint256 ts = _activation[featureId];
        return ts != 0 && block.timestamp >= ts;
    }

    /// Unpacked health beacon: how many blocks the health check has recorded,
    /// and the number/timestamp of the most recent one. `(0,0,0)` = never run.
    function health() external view returns (uint64 count, uint64 blockNumber, uint64 timestampMs) {
        uint256 b = healthBeacon;
        return (uint64(b), uint64(b >> 64), uint64(b >> 128));
    }
}
```

- Genesis: append the runtime bytecode (via
  `forge inspect --root contracts KardamomChainState deployedBytecode`) to
  `chains/dev-withdrawals.toml` at `0x42…17`, `nonce 1`. Chains are ephemeral
  in dev/e2e, so extending the existing "full-featured" dev genesis is safe
  (the alloc is additive; the message-passer pin test is unaffected). A new
  pin test `crates/deployer/tests/chainstate_genesis_predeploy.rs` mirrors the
  message-passer one, plus a selector/`SYSTEM_UPGRADER`-constant sync test
  against `kardamom-types`.
- The activation mapping lives at slot 0, so
  `activation_slot(f) = keccak256(pad32(f) ++ pad32(0))`; `healthBeacon` is
  slot 1. Both are computed in Rust by `exec-core` (below) and pinned by a
  cross-check test against the forge storage layout.
- `setFeature` schedules; it does not evaluate. Re-scheduling an already-active
  feature to a future timestamp effectively suspends it — documented, and fine
  for v1 (the authority is trusted; determinism is unaffected).

### 5.4 Engine: the feature gate and the health-check hook

**`crates/exec-core/src/features.rs`** (new, `no_std`, pure):

```rust
pub const FEATURE_HEALTH_CHECK: U256 = /* 1 */;
pub fn activation_slot(feature_id: U256) -> B256;   // keccak(pad32(id) ++ pad32(0))
pub const HEALTH_BEACON_SLOT: B256 = /* pad32(1) */;
/// active ⇔ stored != 0 && stored <= header_ts_ms
pub fn is_active(stored_activation: U256, header_ts_ms: u64) -> bool;
/// count | block << 64 | timestamp_ms << 128   (saturating, so a 2^64 overflow
/// can never corrupt a neighbouring field)
pub fn pack_beacon(count: u64, block_number: u64, timestamp_ms: u64) -> U256;
pub fn unpack_beacon(word: U256) -> (u64, u64, u64);
```

**Hook in `ExecState::on_boundary`** (`crates/engine/src/actor/exec_thread.rs`),
placed after the settle sweep, alignment check, and the optional whole-block
strategy fold, and **before** `mem::take(&mut self.delta)` / BAL handoff:

```rust
let stored = self.read_slot_layered(CHAIN_STATE, activation_slot(FEATURE_HEALTH_CHECK))?;
if is_active(stored, boundary.l2_timestamp) {
    let (beats, _, _) = unpack_beacon(self.read_slot_layered(CHAIN_STATE, HEALTH_BEACON_SLOT)?);
    let word = pack_beacon(beats + 1, boundary.block_number, boundary.l2_timestamp);
    // one WriteSet applied to BOTH self.delta and the block BAL, same value
    apply_system_write(&mut self.delta, &mut self.block_bal, CHAIN_STATE, HEALTH_BEACON_SLOT, word);
    tracing::info!(block_number = boundary.block_number, beat = beats + 1, "health beacon");
}
```

- `read_slot_layered` composes `delta → parent → snapshot` — the same
  precedence `seed_cache_layer` gives the EVM. This is **required**, not a
  nicety: the snapshot source ignores block numbers (returns the last published
  snapshot, up to K=4 blocks behind) and the current block's own `setFeature`
  write only exists in `self.delta`.
- Because `on_boundary` is engine-shared, the executor, the streaming
  validator, and the parallel validator (whose `block_exec` output is folded
  *before* this point) all run the identical hook. The write lands in
  `BlockDelta.storage` → validator `write_set_eq` vs the executor's BAL → both
  sides must agree or the validator fail-stops. That comparison is exactly what
  makes the feature a real activation-parity proof.
- The per-tx `bal_rlp` claims are untouched (the beacon write belongs to no tx);
  the parallel validator's claim comparison is per-tx and unaffected.
- **Mirror in `replay.rs::drive_blocks`** (rebuild-from-L1) so the third
  implementation stays semantically aligned. Note: reconstruction of
  deposit-bearing ranges is already inexact (deposits are absent from DA —
  phase-E gap), so the `setFeature` write itself won't be present in
  reconstructed state until epochs are interleaved; the hook reads whatever
  state says and therefore converges automatically once phase E lands.

#### 5.4.1 Why health check, and why it stays

The feature that proves the upgrade path should not be scaffolding we delete
afterwards — a mechanism this load-bearing deserves a permanent, exercised
consumer. The health beacon earns its place three times over:

1. **Liveness signal.** A single `eth_getStorageAt` on `0x42…17` slot 1 yields
   "the chain has produced N health-checked blocks, most recently block B at
   time T" — atomically, no log scan, no block-range query (this chain serves
   neither `eth_getLogs` nor `eth_getBlockByNumber`). Monitors and dashboards
   get a first-class chain-progress probe, and because the beacon carries the
   L2 timestamp, a stale beacon is distinguishable from a stalled reader.

   **The beacon tracks block production, not wall-clock.** The sealer only
   forces a boundary when new records were sealed, so an idle chain produces no
   blocks and therefore no beats. A monitor must read "beacon unchanged" as
   "the chain has not advanced", which on a quiet chain is correct and healthy —
   `(block, timestamp)` in the same word is what lets it tell idle from stuck.
2. **Standing canary for the upgrade path.** Kept shipped-dormant, it can be
   activated on a testnet after any release to exercise the entire
   multisig → L1 → derivation → activation chain without touching protocol
   semantics. A dormant flag costs one storage read per block (a single
   snapshot lookup, memoized below) and zero writes.
3. **Cross-role divergence detector.** The beacon is in the write set of
   *every* block while active, so executor/validator disagreement surfaces at
   the very next block rather than whenever the next relevant transaction
   happens to land.

It also composes: the beacon is the natural substrate for later checks (an
anomaly bit for a timestamp regression, say). v1 deliberately records rather
than judges — a check that can *fail* needs defined failure semantics (halt?
flag?), and an always-false branch is untestable dead code. Deferred, not
forgotten.

**Cost when dormant.** The gate reads one slot per block: two `BTreeMap` probes
(delta, parent) and, on a miss, one mdbx point lookup — the same cost class as
a single account read, in a block that has already done orders of magnitude
more work. Deliberately **not** memoized: a cache of consensus-relevant state
has to be invalidated correctly on every path that can write `CHAIN_STATE`
(including a deposit executed outside the block scope), and getting that wrong
is a divergence bug. Trading a guaranteed-correct microsecond for a
possibly-wrong nanosecond is not a trade worth making.

**Activation semantics, precisely:**

feature `f` is active **for block N** iff
`activation(f) ≠ 0 ∧ activation(f) ≤ boundary_N.l2_timestamp`,
where `activation(f)` is read from end-of-block-N state (post-deposits,
post-txs). Consequences:

- *Immediate* (`activationTimestamp = 0` on L1): `setFeature` stores
  `block.timestamp`, which during block N's execution is boundary N−1's stamp
  (the documented off-by-one: block N executes with the previous boundary's
  timestamp). Since boundary stamps are strictly increasing,
  `stored = ts(N−1) < ts(N)` — **the upgrade's own block is the first beating
  block**. Deterministic on every replica because both quantities are canonical
  stream data.
- *Scheduled* (`T > 0`): the first beating block is the first block whose header
  timestamp ≥ T. If T is already in the past, it behaves like immediate.

### 5.5 What deliberately does not change

| Component | Why untouched |
|---|---|
| Java sealer / Raft | Relays epoch bytes opaquely; slot math `1 + deposits.len()` already counts the upgrade deposit. |
| Cluster adapter wire format | `KIND_ORIGIN_RECORD`, `RT_EPOCH`, rkyv `EpochRecord`/`Deposit` layouts are unchanged (all fields pre-existed). |
| Batcher / DA frames | Deposits (including system ones) are already excluded from blobs and re-derived from L1. |
| Ingress / sequencer | Sequencers forward epochs verbatim; ingress never sees deposits. |
| `execute_deposit_tx` | Already correct for `mint = 0`; receipt keyed by `source_hash`. |
| State schema / writer | The beacon write is ordinary `BlockDelta.storage`; headers, receipts, trie all flow as-is. |

## 6. Timestamp units — a standing warning

`l2_timestamp` is **epoch-milliseconds** end to end (sealer: `leaderClockMillis`
floored to the 250 ms tick; `BlockEnv.timestamp` is fed unscaled, so Solidity
`block.timestamp` on this chain is ms). Therefore `activationTimestamp` in
`initiateUpgrade` / `UpgradeInitiated` / `setFeature` is **ms**, not seconds —
anyone computing it from L1's `block.timestamp` (seconds) would schedule ~56
years out. Pinned in: contract natspec (both contracts), the Rust doc comments,
the e2e scenario (which schedules `now_ms + 4000`), and this spec.

## 7. Security argument

1. **L1 gate:** only the factory owner (production: a Safe multisig) can emit
   `UpgradeInitiated` — enforced by the lockbox against
   `Ownable2Step(FACTORY).owner()`. The watcher only trusts logs from the
   lockbox address with the exact topic0, on **finalized** blocks (no reorg
   surface by construction).
2. **Derivation gate:** only `derive_epoch` mints deposits with
   `from = SYSTEM_UPGRADER`. A user deposit can never impersonate it: user
   deposit senders pass through `alias_l1_address` (+`0x1111…1111`), so forging
   would require controlling the L1 address `SYSTEM_UPGRADER − 0x1111…1111`,
   whose private key is a keccak preimage search — the same hardness the
   OP-style aliasing scheme already relies on. No L2 EOA can sign as
   `SYSTEM_UPGRADER` either (the address is hash-derived; no known key).
3. **L2 gate:** `KardamomChainState.setFeature` reverts unless
   `msg.sender == SYSTEM_UPGRADER`. Defense in depth with (2).
4. **Content verification:** a validator run with `--l1-rpc-url`/`--lockbox`
   re-derives every epoch from L1 and fail-stops on any mismatch — a forged or
   dropped upgrade deposit in the canonical stream is a proven divergence
   (exit 2), exactly like a forged user deposit (S11 already tests the class).
5. **Replay/dedup:** the domain-1 `source_hash` is position-derived
   (`l1_block_hash`, `log_index`) — the same upgrade tx can never apply twice
   (first-seen dedup), and domain separation keeps system hashes disjoint from
   user-deposit hashes even at identical positions.

## 8. Rollout semantics (why this inherits L1 security)

The invariant: **ship binaries first, flip the flag second.** A feature's code
ships dormant in a release; once operators have rolled it out, the multisig
sends the upgrade tx; the flag activates everywhere at the same canonical
point.

Mixed-version behavior is fail-safe, with two distinct failure points:

- An **old validator** (pre-upgrade-aware binary, running with L1
  verification) halts at the *epoch containing the upgrade tx*: its
  `derive_epoch` doesn't produce the system deposit, so content verification
  reports `DepositsMismatch` — a loud, attributable stop, not divergence.
- An **old executor** would execute the `setFeature` deposit fine (it is plain
  deposit data on the wire) but lacks the health-check hook, so a *new* validator
  fail-stops on the first active block's write-set mismatch. Again: halt, not
  silent fork.

Operational notes:

- The DA watcher's cursor is in-memory and seeds at the finalized tip: if the
  watcher restarts *after* the upgrade tx finalized but before observing it,
  that epoch is skipped (existing seed-skip gap, shared with user deposits).
  Runbook: confirm the L2 receipt (`source_hash_system` of the L1 log
  position) before considering an upgrade applied; re-send if missed. The
  nonce-indexed event makes re-sending unambiguous.
- `deploy/cluster/config/genesis/dev.toml` has no predeploys at all (existing
  gap, same as the message passer); Target C runs need it extended plus the
  lockbox var — deferred with the rest of the cluster wiring.

## 9. Known gaps / future work

- **Rebuild-from-L1** (`kardamom-reconstruct`) cannot yet reproduce
  deposit-bearing ranges (phase E of the derivation spec: KAR2 frame +
  interleaving). Upgrade txs inherit that gap; the hook mirror in `drive_blocks`
  means reconstruction becomes exact automatically once phase E lands. The
  replay path's separate timestamp quirk (it uses block N's own header stamp,
  not N−1's) is pre-existing and noted there.
- **Generic upgrade payloads** (arbitrary target/calldata system deposits)
  would subsume feature flags; deliberately deferred until a second real
  consumer exists.
- **`SPEC_ID` scheduling** via a flag is the obvious real feature #2 but drags
  the EEST/cfg-pinning surface with it.
- **Deactivation** and flag enumeration (an on-chain registry of known feature
  ids) — v1 keeps ids as compiled-in constants; the flag store is generic
  already.
- No `eth_getLogs`/`eth_call` on the ingress RPC — e2e observes activation via
  receipts, direct mdbx reads, and process logs; fine for tests, but explorers
  will eventually want the `FeatureScheduled` event queryable.

## 10. Test plan

### 10.1 Unit / component

- `kardamom-types`: domain-1 `source_hash` anchored vector; `derive_epoch`
  mapping an upgrade log (fields, `is_system_transaction`, calldata bytes);
  interleaved deposit+upgrade ordering by `log_index`; duplicate/foreign-log
  rejection unchanged; rkyv roundtrip of an epoch with a system deposit;
  `encode_set_feature` golden bytes (selector `0x8afdb854`).
- `kardamom-da-watcher`: two-topic filter decode (mixed logs in one block,
  via `MockL1Source`); non-lockbox / wrong-topic rejection.
- `kardamom-exec-core`: `activation_slot` vs forge storage layout;
  `is_active` boundary cases (0, ==, <, >).
- `kardamom-engine`: hook determinism — same input stream with/without prior
  activation produces identical deltas across streaming and parallel paths;
  layered-read correctness (activation written in the same block, and in an
  unsettled parent block); BAL contains the beacon write.
- Validator: `EpochVerifier` accepts a matching upgrade epoch; rejects a
  stream epoch whose upgrade deposit was tampered (extends the S11 class).
- Foundry: lockbox auth/nonce/event; ChainState gating (`vm.prank`),
  immediate-vs-scheduled storage, `isActive`.
- Deployer pins: ChainState genesis bytecode sync; selector/constant sync.

### 10.2 E2E (chain-semantics suite, Target L) — **S13: upgrade feature flags**

New driver `crates/e2e/src/scenarios/upgrade.rs`, bindings in
`tests/chain_semantics/upgrades.rs` (same `include!` pattern). Stack:
`StackConfig { l1: true, validator: true, genesis: Genesis::DevWithdrawals }`.
The upgrade tx is sent exactly like `applyDeployments` in the harness today:
from the impersonated `DEV_OWNER`.

- **S13a `s13a_health_check_activates_immediately`** — the requested full-flow
  exercise:
  1. Pre: one mdbx snapshot of executor state → `activation(1) == 0`,
     beacon word `== 0`; no heartbeat lines in logs.
  2. Impersonated owner sends `initiateUpgrade(1, 0)`; take
     `(block_hash, log_index)` from the L1 receipt; compute
     `source_hash_system` and `await_l2_receipt` on it (status ok, deposit
     type); `receipt_placement` gives the activation block **B**.
  3. Wait a few more blocks, then in **one** mdbx snapshot read
     `(snapshot_block, activation, beacon)` and assert
     `beacon.count == snapshot_block − B + 1` — the beacon beat in *every*
     block from B inclusive (exact, race-free: header + storage commit in one
     txn). Also assert `beacon.block_number == snapshot_block` and
     `beacon.timestamp_ms == header(snapshot_block).l2_timestamp`, which pins
     the packing against real header data.
  4. Validator parity: same exact assertions against the validator's state
     dir; `VALIDATOR_BLOCKS_VERIFIED` advances past B with
     `VALIDATOR_DIVERGENCE == 0` and `VALIDATOR_BAL_MISSING` flat.
  5. The literal per-block message: `wait_for_log_line` on the heartbeat, on
     both executor and validator logs.
- **S13b `s13b_health_check_activates_at_timestamp`**:
  1. Send `initiateUpgrade(1, T)` with `T = now_ms + 4000`; await the
     `setFeature` receipt (block S).
  2. While heads have header-ts < T: snapshot reads assert `activation == T`
     and `beacon == 0` (scheduled ≠ active).
  3. After T: from `read_all_headers`, compute **F** = first block with
     `l2_timestamp ≥ T`; assert `F > S`, that header F−1 is < T, and
     snapshot-exact `beacon.count == snapshot_block − F + 1`.
  4. Validator parity as in S13a.
- **S13c `s13c_upgrade_authority_is_enforced`** (negative controls):
  1. L1: `initiateUpgrade` from `DEPOSITOR_KEY` reverts; no epoch content
     changes (executor head advances, `activation(2) == 0`).
  2. L2: a normal user tx calling `setFeature(2, 0)` directly lands with
     `status == false` (revert receipt) and `activation(2)` stays 0, beacon
     unaffected — users cannot reach the flag store.

Harness additions: `sol!` bindings for `initiateUpgrade`/`UpgradeInitiated`
(contracts.rs), a `read_storage_slot(state_dir, addr, slot)` helper next to
`read_validator_state_root`, and `kardamom_types::upgrades` re-exports. No new
services, no workflow-file changes (the suite runs whole-target; CI picks the
new tests up automatically — mind the ~30 min budget: three more L1-backed
stacks ≈ +3–4 min).

Scenario catalog (`docs/agents/chain-semantics-e2e-suite-spec.md`) gains the
S13 row at implementation time. (Numbered S13, not S12: #171 landed the
verified-L1 light-client scenarios as S12a–d while this design was in review.)

## 11. Implementation plan (4 PRs, stacked)

| PR | Contents | Size feel |
|---|---|---|
| 1. `feat(contracts): upgrade tx + chain-state predeploy` | `ETHLockbox.initiateUpgrade` + event; `KardamomChainState.sol`; foundry tests; genesis alloc in `chains/dev-withdrawals.toml`; deployer pin tests | S |
| 2. `feat(derivation): system upgrade deposits` | `types::upgrades`; `source_hash_system`; `LockboxLog`; `derive_epoch`; watcher filter/decode; `L1Source` rename; verifier/mock updates; unit tests | M |
| 3. `feat(engine): feature gate + health-check hook` | `exec-core::features`; layered read; `on_boundary` hook; `drive_blocks` mirror; engine/exec-core unit tests | M |
| 4. `test(e2e): S13 upgrade scenarios` | harness bindings + helpers; `scenarios/upgrade.rs`; S13a/b/c bindings; failure-modes section | M |

Each lands green independently (1–3 ship dormant behavior; nothing activates
until an upgrade tx exists, which only the e2e PR exercises).

**Two design points changed during implementation**, both recorded above:

- The block-close action does **not** write the EIP-7928 BAL (decision 8's
  original sketch said it did). The BAL attributes accesses to transaction
  indices and the validator verifies claims per tx-index range, so attributing
  a block-close write to a transaction would misdescribe the block *and*
  diverge against a validator recomputing claims from transactions. It still
  rides in the `BlockDelta`, which is what `write_set_eq` compares — so the
  cross-role proof is unaffected.
- `apply_block_close_actions` lives in `exec-core`, not in the engine, because
  the workspace has four block drivers and the zk guest is a fifth consumer of
  the shared sequential driver. One implementation, parameterised by how the
  caller reads state.

## 12. Resolved / remaining questions

**Resolved 2026-08-17:**

1. **Authority root** — `factory owner == upgrade authority` for v1. Production
   points factory ownership at a Safe, which satisfies the multisig requirement
   with no multisig code in kardamom. A dedicated `upgradeAdmin` remains an
   additive change if the roles ever need to split.
2. **Feature permanence** — the first feature is **kept**, and is therefore a
   health check rather than a throwaway `hello`: shipped dormant, activatable
   on any network to exercise the whole upgrade path, and useful in its own
   right as a liveness beacon (§5.4.1).

**Remaining:**

3. **Genesis file**: extend `dev-withdrawals.toml` (chosen here) vs. introduce
   `dev-upgrades.toml` + a harness `Genesis` variant. Extending is less churn;
   rename the file to `dev-full.toml` later if the misnomer grates.
4. **Cluster (Target C) wiring**: the cluster genesis has no predeploys at all
   and the cluster anvil lacks `--slots-in-an-epoch 1`; wiring the semantics
   shard for upgrades inherits those pre-existing gaps (§9).
