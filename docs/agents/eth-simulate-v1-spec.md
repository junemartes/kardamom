# `eth_simulateV1` for Kardamom

## Goal

Implement the `eth_simulateV1` JSON-RPC endpoint exactly as specified by the
Ethereum execution-apis (PR #484), so that any L1-compatible client (curl,
ethers, viem, foundry's `cast`) can speculatively execute one or more
hypothetical blocks of transactions against Kardamom's current head — with
optional block & state overrides, optional ETH-transfer log synthesis, and
optional strict validation. The endpoint MUST NOT mutate live node state and
MUST be callable concurrently with other read-only RPCs.

## Non-goals

- Simulating on top of historical state. Kardamom does not retain it. Base
  block parameter is accepted but only `latest` / `pending` resolve; anything
  else returns `UnsupportedBlockTag`.
- Legacy hardfork support. We target the same SpecId Kardamom currently runs
  (Osaka per executor.rs:43). No code paths for pre-merge clients.
- Custom Kardamom extensions to the response shape. We exactly mirror the
  execution-apis JSON.
- `eth_callMany`, `eth_simulateV2`, or any other variant.
- Mempool / pending-tx awareness. Simulation runs on the committed head only.

## Design

### High-level pipeline

```
RPC handler (rpc.rs)
   └── Node::simulate                          (node.rs)
         ├── acquires inner.read()
         ├── snapshots block_number + db ref
         └── simulate::run(&payload, base, &db, chain_id)   (simulate.rs)
               ├── validates payload (block count, monotonic invariants)
               ├── builds overlay CacheDB layered on the live state
               ├── for each SimBlock:
               │     ├── derives BlockEnv from overrides + previous block
               │     ├── applies state_overrides to overlay
               │     ├── for each call:
               │     │     ├── builds TxEnv (defaults: from=0, gas=remaining)
               │     │     ├── auto-fills nonce iff !validation
               │     │     ├── executes against overlay
               │     │     │     ├── with TransferInspector iff trace_transfers
               │     │     │     └── REVM cfg flags toggled by `validation`
               │     │     ├── collects SimCallResult
               │     │     └── accumulates gas_used
               │     └── synthesizes block header (Block) from gas + overrides
               └── returns Vec<SimulatedBlock>
```

### Overlay state model

The live state lives in `NodeState.db: CacheDB<EmptyDB>` behind the `RwLock`.
Simulation takes a `read()` guard and wraps the live `CacheDB` as a
`DatabaseRef` source for a fresh overlay `CacheDB`:

```rust
let overlay: CacheDB<&CacheDB<EmptyDB>> = CacheDB::new(&state.db);
```

All commits go to the overlay. The overlay persists across all SimBlocks
within a single `eth_simulateV1` call, giving free state inheritance between
blocks. The read lock is held for the duration of the simulation — this
naturally serializes simulation against commits (`submit_raw_transaction`
takes `write()`) and permits parallel simulations. The held-lock approach is
consistent with the current `eth_call` path; we deliberately do not introduce
snapshot-and-release complexity.

### State overrides

`AccountOverride` (from `alloy_rpc_types_eth::state`) is applied to the
overlay before any call in that block executes. For each address:

- `balance` → set `AccountInfo.balance`.
- `nonce` → set `AccountInfo.nonce`.
- `code` → install `Bytecode::new_raw`, recompute `code_hash`.
- `state` → replace **all** storage slots (REVM: insert into `CacheDB.storage`
  for the account and mark the account `AccountStatus::Loaded`).
- `state_diff` → merge into existing storage; previously-set slots that are
  not mentioned remain.
- `state` and `state_diff` set together → reject with
  `SimulateError::invalid_params()`.
- `movePrecompileToAddress` → out of scope. The spec calls behaviour
  "undefined" when used on non-precompiles; we reject with
  `SimulateError::invalid_params()` rather than silently ignore.

### Block overrides & defaults

Each SimBlock yields a `BlockEnv`. Defaults are derived from the *previous*
simulated block (or the live head for the first block):

| Field            | Default                                            | Override field          |
| ---------------- | -------------------------------------------------- | ----------------------- |
| `number`         | previous + 1                                       | `blockOverrides.number` |
| `timestamp`      | previous + 1; first block's "previous" is `0` because Kardamom does not track real block timestamps | `blockOverrides.time`   |
| `gas_limit`      | inherited from previous (initially 30_000_000)     | `blockOverrides.gasLimit` |
| `beneficiary`    | `Address::ZERO`                                    | `blockOverrides.feeRecipient` |
| `basefee`        | inherited (initially 0; Kardamom has no fee market)| `blockOverrides.baseFeePerGas` |
| `prevrandao`     | `B256::ZERO`                                       | `blockOverrides.prevRandao` |
| `blob_excess_gas`| `0`                                                | `blockOverrides.blobBaseFee` (encoded via excess) |
| `difficulty`     | `U256::ZERO`                                       | `blockOverrides.difficulty` |

Strict invariants enforced (matching geth's `simulate.go`):

- Total simulated blocks ≤ `MAX_SIMULATE_BLOCKS` (256, exported by alloy).
- `block.number` strictly greater than previous block's number.
- `block.timestamp` strictly greater than previous block's timestamp.
- `block.gas_limit` ≤ `MAX_BLOCK_GAS_LIMIT` (we adopt 2^63 - 1 like geth).
- Sum of call `gas_limit` ≤ `block.gas_limit` (when `validation=true`).
- Block `number` not in past of base block.

All violations map to `SimulateError::invalid_params()` (code `-32602`).

### Validation modes

`validation` is a single bool but it composes several REVM cfg toggles. We
gate the toggle fields behind REVM cargo features (`optional_balance_check`,
`optional_block_gas_limit`, `optional_eip3607`, `optional_no_base_fee`,
`optional_priority_fee_check`).

| REVM cfg                  | `validation=false` | `validation=true` |
| ------------------------- | ------------------ | ----------------- |
| `disable_nonce_check`     | `true`             | `false`           |
| `disable_balance_check`   | `true`             | `false`           |
| `disable_base_fee`        | `true`             | `false`           |
| `disable_block_gas_limit` | `true`             | `false`           |
| `disable_eip3607`         | `true`             | `false`           |

Per-call defaults under `validation=false`:

- `from` omitted → `Address::ZERO`.
- `gas` omitted → block's remaining gas (`block.gas_limit - cumulative_gas`).
- `nonce` omitted → current account nonce, auto-incremented across successive
  calls from the same sender within the same block.

Per-call defaults under `validation=true`:

- `from` omitted → still `Address::ZERO` (no signature checks performed; we
  trust the supplied `from`).
- `gas` omitted → block's remaining gas, but tx must have sender balance
  ≥ `gas * gas_price + value`.
- `nonce` omitted → current account nonce (no auto-increment; if a sender
  appears twice, the second call must supply the correct nonce).

### `traceTransfers`

When set, a `TransferInspector` is installed for every call. It synthesizes
phantom ERC-20 `Transfer` events for every ETH value movement:

- **Top-level call value**: emitted with `address = tx.from`, `topics =
  [TRANSFER_SIG, from, to]`, `data = value`. Position: before any real logs
  from the call.
- **Internal calls with `inputs.value > 0`**: emitted at `inputs.caller`
  before the call frame executes.
- **`SELFDESTRUCT`**: emitted at `contract` with `from=contract`,
  `to=beneficiary`, `value=balance_transferred`.
- **Gas payment to coinbase**: emitted at end-of-tx with `from=tx.from`,
  `to=block.beneficiary`, `value=priority_fee_per_gas * gas_used` where
  `priority_fee_per_gas = gas_price.saturating_sub(basefee)`. Only the
  priority-fee component is paid to the coinbase; the basefee is burned.
  Skipped when `beneficiary == Address::ZERO` or the value is zero.

`TRANSFER_SIG = keccak256("Transfer(address,address,uint256)")
              = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`.

Phantom logs are merged into the call's log list in the order they were
emitted relative to real logs. `log_index` is assigned across the merged
sequence.

This matches the
[execution-apis](https://github.com/ethereum/execution-apis/blob/main/src/eth/execute.yaml)
and geth's `simulate.go` behaviour.

### Output: block header construction

We synthesize the `Block` field of each `SimulatedBlock`:

- `header.number`, `header.timestamp`, `header.gasLimit`, `header.baseFeePerGas`,
  `header.miner`, `header.difficulty`, `header.mixHash` ← from `BlockEnv`.
- `header.gasUsed` ← sum of per-call `gas_used`.
- `header.logsBloom` ← bloom of the union of all real + phantom logs across
  all calls.
- `header.parentHash` ← hash of previous synthesized block (or `B256::ZERO`
  for the first block since we don't track real headers).
- `header.hash` ← `keccak256(rlp(header))` so clients receive a stable hash.
- `header.transactionsRoot`, `header.receiptsRoot`, `header.stateRoot`,
  `header.withdrawalsRoot` ← `EMPTY_ROOT_HASH`. Computing real tries would
  require an mpt and is not worth the complexity; geth populates these but
  L1-compat clients do not depend on the values.
- `transactions` ← per `return_full_transactions`: full tx objects with
  recovered `from`/`hash` placeholders if true, hashes if false.

### Public Rust interface

In `crates/node/src/simulate.rs`:

```rust
pub fn run(
    payload: &alloy_rpc_types_eth::simulate::SimulatePayload,
    base_block: u64,
    base_db: &CacheDB<EmptyDB>,
    chain_id: u64,
) -> Result<Vec<SimulatedBlock>, SimulateError>;
```

In `crates/node/src/node.rs`:

```rust
impl Node {
    pub async fn simulate(
        &self,
        payload: SimulatePayload,
        block: BlockNumberOrTag,
    ) -> Result<Vec<SimulatedBlock>, NodeError>;
}
```

In `crates/node/src/rpc.rs`, extend `EthApi`:

```rust
#[method(name = "simulateV1")]
async fn simulate_v1(
    &self,
    payload: SimulatePayload,
    block: BlockNumberOrTag,
) -> RpcResult<Vec<SimulatedBlock>>;
```

### Error model

A new `NodeError::Simulate(SimulateError)` variant wraps alloy's
`SimulateError`. Converting to `ErrorObjectOwned` uses `error.code` and
`error.message` directly. The existing `UnsupportedBlockTag` is reused for
non-`latest`/`pending` base blocks.

Per-call validation failures (insufficient balance / wrong nonce / below
basefee under `validation=true`, REVM halts at any time) are reported in
the call's `SimCallResult { status: false, error: Some(...) }`, not as a
top-level abort. Top-level aborts are reserved for *structural* invalidity
of the payload (too many blocks, non-monotonic block number / timestamp,
oversized gas limit, conflicting state overrides). This matches geth's
`simulate.go` behaviour.

## Ethereum spec references

- Execution-apis spec PR: https://github.com/ethereum/execution-apis/pull/484
- Geth reference implementation: `internal/ethapi/simulate.go`
- ERC-20 Transfer event signature: ERC-20 spec
- EIP-1559 (base fee): https://eips.ethereum.org/EIPS/eip-1559
- EIP-4399 (prevRandao): https://eips.ethereum.org/EIPS/eip-4399
- EIP-4844 (blob base fee): https://eips.ethereum.org/EIPS/eip-4844
- EIP-7825 (gas limit cap, already enforced in tx_env_from_request): https://eips.ethereum.org/EIPS/eip-7825

## Testing strategy

**Unit tests** (`crates/node/src/simulate.rs`) — delivered set:

- State overrides: `state_override_sets_balance_observable_via_balance_opcode`,
  `state_override_preserves_live_account_fields`,
  `state_override_state_diff_merges_existing_storage`,
  `state_override_rejects_state_and_diff_together`,
  `state_override_rejects_move_precompile`,
  `state_override_sets_code_callable`.
- Block overrides: `block_override_number_applied`,
  `block_override_timestamp_applied`,
  `block_override_basefee_reaches_evm`,
  `block_override_coinbase_reaches_evm`,
  `block_defaults_number_increments`,
  `block_defaults_timestamp_increments`.
- Defaults: `default_from_is_zero_address`.
- Validation modes: `validation_off_allows_unfunded_value_transfer`,
  `validation_on_rejects_unfunded_value_transfer` (asserts per-call
  SimCallResult{status:false} per spec).
- Nonces: `nonce_auto_increments_within_block_when_validation_off`.
- Multi-block: `multi_block_state_inherited`.
- traceTransfers: `trace_transfers_off_emits_no_synthetic_logs`,
  `trace_transfers_top_level_value_emits_transfer_log`,
  `trace_transfers_selfdestruct_emits_log`,
  `trace_transfers_coinbase_payment_emits_log_when_priority_fee_nonzero`,
  `trace_transfers_no_coinbase_log_when_priority_fee_zero`.
- Errors: `rejects_too_many_blocks`,
  `error_block_number_not_strictly_increasing`,
  `error_block_timestamp_not_strictly_increasing`,
  `error_block_gas_limit_overflow`.
- Output shape: `return_full_transactions_false_returns_hashes`,
  `return_full_transactions_true_returns_full`,
  `revert_returns_status_zero_and_revert_data`,
  `synth_hashes_differ_across_blocks_for_identical_calls`.
- Determinism: `live_state_unchanged_after_simulation`,
  `empty_payload_returns_no_blocks`.

Deferred (tracked for follow-up; not implemented):

- `state_override_state_replaces_storage` — full-replace semantics. Covered
  indirectly by the rejection test for `state` + `state_diff` simultaneously.
- `block_override_prevrandao_applied`, `block_override_gaslimit_applied` —
  the override paths through `BlockEnv` are exercised by other opcode tests;
  add when bytecode probes for `PREVRANDAO`/`GASLIMIT` are needed.
- `multi_block_nonce_inherited` — same-sender across blocks. The nonce path
  is covered by `nonce_auto_increments_within_block_when_validation_off`
  combined with `multi_block_state_inherited`.
- `single_block_nonce_required_validation_on`,
  `validation_on_rejects_wrong_nonce`,
  `validation_on_rejects_below_basefee`,
  `validation_off_allows_below_basefee_gas_price` — additional cfg-toggle
  surface; the cfg flags are exercised by the existing balance tests.
- `default_gas_is_remaining_block_gas` — default-gas path is exercised
  implicitly by every test that omits `gas`.
- `trace_transfers_internal_call_value_emits_log`,
  `trace_transfers_log_ordering_preserved_relative_to_real_logs` — internal
  call value & ordering. The inspector hooks are present and minimally
  unit-tested in `transfers.rs`.
- `error_call_gas_exceeds_block_gas_limit_validation_on`,
  `simulation_returns_proper_block_header` — covered by spec-doc invariants;
  add when a regression demands them.

**Integration tests** (`crates/node/tests/simulate.rs`):

1. `rpc_simulate_v1_round_trip` — start jsonrpsee server, call via
   `jsonrpsee::core::client`, assert SimulatedBlock count + structure.
2. `rpc_simulate_v1_unsupported_base_block` — `block: "0x1"` returns
   UnsupportedBlockTag error.
3. `rpc_simulate_v1_parallel_calls` — spawn N=8 concurrent simulate calls,
   all return correct results, none observe mutations from each other.
4. `rpc_simulate_then_send_then_simulate` — simulate, submit real tx,
   simulate again; second simulation sees the committed tx's effects.

**Determinism guarantees**:

- No real timers, no sleeps. `BlockEnv.timestamp` is set explicitly by the
  caller or defaults to monotonic +1.
- No randomness. `prevrandao` defaults to zero or is supplied.
- Concurrent tests use `tokio::task::JoinSet` with each task reading from a
  shared `Arc<Node>` and asserting against a deterministic snapshot. No
  cross-task communication.
- All overlay state is in-memory, allocated and dropped per test.

## Alternatives considered

**A. Snapshot the entire CacheDB before simulating, release the lock.**
Lower lock contention but adds a deep clone of all state per call, and the
current RwLock is not contended in practice — real txs are infrequent. The
in-place overlay against a held read lock is simpler and faster for typical
loads. Rejected as premature optimization.

**B. Implement traceTransfers via post-hoc state diffing instead of an
inspector.** Would compute `(pre, post)` per call and emit transfers from the
diff. Misses internal call ordering (multiple transfers between the same
pair would collapse into one), doesn't capture selfdestruct intermediates,
and breaks L1 compatibility on log ordering. Rejected.

**C. Expose a separate `simulate` crate.** The whole feature is ~1500 LOC
and tightly coupled to the node's executor + db. A new crate adds workspace
plumbing for no real isolation benefit. Rejected.

**D. Maintain real Merkle Patricia tries to populate `transactionsRoot`,
`receiptsRoot`, `stateRoot`.** Geth does this. Requires pulling in
`alloy-trie` and computing tries per simulation, which is heavyweight and
of no functional value to clients (no L1-compat client validates these on
simulation responses). Use `EMPTY_ROOT_HASH` instead. Rejected.
