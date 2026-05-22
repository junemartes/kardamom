# Sequencer State Persistence

## Goal

Persist the L2 sequencer's "latest world" — accounts, storage, code, recent block
hashes, applied-deposit set, and the latest header — to a single embedded
key-value store so that a restarted sequencer resumes from the exact state it
last durably committed. The store must be fast enough that EVM execution is not
disk-bound on the hot path, and its on-disk schema must be reusable read-only
by future validator nodes that need to bootstrap a snapshot.

## Non-Goals

- **No historical state.** The sequencer never serves historical RPC; only the
  most recent state and the data the EVM can read (`BLOCKHASH`, balances of any
  address, etc.) need to be stored. Past blocks beyond the EIP-2935 window are
  *not* retrievable.
- **No canonical MPT.** The sequencer does not compute a Merkle-Patricia-Trie
  state root. Validators reconstruct the MPT from DA-published batches; the
  sequencer's schema is a flat key/value layout.
- **No multi-process sharing.** A single sequencer process owns the database.
  No separate cache daemon (Redis), no shared mempool, no cross-process IPC.
- **No legacy hardfork support.** Targets the latest stable EVM. EIP-2935 is the
  block-hash mechanism; the legacy 256-block in-memory window is not used.
- **No bespoke WAL.** The KV store's own atomic-commit + fsync provides
  durability. We do not maintain a separate diff log.
- **No mempool persistence.** Restart drops in-flight unsealed transactions;
  clients re-submit. Only durably sealed work survives.

## Design

### Storage engine: MDBX

A single `libmdbx` environment hosts all sequencer-persistent data. MDBX is a
copy-on-write B+tree with single-writer/multi-reader transactions, used in
production by reth and erigon. Properties we rely on:

- Atomic commit-with-fsync — exactly the "seal a block" boundary we want.
- Multi-reader snapshot isolation — concurrent `eth_call` reads do not block
  the writer.
- One process, one file, no compaction tuning. Operationally minimal.
- Sparse virtual mapping — we open with a 1 TiB virtual map size from day one;
  physical disk usage stays proportional to live data.

Rationale for MDBX over RocksDB: read-heavy access pattern (every EVM step is
a state read), single-writer fits the sequencer naturally, and the smaller
operational surface dominates the raw-write-throughput edge RocksDB would have.

### Schema

All tables live in one MDBX env. Keys are big-endian or fixed-layout binary so
that lexicographic order matches semantic order; values use a stable binary
encoding (alloy RLP where it interoperates with Ethereum-native types,
fixed-layout structs otherwise).

| Table                  | Key                              | Value                                                  | Notes |
|------------------------|----------------------------------|--------------------------------------------------------|-------|
| `accounts`             | `Address` (20 B)                 | `AccountRow { balance, nonce, code_hash }`              | One row per touched account. Empty accounts pruned per EIP-161. |
| `storage`              | `Address ‖ B256` (52 B)          | `U256` (32 B), zero-stripped                            | Zero values delete the row. |
| `code`                 | `B256` (code_hash)               | raw bytecode                                            | Content-addressed; shared across accounts. |
| `headers`              | `u64` (block_number, BE)         | encoded `Header`                                        | Includes computed `block_hash`. |
| `applied_deposits`     | `B256` (source_hash)             | `u64` (block_number where applied)                      | Replay protection. |
| `receipts`             | `B256` (tx_hash)                 | encoded `TransactionReceipt`                            | Transitional. Removed once validators run RPC; sequencer benches use this for now. |
| `meta`                 | `&'static str` (tag)             | tag-specific bytes                                      | `chain_id`, `latest_block_number`, `genesis_hash`, `schema_version`. |

Encoding rules:
- `AccountRow` is fixed-layout: 32 B balance ‖ 8 B nonce ‖ 32 B code_hash (72 B
  total). No RLP overhead on the hottest table.
- Storage values are written as 32 B big-endian; a write of `U256::ZERO`
  *deletes* the row, matching EVM semantics.
- Headers use alloy's existing RLP for forward compatibility with validators.
- Receipts are *temporarily* JSON-encoded for time-to-correctness in the
  initial cut; a versioned binary encoding (postcard or RLP) replaces this
  before any production use, and a `schema_version` bump captures the
  migration. JSON keeps the validator-interop story simple in the interim.
- `schema_version` is a `u32` we bump on any incompatible layout change; opening
  an env with a mismatched version refuses to start.

### Block production: adaptive sealing

Replace the current "one block per tx" semantics with a deterministic `BlockBuilder`:

```text
seal block N when:
    gas_used_in_block >= gas_target  (default 30_000_000)
  OR
    now() - block_open_at  >= slot_duration  (default 250 ms)
```

On seal:
1. Compute the new header (`parent_hash`, `state_root` left zero for now —
   validators recompute, see Non-Goals; `number`, `timestamp`, `gas_used`,
   `receipts_root`, `transactions_root`).
2. Run the EIP-2935 system call to write `parent_hash` into the history
   contract.
3. Open a single MDBX write transaction:
   - Upsert/delete all touched `accounts` rows.
   - Upsert/delete all touched `storage` rows.
   - Insert any newly-deployed `code`.
   - Insert the new `header`.
   - Insert any `applied_deposits` and `receipts` produced in the block.
   - Update `meta["latest_block_number"]`.
4. Commit (fsync). On success, ack all included tx-hashes to their waiting
   submitters; on failure, panic — partial commits cannot occur (MDBX is
   atomic), so the only failure modes are I/O exhaustion or DB corruption.

Group-commit emerges naturally: every tx that arrives during an open slot is
included in that slot's seal, paying one fsync amortized across the batch.

### Block-hash history: EIP-2935

Block hashes are *not* stored in a dedicated ring buffer. Instead the
sequencer:

1. Deploys the EIP-2935 history contract at `0x0000F90827F1C53a10cb7A02335B175320002935`
   as part of the genesis allocation, with its bytecode from the EIP.
2. Before executing user transactions in block N, runs a SYSTEM_ADDRESS call
   into the contract that writes `parent_hash` into slot `(N-1) % 8191`.
3. `BLOCKHASH` opcode resolves via the standard EVM path, which reads from
   the contract's storage — which is just our `storage` table.

This means the block-hash history is data, not code: storing it falls out of
storing storage. revm 38 implements the EIP; the only work on our side is
genesis deployment and the system-call invocation at block start.

### Crash recovery

MDBX's atomic commit makes recovery trivial:

1. Open the env.
2. Validate `meta["schema_version"]` and `meta["chain_id"]` against the
   sequencer's compiled-in values; refuse to start on mismatch.
3. Read `meta["latest_block_number"]`. The sequencer resumes building block
   `latest+1`. No replay, no half-written state — a partially-committed seal
   is impossible by construction.

If `meta["latest_block_number"]` is absent, the env is freshly created and the
sequencer writes the genesis allocation atomically as block 0.

### In-process caching

A small LRU sits in front of MDBX:

- `account_cache: LruCache<Address, Option<AccountRow>>` — capacity ~16 k.
- `storage_cache: LruCache<(Address, B256), U256>` — capacity ~64 k.
- Code is content-addressed and effectively immutable once written; we use an
  `Arc<DashMap<B256, Bytes>>` populated on first read.

On commit, the cache is updated coherently with the MDBX write (write-through),
so cached reads always reflect the latest committed state. Reads during an
in-flight block hit a per-block overlay (`HashMap`) on top of the cache, which
is dropped on seal-failure and folded into the cache on seal-success.

These caches are an optimization; correctness does not depend on them.

### Concurrency model

The sequencer is single-writer. We use one tokio `RwLock` around the
`NodeState`, as today (`crates/node/src/node.rs:23-34`):

- `submit_raw_transaction`, `submit_deposit_transaction`, and the seal driver
  take the write lock to mutate the in-flight block overlay.
- `call`, `balance`, `nonce`, `code_at`, `block_number`, `receipt` take the
  read lock and read either the overlay-or-cache-or-MDBX chain.

MDBX itself permits long-lived read transactions concurrent with the single
writer. Read RPCs may take an MDBX read tx without contending with the writer,
once we cross out of the in-flight-block overlay.

## Interfaces

A new crate `crates/state/` exposes the persistence layer. The `node` crate
depends on it.

### Public types

```rust
// crates/state/src/lib.rs

pub struct State {
    env: Arc<mdbx::Environment>,
    cache: Arc<Cache>,
}

impl State {
    /// Open or create the MDBX env at `path`, validating schema/chain id.
    pub fn open(path: &Path, chain_id: u64) -> Result<Self, StateError>;

    /// First-time initialization: write genesis alloc as block 0. Idempotent.
    pub fn initialize_genesis(&self, genesis: &Genesis) -> Result<(), StateError>;

    /// Latest sealed block number. 0 immediately after genesis init.
    pub fn latest_block_number(&self) -> u64;

    /// Read-only accessors used by RPC and `eth_call`.
    pub fn balance(&self, addr: Address) -> U256;
    pub fn nonce(&self, addr: Address) -> u64;
    pub fn code(&self, addr: Address) -> Bytes;
    pub fn storage(&self, addr: Address, slot: B256) -> U256;
    pub fn header(&self, number: u64) -> Option<Header>;
    pub fn block_hash(&self, number: u64) -> Option<B256>;
    pub fn receipt(&self, tx: B256) -> Option<TransactionReceipt>;
    pub fn is_deposit_applied(&self, source: B256) -> bool;

    /// Start a writable view backed by an in-memory overlay. Used by the
    /// `BlockBuilder` to execute txs without touching MDBX until seal.
    pub fn open_block(&self, parent: &Header) -> BlockView;
}

pub struct BlockView {
    state: Arc<State>,
    overlay: BlockOverlay,
    header: HeaderBuilder,
}

impl BlockView {
    /// revm `Database`/`DatabaseRef`/`DatabaseCommit` are implemented on this.
    pub fn finalize(self, receipts: Vec<(B256, TransactionReceipt)>,
                    deposits: Vec<B256>) -> SealedBlock;
}

pub struct SealedBlock { /* header, overlay, receipts, deposits */ }

impl State {
    /// Atomic commit of a sealed block. Updates MDBX + cache. fsync on success.
    pub fn commit(&self, block: SealedBlock) -> Result<(), StateError>;
}
```

`BlockView` implements `revm::Database`, `DatabaseRef`, and `DatabaseCommit` so
the existing `executor::execute` / `execute_deposit` functions work unchanged.

### Block builder

```rust
// crates/state/src/builder.rs

pub trait Clock: Send + Sync {
    fn now_millis(&self) -> u64;
}

pub struct BlockBuilder<C: Clock> {
    state: Arc<State>,
    clock: C,
    config: BuilderConfig, // gas_target, slot_duration_ms
    open: Option<OpenBlock>,
}

impl<C: Clock> BlockBuilder<C> {
    pub fn new(state: Arc<State>, clock: C, config: BuilderConfig) -> Self;

    /// Admit a tx to the currently-open (or newly-opened) block. Executes it
    /// against the block overlay. Returns the tx hash and the block number
    /// it will land in. Does NOT seal.
    pub fn submit(&mut self, env: ExecEnv, tx: TxEnv) -> Result<TxAdmitted, StateError>;

    /// Returns Some(SealedBlock) if a seal is due (gas threshold OR time).
    /// Caller is expected to invoke `State::commit` on the returned block.
    pub fn seal_if_due(&mut self) -> Option<SealedBlock>;

    /// Force a seal regardless of thresholds (e.g., graceful shutdown).
    pub fn seal_now(&mut self) -> Option<SealedBlock>;
}
```

The driver task that decides when to call `seal_if_due` is in the `node` crate;
it ticks `tokio::time::interval(1ms)` and asks the builder. In tests, we drop
the timer and call `seal_if_due` directly after advancing the mock clock.

### Node integration

`crates/node/src/node.rs` is updated to hold a `State` + a `BlockBuilder`
instead of `CacheDB<EmptyDB>`. Public API of `Node` is preserved
(`balance`, `nonce`, `code_at`, `call`, `submit_raw_transaction`,
`submit_deposit_transaction`, `block_number`, `receipt`).

`submit_raw_transaction` semantics change subtly: it now blocks until the
containing block seals and commits. This is the sync-after-fsync durability
guarantee from the design discussion.

## Ethereum Spec References

- **EIP-2935** *(Serve historical block hashes from state)* — block hash
  history is stored at the system contract
  `0x0000F90827F1C53a10cb7A02335B175320002935`, window 8191 blocks. We deploy
  this contract in genesis and invoke its system call before each block's user
  transactions. revm 38 implements the `BLOCKHASH` semantics against it.
- **EIP-161** *(state-trie clearing)* — empty accounts (zero balance, zero
  nonce, no code) are pruned during commit; their row is deleted from the
  `accounts` table.
- **EIP-2929 / EIP-2930** *(access lists, warm/cold)* — purely execution-side;
  no impact on persistence schema.
- **EIP-7702** *(set-code transactions)* — `code_hash` in `AccountRow` covers
  both classic contract code and 7702 delegations; the delegation prefix is
  bytecode like any other. No schema change.

The flat `(address, account)` + `(address ‖ slot, value)` layout matches what
reth and erigon use internally (their "PlainState" tables), which is the path
of least friction for a future shared-schema validator.

## Testing Strategy

Tests are partitioned by component and are deterministic by construction:

- All time-dependent code receives an injected `Clock`; tests use
  `MockClock(AtomicU64)` and advance manually.
- All MDBX usage in tests goes through `tempfile::TempDir`, which gives each
  test an isolated env.
- No `tokio::time::sleep`, no real network, no thread spawning in any test.
  Builder driver tests poll `seal_if_due` directly.
- Tests run serially within a binary (no `#[tokio::test(flavor =
  "multi_thread")]` for state tests); concurrency is exercised only at the
  `Node` integration layer.

The full test list is enumerated in the Plan (next section).

## Alternatives Considered

**Three-tier Redis + WAL + RocksDB.** Rejected for complexity. A second process
for caching adds an IPC hop on every state read, a separate persistence story
for the cache, and two failure domains to coordinate. RocksDB's own block cache
or MDBX's mmap-backed read path already give us in-memory hot data inside the
same address space. A hand-rolled WAL duplicates MDBX's internal log without
adding any property MDBX doesn't already provide.

**RocksDB instead of MDBX.** Reasonable alternative; rejected for now because
the sequencer's workload is read-heavy (every EVM step is a state read) and
single-writer, which is exactly MDBX's sweet spot. RocksDB's higher peak write
throughput is not on the critical path, and its compaction/tuning surface adds
ongoing operational work. We can revisit if benchmarks show MDBX as the
bottleneck.

**Live MPT in the sequencer.** Rejected: maintaining a Merkle Patricia Trie on
the hot path costs ~10× more writes per state change (every trie node up the
path is rehashed) and gives the sequencer nothing — validators recompute the
root from DA. The flat layout is what reth's "PlainState" uses for exactly
this reason.

**Per-tx WAL with explicit checkpoint cadence.** Rejected: MDBX commit *is*
the checkpoint. Splitting the durability boundary from the block boundary
creates two consistency horizons to reason about; folding them into one
(commit-at-seal) is strictly simpler.

**Per-tx block sealing (current behavior).** Rejected: breaks Ethereum
semantics, makes every tx pay a separate fsync, and prevents the natural
group-commit emerging from adaptive sealing. The current code uses per-tx
"blocks" only because there is no persistence today; the move to MDBX is a
natural point to fix it.
