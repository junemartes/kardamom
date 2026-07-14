# Branch-Node-Incremental State Trie — Design

**Date:** 2026-06-23
**Status:** Approved (brainstorm) → planning
**Stacks on:** `claude/validator-nodes` (PR #63) — depends on the validator's trie-aware writer.
**Scope:** Replace the validator's O(all-accounts)-per-block state-root rebuild with a
branch-node-incremental trie (reth's state-root model on `alloy-trie` primitives),
covering both the world-state account trie and per-account storage tries.

---

## 1. Motivation

The #63 validator computes a canonical Ethereum MPT state root, but the implementation
rebuilds the **account trie over every account each block** (and each touched account's
storage trie over all its slots). That is O(all accounts)/block — the one scaling limit
#63 explicitly called out. This work makes root computation **~O(changed keys)/block** by
storing intermediate trie nodes and updating only touched paths.

## 2. Goals / Non-goals

**Goals**
- Node-incremental computation of the world-state root for **both** the account trie and
  per-account storage tries, persisted in the validator's libmdbx, atomic per block.
- Identical root **value** to the #63 rebuild (canonical Ethereum secure-trie root) —
  proven by an incremental-vs-rebuild equivalence test.
- An opt-in/sampled **shadow-check** runtime safety valve (fail-stop on mismatch).
- A few **e2e smoke tests** proving correctness under real cluster load.

**Non-goals**
- Changing the executor (it stays trie-free; v0 emits no root).
- In-place migration from a #63-format DB (validators run from genesis; fresh DB only).
- Parallel execution (separate spec/PR).
- Light-client proof serving.

## 3. Key decisions (resolved during brainstorm)

| # | Decision | Choice |
|---|----------|--------|
| 1 | Trie model | **reth model**: stored `BranchNodeCompact` intermediate nodes + a hashed-state mirror, built on `alloy-trie`'s `HashBuilder` (alloy-trie ships no walker, so we build it). |
| 2 | Scope | **Account + storage tries** both node-incremental (shared walker). |
| 3 | Safety | **Opt-in / sampled shadow-check**: incremental by default; a flag recomputes via full rebuild every Nth block and asserts equality, fail-stop on mismatch. |
| 4 | Migration | Fresh-from-genesis only; bump `SCHEMA_VERSION`; old DBs refused. |

## 4. Storage model (new tables in `kardamom-state`)

Stored intermediate nodes (branch nodes only), keyed by trie **path**:
- `account_trie`: `packed_nibbles(path) → BranchNodeCompact`
- `storage_trie`: `account_hash(32) ++ packed_nibbles(path) → BranchNodeCompact`

Hashed-state mirror (the leaves), keyed by **keccak**:
- `hashed_accounts`: `keccak(addr)(32) → {nonce, balance, code_hash, storage_root}`
- `hashed_storage`: `account_hash(32) ++ keccak(slot)(32) → U256`

**Why the mirror is required:** the walk feeds `HashBuilder` leaves in trie order
(`keccak(key)`), but `accounts`/`storage` are keyed by raw address/slot and keccak is not
invertible — a leaf's value cannot be read back from its hashed key without it. The raw
tables remain (revm reads state by raw address during execution); the hashed tables are an
**additional** mirror the writer maintains in the same txn. Cost: ~2× the accounts+storage
on-disk footprint and a few extra writes/block — an easy trade off the hot path. The #63
per-account `storage_root` field folds into `hashed_accounts`.

## 5. Components (all new, under `crates/state/src/trie/`)

- `node.rs` — `BranchNodeCompact` ⇄ mdbx encode/decode (`state_mask`/`tree_mask`/
  `hash_mask` u16 masks, `hashes: Vec<B256>`, `root_hash: Option<B256>`).
- `cursor.rs` — `TrieCursor` (stored branch nodes, `seek`/`next`) + `HashedCursor`
  (leaves in hashed order), both over mdbx cursors (storage variants range-scoped by
  `account_hash`).
- `walker.rs` — `TrieWalker`: drives `HashBuilder` from the cursors + a `PrefixSet`,
  emitting `(root: B256, updates: TrieUpdates)`. `TrieUpdates` = branch nodes to upsert
  (from `HashBuilder::split()`) + paths to delete (emptied/collapsed subtries).
- `prefix_set.rs` — `PrefixSet` built from a block's changed keys.
- `mod.rs` — `StateRoot::incremental(txn, prefix_sets) -> (B256, TrieUpdates)`; retains the
  existing rebuild functions (`state_root`, `storage_root`) as the shadow-check oracle.

## 6. Algorithm (per block, one mdbx txn)

1. **Prefix sets** from the `BlockDelta`: account set = `keccak(addr)` for every changed
   account **and** every account whose `storage_root` changes; per-account storage set =
   `keccak(slot)` for that account's changed slots.
2. **Update the hashed mirror**: write/delete `hashed_accounts` + `hashed_storage` rows
   (zeroed slot / selfdestructed account ⇒ delete).
3. **Storage tries first**: for each account with storage changes, walk its storage trie
   with its prefix set → new `storage_root` + storage `TrieUpdates`; stamp `storage_root`
   into that account's `hashed_accounts` row; apply updates to `storage_trie`.
4. **Account trie**: walk the account trie with the account prefix set → new state root +
   account `TrieUpdates`; apply to `account_trie`; write `meta[KEY_STATE_ROOT]`.

**Walker.** Guided by the prefix set: a subtrie whose path is **not** in the set is
untouched → `HashBuilder::add_branch(path, stored_hash, stored_in_database=true)` (skip the
whole subtrie via the stored node's hash); a subtrie **in** the set is descended — emit its
hashed leaves via `add_leaf(keccak_key, value)`, recurse into sub-branches (stored hashes
for unchanged ones). `HashBuilder::root()` = new root; `split() → HashMap<Nibbles,
BranchNodeCompact>` = nodes to upsert; emptied/collapsed subtries are deleted. Net work
~O(changed keys + touched branch nodes).

**Deletions/collapses** fall out naturally: the removed leaf's prefix is in the set, the
hashed cursor doesn't yield it, `HashBuilder` rebuilds the parent without it (collapsing
single-child branches). Implementation may use reth's seek-driven cursor state machine or
an equivalent prefix-set-guided recursion; both are incremental, and the descend-vs-skip +
deletion logic is the entire correctness surface.

## 7. Writer integration, shadow-check, config

`StateWriter::spawn_with_trie(env, TrieMode)` replaces #63's rebuild with
`StateRoot::incremental`, in the existing single atomic txn. `TrieMode`:
- `Incremental` (default) — incremental only.
- `ShadowCheck { every_n }` — also full-rebuild every Nth block and assert equality;
  **fail-stop on mismatch** (writer returns error → validator exits non-zero, same as a
  divergence).

Surfaced on the validator binary as `--trie-shadow-check[=N]`. The executor's plain
`StateWriter::spawn` is unchanged (no trie). `seed_genesis` is extended to populate the
hashed mirror and the initial account/storage tries so the genesis state root is correct.

## 8. Errors / migration

- Walker invariant violations (a referenced stored node is absent) and shadow mismatches
  are fatal — they halt the writer (crash-loop for the orchestrator), never silently
  produce a wrong root.
- `SCHEMA_VERSION` bumped; the existing schema-version check refuses an old-format DB.
  No in-place migration; fresh-from-genesis validators only.

## 9. Testing

- **Unit:** `BranchNodeCompact` encode/decode round-trip; `TrieCursor`/`HashedCursor`
  seek/iter; walker on tiny tries (insert/update/delete/collapse).
- **Incremental-vs-rebuild equivalence (core):** apply N randomized blocks incrementally;
  after each, assert incremental root == full rebuild over `hashed_accounts`/
  `hashed_storage`. Cover deletes, storage-only changes, account create/destroy,
  single-child collapse, and empty→non-empty→empty transitions.
- **Known vectors:** empty trie root (`EMPTY_ROOT_HASH`); genesis state root vs the #63
  rebuild for a known alloc.
- **Writer:** the #63 3-block trie-writer test, now on the incremental path, incl.
  persistence across reopen.
- **Shadow-check:** an injected wrong root (test hook) ⇒ assert fail-stop.
- **e2e smoke (goal):** run the validator cluster smoke test with `--trie-shadow-check`
  enabled; assert it still syncs from genesis, keeps up within the lag bound, reports
  **zero divergence and zero shadow-mismatch**, and advances its state root — proving the
  incremental trie yields correct roots under real load.

## 10. Risks

- **Walker correctness** (descend/skip, deletion/collapse) is the dominant risk →
  exhaustive randomized incremental-vs-rebuild equivalence + the runtime shadow-check.
- **Hashed-mirror consistency** with the raw tables → both written in the same atomic txn
  from the one `BlockDelta`; covered by the writer/persistence tests.
- **Storage footprint** ~2× accounts+storage → accepted (off hot path).
