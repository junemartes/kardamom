# Fixes — WP-TRIE (state trie + obs)

## F09.1 [H/logic] — walker removals miss stored nodes under extensions → FIXED

**Files**: `crates/state/src/trie/walker.rs`, `crates/state/src/trie/mod.rs`

Root cause confirmed: `tree_mask` bit `i` means "the child *subtree* contains stored
nodes" (alloy-trie sets the parent bit via extension propagation in
`store_branch_node`/`update_masks`), so a stored node can live at a deeper path than
`parent+[i]`. When the exact-path `get_branch_node` misses and the subtrie is rebuilt
from leaves, `removals = visited − updated` can never delete those deeper nodes —
they become stale orphans a later walk can exact-hit and `add_branch`-skip via stale
hashes (silent root divergence).

Fix (the findings file's first suggested option): the walker now records every
exact-get-miss path in a new `TrieUpdates::cleared` list (`WalkLog` replaces the bare
`visited` vec), and `apply_trie_updates` range-deletes each cleared prefix (via the
existing `del_prefix`) **before** applying upserts, in the same txn — so no stored
node under a rebuilt-from-leaves region can outlive the rebuild. The visited−updated
exact removals are kept for collapsed nodes that *were* visited. Since orphans can no
longer persist, the mask/table invariant ("tm bit clear ⇒ nothing stored under that
child") holds inductively and the skip path only ever consumes nodes produced by the
latest build of their region.

Proven by the new regression test (below): with the prefix-clear temporarily
disabled, `extension_collapse_regrow_no_stale_orphans` fails first at the
orphan assertion ("stale orphan left at [n0,n1]") and — with that assertion
neutralized — at "root mismatch at block4" (the silent wrong-root), exactly the
predicted failure chain. With the fix, all tests pass.

## F09.3 [L/logic] — `let _ = txn.del(...)` swallows all mdbx errors → FIXED

**Files**: `crates/state/src/trie/mod.rs` (`update_for_block`, hashed_storage zero-slot
delete + hashed_accounts empty-account delete)

Both sites now match `MdbxError::NotFound` (tolerated) and propagate every other
error, same as the sibling `del_prefix`/`apply_trie_updates` code.

## F09.4 [L/quality] — differential test drove a reimplementation over a too-small pool → FIXED

**Files**: `crates/state/src/trie/incremental_tests.rs`

- `apply_block` now builds a real `kardamom_types::BlockDelta` (account deletes as
  EIP-161-empty upserts, exactly what the executor emits) and calls the production
  `trie::update_for_block` — the test-local copy of the mirror/delete logic and its
  duplicated `del_prefix` are gone, so the gate can no longer drift from production.
- Pool grown to 160 addresses / 10 slots over 80 blocks with 1-in-4 deletes, so
  extension geometry and collapse/regrow cycles actually occur.
- New orphan detector `assert_node_tables_match_fresh_build`: every 10 blocks (and
  at the end) the cumulative model state is replayed into a fresh env as one block
  and both node tables must be byte-identical to the incrementally-maintained ones
  (stored branch nodes are a pure function of the leaf set — empirically confirmed).
- New deterministic regression `extension_collapse_regrow_no_stale_orphans`: mines
  addresses whose keccak'd keys share 1–4 leading nibbles to construct
  stored-branch-under-extension → collapse-with-drift → regrow-with-tree_mask-into-
  the-orphaned-region → stale-hash-skip, asserting the oracle root each block, the
  orphan's absence after the collapse, geometry sanity (a stored node really is at
  the 2-nibble path under an extension), and final table equality.

## F09.5 [L/quality] — "~O(changed keys)" doc overstates → FIXED (docs)

**Files**: `crates/state/src/trie/mod.rs` (module doc), `crates/state/src/trie/walker.rs`
(module doc)

Module doc now states the real cost model: the skip only fires for stored nodes at
exact child paths with hash bits set; extension-shaped children and subtries whose
nodes are stored deeper get re-walked from leaves even when unchanged, so
small/sparse tries (and any trie whose top-level node is an extension) repeatedly pay
full-subtree rebuilds. The walker header documents the new clearing scheme. No
algorithmic change attempted (would be the reth range-seek cursor redesign).

## F09.6 [N/quality] — schema.rs key-encoding comment wrong (+ node_key duplication) → FIXED (owned part)

**Files**: `crates/state/src/schema.rs`, `crates/state/src/trie/cursor.rs`,
`crates/state/src/trie/mod.rs`

Comment corrected: node keys are raw **unpacked** nibbles (one per byte), not
"len-prefixed packed nibbles", with a pointer to `trie/cursor.rs::node_key`. That
`node_key` is now `pub(crate)` and reused by `apply_trie_updates` (duplication
removed; the third inline site in `update_for_block` builds *hashed-mirror* keys, a
different format, so it stays). The `ci-cluster.sh` comment half belongs to WP-OPS.

## F24.1 [N/quality] — no layout coverage for live at-rest encoders → FIXED

**Files**: `crates/state/src/schema.rs` (tests)

Added `block_key_layout_is_pinned` and `header_value_layout_is_pinned`, asserting
`encode_block_key` / `encode_header_value` against literal byte arrays (BE fields,
reserved tail zeroes) — no decoder needed.

## F13.7 [L/logic] — changed `--chain` genesis silently ignored → FIXED

**Files**: `crates/state/src/genesis.rs`, `crates/state/src/meta.rs` (new
`KEY_GENESIS_DIGEST`), `crates/state/src/error.rs` (new `StateError::GenesisMismatch`)

`seed_genesis` now computes an order-insensitive keccak digest of the allocations
(sorted accounts: address/nonce/balance/code_hash; sorted code hashes — bytecode is
content-addressed so the hash pins the bytes), persists it in the same atomic seed
txn, and on any later start with the flag present verifies the supplied alloc against
it — mismatch fails startup with `GenesisMismatch` instead of silently running on
divergent genesis. Envs seeded before the digest existed get it backfilled on the
next start (cannot verify retroactively; noted in the code). Tests:
`seed_genesis_rejects_changed_alloc` (incl. order-insensitivity),
`seed_genesis_backfills_missing_digest`.

## F13.8 [N/quality] — misleading crash-safety comment in seed_genesis → FIXED

**Files**: `crates/state/src/genesis.rs`

Comment now states the truth: flag + digest + allocations commit in one RW txn, a
mid-seed crash aborts everything, and put order within the txn is irrelevant.

## F03.2 [M/logic] — exporter bind failure returns Ok from init → FIXED (finding premise incorrect at HEAD; behavior pinned by regression test)

**Files**: `crates/obs/tests/init_port_in_use.rs` (new), `crates/obs/src/lib.rs` (comment)

The finding's premise ("the HTTP listener binds when the exporter future is first
polled, after init returned Ok") is **wrong for the dependency version in use**:
metrics-exporter-prometheus 0.18.3's `new_http_listener` calls
`std::net::TcpListener::bind` synchronously inside `PrometheusBuilder::build()`
(verified in the crate source and empirically), so a port collision reaches
`ready_tx` and `init` returns `Err` — no production change needed. Because older
versions *did* defer the bind (likely the source of the finding), the behavior is now
pinned: the new `init_fails_fast_when_port_already_bound` integration test holds a
bound port and asserts `init` fails with the build error, so a dependency upgrade
that regresses to lazy binding fails CI here. The lib.rs comment documents that the
fail-fast guarantee includes the bind and what to do if the test ever breaks.

## F03.3 [N/logic] — TOCTOU free-port pattern in obs test → FIXED

**Files**: `crates/obs/tests/init_without_runtime.rs`

The pick-then-rebind is now retried (up to 5 attempts, fresh ephemeral port each
time) — safe because a bind failure happens before the global recorder is installed.
Stale "binds asynchronously" comment corrected (bind is eager; only the accept loop
starts asynchronously).

## Verification

All commands run from the repo root (shared target dir):

- `cargo test -p kardamom-state` — **pass** (42 lib tests incl. the new/extended trie
  differential + regression tests, genesis digest tests, schema layout tests; all
  integration suites pass; docker-gated suites skip as before).
- `cargo test -p kardamom-obs` — **pass** (5 integration tests incl. new
  `init_port_in_use`; dashboards test ok).
- `cargo check -p kardamom-state -p kardamom-obs --all-targets` — clean, no warnings.
- `cargo check -p kardamom-executor -p kardamom-validator` — clean (downstream
  consumers of the changed `StateError`/`TrieUpdates`/genesis API).
- Negative proof for F09.1: temporarily disabling the prefix-clear in
  `apply_trie_updates` makes `extension_collapse_regrow_no_stale_orphans` fail with
  "stale orphan left at [n0,n1]…(F09.1)"; with the orphan assertion also neutralized
  it fails with "root mismatch at block4" (silent divergence reproduced end-to-end).
  Both temporary edits reverted.

No Cargo.toml changes were needed.
