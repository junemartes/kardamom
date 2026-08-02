# Allocation report — execution path, per op family (2026-08-03)

Method: DHAT (per-callsite attribution) over the production `ExecScope`
path, 3,168 txs per family through `execute_tx` on `MockStateDatabase`.
Families isolate contract-execution cost from engine fixed cost:
`KARDAMOM_PROFILE_OPS = mix | swap | vault_deposit | vault_withdraw |
clob_place | clob_cancel | transfer` (bench/tests/alloc_profile.rs).

## Headline

Two rounds of squashing, measured at the mix:

| stage | allocs/tx | bytes/tx | wall/tx | 1-core Mgas/s |
|:--|--:|--:|--:|--:|
| pre-ExecScope | 88.0 | 421,452 | 276 us | 161 |
| ExecScope (block-scoped EVM+cache) | 22.2 | 9,356 | 75 us | 597 |
| + streamed hash + created-only code | **18.2** | **5,398** | **63.5 us** | **~702** |

Total: **-98.7% bytes/tx, 4.3x single-core execution throughput.**

## Per-family (after all fixes)

| family | allocs/tx | bytes/tx | wall/tx | gas/tx |
|:--|--:|--:|--:|--:|
| transfer | 6.1 | 2,660 | 20.3 us | 21,000 |
| vault_deposit | 19.1 | 4,952 | 63.2 us | 39,113 |
| clob_cancel | 16.3 | 5,475 | 57.4 us | 54,294 |
| mix | 18.2 | 5,398 | 63.5 us | 44,569 |
| swap | 20.1 | 5,969 | 67.5 us | 44,773 |
| clob_place | 27.9 | 7,940 | 103.6 us | 90,341 |

The suspicion that contract execution hid unmeasured allocations was
right: a CLOB place allocated 4.5x a transfer (now 3x). The engine fixed
cost is the transfer row; everything above it is contract-proportional
(slots touched, accounts loaded, logs emitted).

## What was found and fixed this round

1. **`WriteSet::hash` serialized the ENTIRE write set — including full
   contract BYTECODE — into a per-tx buffer** (3.1KB/tx on clob_place,
   23% of all bytes; the single largest site). Fixed: streamed into
   `Keccak256::update` field by field. Byte sequence identical, hash
   unchanged, zero allocation.
2. **`write_set_from_evm_state` copied the bytecode of every contract
   merely CALLED** (revm populates `info.code` on load; the capture
   couldn't tell loaded from created — 1.6KB/tx, 12%). Fixed: capture
   only for `account.is_created()` — a called contract's code is already
   durable; only CREATE introduces bytes the delta must carry. NOTE:
   consensus-affecting (write-set hashes change vs old binaries) —
   executor and validator deploy together, as this repo does.

## Where the remaining ~5.4KB/tx (mix) lives

| owner | ~bytes/tx | site | squash path |
|:--|--:|:--|:--|
| kardamom `WriteSet` | ~2,700 | three `BTreeMap`s allocate 1KB-class leaf NODES for a handful of entries each (accounts 1,024B + storage 936B + code 720B) | replace with sorted `SmallVec`s — determinism via sort-on-build; the biggest remaining win, touches WriteSet consumers |
| revm journal | ~1,400 (2 allocs) | per-touched-account storage `HashMap` inside `JournaledAccount`, rebuilt per tx (`sload` path) | upstream: journal map pooling across `transact` calls on a kept EVM; worth an issue/PR to revm |
| revm state | ~724 | `outcome.state` HashMap (journal finalize) | inherent to the transact API; pooling upstream |
| revm boxes | ~500 | `Box<AccountInfo>` x3, `Box<CallInputs>` per call frame | upstream small-box arena; low priority |
| logs | ~320 (swap) | `Vec<Log>` + topic/data bytes per emitted event | inherent (receipts carry them); could pool |

Engine-side the floor is now roughly the `WriteSet` BTree nodes; after a
SmallVec conversion the path would be ~2.5KB/tx mix, dominated by revm
internals — at which point further gains belong upstream.

## Reproduction

```
cd crates/bench
KARDAMOM_PROFILE_OPS=clob_place cargo test --test alloc_profile --release -- --ignored --nocapture
# per-callsite data: dhat-heap-clob_place.json (view with dhat/dh_view.html)
```
