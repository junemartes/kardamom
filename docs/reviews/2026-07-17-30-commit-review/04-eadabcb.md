# 04 eadabcb — docs: failure-modes page + architecture/state diagrams; chaos: archive-driver-loss case (#73)

## Summary of change
Adds `docs/failure-modes.md` (per-actor failure/recovery behavior with the chaos cases that verify each, plus a known-gaps list) and four JPG diagrams under `docs/img/`, linked from the README. Adds an `archive-driver-loss` chaos case that hard-kills the `archiving-media-driver` task of the `aeron` system job under ingress-0 and asserts pipeline progress via active/active failover, aeron-job recovery within the restart SLO, and ingress back to full strength; wires it into the chaos-ingress CI shard. Also fixes the stale "resume is Phase 2" doc comment in kardamom-executor.rs and adds validator + engine to the README crate list.

## Accuracy check against HEAD
- `inject_hard kardamom-ingress-0 archiving-media-driver` matches the real task name (`deploy/cluster/nomad/aeron.system.nomad.hcl:64: task "archiving-media-driver"`, job "aeron" line 29).
- The executor doc-comment fix is accurate: crash-recovery resume exists (`crates/state/src/recovery.rs` reads `last_committed_block` / `last_committed_end_tx_position`, exactly the key names failure-modes.md cites; `crates/state/src/meta.rs` confirms the meta keys).
- README crate additions (validator, engine, cluster-adapter/client, e2e) match `crates/` at HEAD.
- The sequencer section of failure-modes.md as written here ("×2, sharded by sender", "crash stalls its shard") was accurate at the commit and was properly updated by f2accb1 (#75, racing replicas) — no drift left at HEAD.
- The known-gaps section is honest and still correct at HEAD (no archive-volume-wipe case, no L1-outage case, no divergence-injection e2e; validator-lapse added later by 70d0823 does not cover divergence injection).

## Findings

### F04.1 [low] [logic] — archive-driver-loss baseline can degrade the case to a no-op assertion
- **Where**: deploy/cluster/scripts/chaos.sh (archive-driver-loss arm; at HEAD ~lines 465-480, `aeron_base="$(count_running aeron)"`)
- **What**: `assert_count aeron "${aeron_base}" ...` uses a baseline captured moments before the kill. If the nomad query hiccups and returns 0 (count_running pipes through `2>/dev/null`), the post-kill assertion `>= 0` passes trivially and the case silently loses its "driver restarted" leg. The case also never asserts the count actually dropped, so a kill that misses (e.g. filter matches nothing because `inject_hard`'s `docker ps --filter` found a different container) is only caught indirectly via `inject_hard`'s own failure check.
- **Still present at HEAD**: yes
- **Suggested fix**: Fail fast if `aeron_base` is empty or 0 before injecting, e.g. `[ "${aeron_base:-0}" -gt 0 ] || fail "..."`.

### F04.2 [nit] [quality] — ~1MB of JPG diagrams committed without editable source
- **Where**: docs/img/*.jpg (at the commit and HEAD)
- **What**: Four raster diagrams (~984KB total) are committed with no source (mermaid/drawio/svg). They already churned once (f2accb1 re-rendered two of them, +8KB history each time); every architectural change re-adds full-size binaries to history, and the diagrams can only be updated by whoever holds the original tooling.
- **Still present at HEAD**: yes
- **Suggested fix**: Commit the diagram source alongside (or instead of) the JPGs, or use mermaid in the markdown where feasible.

## Verdict
A genuinely good docs commit: failure-modes.md is specific, grounded in real case names, code paths, and metric semantics that all verify against HEAD (task names, meta cursor keys, chaos case wiring), it candidly lists the untested surface, and the drive-by executor doc-comment fix removes real drift. The new chaos case tests a previously uncovered shared-failure-domain scenario and its assertions are ordered sensibly. Only minor robustness (baseline capture) and repo-hygiene (raster-only diagrams) quibbles.
