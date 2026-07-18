# WP-J fixes — Java sealer-service + repo hygiene

Branch: claude/commit-review-30. All Java changes under `cluster/sealer-service/**`; hygiene changes to the index (`git rm -r --cached`) and root `.gitignore` only.

## F12.1 [H] FIXED — snapshot restore truncated at first fragment
- Files: `cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/SealerClusteredService.java` (`readSnapshot`/`writeSnapshot`), new test `cluster/sealer-service/service/src/test/java/io/kardamom/sealer/cluster/SnapshotRestoreTest.java`
- `loadFromSnapshot` replaced by static `readSnapshot(Image, IdleStrategy)`: polls through an `ImageFragmentAssembler`, concatenating assembled messages until end-of-stream, so any fragment count round-trips. Write side (`writeSnapshot`) now chunks the snapshot at `publication.maxMessageLength()`, so a snapshot of ANY dedup-window size round-trips (needed once the default window grew for F02.3). Regression test `snapshotLargerThanMtuRoundTrips` pushes a ~160KB snapshot (5000 ids) through a real embedded media driver at `mtu=4096` and asserts byte-identical reassembly + behavioural state equality; it fails with the old single-fragment poll.

## F12.2 [H] FIXED — unreadable/empty snapshot silently restarted at genesis; onTakeSnapshot swallowed offer failures
- Files: same `SealerClusteredService.java` (`onStart`, `readSnapshot`, `writeSnapshot`), test `SnapshotRestoreTest.emptySnapshotImageIsFatal`
- The genesis-fallback branch is gone: an image that closes before end-of-stream or carries zero bytes throws `IllegalStateException` (fail-stop instead of deterministic-state divergence). `onTakeSnapshot` (via `writeSnapshot`) now throws on any terminal offer result (`CLOSED`/`MAX_POSITION_EXCEEDED`) instead of exiting silently and recording an empty snapshot.

## F07.1 [H] FIXED — snapshot-restored member served bogus REPLAY_DONE for pre-snapshot ranges
- Files: `SealerClusteredService.java` (`onStart`), test `SnapshotRestoreTest.restoredMemberRefusesPreSnapshotReplay`
- On restore, retention floors are initialized from the restored state: `firstRetainedIndex = state.canonicalCount(); firstRetainedBlock = state.blockNumber()`. Deviation from the findings file's suggested `blockNumber() + 1`: `blockNumber()` is the NEXT block to be stamped (and therefore the first boundary this member can retain), so it is the exact floor; `+1` would falsely refuse a client that is exactly caught up to the restore point. The test pins both directions: a pre-snapshot cursor gets `REPLAY_UNAVAILABLE` carrying `(canonicalCount, blockNumber)`, and a cursor exactly at the restore point completes with `REPLAY_DONE`.

## F07.3 [M] FIXED — replay served synchronously on the cluster service thread (1s/frame deadline)
- Files: `SealerClusteredService.java` (`PendingReplay`, `handleReplayRequest`, `drainPendingReplays`, `serveReplayChunk`, `offerOnce`, `onTimerEvent`, `onNewLeadershipTermEvent`)
- Replay requests now only REGISTER a per-session `PendingReplay` cursor; frames are served in bounded chunks (256/event) of single non-blocking offers from a self-rescheduling ~1ms cluster timer (`REPLAY_TIMER_CORRELATION_ID`), which exists only while replays are pending and is re-armed across leadership changes. Back-pressure pauses the drain until the next timer event instead of spinning; a session accepting nothing for 5s is closed (never silently skipped). Eviction outrunning an in-flight replay converts it to an honest `REPLAY_UNAVAILABLE`. Sessions mid-replay are skipped by live record/boundary broadcasts (those frames are already retained and arrive via the drain, in order), which both avoids duplicate delivery and prevents replay back-pressure from tripping the live path's 1s deadline-then-close on a healthy catching-up client.
- Note: first attempt used `doBackgroundWork` per the findings file's suggestion; Aeron 1.44 rejects session offers there ("sending messages or scheduling timers is not allowed from doBackgroundWork" — confirmed by TestCluster run), hence the cluster-timer design (log-driven callbacks are the only sanctioned offer context).

## F07.5 [L] FIXED — MAX_POSITION_EXCEEDED returned without closing (zombie session)
- Files: `SealerClusteredService.java` (`offerToSession`, `offerOnce`)
- Both offer paths now close the session on any terminal result other than `CLOSED` (which means it is already closed). The old `offerBytesToSession` helper (the other zombie path) was removed with the synchronous replay loop.

## F12.6 [M] FIXED — CanonicalSealerState.load did not validate idCount
- Files: `cluster/sealer-service/core/src/main/java/io/kardamom/sealer/CanonicalSealerState.java` (`load`), tests in `CanonicalSealerStateTest` (`load_rejects_id_count_above_capacity`, `load_rejects_truncated_snapshot`)
- `load` now throws a descriptive `IllegalArgumentException` when `idCount` is negative, exceeds `dedupCapacity`, or exceeds the remaining buffer bytes (long math, no overflow). Chose fail-loud over the suggested optional evict-from-front for a smaller capacity: silently truncating the window would make dedup behaviour diverge from a fresh state with the same config — exactly the determinism hazard the finding describes; shrinking the window should be an explicit migration.

## F02.3 [M] FIXED (in-scope part) — 8192-id dedup window vs unbounded replica lag
- Files: `SealerClusteredService.java` (`DEFAULT_DEDUP_CAPACITY`), `ClusterNode.java` (sysprop default), `cluster/sealer-service/README.md` (new "Dedup window sizing" section)
- Default window raised 8192 → `1<<17` (131072): tolerated replica stall at 10k unique tx/s goes ~0.8s → ~13s, for ~20MB heap and a ~4MB snapshot (safe now that snapshot I/O is chunked, F12.1). The quantitative invariant (`dedupCapacity > worst-case stall × peak unique throughput`, all members must agree) is documented at the constant and in the README. Remainder is cross-WP and NOT done here: a sequencer-side freshness horizon (WP-SEQ code) and the spec doc (`docs/agents/replicated-sequencer-shards-spec.md`, unowned) still overstate the guarantee for live laggards.

## F01.2 / F05.2 / F06.2 [M] FIXED — tracked Gradle caches + build outputs
- `git rm -r --cached cluster/sealer-service/.gradle cluster/sealer-service/core/build` (34 files staged as deleted; `service/` had no tracked build outputs). The existing `cluster/sealer-service/.gitignore` already ignores `.gradle/`, `build/`, `**/build/`, so the paths stay untracked after local builds; `gradle/wrapper/gradle-wrapper.jar` remains tracked. Blobs persist in history (removal needs a history rewrite — out of scope, as the findings note).

## F12.7 [L] FIXED — committed Python bytecode
- `git rm -r --cached deploy/cluster/scripts/__pycache__` (1 file) and root `.gitignore` gains `__pycache__/` + `*.pyc`. Verified via `git check-ignore`.

## F12.12-java [N] FIXED — malformed-frame drops unmetered on the Java side
- Files: `SealerClusteredService.java` (`onMalformedFrame`)
- Both silent-drop paths in `onSessionMessage` (short ingress envelope, short replay request) now bump a counter and print a grep-able stdout line (`cluster DROPPED malformed …type… totalDropped=N`), throttled to power-of-two counts so a framing-mismatch flood cannot drown the chaos suite's stdout signals. (The Rust decode-path counters belong to other WPs.)

## Verification
Toolchain: OpenJDK 17.0.19 via the repo's Gradle 8.7 wrapper.
- `./gradlew :core:test --rerun-tasks` — 12/12 PASSED (incl. 2 new F12.6 tests)
- `./gradlew :service:test :core:test` — BUILD SUCCESSFUL; service 12/12 PASSED: `ClusterNodeTest` (6), `SealerClusterFailoverTest.egressContinuesGaplesslyAcrossLeaderKill`, `SealerReplayTest` (2, now exercising the timer-driven incremental drain end-to-end in a 3-member TestCluster), new `SnapshotRestoreTest` (3)
- `git check-ignore` confirms all untracked artifact paths are ignored; `git ls-files` confirms only `*.gradle` build scripts and the wrapper jar remain tracked under `cluster/sealer-service`.
