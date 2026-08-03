# Chaos + cluster test coverage audit (2026-08-03)

Scope: does the suite actually exercise (A) participants joining mid-run with
empty state, (B) long service halts, (C) data-corruption detection?

Method: full inventory of `deploy/cluster/scripts/chaos.sh` + the CI shards,
of the non-chaos layers (`crates/e2e`, `ci-cluster.sh`), and of every
recovery/join/integrity mechanism in product code — then matched
mechanism → test. Line refs are against `3cfd1e0`.

---

## Verdict in one line per axis

| Axis | State |
|---|---|
| **A. Empty-state join** | Covered for executor + validator. **Absent for the Raft cluster member, the sequencer, the batcher and the da-watcher.** |
| **B. Long halts** | Longest deliberate fault is **30 s**. Nothing crosses the 90 s session timeout or the ~321 s retention window ⇒ **the whole `REPLAY_UNAVAILABLE` recovery tier has never executed.** |
| **C. Corruption** | Exactly **one** true byte-corruption case (archive segments). State DBs, checkpoints, the Raft log and DA blobs are never corrupted — and **DA blobs are not verifiable even in principle** (see P0-1). |

---

## P0 — product defects (not test gaps)

### P0-1. DA blobs are never checked against their commitments

`crates/batcher/src/da_store.rs:73-87` validates a fetched blob's **length
only**; `crates/batcher/src/l1.rs:150-164` (`recover_blocks`) then feeds the
bytes straight into reconstruction. Nothing recomputes the KZG commitment or
compares it to the versioned hash L1 committed to.

A DA store that returns right-length wrong bytes is silently accepted, and
`kardamom-reconstruct` rebuilds a **wrong chain** from it. The versioned hash
is right there in the `BatchPosted` event — the check is cheap and the whole
point of the DA design is that L1 pins the content. This is the bottom of the
recovery stack (`docs/failure-modes.md`: "even if every in-cluster durable
copy is lost…"), so an unverified fetch undermines the backstop.

Fix: recompute commitment → versioned hash on fetch; mismatch is a hard error.
Test: flip a byte in a stored blob, assert reconstruct refuses (the S8
non-vacuity control already proves the *gate* can fail — this proves the
*content* check exists).

### P0-2. Peer checkpoints are adopted without an integrity check

`crates/state/src/checkpoint_transfer.rs:130-209` fetches over plain HTTP/1.0
to `.fetch.tmp` + rename. No checksum, no length authentication, no chain
identity. Since #143 the **validator** adopts these too, then bootstraps its
trie from them — so a truncated or corrupted transfer becomes the validator's
state, and its own shadow-check cannot object (it rebuilds from the same
adopted mirror).

Related and already known: checkpoint restore has **no chain-identity binding**
(memory: recovery-C follow-up — a stale checkpoint from a previous chain was
adopted and wedged a node). Same fix shape: stamp checkpoints with
`(genesis digest, chain id, block, content hash)` and verify on restore.

---

## A. Empty-state join

### Covered
| Component | Case | Strength |
|---|---|---|
| Executor | `state-checkpoint-restore` (`chaos.sh:1104`) | Strong, but harness *hands it* the peer checkpoint |
| Executor | `replay-window-resync` (`chaos.sh:1168`) | Strong self-heal: greps both `fetched checkpoint from peer` and `restored state from checkpoint` |
| Validator | `validator-join` (`chaos.sh:391-481`) | Strong: adoption + `trie bootstrap complete` greps, lag ≤ 25, root observed, 0 divergences |

### Gaps, worst first

**A-1. The Aeron cluster (Raft) member — the ordering authority.** No case
wipes a member's `cluster/` + `archive/` dirs. Catch-up would run through
Aeron's snapshot/log replication (`ClusterNode.java:124-137`), and:

- **nothing in the repo ever takes a snapshot** — no `snapshotIntervalNs`, no
  `ClusterTool snapshot` in `deploy/`, `.github/` or the Java contexts. The
  snapshot write/restore path (`SealerClusteredService.java:225-243`,
  `:488-510`) therefore fires only on a manual admin action;
- the one multi-member failover test **explicitly disclaims snapshots**
  (`SealerClusterFailoverTest.java:332-342`: "DO NOT reuse this wrapper as-is
  for any snapshot/recovery test"), so C2/C3 are stub-cluster-only
  (`SnapshotRestoreTest`).

So the deployed cluster has never restored a member from a snapshot, and a
member joining with an empty dir is untested end-to-end. `docs/reviews/…
07-52a32a2.md:12` already records that a silent-gap bug (F07.1) survived the
kill-heavy chaos suite precisely because snapshots never happen.

**A-2. Sequencer.** No empty-state join case. The product admits the
degradation in a startup log (`kardamom-sequencer.rs:205-211`): a restarted
replica seeds every floor at 0 and **publishes nothing for established
senders** until receipt floors catch up per-sender — and a sender that goes
quiet is never recovered by that replica. Unit-pinned
(`replicated_shard_racing.rs:230`) but never demonstrated in-cluster;
`assert_replica_healthy` (`chaos.sh:711-727`) only counts metric lines and is
documented as not asserting republication.

**A-3. Batcher.** Zero chaos cases, and it became a live service *today*
(#39). Cold start with no cursor, the L1-reconcile matrix and the
foreign-writer fail-stop are unit-tested (`live.rs:391`, `:407`,
`anvil_e2e.rs:157`) but never exercised in-cluster; `run_feed` itself has no
test at all.

**A-4. da-watcher.** Zero chaos cases; zero references in `chaos.sh`.

**A-5. Ingress late-join on tx_data (#31).** Issue #31 is referenced nowhere
in the repo; no test covers a replica joining a live multicast stream and
missing pre-subscription history.

---

## B. Long halts

Longest **deliberate, fixed-parameter** fault in the suite: **30 s**
(`LAPSE_S`, `SEQ_LAPSE_S`). Longest actual outage: `archive-corruption`'s node
drain (minutes, unbounded by any timer) and quorum-loss's second sealer node
(~380 s worst case) — but neither is *measured* as a recovery clock.

| Threshold | Value | Crossed by a deliberate fault? |
|---|---|---|
| Aeron client liveness | ~10 s | **Yes** — both 30 s freezes (3×). This is the crash-only path the lapse cases rely on. |
| Cluster session timeout | **90 s** (`ClusterNode.java:175`) | **No.** |
| Cluster retention | 65,536 frames ≈ **321 s** @200 tps (`SealerClusteredService.java:109`) | **No.** |

**B-1. The `REPLAY_UNAVAILABLE` recovery tier has never run.** Verified by
grep: no assertion anywhere in `deploy/cluster/scripts/` or `crates/e2e/`
references `REPLAY_UNAVAILABLE` or `resync_total` — only comments. Untested
consequences:

- executor exit-time repair (`kardamom-executor.rs:538-597`) — recovery-D;
- validator exit-time repair (`kardamom-validator.rs:709-772`) — **added today
  as #143**;
- batcher fail-stop on unpostable ordering (`live.rs` → `bin:436-450`);
- `park_state_db` (`checkpoint.rs:215`) — used by both repairs, zero tests.

And the case *named* for it, `replay-window-resync`, doesn't exercise it: it
wipes the state dir, so the restart takes the **cold-start** peer-fetch branch
(`kardamom-executor.rs:233-245`), never the refusal path. Its own comment
claims retention exhaustion; the mechanism is a different one.

**B-2. Session expiry is untested, and the comment is wrong.**
`chaos.sh:1614` says "a >15s total outage exceeds the session timeout"; the
configured value is **90 s**. Nothing holds a client out that long, so session
expiry → re-establishment → replay-from-cursor is unexercised. (Note the #141
wedge lived exactly in this neighbourhood.)

**B-3. Quorum loss can't exhaust retention by construction** — the canonical
stream stops advancing while quorum is lost, so no frames are retained. Any
retention drill must halt a *consumer*, not the cluster.

---

## C. Corruption detection

**Covered:** `archive-corruption` (`chaos.sh:1364-1536`) — a 16-byte,
length-preserving flip inside a real data frame's payload, detected by
CRC-armed `ArchiveTool verify`, then `--diff` names it, `--heal` repairs it,
post-heal verify is clean. This is a genuinely strong closed loop (and the
picker was hardened in #126).

**Never corrupted anywhere:**

| Surface | Status |
|---|---|
| Executor/validator mdbx bytes | Only unit-level (`integrity.rs:529`); never in-cluster |
| **Checkpoints in transit / at rest** | Never — and now adopted by the validator (P0-2) |
| Raft log / snapshot on a sealer | Never |
| In-flight receipts / tx payloads on multicast | Never |
| State trie / hashed mirror | Only the shadow-check's happy path (`writer.rs:502`); the mismatch branch is never triggered |
| **DA blobs** | Never — and unverifiable by design (P0-1) |

**C-1. The corrupt-BAL divergence drill is Target-L only.** S7
(`divergence.rs`) proves the latch fires and the validator exits 2 — on the
single-host stack. The cluster only asserts `divergence_total == 0`; it never
injects a divergence, so the in-cluster tripwire is unproven.

**C-2. The cluster never verifies its own state or its L1 recoverability.**
`kardamom-statecheck` and `kardamom-reconstruct` are **built** by
`cluster-e2e.yml:121` but invoked by no cluster script — they run only at
Target L. So no cluster run ever checks state-DB integrity, executor↔validator
byte parity, or that the chain is rebuildable from L1.

---

## D. Vacuity + hygiene findings (existing assertions that can pass wrongly)

| # | Finding | Where |
|---|---|---|
| D-1 | `assert_executor_stalled` defaults **both** samples to 0 on scrape failure and passes on `e1<=e0` — the core assertion of `cluster-quorum-loss-recover` (that the pipeline must NOT progress) is vacuous exactly when the exporters black out, which is what a two-node kill causes | `chaos.sh:897-905` |
| D-2 | `val_metric` ends `|| true` and callers use `${x:-0}`, so every `divergence == 0` check passes on a failed scrape — `validator-lapse`, `validator-join`, and `ci-cluster.sh:783` | `chaos.sh:188-199` |
| D-3 | `graceful-ingress` asserts `>= 1` running ingress out of a `count = 2` job with no killed-marker ⇒ the **untouched peer satisfies it on the first poll**; the killed replica's return is never observed | `chaos.sh:1054`, `ingress.nomad.hcl:49` |
| D-4 | Trailing `assert_count ingress 2` in both archive cases runs after the markers were consumed ⇒ bare count, passes if the task never died | `chaos.sh:1252`, `:1361` |
| D-5 | `node-failure-executor`'s first `assert_count executor 2` is a bare count satisfied by the two survivors — it never observes the outage | `chaos.sh:1094` |
| D-6 | `graceful-sequencer` / `hard-sequencer` don't pin the load to the killed shard ⇒ ~50 % of runs kill a replica the load never uses | `chaos.sh:986-996` |
| D-7 | The `semantics` shard gets the **weak** validator verdict (forward progress only, no lag bound) purely because it sets `RUN_LOAD=0` | `ci-cluster.sh:743-776` |
| D-8 | Load shard's must-deliver is downgraded to a warning when all drop counters read 0 (`ack_proves_receipt`) — guarded, but it means `missing>0` no longer fails the shard | `accounting.rs:207-232` |
| D-9 | `sealer-graceful` / `sealer-hard` are dead cases targeting a job that no longer deploys; they'd `fail` immediately if ever run | `chaos.sh:1079`, `:1087` |
| D-10 | `chaos.sh`'s header case list and `chaos-iter.sh` are both stale (missing 5–6 cases incl. `validator-join`) | `chaos.sh:55-60`, `chaos-iter.sh:20-24` |
| D-11 | Blast radius is fixed: only shard 0 / replica A / node 0 are ever targeted. `ingress-1`, `seq-b`, shard 1 and the control plane are never killed | `chaos.sh:102`, `:986-996` |

---

## Recommended work, in order

**P0 — close the two product holes**
1. **DONE (PR #152)** — Verify DA blob content against the versioned hash in `recover_blocks`; test with a flipped blob byte.
2. **DONE (PR #152)** — Stamp + verify checkpoint integrity and chain identity on restore (folds in the known recovery-C follow-up). Checkpoints are now self-contained dirs (`mdbx.dat` + `MANIFEST`, one atomic rename) so any copy mechanism carries the proof.

**P1 — exercise the tier that has never run**
3. **DONE** — `retention-overrun` + `retention-overrun-validator` on the new
   `chaos-retention` CI shard (retention 16384, adaptive SIGSTOP freeze sized
   from the same env var, verified-freeze per #108; also crosses the 90s
   session timeout, covering the long-halt leg of item 5). Original ask:
   deploy the cluster job with a small
   `-Dkardamom.cluster.retention`, halt a consumer past it, assert
   `REPLAY_UNAVAILABLE` → `resync_total{outcome=peer-checkpoint}` → park → restart →
   rejoin. Covers executor **and** validator repairs (#143) and `park_state_db`.
4. `cluster-member-rejoin` chaos case: take a snapshot (add periodic snapshots
   or an explicit `ClusterTool snapshot` step), wipe one member's cluster dir,
   assert it rejoins via snapshot/log catch-up and the pipeline is unaffected.
5. Long-halt case: freeze a consumer **past the 90 s session timeout**; assert
   session re-establishment + gapless replay. Fix the wrong comment at
   `chaos.sh:1614`.

**P2 — fill the component gaps**
6. `sequencer-join` (empty-state replica; pin the F02.1 degradation in-cluster).
7. `batcher-kill` (mid-post kill: cursor survives, no double post, no gap) and a
   da-watcher case.
8. In-cluster corruption drill: corrupt a state DB row / a checkpoint, assert
   `kardamom-statecheck` catches it.
9. Wire `kardamom-statecheck` (executor↔validator parity) and
   `kardamom-reconstruct` (rebuild-from-L1) into a cluster shard.

**P3 — de-vacuify (cheap, high value)**
10. D-1, D-2 (scrape-failure-as-zero), D-3–D-5 (killed-marker counts), D-6
    (shard pinning), D-7 (semantics lag bound), D-9/D-10 (dead + stale), D-11
    (rotate blast radius).
