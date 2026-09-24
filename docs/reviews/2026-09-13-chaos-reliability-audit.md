# Chaos reliability and final-state audit

Audited the 25 Rust cases, their shared harness, the load engine and accounting,
validator verdict, deployment jobs, and the seven cluster CI shards. The base is
PR #340 (`779cccbe`), the end of the review-fix stack. Open PRs are listed separately;
their presence does not mean their scenarios run on this base.

## Assessment

The suite exercises important recovery paths, but a green run alone does not prove
high reliability across every component. Several verdicts accepted incomplete evidence.
The changes accompanying this audit close those false-pass paths and add persisted
state comparison. Missing fault scenarios remain explicit below.

## Assertion defects addressed

| Defect | Required evidence |
|---|---|
| Chaos load invents receipt status 1 when a receipt lookup fails | A real receipt, from lookup/feed/drain. An unresolved accepted transaction remains missing. |
| A failed load can read an old passing report because its inner `Result` is ignored | Propagate the task error and require its boolean result to agree with its report. |
| Raft cases ignore every failure except `missing` | Require the complete load verdict and zero missing receipts. Chaos accounting already tolerates duplicate-submit drops. |
| Missing pre-fault samples become zero | Require a real executor baseline and a complete ingress-pair baseline; missing scrapes cannot establish victim progress. |
| Executor convergence happens before the remaining load/drain | Recheck every executor after the load finishes. An empty fleet cannot pass. |
| Chaos validator verdict allows arbitrarily large lag | Require bounded validator catch-up after every case and at the shard tail. |
| Separate successful/error-counter scrapes can hide a failed scrape or restart | Read BAL verification, trie checks, divergence, and mismatch evidence in one final response. Optional error counters may be absent only with positive cursor/verification/check evidence. |
| Quorum recovery leaves the third member's return best-effort | Require restart and three running members. Stopping allocations do not count as running. |
| Historical stopped allocations can identify the current Raft leader | Restrict leader selection to desired-running allocations. |
| Historical repairs on any node can satisfy retention recovery | Require all three repair log counts to increase on the victim after the baseline. |
| Stall check accepts a regressing block cursor | Require equal real before/after samples. |
| Equal block heights do not prove equal state | Run the offline state gate below on every shard. |

## Persisted-state gate

After the load, chaos/semantics cases, ingress churn and live validator verdict:

1. Save the registered cluster, executor and validator job definitions.
2. Stop ordering and require every executor and the validator to drain to the same
   nonzero block. Failure to drain is a failure, not an exemption from comparison.
3. Stop the state writers. Require terminal Nomad allocation states and check the
   inner Docker daemon for remaining writers before copying each database.
4. Copy every executor DB and the validator DB into a named `chaos-state-*` directory.
5. Require executed receipts, a clean integrity sweep on every copy, and a validator
   persisted root that equals its rebuilt root.
6. Compare every executor with the validator: accounts, storage, code, headers,
   receipts, transaction hash indexes, and shared chain metadata/cursors. Executor
   genesis roots are not compared with the validator's live trie root.
7. Attempt restoration of all three jobs on both success and failure. Restore the
   complete saved definitions and wait for their desired allocations to run.

The comparison runs on stopped copies, never concurrently with live MDBX writers.
A wrong balance at the same height fails the regression test even when receipts agree.
Missing databases and an empty executor list also fail. Evidence remains at the path
printed in the shard log; CI uploads it on failure.

This is a shard-end persistence check. It complements per-case live checks; it does
not claim a separate offline image after each individual injection. Replica equality
also does not replace application-level expected balance/storage assertions from the
chain-semantics suite.

## Fault coverage and remaining work

| Component / surface | Exercised on this base | Remaining reliability evidence |
|---|---|---|
| Executors | TERM/KILL, whole-node loss, checkpoint restore, cold peer bootstrap, retention overrun | Sustained network partition, disk-full/I/O errors, corrupted state/checkpoint drill in the deployed cluster |
| Ingress | TERM/KILL, rotating hard-kill victim, archive driver loss, churn | Simultaneous ingress loss and durable receipt availability after losing all volatile caches |
| Sequencers | TERM/KILL on loaded lane 0, twin survival, verified lapse, lookup blackout, scale out/in | Rotate lane and replica placement; prove an established sender works through the recovered replica while its twin is unavailable |
| Raft | Leader/follower kill, snapshot restart, blank member replay, two-node quorum loss, CPU starvation | Asymmetric network partitions, persisted log/snapshot corruption, bounded blank-member recovery with lifetime-log growth |
| Validator | Verified lapse, empty-state join, retention repair, BAL/trie checks | Deployed corrupt-BAL/state negative control; bounded recovery after prolonged BAL loss. Local semantic injection coverage is not a cluster drill. |
| Batcher / L1 | Live service and separate semantics tests | Mid-post kill, lost cursor, long L1 outage/reorg, no skipped or duplicate posting; rebuild the faulted chain from its posted DA and compare state |
| DA watcher | Runs as a deployed service | Kill/rejoin, checkpoint fallback, missed/forked L1 events, proof of reconstructed state after recovery |
| Consul / Nomad | Used by every deployment; discovery degradation has unit tests | Explicit control-plane partitions/restarts, stale service membership, loss of discovery while traffic continues |
| Archive | Driver loss, manual peer copy after wipe, byte corruption detected and healed | Distinguish manual repair from autonomous recovery; ordering archive and total durable-copy loss |
| Redis / mirrors | Not in this base | Open #315 adds Redis partition, kill, freeze/failover, mirror rebuild; failover-head regression remains deferred there |
| Machine replacement | Restart existing nodes | Open #311 and #317 add actual executor/sealer replacement with new addresses and blank-member replay |

Other evidence limits worth fixing next:

- A Nomad log HTTP 404 becomes an empty log. The absence of a divergence line cannot
  prove no historical divergence if logs were lost or collected. Persist error evidence.
- Most health checks use one successful sample. Add a recovery observation window and
  fresh transactions after recovery to measure sustained service, including p99 latency.
- The load's injection gate observes ingress traffic, not a new canonical commit on
  the victim's path. Fault duration and load lifetime need explicit overlap evidence.
- Fault cleanup is uneven outside the new state gate. Frozen/drained nodes should have
  cleanup attempts on every error exit, while preserving the original failure.

## Validation and limits

Regression tests cover load task/report failures, strict Raft verdicts, absent metrics,
positive error counters, and real MDBX state comparison. An isolated Nomad dev-agent
integration test exercises job stop, full-definition restore, and allocation recovery.
`just style` checks the full workspace and all features, including the shard wiring.

The complete seven-shard cluster run has not been executed locally for this audit.
The new stop/drain/copy/restore sequence still needs that end-to-end validation; its
MDBX checks and Nomad lifecycle are tested separately. Do not treat local unit success
as proof of the remaining failure surfaces or an availability SLO.
