# Cluster log purge and blank-member seed

Status: design, not implemented. Issue #195, leg 3. Audit follow-up item 4
in `docs/reviews/2026-08-03-chaos-coverage-audit.md`.

## Problem

A wiped cluster member is caught up by log replication from position 0.
Aeron 1.44 static membership does not transfer snapshots to a blank
member. Snapshots bound only intact-directory restarts. Nothing purges the
log. So the cost of a blank rejoin grows with the lifetime of the cluster.

## Facts that constrain the design

These come from the Aeron 1.44 jar (`javap`) and from the current code.

1. There is no `ClusterTool.purge` in 1.44. The related verbs are
   `seed-recording-log-from-snapshot` and `invalidate-latest-snapshot`.
   `AeronArchive` has `purgeSegments`, `detachSegments`, and
   `deleteDetachedSegments`.
2. A blank member replicates the leader's log. If the leader purges the
   segments below its latest snapshot position `P`, the recording starts
   at `P`. A blank member then has nothing that covers `[0, P)`. So a
   purge on the leader alone likely breaks blank rejoin instead of
   bounding it. This is not verified against the 1.44 `Election` source
   (`FOLLOWER_LOG_REPLICATION`, `LogReplication`,
   `RecordingLog.createRecoveryPlan`). Verify it first.
3. The SNAPSHOT action is a replicated log entry. Every member's own
   `RecordingLog` has the snapshot entries (`getLatestSnapshot`). A
   per-member purge is possible in principle. It needs a margin for an
   intact follower that lags behind the snapshot.
4. The `cluster-member-rejoin` chaos case proves "started blank" by a
   growth in the `sealer state FRESH at genesis` count
   (`deploy/cluster/scripts/chaos-cases-cluster.sh`). A member that is
   seeded from a snapshot logs `sealer snapshot RESTORED` instead. Any
   seed design must rework that case in the same change.
5. `ClusterBackup` is on the classpath. It can fetch snapshots and log
   segments from a live cluster into a local directory.

## Options

### A. Purge on every member, no seed

Each member purges its own log segments below the latest snapshot
position, minus a margin. A blank member still replicates from position
0, and the leader no longer has that data. Rejected unless fact 2 turns
out to be wrong.

### B. Seed a blank member from a snapshot, then purge

Operator or launcher flow for a blank member:

1. Fetch the latest snapshot recordings and the log tail from a live
   member into the local archive. `ClusterBackup` does this.
2. Run `ClusterTool.seedRecordingLogFromSnapshot` on the local cluster
   dir. The member then starts from the snapshot, not from genesis.
3. Join the cluster. Log replication covers only the tail after the
   snapshot.

With this in place, every member can purge log segments below the
snapshot position, minus a margin. This is the design that bounds rejoin
cost by snapshot age.

Cost: a `ClusterBackup` step in the launcher, or a sidecar, plus the
chaos-case rework in fact 4. `SealerTestService` also needs the
snapshot-capable wrapper the disclaimer in that file describes.

### C. Leave the log, cap the lifetime

Do nothing in the cluster. Rotate members on a schedule, so no log is
older than the rotation period. This does not bound the cost, it caps it
by policy. Cheap, but it moves the problem to operations.

## Recommendation

B. Do it in two PRs:

1. The seed path plus the chaos-case rework. Prove that a blank member
   seeded from a snapshot rejoins in bounded time, with the log intact.
2. The purge, with a margin of at least two snapshot intervals, after 1
   is green in CI.

Verify fact 2 against the 1.44 source before either PR.

## Out of scope

- The silent exit-137 deaths at about 110 s during catch-up (issue #195,
  leg 1). No local repro; CI only.
- The join watchdog. That landed with this spec (`JoinWatchdog.java`).
