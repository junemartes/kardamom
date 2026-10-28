# Local component recovery tests

Run a production actor against controlled neighboring actors before using a
deployed chaos shard:

```sh
just test-component sequencer
just test-component executor-commit
just test-component executor-pipeline
just test-component all
```

Thirteen targets run 94 checks without external services. They cover sequencer admission, executor
commit and execution-pipeline behavior, ingress accounts/receipts/DA halt, egress
ownership/retention/peer catch-up, batcher recovery, and L1 source/watcher recovery.
Run one target to iterate, or `all` for the current matrix. These thirteen targets
start no Docker, Nomad, Redis or Java cluster. The HTTP-source target uses real
HTTP with a paused clock after requests reach the fault proxy. It exercises the
unchanged 30-second deadline and backoff in about 0.12 seconds of test execution.
Compilation adds time. These timings measure tests, not transaction performance.

`just test-component state-mirror` adds four checks with one disposable Redis
container at a time and a real 1,025-account checkpoint. It needs Docker, but starts
no chain services. The tests cover partial scan interruption, partial live writes,
script loss, input close and monotone replay. Redis ACL logs prove write refusal
before recovery. Container and scratch storage owners clean up on test exit.

`just test-component state-mirror-coverage-gap` runs two automatic recovery
regressions. An interrupted scan leaves 512 of 1,025 accounts. Another mirror
publishes position 30. The replacement must restore every untouched account and
retain newer live rows before publishing progress, even with a persisted local
head. Both targets run in the existing Docker CI job. `all` runs 148 checks.

The mirror syncs an empty `head.rebuild` marker before a scan or projection clear,
and when a projection write fails. Its presence requires a checkpoint rebuild on
restart regardless of other writer heads. Only a complete scan removes and syncs
the marker; a later head write cannot clear it. Keep the marker with the mirror's
head directory across task replacement. An older binary ignores this new marker
and retains its prior recovery behavior; use an explicit rebuild when rolling
back a mirror with incomplete recovery. This local recovery obligation does not
certify shared cache generations or detect loss of the entire mirror directory.
The broader bucket coverage protocol remains in
[issue #397](https://github.com/junemartes/kardamom/issues/397).

`just test-component checkpoint-copy` adds three checks for the checkpoint proof
transport. One disposable container holds a real MDBX checkpoint on tmpfs. The test
reproduces the failed `docker cp`, verifies a streamed `docker exec`/tar copy through
the production restore code, then requires an unmanifested copy to fail. The other
checks distinguish a growing live log from missing-file or mixed archive failures.
This covers the harness failure that prevented the disk-full case from reaching its
fault; executor behavior during ENOSPC still needs the existing chaos shard.

`just test-component sealer-actor` runs 38 Java service checks with controlled
cluster/session peers, explicit timestamps, four-frame carry/replay windows, and
snapshot restore at each log cut. `just test-component sealer-raft` runs seven checks
with three real in-JVM Aeron consensus members, media drivers and archives. They
cover leader loss, snapshot restart/stall, session replay, eviction and carry
activation. The first targets take about 14 and 38 seconds including Gradle work.
These reuse the existing Java fixtures and start no chain stack. The Raft group
still uses real scheduling and local Aeron transport; the actor clock does not accelerate
its consensus protocol.

Java targets force test execution and remove their selected old XML reports before
running. The runner requires fresh reports, preserves them with the logs, names
every passing/failing test, and rejects skipped or missing expected tests even
when Gradle exits zero. A negative control that removes seed reoffering on a new
leadership term must fail the real service test.

The executor fixtures mock the state-writer durability signal. Egress tests use a
real temporary store and peer HTTP server; batcher tests use a real temporary spool
and an HTTP endpoint that refuses dispersal. L1 tests exercise real source/watch
logic, wire parsers and cursor files with controlled providers and publisher peers.
The catalog records the remaining limitations for each target.
The command records its source revision, exact commands, elapsed time and full logs
under a unique `/tmp/kardamom-components-*` directory. `results.json` names every
passed test and any expected tests that did not run. An empty test selection cannot
pass. Each suite has a ten-minute process limit including compilation; timeout or
interruption terminates its process group. The runner keeps partial actor evidence
separate from complete chaos-case validation and CI qualification.

The fixture runs the production `AdmissionLoop::run` on a worker thread. It keeps
the production `ClusterRefPublisher`, `SealerRules`, and wire codecs. Controlled
peers supply bounded transaction and account channels, cluster ingress outcomes,
and canonical frames. Tests prove the fault is active before releasing it. They
compare exact wire bytes, including transaction identity, nonce, lane, session and
position. Recovery must work without adding client traffic to wake the actor.
Every wait has a deadline, and shutdown runs on both success and assertion failure.

The first cases cover output backpressure and disconnect, lost session offers,
missing lookup replies, stalled and closed input, delayed canonical confirmation
with a full outbox, permanent encoding failure, and shutdown during disconnect.
Negative controls fail when session replay is removed, a disconnected output is
falsely acknowledged, a permanent output error is retried, or the egress prunes at
the higher posted/final head, or the production provider-read deadline is removed. These
mutations are validation experiments; they are not runtime changes.

The [coverage inventory](reviews/2026-10-28-component-recovery.json) names every
registered chaos case, plus the packet-delay case pending in PR #917. The registry
test rejects a new chaos case without a corresponding plan. Each family lists the
real actors, boundary faults, recovery invariants and dependencies that still need
real integration. `partial` means that named local tests validate some boundary
assumptions. It does not mean the complete distributed case has passed locally.
Thirty-four cases have partial mappings. No full case is marked locally validated yet.

For each remaining case:

1. Review the case's fault and recovery assertions against the production actor.
2. Run that actor with bounded, controllable input and output peers. Keep real
   protocol parsers, state machines and durable storage where the invariant needs
   them. Reuse existing actor fixtures instead of modeling the actor again.
3. Observe a refused write, missing reply, blocked queue, failed syscall or stopped
   actor before asserting recovery. A test that never activates its fault fails.
4. Restore dependencies in both relevant orders. Require exact state, transaction
   and receipt identity, no lost retained work, bounded queues, and bounded shutdown.
5. Make the test reject a deliberately broken production behavior. Record the
   source revision, command, result, runtime and any remaining assumptions.
6. Run the corresponding existing chaos shard for network, process and deployment
   evidence. Keep the original deadlines and workload unless a separate reviewed
   experiment requires a change.

Raft partition/quorum-loss safety needs further real-member fault cases. Sentinel promotion needs
real Redis/Sentinel processes. Checkpoint durability needs real storage and crash
or syscall faults. Nomad placement, container ownership, advertised addresses and
kernel network scheduling need integration checks. Mocking those guarantees would
assume the behavior under test. Small local groups can validate many of them
without deploying the entire chain. The local Raft fixture covers selected lifecycle
and replay cases; local Sentinel groups and the remaining faults are still needed.

Component success does not replace latency/throughput measurements or the required
ten consecutive full CI rounds on one unchanged final revision.

Local fixtures use small retention windows and controllable clocks to cross the
same production boundaries quickly. Tests must assert behavior immediately before,
at and after a threshold. Advancing Tokio time does not advance `std::time::Instant`,
Redis expiry, filesystem timestamps or a remote process. Such dependencies need their
own control or a bounded real-service check. Production retention and timeout defaults
remain unchanged. L1 source backoff and egress sync scheduling use the same Tokio
monotonic clock as their tests. Without the test clock, Tokio reads the system
monotonic clock. The tests cross a one-hour source backoff and two one-hour egress
sync intervals instantly. Egress uses a 10-block retention window and 10-block
segments, checks just before and at each boundary, then reopens the real store and
compares every retained frame. L1 watcher tests also use a paused clock for
republish and startup deadlines. Other actors and external service clocks still
need individual review; virtual time does not prove operating-system scheduling.
