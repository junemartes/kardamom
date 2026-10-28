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

`just test-component state-mirror-coverage-gap` is a failing diagnostic for a real
coverage bug. An interrupted scan leaves 512 of 1,025 accounts. Another mirror
publishes position 30. Automatic restart sees that head, skips the scan and publishes
progress with missing accounts. The diagnostic requires those accounts to be restored
and therefore fails on the current implementation. The four recovery checks use an
explicit rebuild; they do not prove automatic recovery. This gap relates to the
coverage protocol in [issue #397](https://github.com/junemartes/kardamom/issues/397).
`all` includes the diagnostic and remains red until the gap is fixed: 98 passing
checks do not cancel a failed correctness assumption. The existing Docker CI job
runs the four recovery checks; the known failing diagnostic remains separately named.

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
Twenty-seven cases have partial mappings. No full case is marked locally validated yet.

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

Raft quorum/election safety needs real consensus members. Sentinel promotion needs
real Redis/Sentinel processes. Checkpoint durability needs real storage and crash
or syscall faults. Nomad placement, container ownership, advertised addresses and
kernel network scheduling need integration checks. Mocking those guarantees would
assume the behavior under test. Small local groups can validate many of them
without deploying the entire chain, but those groups are still to be implemented.

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
