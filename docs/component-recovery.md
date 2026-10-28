# Local component recovery tests

Run a production actor against controlled neighboring actors before using a
deployed chaos shard:

```sh
just test-component sequencer
just test-component executor-commit
just test-component executor-pipeline
just test-component all
```

Three targets are registered so far: seven sequencer tests, nine existing executor
commit-thread tests, and three existing execution-pipeline tests. They need no
Docker, Nomad, Redis, Java cluster or full-stack deployment. The first combined run
takes about eight seconds including incremental compilation. The sequencer test
binary takes about 0.2 seconds. These measure test execution, not transaction
performance. The executor fixtures mock the state-writer durability signal; they
do not prove disk durability.

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
Three negative controls fail when session replay is removed, a disconnected
output is falsely acknowledged, or a permanent output error is retried. These
mutations are validation experiments; they are not runtime changes.

The [coverage inventory](reviews/2026-10-28-component-recovery.json) names every
registered chaos case, plus the packet-delay case pending in PR #917. The registry
test rejects a new chaos case without a corresponding plan. Each family lists the
real actors, boundary faults, recovery invariants and dependencies that still need
real integration. `partial` means that named local tests validate some boundary
assumptions. It does not mean the complete distributed case has passed locally.
No full case is marked locally validated yet.

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
