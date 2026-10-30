# Local component recovery tests

Run a production actor against controlled neighboring actors before using a
deployed chaos shard:

```sh
just test-component sequencer
just test-component executor-commit
just test-component executor-pipeline
just test-component all
```

The initial Rust targets cover sequencer admission, executor
commit and execution-pipeline behavior, ingress accounts/receipts/DA halt, egress
ownership/retention/peer catch-up, batcher recovery, and L1 source/watcher recovery.
Run one target to iterate, or `all` for the current matrix. These targets
start no Docker, Nomad, Redis or Java cluster. The HTTP-source target uses real
HTTP with a paused clock after requests reach the fault proxy. It exercises the
unchanged 30-second deadline and backoff in about 0.12 seconds of test execution.
Compilation adds time. These timings measure tests, not transaction performance.

`just test-component state-mirror` adds five checks with one disposable Redis
container at a time and a real 1,025-account checkpoint. It needs Docker, but starts
no chain services. The tests cover partial scan interruption, partial live writes,
script loss, input close and monotone replay. Redis ACL logs prove write refusal
before recovery. Container and scratch storage owners clean up on test exit.

`just test-component state-mirror-coverage-gap` runs two automatic recovery
regressions. An interrupted scan leaves 512 of 1,025 accounts. Another mirror
publishes position 30. The replacement must restore every untouched account and
retain newer live rows before publishing progress, even with a persisted local
head. Both targets run in the existing Docker CI job. `all` runs 228 checks across 36 targets.

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

`just test-component checkpoint-fault` exercises the exact ENOSPC controller in
one disposable Python container. It writes an 80-MiB baseline on the fixed 512-MiB
tmpfs, requires actual ENOSPC and zero free bytes, preserves the baseline digest,
then frees space and writes a 160-MiB replacement. The files test controller and
filesystem behavior; they are not MDBX images. Nine Python controls also verify
mount ownership, the byte bound, missing images, error classification and cleanup.
The controller accepts accumulated images above 64 MiB; the kernel's unchanged
512-MiB mount limit bounds the fault. The deployed case still requires actual
checkpoint/log failure, same-process transaction progress and verified restore.
The local probe uses `python:3.13-alpine`, no network or extra capabilities, a
read-only root and a 120-second deadline. It runs in the existing Docker CI job.

`just test-component state-aux-enospc` fills an isolated eight-MiB tmpfs until the
kernel returns ENOSPC. The real checkpoint copy and block-log thread must both
report error 28 while the same state writer continues committing on a separate
filesystem. A failed copy must leave only the earlier complete checkpoint visible.
After freeing space, a new base plus the resumed log must restore exactly block 5,
including account values and all ten storage keys written across the five blocks.
Each observation also rejects keys from blocks not committed yet. The fixture uses
five blocks, a one-byte segment limit and a two-entry log queue. It takes about four seconds including
container startup, without a chain stack. Negative controls fail when a copy uses
its public name before completion or the log thread dies on its first write error.

This Linux Docker target mounts the test executable and the shared libraries
reported by `ldd` read-only into one disposable container. It uses no network or
extra capabilities. The parent bounds execution and removes the container on exit.
The existing Docker CI job runs it. It does not prove state-volume ENOSPC, EIO,
power loss, checkpointer scheduling, Nomad mounts or full transaction progress.

`just test-component state-enospc` adds two checks on the authoritative state
volume, using two blocks and a one-MiB code payload. A full eight-MiB tmpfs must
stop the real writer with error 28 and close snapshot publication. Reopening must
preserve the prior account, storage and replay cursors, with no failed block,
code, receipt or transaction index. Freeing space and replaying the batch must
commit it exactly. A second check exercises unsynchronized transactions and retry.

The synchronized check reproduces a product panic in the database binding:
MDBX ends the failed transaction, but the wrapper aborts its released handle again.
The pinned patch in [vendor/signet-libmdbx/VENDORED.md](../vendor/signet-libmdbx/VENDORED.md) marks the failed handle
ended before returning its error, except for thread mismatch. It changes commit
error cleanup; successful commits retain their existing completion marker. The
unsynchronized test checks visible recovery behavior, not the number of abort
calls. Both tests run in the existing Docker job. They do not prove EIO, power
loss, trie mode, process supervision or full-chain recovery.

`just test-component chaos-election` runs the real recovery observer and Nomad
HTTP client against a loopback peer. An unavailable old allocation must exhaust
its inner log-read budget, then the observer must refresh allocations and certify
a closed majority within the remaining election deadline. Persistent missing logs
must fail with the last read error; readable logs without a closed majority must
also fail. The three checks take about 3.3 seconds with a 200-ms inner budget.
This tests the chaos harness, not consensus or service recovery. The ordinary Rust
CI test job runs these checks.

`just test-component sealer-actor` runs 38 Java service checks with controlled
cluster/session peers, explicit timestamps, four-frame carry/replay windows, and
snapshot restore at each log cut. `just test-component sealer-raft` runs seven checks
with three real in-JVM Aeron consensus members, media drivers and archives. They
cover leader loss, snapshot restart/stall, session replay, eviction and carry
activation. The first targets take about 14 and 38 seconds including Gradle work.
These reuse the existing Java fixtures and start no chain stack. The Raft group
still uses real scheduling and local Aeron transport; the actor clock does not accelerate
its consensus protocol.

`just test-component sealer-raft-network`, `sealer-raft-members` and
`sealer-raft-fleet` run twelve checks on three real member processes. Each
member runs the production `ClusterNode` main class in its own JVM, with the
deployed tick, Raft timeouts and member settings. Each member has its own
loopback address, its directories on tmpfs, and a shared driver for its
member feed. The members name each other through a hosts file, as the deploy
names them through Consul. A test client speaks the production ingress and
egress codecs, and each member feed gives the canonical frames of that member.
Every check requires one leader per term from the TERM lines of all members,
and the same canonical frames, byte for byte, on every member.

The checks cover a one-way partition, 10 % loss and a 250 ms delay of the
frames from the leader to its peers, a CPU squeeze of every member, a leader
kill, a follower kill with a snapshot restore, a rejoin after a log purge, two
blank members with an offline copy, a node replacement on a new address, a
quorum loss, a total loss, and the sealer part of a fleet wipe with a seed.
A test endpoint supplier in each member driver applies the link faults; the
receiving member drops, loses or holds the frames from the faulted member.
Each check takes 15 to 55 seconds. A member stops when its test JVM dies, and
the next group deletes the tmpfs directories of dead test JVMs.

The squeeze check also runs a consumer session with the production cursor
rules, as the validator reads the stream. Its stream must reach the live head
with no gap and equal the members' order. `just test-component reconstruct-seed`
posts two blocks to Anvil, rebuilds the state from the L1 log and the DA proxy,
and writes the sealer seed of the fleet wipe at the posted head.

Two optional member settings serve these checks. `kardamom.cluster.adminHost`
binds the admin endpoint to one address, so each member on one host keeps the
one admin port that its peers ask. `kardamom.cluster.clockClass` names the
cluster clock class. The two-blank check moves a test clock past the 60 s
startup canvass, as that wall time would. Both settings are unset in the
deploy, and the defaults stay the same.

Java targets force test execution and remove their selected old XML reports before
running. The runner requires fresh reports, preserves them with the logs, names
every passing/failing test, and rejects skipped or missing expected tests even
when Gradle exits zero. A negative control that removes seed reoffering on a new
leadership term must fail the real service test.

`just test-component cache-sentinel` runs three checks with a real primary, replica
and three Sentinels. It freezes or kills the primary, observes a failed cache read,
requires promotion, flushes scripts and requires the same cache client to recover.
It restarts the old primary and rejects an older account update. Losing only the
replica must still allow primary writes and return zero replication acknowledgements.
The fixture uses 500ms failure detection and a five-second failover interval, with
30-second recovery bounds. Redis uses its own real clock. Production defaults stay
unchanged. Docker sends the stop signal from outside the PID namespace; the test
also checks the Redis process state, so a successful but ineffective signal cannot
pass. Role probes reconnect when Redis closes clients during demotion.

This target requires Linux Docker bridge routing: Sentinel advertises container
addresses to the real client. It starts no chain services and removes its five
containers and network after each test. It does not reproduce CPU-pressure TILT,
whole-chain fallback or Nomad placement. Both negative controls fail at recovery:
not replacing the cached connection, and loading a script only once before failover.
The existing Docker CI job runs this target.

`just test-component executor-recovery` runs fifteen executor recovery checks.
Each executor is a child process of the test binary. It runs the production state
startup in `kardamom_executor::state`, the engine, the MDBX writer, the checkpoint
rounds, the checkpoint server and the replay repair. The binary uses the same state
module. The parent plays the sealer: it appends production egress frames to a
retained log. Each process reads the log through one replay session, from its
durable cursor and with an overlap below it. The parent can set a retention floor,
so that an old cursor gets `REPLAY_UNAVAILABLE`, and can take every member away.

The cases stop processes with SIGTERM, kill them with SIGKILL, wipe their state or
checkpoints, corrupt one image byte, replace a node with empty storage on a new
address, and start a build that fails its start checks. They also rebuild an
executor image from DA batches through the production batch codec and reconstruction.
Every case ends with each replica stopped at the same head. Its DB must pass the
integrity sweep and equal an undisturbed replica table by table. Each process must
publish its boundaries in order, and no transaction may get two receipt hashes. A
replica that kept its disk publishes no boundary twice. The checks also require the
restore, fetch, quarantine and resume log lines in their order, `/ready` before each
rolling step, and the committed nonce and receipt answers of a restarted replica.

Storage is on tmpfs when it has room. A process kill keeps the page cache, so the
fault is a process crash, not a power loss. The fifteen checks take about 5 seconds
of test execution in parallel on a quiet host; compilation adds time. Nomad
placement, node containers, Consul records, image rollout and the deploy gate stay
in the chaos shard; each ported case names them under `cluster_only_checks`.

`just test-component executor-exec-stream` adds the executor stream on a real media
driver and archive in one disposable container. A stopped and a killed replica each
record a new session, and its recorded cursor moves. While one replica is dead, the
cursors of its peers move. `just test-component sealer-record-lag` runs the existing
sealer checks that take the best cursor over the executors.

`just test-component executor-checkpoint-enospc` runs one executor process in a
disposable container, with its checkpoints and block log on a 16-MiB tmpfs. The
parent fills the tmpfs until the kernel refuses a write. The block log and the base
round must fail with error 28 while the same process applies transactions. After the
filler goes, a new base above the fault must restore with its log past the base.

`just test-component executor-total-wipe-l1` posts the blocks with the production
batch post to a real Anvil and the DA store. Every state and checkpoint goes, and the
image comes from the `BatchPosted` scan and the DA reads alone. Each executor must
resume from the image cursor with no restore or fetch.

`just test-component state-mirror-total-loss` and `cache-sentinel-total-loss` kill a
primary, a replica and three Sentinels at once. Fixed host ports stand in for stable
service addresses. While the group is down, the production reader degrades without a
stall and the mirror marks a rebuild. The group returns empty, the Sentinels name a
primary, and the mirror rebuilds every account once a checkpoint reaches its first
live batch. A second check kills three mirrors with the group. Two production
readers must count a degraded read with no stall. Redis returns empty first, then
the mirrors, then a newer checkpoint and a live batch. Every mirror must rebuild,
and each reader must count a Redis hit and no degraded read. The fixture takes its
host ports from 20000-29999 under a lock file, so two runs on one host cannot share
a port, also while the containers are down.

`just test-component transport-mds-remove` removes MDS receive destinations while two
streams run, and returns a node to the same control endpoint three times. Every frame
of the live streams must arrive in order, and the driver must log no error. On the
deployed Aeron 1.45 driver, this does not reproduce the driver crash of issue 545.

`just test-component state-writer-crash` runs two real durable MDBX process-crash
checks in about 0.3 seconds of test execution. A child runs the production writer.
An owned metrics recorder blocks its existing synchronous callback after account
writes but before commit, or after commit but before snapshot publication. The
parent observes the exact cut, sends SIGKILL, and verifies signal termination.
Recovery must discard the uncommitted block or preserve the committed block,
including account/storage values, receipts, hash index and both replay cursors.
The writer must accept the next block after reopening. Each child has a deadline
and is killed and reaped on assertion failure. The test uses only two blocks and
owned temporary storage. The ignored child entry point is invoked by the two
ordinary tests; the existing workspace CI test job runs them automatically.
This proves process-crash boundaries while the host filesystem stays alive. Power
loss, ENOSPC/EIO, trie mode and full executor publication remain separate cases.

The executor commit/pipeline fixtures mock the state-writer durability signal. Egress tests use a
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

The [coverage inventory](../scripts/ci/component-recovery.json) names every
registered chaos case, plus the packet-delay case pending in PR #917. The registry
test rejects a new chaos case without a corresponding plan. Each family lists the
real actors, boundary faults, recovery invariants and dependencies that still need
real integration. `partial` means that named local tests validate some boundary
assumptions. It does not mean the complete distributed case has passed locally.
Forty-one cases have partial mappings. No full case is marked locally validated yet.

`just component-catalog` checks the catalog. `just component-summary` prints the
planned, partial and ported counts for each family. A case is ported when its
status is `ported`. The status `ported` and `full_case_locally_validated: true`
always go together. A ported case needs:

- At least one local test.
- A local test in a suite with a `ci_mode`, so that CI runs it. The modes are
  `in-process`, `docker` (a real container dependency) and `gradle` (the Java
  sealer).
- A negative control for one of its local tests. A control that names tests
  (`test`, `tests` or `failed_tests`) guards only those tests. A control that
  names no test guards every test of its suite.

The optional case field `cluster_only_checks` is a list of strings. Each string
names a check that needs the deployed cluster, and gives the reason. The cluster
chaos suite keeps these checks.

The isolated component suite is the pull request gate for recovery behaviour. The
cluster chaos suite runs nightly. Every nightly chaos failure gets an isolated test
that reproduces it:

1. Write the test against the real actor, as the steps below describe.
2. Add the test to the `tests` of its suite in `local_suites`.
3. Add `{"suite": ..., "test": ...}` to the `local_tests` of the failed case.

The `local_tests` entry links the test to the failed case. If the failure needs
the deployed cluster, add the check to `cluster_only_checks` of the case instead.

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

The member-process Raft checks cover partition, loss, delay, squeeze, kill,
wipe and quorum-loss cases; kernel queues and whole-node power loss stay in
the cluster suite. Sentinel promotion needs
real Redis/Sentinel processes. Checkpoint durability needs real storage and crash
or syscall faults. Nomad placement, container ownership, advertised addresses and
kernel network scheduling need integration checks. Mocking those guarantees would
assume the behavior under test. Small local groups can validate many of them
without deploying the entire chain. The local Raft fixture covers selected lifecycle
and replay cases; the local Sentinel fixture covers promotion and client recovery.
Other fault combinations remain open.

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

`just test-component chaos-alert` runs six paused-clock checks for the alert hold
observer used by `l1-liar`. A 30-second hold starts at the first completed active
read. Later consecutive active reads must span that duration. Inactive, unknown
or failed reads reset it. A slow first read cannot certify earlier time, and a
hanging read cannot extend the 60-second total budget. Failure evidence reports
the last completed observation and the longest observed active span. These checks
do not run Prometheus or establish alert behavior under runner contention.

`just test-component transport-pressure` runs eight real Aeron runtimes against
one disposable media driver. Independent output streams lack subscribers while a
live stream from a separate publisher runtime must deliver every 1-KiB frame. The output offers must remain pending
through the sample, then report NOT_CONNECTED at the existing deadline. Late
subscribers must receive a fresh pending frame without an expired frame preceding
it. Quiet-arrival samples also keep the receiving runtime asleep until UDP input arrives.
Delivery is checked again after recovery. The driver and scratch files are
removed on exit. The target runs in the existing Docker CI job.

For an isolated transport profile, set `KARDAMOM_PROFILE_RUNTIMES` to 1–64 and
`KARDAMOM_PROFILE_FRAMES` to 1–4096. Defaults are eight runtimes and 256 frames per
phase. `TRANSPORT_SAMPLE` lines report process CPU ticks, elapsed time and delivery
p50/p95/p99. CPU ticks cover the Rust process; they exclude the Java driver. A
single rejoin observation is not a population percentile. This profile does not
measure transaction performance or reproduce the full deployment.

`just test-component chaos-projection` checks the exact account recovery observer.
Each observation refreshes Sentinel discovery and checks the primary ROLE on the
connection used for its account reads. The node contract resolves the address;
these reads do not depend on an inner Docker command or Consul DNS. Failed reads
remain missing evidence within the existing 120-second budget. One complete
observation must match the nonce and full-width balance of both the used sender
and the untouched funded account, with a valid transaction index. Rows from
separate observations cannot combine into a pass. Persistent and hanging failures
still fail at the deadline. Five local controls exercise these verdicts; they do
not certify the deployed Redis or mirror recovery. Restoring immediate error
propagation makes the transient-recovery and persistent-deadline controls fail.

`just test-component chaos-cache-baseline` checks exact nonce, full-width balance,
transaction-index shape, and executor agreement before the cache comparison.

The mirror actor target also pauses a real Redis server for 400 ms. The head
observer retains its 200-ms request timeout and five-second recovery budget;
a transient observation error no longer aborts that recovery wait. This test
checks observer behavior, separately from the actor's recovery assertions.

`just test-component batcher-null-receipts` runs the real batcher feed, sender,
startup and spool against Anvil and the deployed settlement contract. It uses the
same null-receipt and swallowed-log faults as the L1 chaos case. Mining control
holds the second post in the mempool: the spool must retain it and the cursor must
stay at the first confirmed batch. With the fault still active after mining, the
actor must reconcile the canonical post and advance exactly once. A second test
cancels the actor before mining, then restarts it with receipts and logs still
hidden and an indexer that never answers. Startup must use contract truth; replay
of the covered block followed by the next block must produce no duplicate post.

Both checks require exact contract batch indices, block ranges, reconstructed DA
payloads, durable cursors, account nonces and an empty recovered spool. They use
three empty boundary blocks, a two-entry input channel and a real file-backed local DA HTTP
service. Missing Anvil fails instead of skipping. The tests run in the existing
Docker CI workflow, but need no Docker or chain stack locally. The existing DA
helper serves until the test process ends; Anvil and scratch directories have
fixture owners. The two checks take about 1.5 seconds of test execution; compilation
adds time.

Production startup now constructs its feed through one private method shared with
the fixture. The constructor arguments, defaults and side-effect order stay the
same. This is a service-wiring extraction, with no posting algorithm or hot-path
change. No product defect is established by these checks. Negative controls
reject premature cursor persistence, ignoring contract coverage during replay,
and ignoring reconciliation after a hidden receipt.

The tests shorten provider polling only. They preserve the production 120-second
receipt deadline and do not claim to test its expiry. Alloy uses a wall-clock
instant for timeout bookkeeping, so advancing only Tokio time would not prove
that deadline. Deterministic mining and cancellation reproduce the recovery cuts
without waiting for expiry. Follower recovery, alert behavior, distributed DA,
Nomad restart and the rest of the deployed case remain separate validation.

`just test-component batcher-post-age` adds nine checks for the actual post-age
actor and its archive seed. A loopback archive accepts the real HTTP request and
never answers. Live follower records must still update the exported age, and
closing input must stop the actor. The old loop fails both controls because it
awaits the seed inside its tick before reading more input.

The actor now owns at most one archive-read task. The complete three-RPC seed
shares the existing observation interval as its deadline, ten seconds in service
wiring. Timer ticks and follower input continue while that task waits. Completion
publishes the seeded age immediately; failures can retry on a later tick. The
actor cancels the owned task on exit. Tests also require no overlapping attempts,
one deadline across all three RPCs, successful seed publication without another
tick, and no age regression when an older seed follows a newer live post.

These are product changes to the observability actor's startup and recovery.
Transaction posting, receipt deadlines, stored formats and network topology stay
unchanged. The input trait is private and production still uses the same Aeron
subscriber. The test replaces only its decoded-record boundary with a two-entry
channel and uses the real archive client and metric emission. Virtual time starts
after the actual HTTP request reaches its peer; it advances scheduling and RPC
deadlines. The exported age still uses real wall time, bounded by observations
before and after the record. Nine checks execute in about 0.06 seconds, excluding
compilation. No client latency or throughput improvement is claimed.

The deployed null-receipt case can pass while the batcher confirms through
contract reconciliation. Its stale-post alert measures the last post certified by
the follower, whose sources disagree on swallowed logs. That alert alone cannot
prove receipt confirmation is stuck. The deployed case does not stall the
batcher's archive HTTP seed; this target covers that missing output-failure
combination. It does not prove Prometheus behavior or full follower recovery.


`just test-component chaos-cache-backlog` runs fifteen checks for the baseline,
held-head predicates and recovery observer used by `mirror-backlog-client-restart`.
The baseline and recovery reads refresh primary discovery and use the real Redis
client. One transaction reads ROLE, all three heads, and the account row. A primary
that demotes after connection setup cannot certify that snapshot. The fixture
checks the actual command sequence over TCP, including the requested account and
keys. A silent EXEC reply exercises the client's unchanged three-second deadline;
Tokio time pauses only after the peer receives that request.

Baseline errors retry inside one 120-second budget that includes head and account
reads. Recovery retains its 300-second budget and requires all writers to advance
twice with exact account state on the same primary. Promotion resets the first
observation. Missing heads, incorrect accounts and held progress cannot pass.
Persistent failure reports the last observation error at the deadline. Virtual
clock controls preserve these complete budgets without waiting minutes.

This fixes a chaos observation path that failed before its fault was armed in CI.
It does not attribute the original nested-Docker MGET timeout to Redis, DNS or
Docker. Fault mutation and cleanup still use the pinned primary through Docker;
the assertions during the fault require that primary to keep the exact stale
state. No product service or hot path changes. The local checks do not establish
Sentinel promotion, mirror coverage, client fallback, distributed load or full-case
equivalence. The existing deployed shard must still establish those properties.

`just test-component chaos-archive-stage` checks the archive-corruption fixture's
selected-file transfer with real tar and unpacking. Each selected segment retains
its bytes and recording name; a 64MiB unrelated segment and the catalog stay out
of the transfer. Restoring the whole-archive copy fails both checks. The drill
still uses the repair tool and live CRC verification, and its traffic and recovery
deadlines stay unchanged. These two checks do not certify the distributed case.


`just test-component archive-recovery` creates one real Aeron recording per test,
with twelve 512-byte transaction payloads and one 64-KiB segment. The corruption
check changes payload bytes without changing the file length. CRC verification
must name the recording and report a checksum failure. The production diff and
repair functions must restore exactly the named segment without changing the
catalog. After marking the recording valid, CRC verification must pass. The wipe
check removes the entire owned archive directory, then restores the segment and
catalog from a saved copy through the production mirror function. It requires
no old archive mark file before restart. Both tests restart the real archive
and require `ArchiveRefetcher` to return every original payload, position and
publisher session exactly.

The target needs Linux and Docker and runs in the existing Docker CI job. It owns
its containers and scratch files, including cleanup after an assertion fails.
An external driver is never adopted. Before host-side repair, a bounded utility
container transfers ownership of the stopped test archive to the test user; this
matches the repair process's access to its files. The first complete two-test run takes
12.15 seconds of test execution; a later run on the shared host takes 43.68 seconds
and 49.85 seconds including Cargo. The complete matrix, at 210 checks then, passed in
263.05 seconds; its archive target takes 15.39 seconds including Cargo. These are
observed iteration timings, not a throughput measurement or a fixed latency
guarantee. Production term lengths, segment sizes and timeout
defaults stay unchanged. Aeron IPC/UDP, Docker and the JVM use real clocks.

The saved copy comes from the same stopped recording. This does not establish
independent peer recording, the distributed tar/catalog-transfer path, repair CLI
or Nomad drain/replacement behavior, live-ingress admission, executor progress, multi-segment recovery or transaction
load throughout a fault. These remain distributed requirements. The target adds
partial evidence for `archive-corruption` and `archive-tx-data-wipe`; neither full
chaos case is certified by these tests. Compiled negative controls fail when CRC
verification, segment repair or catalog copying is removed, and when replay asks
for a different publisher session.

`just test-component chaos-cache-readers` checks the recovery observer with two
independent HTTP peers. Every ingress must complete a cold read and increase its
own Redis lookup counter without increasing its degraded-read counter. Missing
exporters, failed RPCs, falling counters and recovery split across observations
cannot pass. A successful peer cannot mask an unrecovered peer. The observer keeps
its 120-second budget and reports each reader's latest counters or error. These
checks validate the observer; distributed Redis recovery and exact final state
still require the full chaos case.
