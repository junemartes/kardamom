# Dynamic Sequencer Sizing

- **Date:** 2026-09-03
- **First draft:** 2026-08-31
- **Status:** Draft. Design review in progress.
- **Topic:** Change the number of active sequencer shards at run time. Keep
  the canonical log correct. Keep the availability cost bounded.
- **Supersedes:** the non-goal "shard scaling" in
  `docs/agents/replicated-sequencer-shards-spec.md:110-112`.

## 1. Context

The sequencer tier shards senders by address. The rule is
`shard = keccak256(sender)[..8] % M`. Today M is static configuration. A
code survey found these facts:

- The ingress and the sequencer each have a copy of the routing function.
  The two copies must agree byte for byte (`crates/ingress/src/routing.rs`,
  `crates/sequencer/src/partition.rs`).
- The sequencer is stateless. Its `next_nonce` map is a cache. A cold sender
  starts at nonce 0 (`crates/sequencer/src/sequencer.rs:431-440`). The
  floor of a sender rises only when a receipt for that sender arrives
  (`feeds.rs:223-239`). No other nonce source exists. The comment in
  `state/mod.rs:68-71` describes a state lookup. That code was removed.
- A restarted replica does not regain coverage of an established sender
  until a receipt arrives. The twin publishes, so the receipt comes. This is
  the open issue F02.1 (`main.rs:214`,
  `docs/reviews/2026-07-17-30-commit-review/fixes-WP-SEQ.md:52`).
- A future-nonce transaction waits in a `PendingBuffer`. The entry has no
  time limit. It waits until its gap fills.
- A ref carries `shard_id`. This field names the tx_data archive that holds
  the envelope (`crates/types/src/txref.rs:47-58`). The executor joins on
  (shard_id, session, position). The sequencer writes its own id into the
  field (`sequencer.rs:149-156`). Today the two values are equal.
- The sealer has a global per-sender contiguity guard
  (`CanonicalSealerState.java:254-270`). The guard has two documented holes.
  An unknown sender seeds at any nonce. An LRU eviction re-seeds. If two
  shards publish for one sender through a hole, the reject classifier can
  drop a ref that was never committed (`resync/mod.rs:315-368`). That is a
  permanent transaction loss. The executor turns a sealed gap into a
  deterministic skip receipt, so the state stays correct.
- Every M-consuming process builds a fixed vector of Aeron handles at
  startup. The ingress opens M publishers. The executor, the validator, and
  the batcher open M subscriptions (`crates/engine/src/bin_support.rs`). No
  process can resize its vector without a restart.
- The sequencer opens one tx_data subscription (`main.rs:202`).
- The wire caps M at 256. The fields `sequencer_id`, `TxRef.shard_id`, and
  the Raft record field are `u8`.
- The code default is M=8. The deploy runs M=2. A service that starts
  without the explicit flag opens the wrong fan.
- Six copies of the value "2" exist across `group_vars`, the sequencer TOML
  template, and five Nomad jobs. The script `check-contract.py` checks none
  of them. The script `chaos.sh` has a precomputed `keccak % 2` account
  table.
- The Java sealer does not depend on M. It keys on the sender and on the
  `canonical_id` only.
- Deposits do not depend on M. All sequencers publish the same `DepositRef`
  records. The cluster dedups them.
- The ingress receives receipts from the executors. It waits for a receipt
  for 30 seconds (`pending_receipt_timeout`). Then it returns a timeout
  error to the client.
- No component serves `eth_getTransactionCount`. The ingress returns a
  "deferred" error (`crates/ingress/src/json_rpc.rs:9-10`). The executor has
  no listener except the metrics exporter.

## 2. Goals and non-goals

**Goals**

- Change the active shard count with one operator runbook. Do not redeploy
  the full stack.
- Keep the canonical log correct. Per-sender nonces stay dense and
  ascending. Do not lose a transaction through the guard holes.
- **Do not drop an accepted transaction within its lifetime.** A
  transaction lives for one expiry time. Within that time, no resize step
  drops it. At expiry, the client gets an explicit error. The client can
  resubmit.
- Keep the sealer, the deposit path, the executor join logic, and the DA
  format unchanged.
- Close issue F02.1. A restarted replica must regain coverage of an
  established sender without help from its twin.
- Make the shard contract machine-checked.

**Non-goals**

- An autoscaler policy. This spec builds the mechanism. A later spec can
  drive it.
- More than 8 physical lanes, or live lane creation. See section 3.1.
- Sealer membership changes, or more than 2 racing replicas per shard.
- Live map distribution to the ingress without a restart. See section 3.6.
- Same-nonce replacement transactions. This is a separate work item.

## 3. Design

The design has five parts:

1. A fixed lane plane of 8 lanes.
2. A two-level shard map. The map is configuration, not a log record.
3. A transaction expiry in the sequencer.
4. A nonce lookup from the sequencer to an executor.
5. A resize protocol that uses only restarts and waits.

Parts 3 and 4 make the protocol simple. After the expiry time, the old
shard holds nothing that the new shard has not seen. With the lookup, a
cold replica heals on its own. So every reconfiguration step is a restart
with a new config. The sealer does not take part. The log gets no new
record kinds.

### 3.1 Fixed lane plane

Preallocate **K_max = 8 physical lanes** at deploy time. Every process opens
all 8 lanes at startup:

- The ingress opens 8 tx_data publishers and 8 recorder threads.
- The executor, the validator, and the batcher open 8 tx_data
  subscriptions.
- Each sequencer process opens one or more lanes. See section 3.5.

An idle lane costs one handle and one idle stream. All lanes share the one
multicast group. A lane is a stream id, `base + lane`. No channel or
firewall change is necessary.

This choice removes every fixed-vector blocker at once. The dynamic part
of the design is then only: which lanes are active, and which senders map
to them. K_max = 8 matches the code default. The flag and the default agree
again.

### 3.2 Two-level shard map

Split the routing into a fixed level and a dynamic level:

1. **Fixed:** `vslot = keccak256(sender)[..8] % 256`. This never changes.
   It is the only rule the ingress and the sequencer must share forever.
2. **Dynamic:** a versioned table `map[v]: vslot -> lane`. The table has
   256 bytes.

Define **map v0 as `lane = vslot % M`**. For M = 2 and M = 8, 256 % M is 0.
So v0 assigns every sender exactly as `keccak % M` does today. The refactor
is a no-op at the current M. A parity test pins this. The identity holds
only when M divides 256. Do not seed v0 with M = 3.

The map is configuration:

- The ingress config holds the full table.
- Each sequencer replica config holds its lane and its **vslot set**. The
  wrong-shard guard in the sequencer drops an envelope whose vslot is not
  in the set.

During a resize, two shards hold the same vslot in their sets. Section 3.5
explains why this is safe. Outside a resize, the ingress table must equal
the union of the sequencer vslot sets. The contract check enforces this.

### 3.3 Transaction expiry

Each `PendingBuffer` entry gets a deadline:

- Stamp the deadline at arrival. Use the local monotonic clock. The two
  racing replicas then expire an entry at nearly the same instant. A small
  difference is harmless.
- Set the deadline to arrival plus `tx_ttl`. One config value in
  `group_vars` drives `tx_ttl` in the sequencer and
  `pending_receipt_timeout` in the ingress. Today that value is 30 seconds.
- Refresh the deadline in `reinsert_for_retry`. A rebuffered entry is a new
  parked entry.
- Expire only an entry whose nonce is above the expected nonce. Never
  expire an entry that the state machine already advanced past.
- Keep a deadline heap of (deadline, sender, nonce). The sweep runs on the
  loop tick. Its cost is proportional to the expired entries, not to the
  senders.
- Report each expiry on the tx_errors channel, with a new reason
  `Expired`. A `kardamom_subscribeReceipts` client sees it as a `TxError`
  event. A plain HTTP client sees only the ingress timeout.

The expiry gives the design a bound. No entry that waits on a nonce gap
outlives `tx_ttl`. An entry that waits on sealer backpressure lives until
the sealer recovers. The refresh on reinsert causes this. The runbook must
handle it. See section 3.5 step 5 and section 3.8.

### 3.4 Nonce lookup from an executor

**Executor side.** The executor gains a small read-only query endpoint. It
serves the nonce of an account from the same snapshot source that revm's
`basic()` uses. The transport is HTTP JSON-RPC on the executor node. This
is new code. The executor has no request listener today.

**Sequencer side.** The lookup runs when a cold sender first appears with a
nonce above 0. A fresh account at nonce 0 publishes as today, with no
query. The sequencer:

1. Parks the transaction, as today.
2. Adds the sender to an in-flight set. One lookup per sender at a time.
   The lookup has a timeout. The timeout removes the sender from the set.
   A later transaction from that sender then starts a new lookup.
3. Sends the query from a task off the polling thread. The polling thread
   must stay dedicated to tx_data (`main.rs:262-266`).
4. Delivers the answer as a `FloorUpdate` on the existing floor channel.
   The floor advances through `advance_floor`, with a max merge. It never
   uses `seed_next_nonce`.
5. The parked entry drains on the next loop tick.

The sequencer asks any executor. It gets the executor list from Consul or
from a static list in its config. An executor at any height gives a valid
answer, because the answer is a lower bound.

**Why this is safe, against F02.1.** The reverted fix in
`fixes-WP-SEQ.md` was a locally inferred fast-forward. It adopted holes
that a client abandoned. That published nonce gaps. A committed-nonce
lookup is different. It can lag the truth. It can never lead the truth. A
floor that lags is safe. It only makes the sequencer park a transaction a
little longer.

**The F02.2 case.** The executor answers `c`. The old shard has ordered
nonces `c` to `j-1`, which are not executed yet. The new shard receives a
transaction at nonce `j`. It parks the transaction, because `j > c`. The
receipts for `c` to `j-1` arrive. The floor rises to `j`. The parked entry
drains. No harm.

**Failure mode.** If no executor answers, the sender stays parked. The
entry expires after `tx_ttl`. The client gets an explicit error. The log
stays correct.

**Follow-on, out of scope.** With this endpoint, the ingress can serve
`eth_getTransactionCount` by proxy. That closes the "deferred" error in
`json_rpc.rs`.

### 3.5 Resize protocol

The protocol is a warm overlap. Both shards publish in parallel for a short
time. Their refs are identical, because both run the same state machine on
the same stream. The sealer dedup absorbs the duplicates. This is the
racing-replica pattern that exists today, with four publishers instead of
two on the moved vslots.

Scale-out example: move a set of vslots from lane 0 to the new lane 2.

1. **Start the new shard.** Nomad starts the shard-2 job group with two
   racing replicas. The config of each replica holds:
   - lane 2,
   - the vslot set of lane 2 under map v+1,
   - the old lanes to subscribe to, here lane 0,
   - the incoming vslots, which start in **shadow mode**.

   Each replica subscribes to lane 2 and to lane 0. No ingress change is
   necessary. The replica joins the multicast group of lane 0. Each replica
   also widens its receipt filter to its full vslot set.
2. **Warm up.** In shadow mode, the replica runs the normal state machine
   on the incoming vslots. It suppresses the ref publisher and the
   tx_errors publisher for those vslots. The suppression is per vslot, not
   per process. A scale-in moves vslots onto a shard that keeps publishing
   for its existing senders. Wait for `tx_ttl` plus a margin. The margin
   covers scheduling jitter. The timer starts when the old-lane
   subscription reports that it is connected. It does not start at process
   start. After the wait, the replica's buffers are a
   superset of the old shard's live state for the incoming vslots.
3. **Take over.** Each new replica leaves shadow mode on a local timer. It
   starts to publish for the incoming vslots. A local timer is safe here.
   Both shards publish identical refs. A cold sender on the new shard
   heals through the lookup of section 3.4.
4. **Switch the ingress.** Re-render the ingress config with map v+1.
   Restart the two ingress allocs one at a time. New envelopes for the
   moved vslots now land on lane 2. An envelope from a not-yet-restarted
   ingress lands on lane 0. Both shards serve it.
5. **Drain.** Wait `tx_ttl` after the last ingress restart. After that,
   every entry that waits on a nonce gap has drained or expired. An entry
   that waits on sealer backpressure can survive. So the runbook reads the
   pending-buffer depth for the moved vslots on both old replicas. It
   requires zero on both before step 6.
6. **Restart the old shard.** Restart the lane-0 replicas one at a time
   with the vslot set of map v+1. Each restarted replica is cold. It heals
   through the lookup. The twin covers it meanwhile. For a scale-in, stop
   the job group instead.
7. **Collapse the new shard.** Restart the lane-2 replicas one at a time
   with the final config. The final config has no old lane and no shadow
   vslots.

Scale-in is the same protocol. The leaving shard's vslots move to the
remaining shards. The remaining shards get the old lane and the incoming
vslots in their config. Nomad stops the leaving group at step 6.

**Ref lane.** During steps 3 to 6, a new replica publishes refs for
envelopes that live on lane 0. The ref must carry lane 0 in `shard_id`.
So `RefMetadata` gets a `lane` field. The subscription that received the
envelope sets it. The function `make_txref` writes it into the ref. Today
the lane equals the sequencer id, so this is a no-op. It ships in
milestone 1.

### 3.6 Ingress map distribution

The ingress does not hold a cluster connection. The map reaches the ingress
by config re-render plus a rolling restart of the active/active pair. This
loses no transactions. An envelope that the ingress already published is
safe. A client that waits on the restarted ingress sees a connection error.
It must resubmit. A resubmit of a sealed transaction gets the cached
receipt or a duplicate error. Milestone 6 adds a graceful drain: the
ingress stops accepting, waits for its pending waiters up to `tx_ttl`, then
exits. The Nomad job gets a matching `kill_timeout`.

### 3.7 Availability cost

- In-flight transactions: no drops. The old shard serves through the
  warmup. Both shards serve through the drain. The dedup absorbs the
  duplicates.
- Envelopes from a not-yet-restarted ingress: no drops. Both shards serve
  the old lane through the drain.
- Buffered future-nonce transactions: no drops within `tx_ttl`. The new
  shard builds the same buffers from the shared stream during the warmup.
  An entry parked before the warmup drains or expires on the old shard.
- Unmoved senders: no effect.
- Deposits: no effect.

Residual effects:

- The sealer sees four publishers on the moved vslots during the overlap.
  The dedup absorbs them. The ingress load on the sealer doubles for those
  vslots for about two `tx_ttl` periods.
- A replica can expire an entry after its twin published it. The client
  then gets a spurious expiry error. The receipt is the truth.
- During the overlap, four replicas hold the same parked entry. A
  subscription client can receive up to four `Expired` events for one
  transaction.
- The sequencer now has a request dependency on the executors. A lookup
  failure delays a cold sender. It does not break the log.
- A resize takes at least two `tx_ttl` periods plus the restarts. Resizes
  are rare.

### 3.8 Deploy and contract changes

- **Nomad:** one job group per active lane, generated from the map version.
  Each group has `count = 2` and `distinct_hosts`. Replace the two
  hardcoded port pairs with a per-lane port lane. The metrics port is
  `9001 + 10 * lane`. The egress port is `40210 + 10 * lane`. Document both
  in `group_vars`. This supersedes the `--partition-offset` rotation. That
  formula guarantees cross-placement only when M equals the sequencer node
  count. Each group passes an explicit lane and vslot set instead.
- **Ansible:** `node_classes.sequencer.count` follows the active lane
  count. The hcloud path from the hybrid-fleet plan adds machines.
- **Contract:** extend `check-contract.py`. It checks every mirror of the
  map, the port lanes, the `tx_ttl` mirror, and the `ACCT_SHARD` table in
  `chaos.sh`. Outside a resize, the ingress table must equal the union of
  the sequencer vslot sets.
- **Runbook:** a `scale-sequencers.sh` that runs the seven steps of
  section 3.5. It refuses to start a second resize while one is in flight.
  It refuses to start a resize while sealer backpressure is active. The
  runbook owns the overlap window. The contract check owns the steady
  state.

## 4. Milestones

1. **Vslot refactor, no-op.** Add `vslot_for` and the map layer behind
   `partition_for` in the ingress and the sequencer. Ship map v0 as
   `vslot % M`. Add the `lane` field to `RefMetadata` and write it into the
   ref. Correct the `sequencer_id` names in the engine crate errors and
   docs. Parity test: the assignment is identical to `keccak % M` for M in
   {2, 8}.
2. **Lane plane.** All processes open 8 lanes. Add the contract check for
   the mirrors. Remove the u32 to u8 truncation casts in the ingress main.
   Exit: the cluster runs unchanged with 6 idle lanes.
3. **Expiry.** The deadline heap, the sweep, the refresh on reinsert, and
   the `Expired` error reason. One `group_vars` value drives `tx_ttl` and
   `pending_receipt_timeout`. Exit: a parked entry expires at `tx_ttl` with
   an explicit error, and a rebuffered entry does not.
4. **Nonce lookup.** The executor query endpoint. The sequencer lookup
   task, the in-flight set, and the floor delivery. Exit: a restarted
   replica regains coverage of an established sender with its twin stopped.
   This closes F02.1.
5. **Multi-lane sequencer.** The subscription set, the per-vslot shadow
   mode with both publishers suppressed, the vslot-set guard, the widened
   receipt filter, and a per-vslot pending-buffer depth gauge. Exit: a
   scripted 2 to 3 resize on the e2e harness moves senders with zero loss
   within `tx_ttl` and no wedge. The scenario includes a sender that is
   idle for longer than `tx_ttl` before the resize and submits after the
   take-over.
6. **Deploy.** Generated Nomad groups, port lanes, the ingress graceful
   drain and `kill_timeout`, the ingress re-render and roll,
   `scale-sequencers.sh`, and the contract checks.
7. **Verification.** An e2e resize-under-load scenario with nonce-gap
   assertions for moved senders. A chaos case: kill a new-shard replica
   during the overlap. A chaos case: all executors unreachable during a
   lookup.
8. **Doc repair.** Fix the five stale sites: the preferred/follower prose
   in `crates/sequencer/src/lib.rs:10-18`, the fast-forward text in
   `docs/agents/replicated-sequencer-shards-spec.md:62-73`, the nonce-0
   comment in `feeds.rs:201-211`, the state lookup comment in
   `state/mod.rs:68-71`, and the F02.1 log line in `main.rs:214`. Update
   the F02.1 status in `fixes-WP-SEQ.md`. Mark the superseded non-goal.

## 5. Risks and open questions

- **Same-nonce replacement during the overlap.** A replacement can race a
  laggard replica. The guard's `nonce < expected` path drops it as
  committed. The ingress can detect a same-nonce resubmit only in its own
  pending registry. A replacement sent to the other ingress goes
  undetected. Replacement is unsupported today. This spec does not change
  that.
- **Executor endpoint load.** A burst of cold senders above nonce 0 becomes
  a burst of lookups. The in-flight set bounds it to one per sender. Add a
  rate limit and a metric. Measure it in milestone 4.
- **Expiry and backpressure.** The refresh on reinsert must cover every
  rebuffer path. Audit `flush_drained` and `reinsert_for_retry` in
  milestone 3.
- **Guard holes remain.** Exclusive ownership after the drain removes the
  resize loss path. LRU eviction and empty-map snapshots still exist. A
  persistent guard map in the sealer snapshot is a separate hardening item.
- **u8 cap.** Lanes, `shard_id`, and `sequencer_id` cap at 256. K_max = 8 is
  far below it.
- **Clock skew between replicas.** The expiry uses local clocks. The two
  replicas of a shard can expire an entry a few milliseconds apart. The
  effect is one spurious error at most. It cannot cause a loss.
