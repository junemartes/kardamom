# Rolling deploys without downtime

Status: designed, not built. Grounded in the deploy of 2026-09-30 to 2026-10-01 on staging.

## 1. Problem

A deploy today registers every changed job, one after the other, and waits only until the
new allocations show `running`. Four things make that a service interruption, or a risk of
one, even though every service of the pipeline has a twin or a replica set:

- "Healthy" means "the task stayed up for ten seconds" for every job but the ingress. A
  restarted executor that has not caught up, a sealer member that has not rejoined the Raft
  log, a batcher that fail-stopped on its first tick: all count as healthy, and the deploy
  moves on.
- The sealer job has no `update` stanza. Nomad's defaults replace the three Raft members one
  after the other with a ten-second health window, so a member that is slow to rejoin can be
  followed by the next member's stop. Two of three down is a stalled chain.
- A sealer that restarts from a snapshot keeps only the egress frames since that snapshot.
  Every consumer behind that floor (an executor, the validator, the batcher) is refused a
  replay and must resync. Before the batcher's spool (#471) that was a permanent DA gap.
- Every single-instance job (validator, batcher, da-watcher, indexer, proxy, monitoring)
  binds a static port and node-local state, so it can only restart in place: the service
  is down from the stop to the new task's readiness.

The deploy also cannot roll back: `auto_revert` is off everywhere, and no previous manifest
is kept.

## 2. What exists

| Mechanism | Where | State today |
|---|---|---|
| Nomad `update` stanza | every job but the sealer, aeron, redis, validator, monitoring | `max_parallel = 1`, `health_check = "task_states"`, 10 s, 2 min |
| Consul service checks | ingress (tcp), DA jobs (http), redis (tcp), monitoring (http) | the pipeline services (sequencer, executor, sealer, validator, batcher) have none |
| Deployment health in the role | `roles/workloads/tasks/wait.yml` | waits for `ClientStatus = running`, never reads `/v1/deployment` |
| Change detection | `job.yml` plan with `Diff` | registers only a changed job; `EnforceIndex` |
| Digest-pinned images | `images.digests`, cosign | one manifest per release; the previous one is not kept |
| Twins and replicas | ingress ×2 behind the load balancer, sequencer 2 per lane, executor ×3, sealer 3 (Raft), redis 1+1 with 3 sentinels | the data path survives one instance per class |

## 3. Design

### 3.1 Readiness is a service check, not a task state

Every service gets a Consul health check that means "this instance does its job", and its
`update` stanza switches to `health_check = "checks"`. Nomad then holds the next instance
until the current one passes the check for `min_healthy_time`.

| Service | Check | Passes when |
|---|---|---|
| ingress | http `GET /health` on the JSON-RPC port | the cluster session is open and the receipt feed is attached; fails during the drain |
| sequencer | http on the exporter: `/ready` | the lane publishes and the twin's dedup window is attached |
| executor | http on the exporter: `/ready` | the reader is at the sealer's head within `N` records (the lag gauge it already exports) |
| sealer | tcp on the ingress port plus a script check: `kardamom-cluster member-status` | the member reports `FOLLOWER` or `LEADER` with the log position within `N` entries of the leader |
| validator | http `/ready` | synced within its lag budget |
| batcher | http `/ready` | the feed loop runs and the spool is loaded |
| state-mirror, da-watcher, indexer | http `/ready` | the follower advanced within the last two ticks |

The exporter already runs in every Rust service (`kardamom_obs`); a `/ready` route beside
`/metrics` is one handler per service that reads the same gauges the dashboards show.

### 3.2 The deploy waits for the deployment, not for the allocations

`wait.yml` reads `/v1/deployment/<id>` and requires `Status = successful`. A deployment that
Nomad marks `failed` fails the playbook at that job, before the next job is touched. This
is the one change that turns the role's sequence into a staged rollout: each job rolls
instance by instance under its own check, and the role moves on only when the job is whole
again.

### 3.3 Order of the roll

The role deploys in dependency order already. The roll inside a deploy keeps that order,
and adds a gate between the classes that share a stream:

1. substrate (aeron media drivers): one node at a time, `max_parallel = 1`; the system job's
   update stanza with a tcp check on the driver's control port.
2. sealer: one member at a time, `min_healthy_time = 60s` after the member-status check
   passes, so the member has rejoined and caught up before the next one stops. Two members
   never restart together. The leader goes last: the role reads the leader from
   `member-status` and submits the job with a constraint that orders the followers first,
   or restarts the leader's allocation explicitly after the followers (Nomad has no
   "last" ordering, so the role does it: two deployments, followers then leader).
3. sequencer: lane by lane, one replica at a time; the twin keeps the lane publishing.
4. executors: one at a time, each waiting for `/ready` (caught up).
5. ingress: one at a time; the load balancer's health check (3.5) takes the draining one
   out before Nomad stops it.
6. the followers and singletons (validator, batcher, da-watcher, indexer, DA proxy): in
   place, with their readiness checks; see 3.4 for the window.

### 3.4 Singletons: a short window, not none

A singleton with a static port and node-local state cannot run twice on its node. Two of
them can still restart without loss:

- The batcher keeps its consumed blocks in the spool (#471) and resumes just past it. The
  gap is the restart time, and nothing is lost.
- The validator resumes from its cursor; a restart within the sealer's retention costs
  the restart time only.

A singleton whose window matters gets a second instance on a second node in a later step:
the validator and the batcher are both stateless with regard to the chain (their state is
derivable), so a standby that follows the stream and takes the lease (the batcher already
has a lease concept on L1: the settlement's `l1Batcher`) is a design of its own. Not in
this spec.

### 3.5 The load balancer takes a draining ingress out

The provider load balancer gets an http health check on `/health` with a short interval
(5 s, two failures). The ingress's drain (`kill_timeout = tx_ttl + 10 s`) makes `/health`
fail from the first second of the drain, so the balancer stops routing new connections
while the in-flight ones finish. Terraform (`terraform/hetzner`, the `kardamom-staging-rpc`
balancer) carries the check.

### 3.6 Canary for the stateless classes

For the ingress and the sequencer, `canary = 1` with `auto_promote = false`: one new
instance joins beside the old ones, the role runs the smoke against the canary (the ingress
by its node address; the sequencer through a transaction pinned to its lane), and promotes
on success. A failed canary is removed, and the old instances keep running. This is the
only place a bad release is caught before it reaches a majority.

### 3.7 Roll back

The deploy keeps the last successful manifest beside the current one
(`images.digests.previous`, written by the record role). `just rollback <env>` runs the
deploy with that manifest; it is a normal rolling deploy of older images. `auto_revert` is
turned on for the stateless classes (ingress, sequencer) where Nomad's revert is safe,
and stays off for the stateful ones, where the role's rollback is the path.

## 4. What this does not cover

- A schema change in a durable format (the state DB, the archive, the KAR1 frames): a
  rolling deploy across two versions requires the new version to read the old format, and
  that is a rule for every change to those formats, not a deploy mechanism.
- A change to the Raft log's entry format: the sealer members must agree; such a change
  ships behind a version in the entry and rolls followers first.
- A Nomad or Consul upgrade: the bootstrap play, not the deploy.

## 5. Order and estimate

| Step | Work | Proof |
|---|---|---|
| 1 | `/ready` routes in the Rust services; Consul checks on every job; `health_check = "checks"` | a deploy of a service that fails its check stops the deploy |
| 2 | `wait.yml` reads the deployment status; a failed deployment fails the play | the chaos suite's "deploy a broken image" case (new) |
| 3 | the sealer's update stanza and the followers-first leader order; the member-status command | a rolling sealer deploy under load keeps the chain advancing (the existing load shard with a deploy in the middle) |
| 4 | the load balancer health check; the ingress drain fails `/health` | a rolling ingress deploy with a sender attached loses no transaction |
| 5 | canaries for ingress and sequencer; the smoke against the canary | a canary with a broken image is removed, the old instances serve |
| 6 | the previous manifest and `just rollback` | a rollback after a bad deploy restores the previous images under the same checks |

Steps 1 and 2 remove the risk; 3 and 4 remove the stalls; 5 and 6 add the safety net.

## 6. Open questions

1. The sealer member-status check needs a command the check can run inside the node
   container: the Java service exposes the member state on its control port today only
   through logs. A small admin endpoint on the sealer is the cleanest.
2. `min_healthy_time` for an executor depends on its catch-up time after a restart, which
   depends on the chain's rate. The check should express "caught up", not a fixed time.
3. The validator's `reschedule { attempts = 0 }` (a divergence must stay visible) conflicts
   with a rolling deploy's reschedule on a failed node; the deploy must distinguish a
   divergence halt from a node loss.
