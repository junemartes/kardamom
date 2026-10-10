# Chaos suite

The chaos suite injects faults into a running cluster under steady load. It then checks that the pipeline recovers and that the persisted state is correct.

- The code is in `crates/chaos`. The shard list is in `crates/chaos/src/shard.rs`.
- [`failure-modes.md`](failure-modes.md) says what each fault does and how the actor recovers. This document says how the suite judges a run.
- The load harness is `kardamom-load` (`crates/bench/src/load/`). The suite runs it in process.

## Running a shard

A shard is one test. It brings up its own container cluster, runs its stages, and tears the cluster down on success.

| Command | What it does |
|---|---|
| `just shard <shard>` | Runs the shard end to end, with the cluster lifecycle, as CI does. |
| `just container-test <shard>` | Runs the shard against a cluster that is already up. It sets `KARDAMOM_CHAOS_REUSE=1`. |

- Run both recipes from the repository root. The root `justfile` forwards them to `deploy/cluster/justfile`.
- The default shard is the value of `SHARD`, or `load`.
- CI runs every shard in `.github/workflows/cluster-e2e.yml`. Each shard has its own runner and its own cluster.
- The host needs Docker, OpenTofu, Ansible and the prebuilt artifacts. `just cluster-bootstrap` installs the host tools.
- On a failure, the cluster stays up. `just container-diagnostics` collects the state of the nodes and the jobs.
- The bring-up sets `KARDAMOM_CLUSTER_BOOTSTRAP=1`. The deploy opens the sealer bootstrap only for a cluster job that Nomad does not know, so a bring-up over a running cluster opens none. See [Sealer bootstrap](../deploy/cluster/README.md#sealer-bootstrap).

### The stages of a shard

Every shard runs these stages in this order.

1. The smoke gate. One transfer from the gate account (`KARDAMOM_CHAOS_GATE_ACCOUNT`) through ingress-0 must get a receipt with status 1. The running batcher must still hold its `tx_data` lane subscriptions.
2. The shard body:
   - A chaos shard runs its cases in order. See [Shards](#shards).
   - The `load` shard runs the sustained load. The `semantics` shard runs the chain-semantics suite.
3. The ingress churn. The suite stops the ingress-0 allocation and sends one transfer through ingress-1.
4. The validator verdict. See [Validator verdict](#validator-verdict).
5. The persisted-state audit. See [Persisted-state audit](#persisted-state-audit).

### Chaos case spine

Every chaos case follows the same steps.

1. Pick a funded account and start the case load.
2. Wait for the injection gate. See [Gates](#gates).
3. Run the case body. It injects the fault and checks the case-specific result.
4. Check the progress of the pipeline. A cluster case and a node-failure case use the executor block gauge with a wide window.
5. Wait for the load to end. Judge the load verdict. See [The load verdict](#the-load-verdict).
6. Check that the executors converged.
7. Run the recovery probe.
8. Run the validator verdict.

## Shards

There are fourteen shards. `just container-test` lists their names. Thirteen run on every pull request. `chaos-integrity` runs nightly.

| Shard | Cases, in run order | Shard settings |
|---|---|---|
| `load` | Sustained load: transfers, then the DeFi mix | `LOAD_DURATION_S=300`, `LOAD_TARGET_TPS=300`, `LOAD_SENDERS=6` |
| `semantics` | The chain-semantics suite (`SEMANTICS_CASES`) | `RUN_LOAD=0` |
| `chaos-executor` | `graceful-executor`, `hard-executor`, `node-failure-executor`, `node-replace-executor`, `state-checkpoint-restore`, `replay-window-resync`, `deploy-broken-image` | `RUN_LOAD=0`, the recorded cursor on (`KARDAMOM_EXEC_CURSOR=on`) |
| `chaos-ingress` | `graceful-ingress`, `hard-ingress`, `archive-driver-loss`, `archive-tx-data-wipe`, `archive-corruption` | `RUN_LOAD=0` |
| `chaos-sequencer` | `graceful-sequencer`, `hard-sequencer`, `sequencer-replica-kill`, `sequencer-lapse`, `validator-lapse`, `validator-join`, `lookup-blackout`, `resize-scale-out-in` | `RUN_LOAD=0` |
| `chaos-cluster` | `cluster-leader-kill`, `cluster-follower-kill`, `cluster-member-rejoin`, `node-replace-sealer`, `cpu-squeeze` | `RUN_LOAD=0`, sealer snapshot interval 60 s (`KARDAMOM_CLUSTER_SNAPSHOT_S=60`), `SQUEEZE_CYCLES=3`, `SQUEEZE_S=60`, `SQUEEZE_CPUS_PER_NODE=0.4` |
| `chaos-fleet` | `executor-fleet-loss-recover`, `executor-fleet-wipe-recover`, `executor-fleet-total-wipe-recover`, `redis-total-loss-recover`, `cluster-quorum-loss-recover`, `cluster-total-loss-recover`, `sealer-fleet-total-wipe-recover` | `RUN_LOAD=0` |
| `chaos-coordinated` | `ingress-pair-loss-recover`, `sequencer-lane-loss-recover`, `pipeline-blackout-recover` | `RUN_LOAD=0` |
| `chaos-combined-ordering` | `ingress-sequencer-loss-recover`, `ingress-sealer-loss-recover`, `sequencer-sealer-loss-recover`, `ingress-sequencer-sealer-loss-recover`, `ingress-sequencer-sealer-reverse` | `RUN_LOAD=0` |
| `chaos-combined-exec` | `executor-sealer-loss-recover`, `executor-sealer-validator-recover`, `ingress-executor-loss-recover`, `read-path-loss-recover` | `RUN_LOAD=0` |
| `chaos-retention` | `retention-overrun`, `retention-overrun-validator` | `RUN_LOAD=0`, egress retention 6144 frames (`KARDAMOM_CLUSTER_RETENTION=6144`) |
| `chaos-cache` | `redis-partition-ingress`, `redis-primary-kill`, `redis-primary-freeze`, `mirror-kill-rebuild` | `RUN_LOAD=0` |
| `chaos-l1` | `l1-liar`, `l1-null-receipts`, `follower-instance-loss`, `follower-total-loss`, `two-day-outage`, `batcher-outage-past-retention` | `RUN_LOAD=0`, retention 6144, snapshot interval 60 s, `L1_FAULT_S=60`, indexer poll 2 s, da-watcher silence 30 s, the L1 fault proxy on |
| `chaos-integrity` (nightly) | `executor-restart-storm` | `RUN_LOAD=0` |

Case order matters in five places.

- `resize-scale-out-in` runs last in `chaos-sequencer`. A resize leaves the shard map at a later version.
- `sealer-fleet-total-wipe-recover` runs last in `chaos-fleet`. It restarts the chain from a state rebuilt from L1, the longest recovery. A failure there must not hide the other cases.
- `pipeline-blackout-recover` runs last in `chaos-coordinated`. It can leave an entry that no archive serves.
- `mirror-kill-rebuild` runs last in `chaos-cache`. It flushes the projection.
- `executor-sealer-loss-recover` runs first in `chaos-combined-exec`. The persisted-state audit runs after it, before the other cases build on the state it leaves.

`chaos-l1` runs the persisted-state audit after every case, not only at the end of the shard. Each case leaves a DA record that a silent gap could hide in. `chaos-combined-exec` runs it after `executor-sealer-loss-recover`: the executor nodes and the sealer nodes die at once, so the state every executor kept is compared with the validator's before the next case.

After each of these audits, the next case waits until the chain runs again. See [Recovery after an audit](#recovery-after-an-audit).

`KARDAMOM_CHAOS_CASES` narrows a run to a space-separated list of case names. An unknown name fails before any load starts.

The nightly shard holds the long repetition cases. The `cluster-e2e` workflow runs it on its schedule (03:17 UTC), alone. Adding the `chaos-nightly` label to a pull request runs it alone too, in a run of its own: the `labeled` event starts the run, and no other label starts one. The build job of the workflow picks the shard list. A nightly run has its own concurrency group, so it never cancels the regular run of the ref. A failed shard of the scheduled run opens an issue `nightly: the <shard> shard failed` with the `chaos-nightly` label, or adds the run to the open issue of that shard.

### Cases

Each case name links to the section of [`failure-modes.md`](failure-modes.md) that holds its failure behavior. A case with no section of its own links to the nearest one.

**Executor shard**

- [`graceful-executor`](failure-modes.md#executor): stops one executor allocation with a graceful stop. The job returns to three replicas. The restarted executor records a new `exec_txs` session, and its recorded cursor advances.
- [`hard-executor`](failure-modes.md#executor): kills one executor task. While it is down, the sealer's best recorded cursor (`kardamom_ingress_cluster_recorded_head`) must move within 20 s. With `KARDAMOM_EXEC_CURSOR` off, the case skips this check with a log line. The job returns to three replicas. The restarted executor records a new `exec_txs` session, and its recorded cursor advances.
- [`node-failure-executor`](failure-modes.md#executor): kills a whole executor node. The fleet keeps progressing with two replicas. The node returns.
- [`node-replace-executor`](failure-modes.md#executor): replaces an executor node through the Terraform root. The new node has a new address and empty volumes.
- [`state-checkpoint-restore`](failure-modes.md#executor): wipes the state of executor-0. It restores from the checkpoint of executor-1.
- [`replay-window-resync`](failure-modes.md#executor): wipes executor-1 with its checkpoints. It fetches a peer checkpoint and resumes.
- [`deploy-broken-image`](failure-modes.md#deploys): deploys a manifest with a bad executor image. The deploy must fail, and the old replicas must keep running.

**Ingress shard**

- [`graceful-ingress`](failure-modes.md#ingress-xn-activeactive): stops one ingress allocation with a graceful stop. The job returns to two replicas.
- [`hard-ingress`](failure-modes.md#ingress-xn-activeactive): kills one ingress. The victim rotates with the CI run id (`INGRESS_VICTIM`).
- [`archive-driver-loss`](failure-modes.md#substrate-the-shared-failure-domain): kills the Aeron media driver under ingress-0.
- [`archive-tx-data-wipe`](failure-modes.md#substrate-the-shared-failure-domain): wipes the `tx_data` archive of ingress-0. The case restores it from ingress-1 and verifies it.
- [`archive-corruption`](failure-modes.md#substrate-the-shared-failure-domain): flips bytes in an archive segment. A CRC verify must find it, and a targeted heal must fix it.

**Sequencer shard**

- [`graceful-sequencer`](failure-modes.md#sequencer-2-shards-x-2-racing-replicas): stops a lane-0 replica with a graceful stop.
- [`hard-sequencer`](failure-modes.md#sequencer-2-shards-x-2-racing-replicas): kills the lane-0 replica on sequencer-0.
- [`sequencer-replica-kill`](failure-modes.md#sequencer-2-shards-x-2-racing-replicas): kills the same replica. The restarted replica must export metrics again.
- [`sequencer-lapse`](failure-modes.md#sequencer-2-shards-x-2-racing-replicas): freezes a replica for `SEQ_LAPSE_S`. The replica must detect the lapse and resync.
- [`validator-lapse`](failure-modes.md#validator-off-the-hot-path-halts-on-divergence): pauses the validator for `LAPSE_S`. It must catch up and verify with no divergence.
- [`validator-join`](failure-modes.md#validator-off-the-hot-path-halts-on-divergence): kills the validator and wipes its state. The new validator adopts a peer checkpoint and verifies live.
- [`lookup-blackout`](failure-modes.md#sequencer-2-shards-x-2-racing-replicas): cuts the executor and Redis traffic of a sequencer node. It kills the lane-0 replica there.
- [`resize-scale-out-in`](failure-modes.md#sequencer-2-shards-x-2-racing-replicas): grows the sequencer from two lanes to three under load, kills a replica of the new lane, and shrinks back.

**Cluster shard**

- [`cluster-leader-kill`](failure-modes.md#sealer-the-aeron-cluster-raft): kills the Raft leader. The pipeline must keep committing.
- [`cluster-follower-kill`](failure-modes.md#sealer-the-aeron-cluster-raft): kills a follower. The restarted member must restore from a snapshot.
- [`cluster-member-rejoin`](failure-modes.md#sealer-the-aeron-cluster-raft): waits up to 6 minutes for the `cluster LOG PURGED` line of the leader. A replay from position 0 is then impossible. The case then kills a follower and wipes its cluster and archive directories. The blank member must seed from a peer snapshot and reach the head that the executors had at the wipe.
- [`node-replace-sealer`](failure-modes.md#sealer-the-aeron-cluster-raft): replaces the node of a follower through the Terraform root. The blank member must start blank and reach the head that the executors had at the replacement.
- [`cpu-squeeze`](failure-modes.md#validator-off-the-hot-path-halts-on-divergence): throttles the CPU of every pipeline node in cycles. The validator may slow down. It must never diverge.

**Fleet shard**

- [`executor-fleet-loss-recover`](failure-modes.md#executor): all three executor nodes die. Each executor resumes from its own state.
- [`executor-fleet-wipe-recover`](failure-modes.md#executor): kills the three executor tasks, stops the job, and wipes every state database while no executor runs. Each executor restores from its local checkpoint.
- [`executor-fleet-total-wipe-recover`](failure-modes.md#executor): all state and checkpoints are wiped. Every executor resumes from a state rebuilt from L1.
- [`redis-total-loss-recover`](failure-modes.md#redis-account-cache): the whole Redis job stops. Every state mirror must rebuild. No sentinel may restart on the cold start.
- [`cluster-quorum-loss-recover`](failure-modes.md#sealer-the-aeron-cluster-raft): two sealer members die. The pipeline must stall, then recover.
- [`cluster-total-loss-recover`](failure-modes.md#sealer-the-aeron-cluster-raft): all three sealer members die and return with their logs.
- [`sealer-fleet-total-wipe-recover`](failure-modes.md#proof-sealer-fleet-total-wipe-recover): all three sealer members lose their directories. The chain restarts after the posted head from a seed and a state rebuilt from L1. The blocks after the posted head are reverted.
  - The ingresses stay down for the whole rebuild. So the case load gets 120 submit retries, as `da-lag-halt` does.
  - On a failure, the case deletes the bootstrap variable and registers every saved job again. This is best effort.

**Coordinated shard**

- [`ingress-pair-loss-recover`](failure-modes.md#coordinated-failures-chaos-coordinated-shard): both ingresses die.
- [`sequencer-lane-loss-recover`](failure-modes.md#coordinated-failures-chaos-coordinated-shard): both replicas of lane 0 die.
- [`pipeline-blackout-recover`](failure-modes.md#coordinated-failures-chaos-coordinated-shard): every node except the control node dies at once. The case prints the number of void decisions and does not assert it.
  - The da-watcher dies with the sealers. An epoch that it published and the sealers did not commit is published again, with no operator step.
  - After the recovery, the L1 origin of the sealer must reach the last epoch that the da-watcher published, within 180 s. The budget holds the 90 s grace of the sequencer before `origin_gap` and an election after a full restart.

**Combined ordering shard**

Each case takes two or three classes down at the same time: the ingresses (both tasks, then the job stops), the sequencers (all four tasks, then the job stops), the sealers (all three nodes). The job stops because Nomad restarts a killed task in seconds; a task that runs again before its peers are down is a single-replica case. It holds them down for 60 s, judges the pipeline while they are down, and brings them back in a set order. The classes return in dependency order (sealers, sequencers, ingresses) or against it. A class counts as back when the allocations of its posted job version run, or, for killed nodes, when its job reaches its count; the pause to the next class starts then. With `all at once`, every job is posted and every node started before the first wait. A class that returns before the class it needs must wait: every allocation of its job must run with no restart until the next class returns, and again at the end of the case. See [Combined outages](failure-modes.md#combined-outages-chaos-combined-ordering-shard).

Every case makes these checks after the return: every class is back at its count, the members elected a leader when they were down, both ingresses are live, the executors advance, and the common tail (the load verdict, the converged executors, the recovery probe, the validator verdict).

- [`ingress-sequencer-loss-recover`](failure-modes.md#combined-outages-chaos-combined-ordering-shard): the ingresses and the sequencers die. While they are down, the executor block gauge advances and the applied-transaction counter stays flat. The sequencers return, then the ingresses 30 s later. The sealer's L1 origin must reach the last epoch the da-watcher published. No lane-0 replica may hold a ref below a floor.
- [`ingress-sealer-loss-recover`](failure-modes.md#combined-outages-chaos-combined-ordering-shard): the ingresses and the sealers die. The executor gauge must stay flat. The ingresses return first and must wait 60 s for the sealers with no restart. At most one member led each leadership term.
- [`sequencer-sealer-loss-recover`](failure-modes.md#combined-outages-chaos-combined-ordering-shard): the sequencers and the sealers die. The executor gauge must stay flat, and both ingresses must refuse a submit on `sealer_no_quorum`. Both classes return at once. One leader per term, no origin gap, and a lost epoch must be filled again: the da-watcher's re-publish counter rises. The sequencer's origin-gap counter is not a baseline, because it resets with the sequencer.
- [`ingress-sequencer-sealer-loss-recover`](failure-modes.md#combined-outages-chaos-combined-ordering-shard): all three classes die. The executor gauge must stay flat. They return in dependency order, 30 s apart. One leader per term, no origin gap.
- [`ingress-sequencer-sealer-reverse`](failure-modes.md#combined-outages-chaos-combined-ordering-shard): all three classes die. They return against the dependency order, 45 s apart: the ingresses, then the sequencers, then the sealers. Each class must wait for the next with no restart. One leader per term, no origin gap.
- The load of every combined case gets 90 submit retries. The submit ingress is dead for about five minutes, and a dead ingress refuses a connection at once.

**Combined exec shard**

Each case takes the executors down with one or two other classes: the executor nodes (`docker kill`), or the three executor tasks with their job stopped when a class on the executor nodes must return before them. The other classes are the sealers (the three nodes), the validator (its task, then the job stops), the state mirrors (the three tasks, then the job stops), Redis (the job stops) and the sequencers and the ingresses as in the ordering shard. The hold, the judgement while down, the staggered return and the common tail are the ordering shard's. See [Combined outages with the executors](failure-modes.md#combined-outages-with-the-executors-chaos-combined-exec-shard).

The executors are dark in every case, so the head the judgement reads is the highest head any live client reports: the executor block, the sealed head an ingress reads from the sealer's status frames, or the validator's committed block. A stall keeps that head flat. A seal-only case advances it with no applied transaction on a live executor. A case with Redis down also requires that a cold balance read counts as degraded while Redis is down.

- [`executor-sealer-loss-recover`](failure-modes.md#combined-outages-with-the-executors-chaos-combined-exec-shard): the executor nodes and the sealer nodes die. The head must stay flat and both ingresses must refuse on `sealer_no_quorum`. The executors return first and wait 60 s for the sealers. At most one member led each leadership term. The persisted-state audit runs after the case.
- [`executor-sealer-validator-recover`](failure-modes.md#combined-outages-with-the-executors-chaos-combined-exec-shard): the executor tasks, the sealer nodes and the validator task die. The head must stay flat. The sealers return, then the executors 30 s later, then the validator 30 s after that. One leader per term. The validator verdict of the tail proves that the restarted validator verifies live.
- [`ingress-executor-loss-recover`](failure-modes.md#combined-outages-with-the-executors-chaos-combined-exec-shard): the executor nodes and the ingress tasks die. The sealers order the transactions in flight, so the head advances. The executors return first and publish the receipts of their replay while no ingress listens; the ingresses return 60 s later. Before the fault, the case sends one transfer from the gate account through ingress-0 and waits for its receipt. After the return, every ingress must serve that receipt and must count a receipt served from an executor's state since its restart (`kardamom_cache_lookups_total{layer="receipt",outcome="state_hit"}` above zero; the counter starts at zero with the process): the restarted memory caches hold nothing, so the state query served it. The load's drain asks every ingress for the receipts it still misses, which drives the same path at scale.
- [`read-path-loss-recover`](failure-modes.md#combined-outages-with-the-executors-chaos-combined-exec-shard): the executor tasks, the redis job and the state-mirror tasks die. The head advances, and a cold read degrades. Redis returns empty, the mirrors return 30 s later and wait for a checkpoint, the executors return 30 s after that. Every mirror must log `rebuild: done`, and the readers must use Redis again with no degraded read.
- [`sequencer-executor-redis-loss`](failure-modes.md#combined-outages-with-the-executors-chaos-combined-exec-shard): the sequencer tasks, the executor tasks and the redis job die: no source of a sender floor is left. The head advances, and a cold read degrades. The sequencers return first and must park every established sender, Redis returns empty 30 s later, the executors 30 s after that. Every lane-0 replica must have asked for a floor (`kardamom_sequencer_nonce_lookup_requests_total`) and got one from an executor or from Redis (`kardamom_sequencer_nonce_lookups_total{outcome="ok"|"redis"}`), and no ref may sit below a floor. The load is pinned to shard 0. The load verdict proves that no nonce gap remains.
- `sequencer-executor-redis-loss` is in no shard yet. Run it by name with `KARDAMOM_CHAOS_CASES`. The Aeron driver error (#545) can fail it. See [Known gaps](failure-modes.md#known-gaps-untested-failure-surface).
- On a local host, the shard takes about 45 minutes after a bring-up of about 21 minutes. `read-path-loss-recover` alone takes about 12 minutes with the smoke gate and the tail. The job timeout is 120 minutes.
- The load of every exec case gets 90 submit retries. With the executors down and the ingress up, each attempt parks 30 s at the ingress and times out with no receipt.

**Retention shard**

- [`retention-overrun`](failure-modes.md#executor): freezes an executor past a small retention. It must adopt a peer checkpoint.
- [`retention-overrun-validator`](failure-modes.md#validator-off-the-hot-path-halts-on-divergence): freezes the validator past the retention. It must adopt a peer checkpoint and verify again.

**Cache shard**

- [`redis-partition-ingress`](failure-modes.md#redis-account-cache): drops the packets from ingress-0 to Redis.
- [`redis-primary-kill`](failure-modes.md#redis-account-cache): kills the Redis primary.
- [`redis-primary-freeze`](failure-modes.md#redis-account-cache): freezes the Redis primary past the sentinel timeout.
- [`mirror-kill-rebuild`](failure-modes.md#redis-account-cache): kills the state mirrors and flushes the projection. Every mirror must rebuild.

**L1 shard**

- [`l1-liar`](failure-modes.md#l1-follower): serves a wrong block hash, a broken parent chain and swallowed settlement logs, one after the other.
  - The L1 follower reads two sources: the proxy and its second source (`/second`). Each lie is a disagreement of the two: the follower halts and publishes none of it, and the da-watcher pauses with the follower as its root (`kardamom_paused{root_service="l1-indexer"}`).
  - Each one resumes by itself when the lie stops. No operator step.
- [`l1-null-receipts`](failure-modes.md#batcher-live-service-cluster-egress-driven): serves null receipts and swallowed logs, with a batcher restart inside the fault.
- [`follower-instance-loss`](failure-modes.md#l1-follower): freezes the follower instance on `ingress-0` for the fault window, during load. The da-watcher must not pause or wait, must publish past its start, and must publish one epoch for each block. The batcher must post, and no origin gap may remain.
- [`follower-total-loss`](failure-modes.md#l1-follower): stops the `l1-indexer` job for 75 s, past the da-watcher's silence window. The da-watcher must pause with the follower as its root. After the restart, it must resume with one epoch for each block since the start, and no origin gap may remain.
- [`two-day-outage`](failure-modes.md#batcher-live-service-cluster-egress-driven): replays the events of a two-day L1 outage.
  - The redeploy of the followers restarts the da-watcher. Its cursor file must stand at or before the L1 origin of the sealer, within 60 s.
  - The follower and the da-watcher resume by themselves when the fault clears.
- [`batcher-outage-past-retention`](failure-modes.md#batcher-live-service-cluster-egress-driven): freezes the batcher while twice the retention flows past its cursor and a sealer snapshot lands. It then thaws the batcher.
  - Each freeze attempt finds the batcher container again. A restart between two attempts can replace the container.
  - The frozen group must land on L1 right after the covered block. The case finds the first batch that ends past the covered block, and that batch must start at the next block. More batches can land before the poll reads L1, so the case does not read the last batch.
  - The thaw has one valid end. The freeze lasts at least the Aeron stall tolerance plus 20 s, and at least two minutes. So a client times out and the batcher exits. Nomad restarts it, and it logs `pending group restored from the spool`.
  - A batcher that keeps running after the thaw fails the case. The failure tells a hung exit (the handler logged `aeron client error; exiting`) from a timeout that never fired.
  - A batcher that restarts and does not log the restore fails the case.
  - The sealer keeps every frame above the posted head. The batcher normally gets its replay served.
  - A sealer that prunes by the window alone refuses the replay. The batcher then rebuilds the gap.
  - In both cases the L1 record must be contiguous.

**Integrity shard (nightly)**

- [`executor-restart-storm`](failure-modes.md#executor): stops and restores the executor job ten times under load.
  - Every round, all replicas must run again within the restart SLO, and every executor must advance within the convergence SLO (`assert_executors_converged`).
  - Every round gives each executor a new `tx_receipts` publication on a new control port, which every subscriber attaches again. This is the shape of the fault behind the must-deliver escalation: a publication that stayed unconnected while its subscribers were attached. See "Dead `tx_receipts` publication" in [`failure-modes.md`](failure-modes.md#executor).
  - At the end, the case counts the escalation lines of the executors (`tx_receipts publication reopened on a new session`, `the process exits so the supervisor restarts it`) and prints them as evidence. The counts do not fail the case.
  - The load gets the same submit retry as the fleet cases. The case window is ten times the restart SLO plus one minute.

**DA cases**

The DA cases freeze the batcher with SIGSTOP, so nothing posts to L1 while the chain keeps sealing. See [Halts and service events](failure-modes.md#halts-and-service-events).

- `da-lag-halt`: seals past the DA-lag budget.
  - The sealer must be the `da_lag` root in the chain status. The ingresses must be paused on it.
  - A submit must fail with the typed error that names `/halt`.
  - The boundaries must keep sealing.
  - After the thaw, the halt must clear with no operator step. The batcher must post past the frozen head. A new transfer must succeed. The chain status must settle.
- `prune-floor`: stays under the budget, but runs past the retention window and a Raft snapshot.
  - The retention must grow past its window. The replay floor must not pass the posted head.
  - The frozen batcher must show as `gone` in the chain status. No root may stand.
  - After the thaw, the retention must return inside its window. The sealer must not refuse the replay of the batcher.
  - The posts on L1 must be contiguous.
- `canary-da-lag`: seals past the DA-lag budget, as `da-lag-halt` does, and watches the transaction canary.
  - The canary's `transfer` probe must report `rpc_error{code="-32010"}`.
  - Alertmanager must hold a canary page as inhibited by the halt's page.
  - After the thaw, the halt must clear, and the canary's transfers must succeed again.
- No DA case is in a shard. Run them by name with `KARDAMOM_CHAOS_CASES` against a cluster that has the small setting that the case needs.
- `da-lag-halt` and `canary-da-lag` need `KARDAMOM_DA_LAG_BUDGET_BLOCKS`. `canary-da-lag` also needs a cluster deployed with `CANARY_LOCAL=1`. `prune-floor` needs `KARDAMOM_CLUSTER_RETENTION`.
- The case window is `INJECT_DELAY` plus `RETENTION_FREEZE_CAP_S` plus 2 minutes. `da-lag-halt` and `canary-da-lag` retry their load up to 120 times.

## Gates

A gate is a check that the suite makes before or after a fault. A failed gate fails the case.

### Injection gate

The injection gate stops a case from injecting into an idle pipeline.

1. The suite waits `INJECT_DELAY` seconds after the load starts.
2. The suite waits until the load reports that it signed its queues. The load signs its whole case window before the first submit. The wait is `LOAD_READY_TIMEOUT_S`. A load that exits early fails the case.
3. The flow check starts. The `kardamom_ingress_tx_received_total` counter of at least one ingress must rise above the baseline of that same ingress. The baseline is read before the load starts.
4. The flow check has a budget of `LOAD_FLOW_TIMEOUT_S`. On a timeout, the failure text prints the counter of every ingress. A failed scrape shows as `?`.

### Recovery gates

| Gate | What it requires |
|---|---|
| Ingress pair live | Both ingress exporters answer. Both JSON-RPC ports answer `eth_chainId`. The budget is 120 s. A restarted ingress binds the exporter first and the JSON-RPC port after it joins the cluster. |
| Executors converged | Within `EXEC_CONVERGE_SLO_S`, every executor gauge is scrapeable and within `EXEC_CONVERGE_LAG` blocks of the fleet head. The first sample inside the bound sets a floor. A later sample must show every replica inside the bound and past its floor block. The poll interval is 5 s. |
| Restart SLO | The job returns to its replica count within `CHAOS_RESTART_SLO_S`. A node loss uses `CHAOS_RESCHEDULE_SLO_S`. |
| Recovery probe | See below. |

- The sealer stamps a block on every tick, with or without load. A live replica therefore always advances. A replica that stalled recently can still read as close to the head, so one sample is never enough.
- The failure text of the converged gate names each bad replica: `executor-N=unreachable`, `executor-N=lag:<blocks>` or `executor-N=stalled-at:<block>`.

### Recovery probe

Every case ends with the recovery probe. Two 30 s loads run at the same time, each at half of the case rate.

- One load uses the case account through the ingress of the case load. It stands for a sender with transactions in flight during the outage.
- One load uses the gate account through the other ingress. It stands for a sender with nothing in flight.
- Every offered transaction of both loads must get a receipt.
- Each load must land at least a quarter of its rate in accepted submits.

[`failure-modes.md`](failure-modes.md#the-recovery-probe) explains why the case load and the block gauge cannot prove this.

## The load verdict

The load verdict is the pass or fail result of one `kardamom-load` run. The chaos suite and the `load` shard both use it.

Every run has these checks.

| Check | Rule |
|---|---|
| Acceptance | The ingress must accept more than zero transactions. |
| Delivered | Every accepted transaction must get a receipt. A transaction that is accepted and never receipted counts as `missing`. A run in `Offered` mode (the recovery probe) requires a receipt for every offered transaction. |
| Receipt status | Every receipt must have status `0x1`. |
| Receipt contradictions | Every receipt must describe its transaction. See below. |
| Drops | The sequencer counters for dropped, evicted and backpressured transactions are in the report. A sequencer drop of a past-nonce transaction fails a non-chaos run. |
| Keep-pace | Each executor must advance while the sealer advances, and the gap to the sealer must stay within `LOAD_MAX_GAP` blocks. |
| Liveness | A non-chaos run fails when a service reports `kardamom_service_up=0` at the end. |

**Metric reads.** The checks read the exporters of the executors, the ingress and the lane-0 sequencer replicas.

- The suite gives the load the bridge address of each node from the node contract. The load reads each exporter directly over the bridge.
- When a direct read fails, the load reads the same exporter through `docker exec <node> curl 127.0.0.1:<port>/metrics`. That read ends after 10 s, so a stalled `docker exec` cannot hold the final snapshot.
- The chaos probes use the same reader (`ExporterReader` in `kardamom-bench`). They reach the ingress exporter over the bridge too.
- The report field `scrape_fallbacks` counts these fallbacks in the snapshots that the verdict reads. A high count means that the bridge reads failed. A runner-wide exec stall then degrades the verdict.
- `kardamom-load --metrics-via-docker true` (the CLI default) reads every exporter through `docker exec` only. `false` reads `http://<node>:<port>/metrics` first.

**Chaos mode.** The suite runs the load in chaos mode.

- A transient gap, a missing executor metric or a down service is informational.
- A submit that fails during an outage is retried. It does not count against the delivered check.
- An executor that never recovers still fails the run. So does an undelivered receipt.

**Receipt contradictions.**

- The load records the signer and the nonce of every planned transaction.
- Every confirm path checks each receipt: the HTTP re-fetch, the drain and the WebSocket feed.
- A receipt is a contradiction when it names another sender, names no block, or sits in a block out of order with the other nonces of its sender.
- The count is the report field `bad_receipt`. The verdict message is `N receipt(s) contradict their transaction (sender, block, or block order)`.
- Every case runs a load. Every case therefore proves the content of receipts, not only that they exist.

**The drain.**

- After the send window, the load drains the outstanding receipts until `--drain-timeout`.
- The drain asks the submit ingress first. It then asks every other ingress (`LoadConfig.receipt_rpcs`).
- Every ingress replica consumes the same `tx_receipts` stream. Any replica can answer any receipt. A receipt that only lived in a restarted replica is still on the others.
- The chaos harness fills `receipt_rpcs` with the other ingress URLs. `kardamom-load` and `kardamom-perf` pass an empty list.

**The FROZEN verdict.**

- An executor is `FROZEN` when its block gauge did not advance while the sealer advanced.
- A restarted executor resets its gauge to 0. The load takes a recheck sample a few seconds later. An executor that moves past its final value is `RECOVERING`, not `FROZEN`.
- `FROZEN` fails a run in every mode. The other keep-pace verdicts are `OK`, `GAP>N` and `METRIC-MISSING`.

**Chaos case judging.** A chaos case requires `pass` and `missing == 0`. The case load uses the rule `Accepted`. Only the recovery probe uses `Offered`, at a fixed rate.

## Validator verdict

The validator verdict is the last gate before the audit.

- The validator exporter answers within 120 s.
- The committed block of the validator is within `VALIDATOR_LAG_MAX` blocks of the executors. The budget is `VALIDATOR_SYNC_TIMEOUT_S`.
- `validator_blocks_verified_total` and `kardamom_state_trie_shadow_checks_total` are above zero.
- `validator_divergence_total` and `kardamom_state_trie_shadow_mismatch_total` are zero.
- No validator allocation log shows a halt on divergence. On a halt, the suite prints the flight-recorder dumps.

## Persisted-state audit

The audit compares the persisted state of every executor with the validator. It then rebuilds the state from L1. Every shard runs it once at the end.

1. **Stop ordering.** The suite stops the sealer job first. The consumers then drain.
2. **Find the common head.** The suite reads the head gauge of every executor and of the validator.
   - The common head is the lowest committed block. It must be above zero.
   - Every consumer must answer. The spread must be at most one block.
   - A stopped sealer freezes the durability watermark. A replica can lag by one block, or the validator can lead by one block. A spread of two blocks, an unreachable gauge or no common head fails the audit.
3. **Stop the writers.** The suite stops the executor job and the validator job. It checks that no writer still runs. It copies the state directories to the host.
4. **Check the validator.** The integrity sweep of the validator copy must be clean. The copy must hold an executed workload. The committed state root must equal the rebuilt root.
5. **Compare.** Each executor copy must pass the integrity sweep. `kardamom_state::deep_compare_to` compares it with the validator through the common head.
   - It compares the chain-state tables: accounts, storage, code, headers, receipts and indexes.
   - It skips the `headers` rows past the head and the two last-committed meta keys.
   - A consumer one block ahead holds one empty block. A block past the head with transactions is a difference.
6. **Restore the jobs.** The suite restores the sealer, executor and validator jobs on pass and on failure.
7. **Rebuild from L1.** With the jobs back, the suite runs `kardamom-reconstruct --through-block <head> --expect-root <validator root>`.
   - The batcher posts a block a few seconds after it seals. The suite retries for up to 180 s while the posted batches end before the target.
   - The 180 s count only the 5 s sleeps between the attempts, not the run time of an attempt.
   - Any other failure of the tool ends the rebuild at once, because a retry reads the same record. The log shows the error chain of the tool.
   - The rebuilds of the suite pass no `--lockbox`. The suite never deposits.
   - The rebuilt root must equal the root of the validator.
   - The rebuilt resume cursor must equal the cursor of the validator. A consumer that resumes on the rebuilt state would otherwise skip records or apply them twice.

The suite prints `persisted-state evidence: <directory>`. The directory is a `chaos-state-*` directory in the temporary directory of the host.

- It holds the state copies and the rebuilt state.
- A pass removes it. A failure keeps it.
- Each directory is large (about 1.4 GB per audit), so a run that keeps every directory fills a runner disk.

### Recovery after an audit

The audit restores the jobs when their allocations run, not when the chain runs. The batcher and the ingresses do not restart. They connect again. The first boundary tick of a restored leader can come minutes later. Until an ingress sees a status frame, it refuses every submit with `sealer_no_quorum`. A refused nonce leaves a gap that fails the load of the next case.

So, in a shard that audits each case, the next case starts only after these steps, in this order:

1. A sealer leader ticks the boundary clock. The count of `cluster boundary-clock TICK` lines with `role=LEADER` rises.
2. Every ingress takes submits. Its `kardamom_chainStatus` shows no sealer halt, and the ingress is `running`.
3. The batcher confirms a new post. `kardamom_batcher_batches_posted_total` rises.
4. The executors advance. The highest executor block rises.

The steps share one budget of 300 s. A failure names the first step that did not hold, the time, and the last reading: `the chain did not recover after the persisted-state audit: <step> after <n> s (<reading>)`.

## The L1 fault proxy

`kardamom-l1-fault-proxy` is an L1 JSON-RPC proxy that lies on command. The `chaos-l1` shard uses it. The code is in `crates/l1_fault_proxy`.

- The proxy forwards every call to the upstream L1 (`--upstream`, env `KARDAMOM_L1_UPSTREAM`).
- It changes the reply as the active faults say.
- It listens on `--listen` (env `KARDAMOM_L1_FAULT_PROXY_LISTEN`, default `0.0.0.0:8547`).
- The proxy is a pipe. It does not parse the call. Only the fields that a fault names change.

**Fault kinds.** A fault is a JSON object with a `kind` field.

The proxy serves two sources. The root path lies as the active faults say. The path `/second` serves L1 faithfully, refuses nothing, and serves only a fault scoped to the caller's address (`ForkedChain` with `client`).

| Kind | Parameter | Effect |
|---|---|---|
| `None` | none | Serves L1 faithfully. |
| `WrongBlockHash` | `from_block` | Corrupts the `hash` of every block at or above `from_block`. |
| `BrokenParentChain` | `from_block` | Corrupts only `parentHash`. Each block looks right alone. |
| `SwallowLogs` | `address` | Drops every log of the address from an `eth_getLogs` reply. |
| `NullReceipts` | `from_block` | Answers `null` for the receipts of blocks at or above `from_block`. |
| `ForkedChain` | `from_block`, `client` (optional) | Serves a fork that is consistent in itself from `from_block` on: every block hash, every later parent hash and every log's block hash map through one function. With `client`, only the calls from that peer address see it. |
| `RateLimit` | none | Answers HTTP 429 to every call. |
| `Down` | none | Answers HTTP 503 to every call and closes the connection. `Down` wins over `RateLimit`. |

**Control endpoint.** It shares the listener with the JSON-RPC pipe.

| Request | Effect |
|---|---|
| `POST /fault` | Replaces the active faults with one fault object or a list. The reply shows the active list. A bad body gets HTTP 400. |
| `GET /fault` | Shows the active faults as `{"active": [...]}`. |
| `GET /health` | Answers `{"ok": true}`. The Nomad check uses it. |
| Any other `POST` | Is a JSON-RPC call for the upstream. |

Several faults can be active at once. They model one bad endpoint.

**`KARDAMOM_L1_FAULT_PROXY=1`.** This switch of the deploy changes the cluster in these ways.

- It deploys the `l1-fault-proxy` job (`deploy/cluster/nomad/l1-fault-proxy.nomad.hcl`) before its consumers.
- The indexer (the L1 follower) reads two sources: the proxy, and the proxy's second source `/second`, which serves L1 except a fault scoped to the caller's address. The batcher reads the proxy. The da-watcher reads the follower's stream.
- A lie of the proxy is therefore a disagreement for the follower: it halts and publishes none of it.
- The in-cluster anvil finalizes two blocks behind its head (one slot in each epoch). The followers walk finalized blocks.
- The inbox indexer starts at block 1, so its archive holds every batch.
- The `chaos-l1` shard sets the switch itself. The default is `0`.

[`failure-modes.md`](failure-modes.md#batcher-live-service-cluster-egress-driven) lists what the cases prove, and which two-source checks stay open.

## Environment knobs

`crates/chaos/src/knobs.rs` reads every knob. The source order is:

1. The process environment.
2. The shard settings in the table above.
3. The built-in default in this section.

A value that does not parse fails the run at start. A zero value fails for a knob that must not be zero.

### Case knobs

| Name | Default | Meaning |
|---|---|---|
| `CHAIN_ID` | `412346` | The L2 chain id. |
| `CHAOS_TPS` | `200` | The steady load rate of every case, in tx/s. |
| `CHAOS_CASE_S` | `120` | The least load window of a case, in seconds. A case with a longer floor widens it. |
| `LOAD_MAX_GAP` | `5` | The keep-pace bound: sealer block minus executor block. |
| `LOAD_RETRY` | `2` | The per-submit retry attempts of the load. Some cases set their own value. |
| `CHAOS_RESTART_SLO_S` | `60` | The restart SLO of a task on the same node. |
| `CHAOS_RESCHEDULE_SLO_S` | `200` | The recovery SLO after a node loss. |
| `CHAOS_LEADER_SLO_S` | `45` | The election SLO of the Raft cluster. |
| `CLUSTER_REJOIN_SLO_S` | `360` | The SLO of a blank sealer member: the time to seed, restore and catch up to the head. |
| `INJECT_DELAY` | `10` | The least load time before an injection, in seconds. |
| `LOAD_READY_TIMEOUT_S` | `600` | The time the load may take to sign its queues. |
| `LOAD_FLOW_TIMEOUT_S` | `60` | The time the load may take to reach an ingress after `INJECT_DELAY`. |
| `CHAOS_ACCT_BASE` | `7` | The first funded account that a case may use. |
| `KARDAMOM_CHAOS_GATE_ACCOUNT` | `0` | The funded account of the smoke gate and of the fresh-sender probe. No case load spends it. |
| `INGRESS_VICTIM` | `GITHUB_RUN_ID % 2`, or `0` | The ingress replica that `hard-ingress` kills: 0 or 1. |
| `EXEC_CONVERGE_SLO_S` | `150` | The convergence budget after every case. |
| `EXEC_CONVERGE_LAG` | `50` | The block lag that every executor must be within at case end. |
| `AERON_STALL_TOLERANCE_MS` | `30000` | The Aeron stall tolerance of the deployed cluster. The lapse freezes and the waits after a driver loss derive from it. |
| `SEQ_LAPSE_S` | the tolerance plus 20 s | The freeze window of `sequencer-lapse`. It must pass the tolerance, so the driver evicts the frozen client. |
| `LAPSE_S` | the tolerance plus 20 s | The freeze window of `validator-lapse`. |
| `KARDAMOM_CLUSTER_RETENTION` | unset | The egress retention of the deployed cluster, in frames. The retention cases need it. It must be a positive number. |
| `KARDAMOM_EXEC_CURSOR` | `off` (`on` in `chaos-executor`) | `on` when the deployed executors send their recorded cursor to the sealer. The bring-up of `chaos-executor` deploys it on. `hard-executor` checks the sealer's best cursor only when it is `on`. |
| `KARDAMOM_DA_LAG_BUDGET_BLOCKS` | unset | The DA-lag budget of the deployed sealer, in blocks. `da-lag-halt` needs it. It must be a positive number, so the knob cannot pass 0. |
| `RETENTION_FREEZE_CAP_S` | `600` | The hard cap of the adaptive retention freeze. |
| `L1_FAULT_S` | `60` | The time one L1 fault of the `chaos-l1` cases stays active. |
| `L1_CASE_TPS` | `50` | The case load rate of the `chaos-l1` cases. It is below the steady rate. A fault that stops the batcher must not push its cursor past the small retention. |
| `RUN_LOAD` | `1` | `1` when the load stage ran on this cluster. The chaos shards set `0`. The resize case then takes a load-reserve account. |

### CPU squeeze knobs

| Name | Default | Meaning |
|---|---|---|
| `SQUEEZE_S` | `120` | One squeeze window, in seconds. |
| `SQUEEZE_CPUS_PER_NODE` | `0.75` | The `docker update --cpus` value of every node during a squeeze. |
| `SQUEEZE_RECOVER_S` | `180` | The time the validator gets to verify live after the last release. |
| `SQUEEZE_CYCLES` | `1` | The number of squeeze cycles. |
| `SQUEEZE_RELEASE_S` | `30` | The pause between two cycles. |

### Stage knobs

| Name | Default | Meaning |
|---|---|---|
| `LOAD_DURATION_S` | `60` | The duration of the transfers run of the `load` stage. |
| `LOAD_TARGET_TPS` | `200` | The rate of that run. |
| `LOAD_SENDERS` | `6` | The sender accounts of that run. |
| `DEFI_DURATION_S` | `45` | The duration of the DeFi run. |
| `DEFI_TARGET_TPS` | `100` | The rate of the DeFi run. |
| `DEFI_SENDERS` | `6` | The sender accounts of the DeFi run. |
| `SEMANTICS_CASES` | `nonce-unordered,nonce-gap,rpc-liveness,rpc-vectors,consistency,l1-batch` | The cases of the chain-semantics stage. |
| `SEMANTICS_PARK_MS` | `30000` | The time a receipt may park, in milliseconds. |
| `SEMANTICS_ACCOUNT_BASE` | `1` | The first funded account of the semantics stage. |
| `SETTLEMENT_ADDRESS` | unset | The settlement contract address. When unset, the suite resolves it with `kardamom-deploy`. |
| `VALIDATOR_LAG_MAX` | `10` | The most blocks that the validator may trail the executors at the verdict. |
| `VALIDATOR_SYNC_TIMEOUT_S` | `180` | The time the validator gets to reach that bound. |

### Shard test variables

The shard tests (`crates/chaos/tests/shards.rs`) read these variables.

| Name | Meaning |
|---|---|
| `KARDAMOM_CHAOS_REUSE` | `1` skips the bring-up and the teardown. The suite runs against the cluster that is up. `just container-test` sets it. |
| `KARDAMOM_CHAOS_CASES` | A space-separated list that replaces the case list of the shard. |
| `KARDAMOM_CHAOS_CLUSTER_VARS` | One JSON object of extra Ansible variables for the convergence playbook. |

- Every load starts an account at its live nonce, so a reuse run on a used chain spends used accounts again. `CHAOS_ACCT_BASE` picks the first case account.
- A host without passwordless sudo needs a one-time setup. Set the host sysctls and the bridge multicast snooping by hand. Then pass `{"ansible_become": false}` in `KARDAMOM_CHAOS_CLUSTER_VARS`.
- `RUST_LOG` sets the tracing level of the harness libraries. The default is `warn`.

## Failure diagnostics

After a failed shard, the cluster stays up. The failure dump (`crates/chaos/src/diagnostics.rs`, `just container-diagnostics`) prints these sections. Every section is best effort. The dump never fails.

- The host bridge: `bridge-nf-call-iptables`, the `FORWARD` rules with counters, the bridge link and its multicast snooping.
- The multicast groups that each worker node joined.
- A raw UDP multicast probe from sealer-0 to ingress-0. Zero packets received means that the bridge does not forward multicast.
- A short `tcpdump` capture of the cluster multicast flows, counted by source and destination.
- For every allocation of every job: the task states, the head and the tail of the logs, and the lifecycle lines (cluster sessions, resyncs, mirror rebuilds). The `cluster` job has a longer tail and a consensus timeline of roles, terms and snapshots.
- For each sealer node: the Aeron cluster error logs and the `ClusterTool` errors and member list.
- For every allocation of the `cluster` and `executor` jobs, in every client state: why the task ended.
  - The task state line has `restarts`, `failed`, `started` and `finished`.
  - Each Nomad task event has the time (RFC 3339, UTC), the type, `exit`, `signal`, `oom` and the message. A `Terminated` event carries the exit code and the signal. A `Killing` event shows a kill that Nomad starts.
  - Nomad keeps only the last events of a task, so the restart count covers the rest.
- For each sealer and executor node: `docker inspect` of each inner task container, exited ones included. The line has `status`, `exit`, `oom`, `restarts`, `started`, `finished` and `error`.
  - The docker driver removes a dead container, so the Nomad events are the record that stays.
  - The two records tell a kernel OOM kill from an external `SIGKILL` and from a Nomad kill.

The jobs in the dump are `aeron`, `anvil`, `cluster`, `sealer`, `sequencer`, `executor`, `validator`, `ingress`, `batcher`, `da-watcher`, `l1-fault-proxy`, `l1-indexer`, `redis`, `state-mirror`, `notifier` and `monitoring`.

The harness reads each task log through the control agent of Nomad.

- The control agent forwards the read of a remote allocation to the client agent of the node. A stream failure on that path gives a 5xx answer.
- On a 5xx answer, the harness logs the status and the body. It then reads the same path from the agent of the node that runs the allocation.
- It tries both reads again every 5 s for 30 s. If both agents fail for the whole time, the error has both bodies.
- A 404 reads as an empty log. The allocation is collected, or the task never started.
- A 5xx that says `/alloc/logs: no such file or directory` reads as an empty log only for a terminal allocation (complete, failed or lost). The client collected it and removed its directory, and the server still lists it. This happens after a node kill. For a live allocation the same answer is a failed read, so a check never reads a missing log as a clean one.
- The one-leader-per-term check requires a `cluster TERM` line from every member. A member whose log reads empty fails the check.

Other evidence on a failure:

- A divergence halt prints the divergence evidence and the flight-recorder dumps of the validator.
- A rebuild that misses the target prints the first warnings and errors of the batcher.
- A rebuild that the tool refuses prints the error chain of the tool.
- A failed persisted-state audit keeps its evidence directory.
- When you read a failure, tell "the pipeline stalled" from "the probes went dark" first. [`failure-modes.md`](failure-modes.md#substrate-the-shared-failure-domain) explains the difference.
