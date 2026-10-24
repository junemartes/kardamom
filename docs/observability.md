# Observability

This document covers the metrics, the readiness checks, the dashboards, the alerts, and the log lines of the
Kardamom services. It also covers the `kardamom-bench` load generator and the profiling harness.

- Each service exports Prometheus metrics and a `/ready` check on one HTTP listener.
- Prometheus, Alertmanager and Grafana run as the `monitoring` Nomad job (`deploy/cluster/nomad/monitoring.nomad.hcl`).
  - The job runs on the nodes whose role set holds `monitoring`.
  - The local profile puts it on the aux node. The production profile places it by role set.
    See [../deploy/cluster/PRODUCTION.md](../deploy/cluster/PRODUCTION.md).

## Metrics

Each service binary exports Prometheus metrics on its own HTTP listener.
Set the address with `--metrics-addr` or `KARDAMOM_METRICS_ADDR`.

| Service | Default address | Cluster deploy | Dashboard UID |
| --- | --- | --- | --- |
| `kardamom-sequencer` | `127.0.0.1:9001` | `0.0.0.0:9001 + 10 * lane` | `kardamom-sequencer` |
| `kardamom-batcher` | `127.0.0.1:9002` | `0.0.0.0:9002` | `kardamom-batcher` |
| `kardamom-executor` | `127.0.0.1:9004` | `0.0.0.0:9004` | `kardamom-executor` |
| `kardamom-da-watcher` | `127.0.0.1:9005` | `0.0.0.0:9005` | `kardamom-da-watcher` |
| `kardamom-ingress` | `127.0.0.1:9006` | `0.0.0.0:9006` | `kardamom-ingress` |
| `kardamom-validator` | `127.0.0.1:9007` | `0.0.0.0:9006` | `kardamom-validator` |
| `kardamom-state-mirror` | `127.0.0.1:9007` | `0.0.0.0:9007` | `kardamom-state-mirror` |
| `kardamom-notifier` | `127.0.0.1:9008` | `0.0.0.0:9008` | `kardamom-notifier` |
| `kardamom-canary` | `127.0.0.1:9012` | `0.0.0.0:9012` | `kardamom-canary` |
| `kardamom-egress` | `127.0.0.1:9013` | `0.0.0.0:9013` | none |
| `kardamom-l1-indexer` | `127.0.0.1:9549` | `0.0.0.0:9009` | none |

Notes on the table:

- A sequencer node runs one replica for each lane it serves. Lane `L` uses port `9001 + 10 * L`.
  Each lane has two racing replicas on different nodes.
  Scraping one lane port on every sequencer node gives one replica of that lane for each node.
- The validator and the state mirror have the same default port. The cluster deploy binds the validator to `9006`.
  The ingress also binds `9006`. Run the two on different nodes.
- The cluster jobs bind each exporter to `0.0.0.0`. Prometheus then scrapes the exporters from another node.
  The chaos suite reads the executor metrics over the cluster bridge (`http://<node_ip>:9004/metrics`), not through `docker exec`.
- All binaries read the same `KARDAMOM_METRICS_ADDR` variable. A value shared by services on one host makes them race for one socket.
  Use the `--metrics-addr` flag when you set more than one service on a host.
- The metrics listener runs on its own thread (`kardamom_obs::init`). `/metrics` keeps an answer when the service runtime is stuck.
  A stuck service shows `kardamom_service_up == 1` with gauges that do not change.
- The monitoring job scrapes each exporter by its Consul node name: `<class>-<i>.node.<datacenter>.consul:<port>`.
  The job renders the targets from the node-class counts (`executor_count`, `sequencer_count`, `ingress_count`).
  A larger class gets its targets at the next deploy.
- The services that Consul lists (the validator, the da-watcher, the batcher, the L1 follower, the notifier, the canary and the egress) are targets in every health state.
  The monitoring job scrapes the egress only when the deploy runs it (`KARDAMOM_EGRESS=1`, the job variable `egress`).
  A halt or a pause fails `/ready` by design, so Consul marks the service critical. A healthy-only list would drop the target exactly when the halt and pause alerts need its metrics.
  A dead process stays a registered target and reads as `up == 0`.

### Labels and the sealer series

- Each binary takes `--host-id <STRING>` (env `KARDAMOM_HOST_ID`, default `local`).
  It sets the `host_id` label of each metric. `kardamom_obs::init` adds a `service` label.
- The `Kardamom Overview` dashboard has a `host` template variable. The service dashboards inherit it.
- The sealer is a Java service in an Aeron Cluster. Its Prometheus endpoint is `GET /metrics` on the admin port of each member (40205).
  The monitoring job scrapes it as the job `kardamom-sealer`.
  - `kardamom_sealer_disk_free_bytes{member_id}` is the free space of the disk that holds the Raft log and the retained file of the member.
  - The other series carry the labels `service="sealer"` and `member_id`. They do not carry `host_id`. See "Sealer pipeline" below.
- Each executor also re-exports the output of the sealer as it decodes the cluster egress:
  - `kardamom_sealer_boundaries_emitted_total` counts the boundary frames.
  - `kardamom_sealer_block_number` is the block that the sealer declared.
  - The series are on the executor port with the `host_id` of the executor.
    They show the boundary stream as one executor sees it.

### Cluster client metrics

Every service that drives a cluster session exports the state of that session under its own `service` label. These services are the sequencer, the executor, the ingress, the validator and the batcher.

| Metric | Meaning |
| --- | --- |
| `kardamom_cluster_client_connected` | 1 while the session is open. |
| `kardamom_cluster_client_leader_member_id` | The leader that the ingress publication points at. |
| `kardamom_cluster_client_leader_changes_total` | Redirects and new-leader events. |
| `kardamom_cluster_client_sessions_total` | Sessions opened. |
| `kardamom_cluster_client_egress_age_seconds` | A histogram. For each drain of egress frames, the age of the oldest app message: the clock of this host minus the timestamp that the leader wrote in the message header. |

- A leader change rate above zero in steady state is an election.
- A client that stays disconnected finds no member that answers.
- These series observe the Raft set from its clients, not from inside the JVM.
- The egress age has a resolution of 1 ms. It includes the clock offset between the two hosts. A leader clock ahead of this host gives zero age.

The session thread records these pipeline names (see "Pipeline metrics"):

| Kind | Name | Meaning |
| --- | --- | --- |
| loop | `cluster_client.session` | The duty cycle of a session that publishes. A cycle that moves frames or offers is busy. A cycle that finds nothing, and the wait after it, are idle. |
| loop | `cluster_client.stream_session` | The same loop for a session that subscribes to the canonical stream. A process can drive one session of each kind. |
| step | `cluster_client.offer` | One offer request: the wrap and the publish of its payload. |
| queue | `cluster_client.requests` | The offer requests that wait. Only a session with an ingress seam sets it. |
| queue | `cluster_client.egress_out` | The egress payloads that wait for the consumer. The stream session uses `cluster_client.stream_egress_out`. |

### Host and agent metrics

- `nomad/node-exporter.system.nomad.hcl` runs one `node_exporter` on every node. It listens on port 9100 (Consul service `node-exporter`). It exports the CPU, memory, disk, file systems and network of the host.
- On the local (container) profile, all nodes share one host. There, the exporter reads only the `/proc` collectors: load, memory, network, pressure and vmstat. Twelve exporters that read the host hardware files in `/sys` (cpu, cpufreq, mdadm, nvme) stall in D state and stop the host. The job variable `host_hardware` selects the set.
- Every Nomad agent publishes its own metrics on `/v1/metrics?format=prometheus`.
  - A client publishes the node resources and the allocations.
  - A server publishes the Raft and scheduler state.
- Prometheus discovers both through the local Consul agent. An elastic node is scraped from the moment it joins. The `node` label is the Consul node name.
- On the production profile, the Nomad API uses TLS. Prometheus checks the agent certificate against the CA of `nomad_tls_dir`.

### Naming convention

The pattern is `kardamom_<service>_<subsystem>_<name>_<unit>`.
Examples: `kardamom_sequencer_tx_ingested_total`, `kardamom_executor_block_apply_duration_seconds`.

- The validator metrics start with `validator_`, not `kardamom_validator_`.
  Examples: `validator_committed_block`, `validator_blocks_verified_total`, `validator_bal_missing_total`,
  `validator_receipt_missing_total`, `validator_divergence_total`, `validator_state_root_block`.
  The exception is `kardamom_validator_egress_cursor_index`, which pairs with `kardamom_batcher_egress_cursor_index`.
- The executor exports `kardamom_executor_block_number` for the chain head.
- Each service emits `kardamom_build_info`. It is a gauge with the value 1 and the labels `version` and `sha`.
  Set `KARDAMOM_GIT_SHA` at build time to fill `sha`.
- `kardamom_service_up` is 1 while the exporter is live.

### Halt, pause, and the admin routes

A service that cannot go on holds a halt. A service that waits on a halt upstream, or on an operator, holds a pause.
Both states show on the exporter port. See [failure-modes.md](failure-modes.md#halts-and-service-events) for the causes.
Each cause has a runbook in [runbooks/README.md](runbooks/README.md).

| Route | Method | Peer | Meaning |
| --- | --- | --- | --- |
| `/halt` | GET | any | The lifecycle record as JSON. |
| `/halt/clear` | POST | loopback only | Clears the halt. Answers `cleared <cause>`, or `no halt stands`. |
| `/pause?note=<text>` | POST | loopback only | Pauses the service for an operator. The default note is `operator pause`. |
| `/resume` | POST | loopback only | Ends any pause. |

- A POST route answers 403 to a peer that is not on loopback. Run it on the node of the service.
- The `/halt` record holds these fields:
  - `service` and `state`.
  - `halted` (a boolean) and `pause` (a record, or `null`).
  - While a halt stands: `cause`, `detail`, `recovery`, `runbook`, `since_unix_ms`, and `clears` (`auto` or `operator`).
- Two gauges carry the same state:
  - `kardamom_halt{cause,recovery}` is 1 while the service is halted. The exporter adds `service` and `host_id`.
  - `kardamom_paused{reason,root_service,cause}` is 1 while the service is paused.
    An operator pause has `cause="operator"`.
- `/ready` answers 503 while a halt or a pause stands. This holds for each service, on top of its own rule.
- The ingress answers a submit with a `-32010` error while it is paused. It counts the submit in
  `kardamom_ingress_tx_rejected_total{reason="paused"}`.

### Histogram buckets

`kardamom_obs::init` sets one bucket list on the recorder. All latency histograms use it.

```text
0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0
```

The range is 100 µs to 5 s.

Some histograms have their own buckets:

| Histogram | Buckets |
| --- | --- |
| `kardamom_step_seconds` | 1 µs to 10 s |
| `kardamom_batch_items`, `kardamom_validator_parallel_batches` | The powers of 2 from 1 to 65,536 |
| `kardamom_executor_bal_frame_bytes` | The powers of 4 from 64 B to 64 MiB |
| `kardamom_canary_*` | 5 ms to 1 h |

### Pipeline metrics

Four names locate a bottleneck on the transaction path. Every service records them through
`kardamom_obs::pipeline`. The `service` label tells the services apart.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kardamom_loop_nanoseconds_total` | counter | `loop`, `state` | The time of a thread loop in each state: `busy`, `idle` or `blocked`. |
| `kardamom_queue_depth` | gauge | `queue` | The items that wait in a queue between two loops. |
| `kardamom_step_seconds` | histogram | `step` | The duration of one step of work. A hot loop records it per batch. |
| `kardamom_batch_items` | histogram | `batch` | The items in one batch. |

- The busy share of a loop is `busy / (busy + idle + blocked)`. A share near 1 means that the loop is saturated.
- `blocked` is time with work but no room on the output: a full queue or a back-pressured publication.
- A queue that grows sits in front of the bottleneck.
- A loop adds its time to the counter at most every 100 ms.

## Readiness

Each exporter serves `/ready` beside `/metrics` on the same port. The check answers 200 `ready` when the rule of the service holds.
It answers 503 with the failed conditions when the rule does not hold.

- The rule is a list of conditions on the gauges of the service. A rolling deploy and the Consul checks use it.
- A condition needs the gauge to exist. A gauge that the service has not set yet fails the condition.
- The condition kinds:

| Kind | Holds when |
| --- | --- |
| `equals` | Each series of the gauge equals the value. |
| `present` | At least one series of the gauge exists. |
| `within` | The highest series of a head gauge, minus each series of the gauge, is at most the allowed gap. |
| `fresh` | Each series of the gauge is a unix time that is not older than the allowed age. |

- Each rule also requires `kardamom_service_up == 1`.
- Each service answers 503 while it holds a halt or a pause, whatever its rule says. See "Halt, pause, and the admin routes".

| Service | Rule |
| --- | --- |
| `kardamom-ingress` | `kardamom_ingress_draining == 0`. The RPC port `GET /health` answers 503 while the ingress drains. |
| `kardamom-sequencer` | `kardamom_sequencer_last_boundary_unix_seconds` is fresher than `boundary_silence_ms` (default 10000). The session is open and the egress is attached. |
| `kardamom-executor` | `kardamom_executor_block_number` is within `--ready-lag-blocks` of `kardamom_sealer_block_number`. |
| `kardamom-validator` | `validator_verdict_standing == 0`, and `validator_committed_block` is within `--ready-lag-blocks` of `validator_sealer_block_number`. |
| `kardamom-batcher` | In live mode: `kardamom_batcher_feed_running == 1`. The offline scan needs only the liveness gauge. |
| `kardamom-state-mirror` | `kardamom_state_mirror_serving == 1`. |
| `kardamom-da-watcher` | The last tick (`kardamom_da_watcher_last_tick_unix_seconds`) is fresher than two poll periods plus 10 s. |
| `kardamom-l1-indexer` | Now is before the planned wake time (`kardamom_l1_follower_next_wake_seconds`) plus one poll interval and 10 s. A follower sleeps between finality steps, so a rule on the last tick would fail between steps. |
| `kardamom-notifier` | Liveness only. |
| `kardamom-canary` | Liveness only. A failed probe is a metric, not a readiness failure. |

- `--ready-lag-blocks` (env `KARDAMOM_READY_LAG_BLOCKS`, default 8) is a flag of the executor and the validator.
- The sealer has its own admin server. It is off by default.
  - `-Dkardamom.cluster.adminPort` sets the port. The value 0 turns the server off. The cluster deploy uses 40205.
  - `GET /status` always answers 200 with the status of the member as JSON.
  - `GET /ready` answers 200 when the member has a settled role, its election is closed, and it applied the committed log within the lag budget.
    Otherwise it answers 503 with the same JSON.
  - `-Dkardamom.cluster.readyLagBytes` sets the lag budget. The default is 4 MiB.
  - See [../cluster/sealer-service/README.md](../cluster/sealer-service/README.md).

## Quick start

Start the local cluster. Its deploy includes the monitoring job.

```sh
just container-up
```

Run the recipe from the repository root. See [../deploy/cluster/README.md](../deploy/cluster/README.md) for the recipes.

- Prometheus answers on port 9090, Alertmanager on port 9093 and Grafana on port 3000 of the node that runs the monitoring job.
  On the local profile this is the aux node.
- Read the address of the aux node from the node contract:

```sh
jq -r '.nodes["aux-0"].ip' deploy/cluster/terraform/containers/node-contract.json
```

- Grafana allows anonymous viewing.
  The admin login is `admin` with the job variable `grafana_admin_password` (`kardamom` on the local profile).
- Drive load at the ingress JSON-RPC endpoint with the bench.
  - `transfers` is the write-path workload (`eth_sendRawTransaction`).
  - The chain must prefund the signer accounts of the workload. See "Workflows and signer prefunding".

```sh
cargo run --release --bin kardamom-bench -- \
  --rpc http://127.0.0.1:8545 \
  --concurrency 16 --timeout 30s \
  transfers
```

- The bench prints the latency percentiles to stdout. With `--output <path>` it also writes a JSON report.
- The dashboards fill within a few seconds.

## Dashboards

Each dashboard is a JSON file in `deploy/grafana/provisioning/dashboards-json/`. The file name is the UID.

| UID | Content |
| --- | --- |
| `kardamom-overview` | Cross-service signals: liveness and block height. It has the `host` variable. |
| `kardamom-ingress` | The RPC front door. |
| `kardamom-sequencer` | The sequencer lanes. |
| `kardamom-executor` | The executors. |
| `kardamom-sealer` | The boundary rate, the declared block for each observing executor host, and the commit lag of each executor behind the sealer. It reads the re-exported `kardamom_sealer_*` series. |
| `kardamom-batcher` | Settlement health, including the age of the last post against its alert threshold. |
| `kardamom-da-watcher` | The L1 watcher. |
| `kardamom-validator` | The validator. |
| `kardamom-state-mirror` | The account-state mirror. |
| `kardamom-notifier` | The status feed and the webhooks. |
| `kardamom-canary` | The transaction canary: the success ratio per probe and per endpoint, the failures by outcome, the stage latencies, the last success, the balances, the feed gaps and the stalled accounts. |
| `kardamom-performance` | Throughput and latency against the two load goals: 10,000 tx/s sustained, and 1,000,000 tx/s with a p99 of 1 ms from submit to the executed receipt. It shows the share of each goal, the rate at each pipeline step, the submit-to-sealed and submit-to-executed percentiles, the canary stages, the step times inside the services, the queues and the back pressure. |
| `kardamom-availability` | The service levels against a 99.9 % target over the selected range: the canary success and its error budget, the submit success, the chain liveness, the ingress reachability and the validator verdict. Over time: the failures, the head lag of each replica, the scrape targets, the settlement and the sealer leader changes. |
| `kardamom-bottlenecks` | The pipeline metrics of every service in path order: the busy and blocked share of each loop, the queue depths, the step p50 and p99, the batch sizes, the latency of each stage, and the misses and retries. |

- A dashboard queries `kardamom`-scoped metrics of its service.
- The test `crates/obs/tests/dashboards.rs` checks that each dashboard in `EXPECTED_DASHBOARDS` parses, uses schema 38,
  and queries only `kardamom`-scoped metrics.
  Add a dashboard to that list when you add the dashboard.

## Alerts

`deploy/alerts.yml` holds 25 Prometheus alert rules. The monitoring job loads them as `rule_files`.
Check the file with `promtool check rules deploy/alerts.yml`.

- The monitoring job runs Prometheus and Alertmanager in one allocation.
  - Prometheus sends the firing alerts to that Alertmanager (port 9093 on the monitoring node, Consul service `alertmanager`).
  - The firing alerts also show on the `/alerts` page of Prometheus.
- The rules of this file are neutral: each one reports a fault that the code counts.
- The operator of an environment keeps the tuned rules, the limits and the routing outside this repository.
  The operator gives them to the job in the Nomad variable `nomad/jobs/monitoring`.

| Item | Content |
| --- | --- |
| `rules` | One Prometheus rule file. Prometheus loads it next to `deploy/alerts.yml`. |
| `alertmanager` | The complete Alertmanager configuration, receivers included. |

```sh
nomad var put nomad/jobs/monitoring rules=@rules.yml alertmanager=@alertmanager.yml
```

- The deploy writes the variable when it gets `ALERTMANAGER_CONFIG_FILE` (and, as an option, `PROMETHEUS_RULES_FILE`).
  It adds `deploy/alertmanager-inhibit.yml` to the configuration. See "Secrets" in [`deploy/cluster/README.md`](../deploy/cluster/README.md#secrets).
- The job reads the variable with its workload identity and renders the two files.
  The Alertmanager configuration goes to `secrets/`, because a receiver can hold a token.
- A change to the variable reloads Prometheus and Alertmanager in place (SIGHUP).
- Without the variable, Prometheus evaluates `deploy/alerts.yml` only.
  Alertmanager sends every alert to a receiver that notifies nobody.
  The alerts then show on the Alertmanager page and nowhere else.

| Alert | Severity | Meaning |
| --- | --- | --- |
| `KardamomBatcherLastPostStale` | critical | The last `BatchPosted` block that L1 serves is older than twice `kardamom_batcher_idle_flush_seconds` for 10 minutes. |
| `KardamomBatcherResumeFailures` | critical | A batcher start failed to read L1 in the last 10 minutes. |
| `KardamomL1IndexerTickErrors` | critical | More than half of the indexer ticks fail for 15 minutes. |
| `KardamomDaWatcherTickErrors` | critical | More than half of the da-watcher ticks have an outcome other than `ok` for 15 minutes. |
| `KardamomL1FollowerLag` | critical | The newest `l1_blocks` record is more than 64 blocks (two finality steps) behind the finalized tip that the validator's own L1 read sees (`validator_l1_finalized_block_number`; the follower's own tip without a validator L1 check) for 5 minutes. Runbook: [`l1_follower_lag`](runbooks/l1_follower_lag.md). |
| `KardamomL1FollowerWakeOverdue` | critical | A follower instance is more than 2 minutes past its planned wake time for 2 minutes. Runbook: [`l1_follower_wake_overdue`](runbooks/l1_follower_wake_overdue.md). |
| `KardamomL1SourceDisagreement` | critical | Two L1 sources gave different answers in the last 10 minutes. The follower halts. |
| `KardamomValidatorEpochsUnverified` | warning | The validator committed an epoch without an L1 check in the last 10 minutes. |
| `KardamomValidatorEpochFault` | critical | An epoch on the canonical stream does not match L1. The validator halts. |
| `KardamomValidatorDivergence` | critical | The re-execution of the validator differs from the published block. The validator halts. |
| `KardamomValidatorDown` | critical | Prometheus cannot scrape the validator for 1 minute. A halted validator stays scrapeable, so this alert means the process or the node is gone. |
| `KardamomHaltL1SourceDisagreement` | critical | `kardamom_halt{cause="l1_source_disagreement"} == 1`. |
| `KardamomHaltL1ChainBreak` | critical | `kardamom_halt{cause="l1_chain_break"} == 1`. |
| `KardamomHaltL1Unreachable` | critical | `kardamom_halt{cause="l1_unreachable"} == 1`. |
| `KardamomHaltReplayUnavailable` | critical | `kardamom_halt{cause="replay_unavailable"} == 1`. |
| `KardamomHaltDaLag` | critical | `kardamom_halt{cause="da_lag"} == 1`. |
| `KardamomHaltSealerNoQuorum` | critical | `kardamom_halt{cause="sealer_no_quorum"} == 1`. |
| `KardamomHaltL1CursorUnreadable` | critical | `kardamom_halt{cause="l1_cursor_unreadable"} == 1`. |
| `KardamomHaltValidatorDivergence` | critical | `kardamom_halt{cause="validator_divergence"} == 1`. |
| `KardamomHaltOriginGap` | critical | `kardamom_halt{cause="origin_gap"} == 1` for 1 minute. |
| `KardamomHaltRecordLag` | critical | `kardamom_halt{cause="record_lag"} == 1`. The record-lag guard is off by default, so this alert cannot fire until a later release turns the guard on. |
| `KardamomHaltL1LightClientMismatch` | critical | `kardamom_halt{cause="l1_light_client_mismatch"} == 1`. |
| `KardamomHaltL1FollowerDisagreement` | critical | `kardamom_halt{cause="l1_follower_disagreement"} == 1`. |
| `KardamomPublicationNotConnected` | warning | `kardamom_publication_connected == 0` for 1 minute. A must-deliver publication has no connected subscriber. The publisher holds its receipts. While discovery lists a subscriber, it reopens the publication after one stall budget and exits with code 3 after five. See "Publications". |
| `KardamomServicePaused` | info | `kardamom_paused == 1` for 1 minute. |

- A validator that diverges stays up and keeps `up == 1`.
  The pages for a divergence are `KardamomValidatorDivergence` and `KardamomHaltValidatorDivergence`.
- The twelve `KardamomHalt*` rules have one rule for each halt cause. Each one fires at once (`for: 0m`), except `KardamomHaltOriginGap`.
  - `KardamomHaltOriginGap` waits 1 minute. A restarted sequencer can miss the epoch that the sealer expects, and its twin offers that epoch again within milliseconds. Only a gap that no replica fills pages.
  - Each rule has the labels `severity` and `cause`.
  - Each rule has the annotation `runbook`, a path to the file in [runbooks/](runbooks/README.md).
  - The description names the cause, the `/halt` URL of the service, and the runbook.
- `KardamomServicePaused` is an info alert. A paused service waits on a halt upstream or on an operator.
  - The root halt pages once. The pause does not page again.
  - The `reason` label is `upstream` or `operator`.

### The inhibit file

`deploy/alertmanager-inhibit.yml` holds one Alertmanager inhibit rule.

- It mutes `KardamomServicePaused` while a `KardamomHalt*` alert with the same `cause` label fires.
- An operator pause has `cause="operator"`. No halt alert has that cause, so the rule never mutes it.
- The deploy adds this file to the configuration of `ALERTMANAGER_CONFIG_FILE` before it writes the `alertmanager` item.
  An operator who writes the item by hand copies the rule into it.

These counters need an alert of your own. No rule in `deploy/alerts.yml` watches them:

- `kardamom_executor_resync_total{outcome}` counts the full resyncs after a replay-window overrun.
- `kardamom_egress_copy_mismatch_total` counts the copies of a canonical frame, from another executor, whose bytes differ from the first copy. Any increase means that one executor read another stream: alert on it.
- `kardamom_egress_waiting_dropped_total` counts the times that every executor copy missed a frame, and the egress dropped the frames that waited behind it. The merged stream then stalls until a copy sends the frame.
- `kardamom_egress_caught_frames_total` counts the frames that a peer egress gave after a start, to fill the place between the head and the executor copies.
- `kardamom_egress_rebased_total{reason}` counts the times that the egress started its stream again: after a boundary of a copy with no peer (`boundary`), or at the floor of a peer above its head (`peer-floor`). After a `boundary` start, the store holds no frame before that boundary, and a reader of an older cursor reads another instance.
- `kardamom_executor_egress_frames_dropped_total` counts the canonical frames that the reader did not forward on `exec_frames`, because the queue to the publication was full.

`kardamom_egress_head_distance_frames` is a gauge of each egress instance. It gives the canonical indexes between the stored head and the newest record that any executor copy sent. The egress sets it after each sync of its store.

- The sealer keeps only its replay window. A reader below the window recovers from the store of the egress, so the distance must stay well inside the window.
- The egress does not see the sealed head. The distance does not include the lag of every executor behind the sealer.
- `KardamomEgressHeadNearWindow` in `deploy/alerts.yml` fires when the distance stays above 32,768 for 1 minute: half the default window of 65,536 frames.
  - `peer-checkpoint`: the node fell behind the retention window and repaired itself from a peer checkpoint.
  - `unrecoverable`: the node could not repair itself and waits for an operator.
  - See "Replay-window overrun" in [failure-modes.md](failure-modes.md).
  - The validator counter is `validator_resync_total`. A `peer-checkpoint` increment there means the validator did not verify the blocks up to the adopted checkpoint.
- `kardamom_executor_rollback_total{outcome}` counts the rollbacks of an executor that reads the member feed, after the canonical stream diverged from its committed history (`local-checkpoint`, `peer-checkpoint`, `no-checkpoint`), or from the receipts that it published above that history (`receipts-only`).
  - `local-checkpoint` or `peer-checkpoint`: the executor parked its state and restores a checkpoint below the divergence.
  - `no-checkpoint`: no checkpoint below the divergence exists. The executor stops and waits for an operator.
  - See "A power loss on a majority of the sealers rolls the paired executors back" in [failure-modes.md](failure-modes.md).
- `kardamom_sequencer_receipt_floors_forgotten_total` counts the nonce floors that a rollback of an executor reset to unknown.
- `validator_replica_results_unchecked_total{reason="withdrawn"}` counts the buffered receipts, rows and BALs that a rollback of an executor withdrew.
- `kardamom_checkpoint_log_dropped_total` counts the committed blocks that the state writer did not give to the block log of the checkpoints, because the queue to the log thread was full. Each one is a gap in the log.
- `kardamom_checkpoint_log_failed_total{step}` counts the appends and syncs of the block log that failed (`step` is `append` or `sync`). The next block then starts a new segment.
- `kardamom_checkpoint_copy_unlimited_total` counts the base copies that libmdbx refused into the pipe of the rate limit. Such a copy then ran with no limit.
- `kardamom_checkpoint_unreadable_schema_skips_total` counts the checkpoints an executor, a validator or a state mirror skipped because the image holds a state schema the release does not read.
  - An increment after a rollback is expected: the newest checkpoint comes from the newer release.
  - Growth on a steady fleet means a node runs a release of another schema.
- `validator_bal_sub_reopen_total` counts the reopens of the `tx_bal` subscription after 60 s of silence.
  - A few reopens on an idle cluster are noise.
  - Growth on a chain that progresses means the BAL delivery to that node is broken. The verification coverage drops.
- `kardamom_executor_invalid_tx_skipped_total` counts the deterministically invalid transactions that the executor skipped.
  The skip writes a marker receipt (`status=false`, `gas_used=0`, see `Receipt::is_invalid_skip`).
  - The chain stays live. The result is the same in live execution, recovery replay, and validator re-execution.
  - A non-zero value means a guard upstream (the sequencer nonce fence, the cluster dedup, or the receipt-floor resync) let an invalid record in.
    Find the source.
- `kardamom_sequencer_ref_below_floor` must read 0. See "Sequencer".

## What is instrumented

Each service instruments its own hot path. The metrics of a service are the metrics that its dashboard queries.
The groups that need an explanation follow.

### Batcher

The live batcher exports a settlement-health group on port 9002. See [l1-data-path.md](l1-data-path.md) for the posting rules.

| Metric | Meaning |
| --- | --- |
| `kardamom_batcher_batches_posted_total` | Confirmed L1 posts. In live mode it does not count packed batches. |
| `kardamom_batcher_payload_bytes_posted_total` | Payload bytes of the confirmed posts. |
| `kardamom_batcher_blocks_observed_total` | Blocks that the batcher saw. |
| `kardamom_batcher_last_posted_block` | Last L2 block in a confirmed post. Compare it with `kardamom_executor_block_number` for the DA lag. The ingress cluster gauges give the lag directly. |
| `kardamom_batcher_last_batch_index` | `lastBatchIndex` after the last confirmed post. |
| `kardamom_batcher_pending_blocks` | Closed blocks that wait for the group to fill or flush. |
| `kardamom_batcher_l1_post_retries_total` | Retries of L1 posts. It flags a flaky L1. |
| `kardamom_batcher_skipped_posted_blocks_total` | Blocks that were seen again and dropped because L1 covers them. A restart causes a few. Growth means the cursor file is not persisted. |
| `kardamom_batcher_last_post_age_seconds` | Age of the newest L1 block that carried a `BatchPosted` event, as the L1 follower's `l1_blocks` stream shows it. At start, the follower's archive gives the last post. The value never comes from the batcher's memory. It grows when the batcher stops posting, when the follower's sources hide its posts, and while the follower is down. |
| `kardamom_batcher_idle_flush_seconds` | The idle flush wait. The alert compares the post age with twice this value. |
| `kardamom_batcher_resume_failures_total` | Starts whose L1 read failed. The start retries in the process, so the counter stays scrapeable. |
| `kardamom_batcher_rebuilt_blocks_total` | Blocks rebuilt from references after the sealer refused a replay. |
| `kardamom_batcher_spool_dropped_total{reason}` | Spools dropped at start. `other-version`: a spool of another release. `unreadable`: a block file does not decode. `discontinuous`: the spool does not continue the confirmed cursor. The sealer serves the range again. One after a deploy is expected. Growth means the spool disk is bad. |
| `kardamom_batcher_feed_running` | 1 when the feed loop runs over the restored spool. The readiness rule needs it. |
| `kardamom_batcher_egress_cursor_index` | With `--egress`: the canonical index of the newest record that the batcher took from the egress. A boundary does not move it. Absent on the cluster egress. See [Egress](#egress). |

### L1 sources

The indexer (the L1 follower) exports these metrics. See "Two L1 sources for the followers" in [l1-data-path.md](l1-data-path.md).

| Metric | Meaning |
| --- | --- |
| `kardamom_l1_source_disagreement_total` | Two sources answered differently for one block or one log query. Any increase means an endpoint lies. |
| `kardamom_l1_source_rotations_total{source,reason}` | Rotations of a source out of its set for the backoff. The reasons are `error`, `rate_limited`, and `disagreement`. |

### DA-watcher

| Metric | Meaning |
| --- | --- |
| `kardamom_da_watcher_tick_total{outcome}` | Passes over the `l1_blocks` records. The outcomes are `ok` (epochs published), `chain_break`, and `disagreement`. |
| `kardamom_da_watcher_l1_finalized_block_number` | Newest finalized L1 block that the `l1_blocks` stream carried to the watcher. |
| `kardamom_da_watcher_waiting_for_l1_block{number}` | 1 while the watcher waits for the record of the block `number` names: no archive holds it, and it is not on the stream. The watcher is paused on the follower meanwhile. |
| `kardamom_da_watcher_epoch_origin_block_number` | Newest L1 block with a published epoch. The difference to the finalized block is the origin lag. |
| `kardamom_da_watcher_epochs_published_total` | Epochs published. One for each finalized L1 block. |
| `kardamom_da_watcher_deposits_detected_total` | Deposit publishes. A range that is retried after back-pressure counts again. |
| `kardamom_da_watcher_last_tick_unix_seconds` | Unix time of the last housekeeping tick. The readiness rule uses it. |
| `kardamom_da_watcher_l1_confirmed_origin` | The L1 origin of the sealer, as the boundaries carry it: the last epoch that the sealer committed. The cursor file holds it. |
| `kardamom_da_watcher_epochs_unconfirmed` | Published epochs that no boundary confirmed yet. It stays near 0 while the sealer commits. At 2048, the watcher publishes no new epoch. |
| `kardamom_da_watcher_epochs_republished_total` | Epochs published again, because no boundary confirmed them within 30 s. |
| `kardamom_da_watcher_l1_cursor_persist_failures_total` | Failed writes of the L1 cursor file. A failure is not fatal: a restart publishes epochs again, and the sealer drops them. A growing count moves the restart point further back. |

- A `chain_break` outcome means a record did not descend from the watcher's head. The watcher halts at that block and reads it again from the archives every tick.
- A `disagreement` outcome means two records of one block had different hashes. The watcher halts until an operator clears it.
- The interop watcher exports `kardamom_da_watcher_remote_*` counters with the label `origin` (the peer chain id).

### Publications

The executor exports the state of its must-deliver `tx_receipts` publication. The labels are `topic` and the exporter's `service` and `host_id`. See "Dead `tx_receipts` publication" in [failure-modes.md](failure-modes.md#executor).

| Metric | Meaning |
| --- | --- |
| `kardamom_publication_connected{topic}` | 1 while the publishes of the topic land. 0 while they fail with `NOT_CONNECTED`: no subscriber has an image of the publication. The commit thread writes it on each change. |
| `kardamom_publication_not_connected_seconds{topic}` | How long the escalation clock has counted in the current unconnected period. 0 while the publication is connected, and 0 while discovery lists no subscriber of the topic. The executor reopens the publication when the clock passes one Aeron stall budget (`AERON_STALL_TOLERANCE_MS` plus 5 s, 35 s in CI), and exits with code 3 when it passes five. |

- Every subscription logs `aeron: image available` and `aeron: image unavailable` with the stream id, the channel, the session id and the source address. A subscriber that attached a publisher and shows no `available` line for its session has no image of it.
- The executor logs `tx_receipts publication reopened on a new session` with the old and the new session id at the reopen. The exit line is `the publication stayed unconnected past its budget; the process exits so the supervisor restarts it`, with the topic, the durations and the exit code.

### Executor stream

Each executor exports these metrics for its executor stream (`exec_txs`). See "The executor stream" in [failure-modes.md](failure-modes.md#the-executor-stream-the-executor-records-what-it-joins).

| Metric | Meaning |
| --- | --- |
| `kardamom_executor_exec_stream_recorded_index` | The recorded cursor: the highest canonical index whose records the local archive has written. It never passes the recording position. It moves with the canonical order, also with no transaction load. A flat value on a chain that progresses means that the archive of the node takes no records. |
| `kardamom_executor_exec_stream_session_id` | The Aeron session id of the recorded publication. A restarted executor shows a new value. |
| `kardamom_executor_exec_stream_publish_blocked_ms_total` | Milliseconds that the publisher waited for the archive to take a record. The executor stalls while it grows. |

- The live publication counts its dropped records in `kardamom_log_best_effort_dropped_total{stream_id="1005"}`. A drop is normal while no consumer subscribes.
- With `--exec-cursor` on (`KARDAMOM_EXEC_CURSOR`), the publisher sends the recorded cursor to the sealer. It sends a cursor that moved when 100 ms passed, or at once when it moved by 1024 records. A sent cursor never moves down. The ingress shows the best cursor of the executors in `kardamom_ingress_cluster_recorded_head`.

### Executor input source

With `--carry-bytes` (`KARDAMOM_CARRY_BYTES`, off by default), the executor reads the canonical entries with their bytes from the feed of its paired sealer member, on `aeron:ipc` of the media driver at `--aeron-dir`.

- The executor opens no `tx_data` lane, joins nothing and votes no void.
- It opens no cluster session and sends no recorded cursor. It refuses to start with `--exec-cursor`.
- Switch it on only while the rules of the carry window (feature 5) apply on every member and the member of the executor runs the member feed. The deploy switch is `KARDAMOM_CARRY_EXECUTORS`, and the release gate checks these conditions. See [the carry window runbook](runbooks/carry-window-rollout.md).
- Off: the executor joins the references of the cluster egress with `tx_data`.

With `--carry-bytes`, `--egress <url>` (`KARDAMOM_EGRESS_URL`, off by default) makes the egress the second source of the executor.

- A cursor below the replay window of the member reads the range read of the egress from the cursor to the head of the store. Then the executor reads the live `egress_order`.
- The executor goes back to the feed by the same rule as from the serving service.
- A cursor below the floor of the egress store too stages a peer checkpoint (`kardamom_executor_resync_total`).
- Off: a cursor below the window stages a peer checkpoint at once.

| Metric | Meaning |
| --- | --- |
| `kardamom_executor_input_source{source}` | 1 for the source that the executor reads, 0 for the other. `source` is `feed` (the member feed) or `service` (the serving service, or the egress with `--egress`). |
| `kardamom_executor_source_mismatch_total{source}` | Canonical entries whose `keccak256(raw_tx)` is not the canonical id, by the source that sent them. The executor does not execute such an entry and does not stop: it leaves the source and reads the same index from the other source. |

- The executor leaves the feed when no frame comes for 6 s, when the feed loses its image, or on a mismatch. It goes back to the feed after the feed has one image for 30 s and a replay of the feed from its cursor reaches `REPLAY_DONE`.
- Any increment of `kardamom_executor_source_mismatch_total` needs an operator: a member or a service sends bytes that do not match the ordered id. The log line is `the raw transaction does not hash to its canonical id; replaying from the other source`, with the source and the index.
- A switch logs `switching the input source` with the old and the new source and the cursor.
- A transaction reference at the cursor is a fault of the source: the reader has no `tx_data` lane to join it. The reader leaves the source and reads the same index from the other source.
- When every source gives a reference at the same index, the reader stops with `UnjoinableReference { index }`. The log line is `no source holds the bytes of the transaction reference; stopping`, with the index. A source that never connected counts as one that gives the reference. Only a sequencer that offers a type-0 reference makes such an entry.

### Executor pipeline

The executor records the [pipeline metrics](#pipeline-metrics) on the streaming path: the `tx_data` lanes and the
`tx_ordering` join, then the exec thread, the commit thread and the state writer. A hot loop records a step or a batch
once per block or per publish batch, never per transaction.

| Name | Kind | Meaning |
| --- | --- | --- |
| `executor.tx_data_reader` | loop | The `tx_data` reader threads. Idle is the wait on a lane. Busy is the insert into the join buffer. All lanes add to one series. |
| `executor.ordering_reader` | loop | The `tx_ordering` reader. A join wait is busy. Blocked is a send to a full exec channel or executor stream. |
| `executor.exec` | loop | The exec thread. Blocked is a send to a full commit channel, and the wait for a free commit slot. |
| `executor.commit` | loop | The commit thread. Busy is the publish on `tx_receipts`. Blocked is the pause between two must-deliver attempts. |
| `state.writer` | loop | The state writer thread. Idle is the wait for a block delta. Busy is the commit. |
| `executor.tx_data` | queue | The `tx_data` backlog of the lane that read last. `kardamom_executor_tx_data_queue_depth{shard}` has each lane. |
| `executor.join_buffer` | queue | The envelopes in the join buffer. The `tx_ordering` reader sets it after each join. |
| `executor.exec_stream` | queue | The items from the reader that wait for the executor stream publisher. It holds 4096 items at most. |
| `executor.reader_to_exec` | queue | The messages from the reader that wait for the exec thread. The receipt queue depth bounds it. |
| `executor.exec_to_commit` | queue | The messages from the exec thread that wait for the commit thread. The receipt queue depth bounds it. |
| `executor.commits_inflight` | queue | The blocks that the state writer did not commit yet. At the full commit pipeline depth, the exec thread waits. |
| `executor.join_wait` | step | A join that did not find its envelope at the first lookup: the time to the end of the join. A join that finds it at once records nothing. |
| `executor.archive_refetch` | step | One archive refetch of a join miss. |
| `executor.boundary` | step | The handling of one block boundary on the exec thread. |
| `executor.receipt_publish` | step | One publish of a receipt batch on `tx_receipts`, with its retries. |
| `state.commit` | step | One block commit of the state writer, from the open of the transaction to the end of the mdbx commit. |
| `state.commit_open`, `state.commit_storage`, `state.commit_accounts`, `state.commit_receipts`, `state.commit_trie`, `state.commit_mdbx` | step | The sections of one block commit. `state.commit_trie` records only when the writer keeps a trie. `KARDAMOM_WRITER_TIMING` still prints the same sections. |
| `executor.block_txs` | batch | The records in one sealed block. |
| `executor.receipt_batch` | batch | The receipts in one publish on `tx_receipts`. |

| Metric | Meaning |
| --- | --- |
| `kardamom_executor_join_misses_total` | Joins whose first in-band wait ended without the envelope. The `tx_data` lane fell behind by a whole wait slice. |
| `kardamom_executor_archive_refetch_total{outcome}` | Archive refetches of a join. The outcomes are `ok`, `absent` (the archive does not hold the range) and `error`. |
| `kardamom_executor_archive_refetch_recovered_total` | Envelopes that archive refetches put into the join buffer. |
| `kardamom_executor_void_votes_total` | Entries that every archive refused. For each one, the executor asks the sealer for a void. |
| `kardamom_executor_gas_used_total` | The gas of each sealed block. Its rate is the gas per second. |
| `kardamom_executor_receipt_publish_retries_total` | Failed must-deliver attempts on `tx_receipts`. Each one pauses the commit thread. |
| `kardamom_executor_bal_handoff_dropped_total` | Blocks whose BAL the exec thread dropped, because the queue to the BAL publisher was full. |
| `kardamom_executor_state_commit_duration_seconds` | The wait of the exec thread for a free commit slot when the commit pipeline is full. It is not the commit time: `state.commit` is the commit time. |

### Executor pipeline

The executor records the pipeline metrics on each thread from the `tx_data` lanes to the receipts.

| Kind | Name | Meaning |
| --- | --- | --- |
| loop | `executor.tx_data_reader` | The `tx_data` reader threads. Idle while the lane is empty. Every lane adds to this one series. |
| loop | `executor.ordering_reader` | The `tx_ordering` reader. A join wait is busy. A send to a full exec channel or a full executor stream is blocked. A void park is idle. |
| loop | `executor.exec` | The exec thread. A send to a full commit channel, and the wait for a free commit slot, are blocked. |
| loop | `executor.commit` | The commit thread. The pause between two must-deliver attempts on `tx_receipts` is blocked. |
| queue | `executor.tx_data` | The backlog of the `tx_data` lane that read last. The per-lane backlog is `kardamom_executor_tx_data_queue_depth{shard}`. |
| queue | `executor.join_buffer` | The envelopes in the join buffer. |
| queue | `executor.reader_to_exec` | The messages from the reader that wait for the exec thread. |
| queue | `executor.commits_inflight` | The blocks that the state writer did not commit yet. The exec thread waits at the pipeline depth. |
| queue | `executor.exec_to_commit` | The messages that wait for the commit thread. |
| queue | `executor.exec_stream` | The items that wait for the executor stream publisher. |
| step | `executor.join_wait` | A join that did not find its envelope at the first lookup. A join that finds it at once records nothing. |
| step | `executor.archive_refetch` | One archive refetch of a join miss. |
| step | `executor.boundary` | One block boundary: the settle, the seal and the start of the next block. |
| step | `executor.receipt_publish` | The publish of one receipt batch, with its retries. |
| batch | `executor.block_txs` | The records in each sealed block. |
| batch | `executor.receipt_batch` | The receipts in each published batch. |

| Counter | Meaning |
| --- | --- |
| `kardamom_executor_join_misses_total` | Joins whose first in-band wait ended without the envelope. The `tx_data` lane is a whole wait slice behind. |
| `kardamom_executor_archive_refetch_total{outcome}` | Archive refetches: `ok`, `absent` (the archive does not hold the range) or `error`. |
| `kardamom_executor_archive_refetch_recovered_total` | Envelopes that refetches put into the join buffer. |
| `kardamom_executor_void_votes_total` | Entries that every archive refused. The executor asked the sealer for a void. |
| `kardamom_executor_gas_used_total` | The gas of every sealed block. Its rate is the gas per second. |
| `kardamom_executor_receipt_publish_retries_total` | Failed must-deliver attempts on `tx_receipts`. |
| `kardamom_executor_bal_handoff_dropped_total` | Blocks whose BAL the exec thread dropped, because the BAL publisher queue was full. |

- `kardamom_executor_state_commit_duration_seconds` is the wait of the exec thread for a free commit slot. It is not the commit. The `state.commit` step is the commit.

### State writer

The state writer thread records the loop `state.writer`. The thread has no output queue, so it is never blocked.
Each block commit records these steps:

| Step | Section |
| --- | --- |
| `state.commit` | The whole commit, from the start of the MDBX transaction to the end of its commit. |
| `state.commit_open` | The open of the tables. |
| `state.commit_storage` | The storage slots. |
| `state.commit_accounts` | The accounts. |
| `state.commit_receipts` | The receipts. |
| `state.commit_trie` | The trie update. Only a writer with a trie records it. |
| `state.commit_mdbx` | The MDBX commit, with the sync of the durability mode. |

### L1 follower (inbox indexer)

| Metric | Meaning |
| --- | --- |
| `kardamom_l1_follower_next_wake_seconds` | Unix time of the next planned L1 read. The readiness rule and `KardamomL1FollowerWakeOverdue` use it. |
| `kardamom_l1_follower_published_block_number` | Newest L1 block published on `l1_blocks`. `KardamomL1FollowerLag` uses it. |
| `kardamom_l1_follower_l1_reads_total{read}` | L1 reads. The kinds are `tip`, `headers` (one batch request), `logs` and `light_client`. It shows the provider cost of the follower. |
| `kardamom_l1_indexer_l1_finalized_block_number` | Newest finalized L1 block that the indexer saw. |
| `kardamom_l1_indexer_indexed_block_number` | Highest L1 block with indexed batches and epoch. |
| `kardamom_l1_indexer_last_batch_index` | Highest batch index that the indexer holds. |
| `kardamom_l1_indexer_batches_total` | Batches indexed. |
| `kardamom_l1_indexer_payload_bytes_total` | Payload bytes stored. |
| `kardamom_l1_indexer_tick_total{outcome}` | Ticks. The outcomes are `idle`, `advanced`, and `error`. |
| `kardamom_l1_indexer_last_tick_unix_seconds` | Unix time of the last tick. |

### Sequencer

- `kardamom_sequencer_ref_below_floor` is a gauge. It reports each second.
  - It counts the buffered references that are below the floor of their sender.
  - It is 0 unless a rewind or a floor update stranded a reference. Nothing drains such a reference.
  - The sender is then stuck, and the client sees no error. Any value above 0 is a defect.
- `kardamom_sequencer_fee_rejected_total{partition}` counts the envelopes that the fee gate refused.
  Each one gets a `FeeInvalid`, `FeeTooLow`, or `InsufficientFunds` error on `tx_errors`.
  See [priority-fees.md](priority-fees.md).

The egress feed thread keeps the decision version of the sealer and handles the refused references (egress kind 18).
See "The kept sealer version and the refused reference" in [failure-modes.md](failure-modes.md).

| Metric | Meaning |
| --- | --- |
| `kardamom_sequencer_sealer_version{partition}` | The kept decision version of the sealer: the highest version that a session of the process reported, or the floor `--sealer-version-floor` before the first report. At 5, the sequencer offers each transaction with its bytes (record type 7). |
| `kardamom_sequencer_ref_refused_total{partition}` | The kind-18 frames: the references without bytes that the carry window of the sealer refused. |
| `kardamom_sequencer_tx_drops_total{reason}` | The drop records on `tx_drops`, by reason. `sealer_refused_reference` counts the drops of a kind-18 frame. |

The epoch lane exports these metrics. See "Sequencer" in [failure-modes.md](failure-modes.md).

| Metric | Meaning |
| --- | --- |
| `kardamom_sequencer_l1_origin` | The highest L1 origin that a boundary carried: the last L1 block whose epoch the sealer ordered. The sealer accepts the epoch of the next block only. An operator resumes the da-watcher after this block. |
| `kardamom_sequencer_epochs_unconfirmed` | The epochs that the lane relayed, or took to relay, and that no boundary confirmed yet. The lane holds at most 4096. |
| `kardamom_sequencer_origin_gap_total` | The `ORIGIN_GAP` rejects that the sealer sent to this replica. Each one makes the lane offer its unconfirmed epochs again from the expected block. |

The sequencer records these pipeline names (see "Pipeline metrics"). The v1 and the v2 loop record the same names.
Each lane is its own process on its own metrics port, so the scrape target tells the lanes apart.

| Kind | Name | Meaning |
| --- | --- | --- |
| loop | `sequencer.main` | The publish loop. A pass that did work is busy. A pass that found no input is idle. A pass that the sealer back-pressured is blocked. The wait after a pass counts in the state of the pass. |
| loop | `sequencer.egress_feed` | The egress-watermark feed. It is idle while it waits for an egress frame. |
| loop | `sequencer.receipt_feed` | The `tx_receipts` floor feed. It is blocked while the account rows wait for room in the queue of the v2 loop. |
| step | `sequencer.offer_round_trip` | One offer to the sealer: the request to the session thread, the Aeron publish there, and the reply. The publish loop waits for all of it. |
| step | `sequencer.tx_errors_publish` | One `tx_errors` publish. The publish loop waits for it for each refused transaction. |
| step | `sequencer.account_lookup` | One lookup of a sender's state. The v2 loop times each lookup. The v1 loop times each query to Redis or the executors, and not the answers from the local account layer. |
| batch | `sequencer.offer_refs` | The refs that one offer took. The v1 loop offers at most 13. The v2 loop offers its outbox up to 64 KiB of entries. |
| queue | `sequencer.tx_data` | The envelopes that wait in the `tx_data` lanes. |
| queue | `sequencer.outbox` | The v2 offers that wait for the sealer. The loop stops its intake at `outbox_cap`. |
| queue | `sequencer.accounts` | The v2 account events that wait: rows, lookup answers and rollbacks. The queue holds 65,536. |

- The loop sets its queue gauges once in 64 passes.
- `kardamom_sequencer_nonce_check_duration_seconds` is the nonce state machine in the v1 loop, and the admission filter in the v2 loop.

### Sealer pipeline

The admin endpoint of each sealer member exports these series. Each one carries `service="sealer"` and `member_id`.
They use the names of the shared pipeline metrics, so one dashboard shows whether the sealer is the bottleneck.

The service thread writes the pipeline series. It allocates nothing for them and uses no map.
A ClusteredService cannot see its idle time, so the loop exports busy time only. The Aeron cycle-time series below show the duty cycle.

| Metric | Meaning |
| --- | --- |
| `kardamom_loop_nanoseconds_total{loop="sealer.service",state="busy"}` | The time of the service thread in the session messages, the timers, the snapshots, and the member feed passes that did work. |
| `kardamom_step_seconds{step="sealer.message"}` | One session message. A batch message is one step. |
| `kardamom_step_seconds{step="sealer.flush_window"}` | One flush of the ordering window that admitted records. A member with no ordering window records none. |
| `kardamom_step_seconds{step="sealer.boundary"}` | The work of one boundary timer: the flush, the boundary, the status, and the timer re-arm. |
| `kardamom_step_seconds{step="sealer.snapshot"}` | One snapshot: the window flush and the snapshot write. |
| `kardamom_step_seconds{step="sealer.egress_offer"}` | One offer of a relayed frame to the consumer sessions. It samples one offer in 64. |
| `kardamom_batch_items{batch="sealer.ingress_batch"}` | The entries of one batch message (kind 3). |
| `kardamom_queue_depth{queue="sealer.egress_backlog"}` | The bytes that the egress backlogs hold, after the last drain. |
| `kardamom_queue_depth{queue="sealer.ordering_window"}` | The records that the ordering window holds. |
| `kardamom_sealer_frames_total{outcome}` | The frames that the service did not relay. `outcome` is `malformed`, `unknown_kind`, `contiguity_reject`, `past_deadline`, `window_full`, `da_lag_reject`, `record_lag_reject`, `expired_ref`, `origin_gap` or `remote_origin_reject`. |
| `kardamom_sealer_messages_total{kind}` | The session messages, by the kind byte. A kind shows after its first message. |
| `kardamom_sealer_egress_backpressure_total{result}` | The egress offers that queued their frame. `result` is `back_pressured`, `admin_action` or `not_connected`. A frame behind an open backlog does not count. |
| `kardamom_sealer_egress_stalls_total` | The sessions closed because their backlog made no progress for 1 s. |
| `kardamom_sealer_retained_frames`, `kardamom_sealer_retained_bytes` | The retained frames and their bytes, at the end of the last log entry. |
| `kardamom_sealer_retained_store` | The store of the retained frames: 0 heap, 1 file. |
| `kardamom_sealer_window_exact` | 1 when the egress keeps only its replay window, at any posted head. |
| `kardamom_sealer_decision_version` | The decision version of the open block. |
| `kardamom_sealer_feature_state{feature}` | Features 3, 4 and 5: 0 no activation held, 1 held and off, 2 on. |
| `kardamom_sealer_feature_activation_ms{feature}` | The time of the newest held activation of the feature, 0 when none. |
| `kardamom_sealer_feature_unsupported_total` | The activations of feature 5 on a member with the retained file. The member never stores them. |
| `kardamom_sealer_snapshot_bytes`, `kardamom_sealer_snapshot_version` | The size and the version of the newest snapshot that the member took or restored. |
| `kardamom_sealer_ref_refused_total` | The references of record type 0 that the carry window refused with egress kind 18. |
| `kardamom_sealer_full_ref_below_v5_total` | The full references (record type 7) that the member ordered below decision version 5. |
| `kardamom_sealer_feed_state` | The member feed: 0 off, 1 idle, 2 walking, 3 live, 4 gapped. |

The admin thread reads these series from the status of the member and from the Aeron counters in the CnC file of its media driver. A series of an Aeron counter that does not exist is absent.

| Metric | Meaning |
| --- | --- |
| `kardamom_sealer_append_position` | The position that the archive recorded of the cluster log on this member. |
| `kardamom_sealer_commit_position` | The log position that a quorum appended. Append minus commit is the wait for the quorum. |
| `kardamom_sealer_service_position` | The log position that the service thread applied. Commit minus service is the wait for the service thread. |
| `kardamom_sealer_consensus_max_cycle_seconds` | The longest duty cycle of the consensus module. |
| `kardamom_sealer_consensus_cycle_threshold_exceeded_total` | The duty cycles of the consensus module above the cycle threshold. |
| `kardamom_sealer_service_max_cycle_seconds` | The longest duty cycle of the clustered service. |
| `kardamom_sealer_service_cycle_threshold_exceeded_total` | The duty cycles of the clustered service above the cycle threshold. |
| `kardamom_sealer_archive_recorder_max_write_seconds` | The longest write of the archive recorder. |
| `kardamom_sealer_archive_recorder_write_seconds_total` | The time of the archive recorder in writes. |
| `kardamom_sealer_archive_recorder_write_bytes_total` | The bytes that the archive recorder wrote. |
| `kardamom_sealer_driver_errors_total` | The errors that the media driver logged. |
| `kardamom_sealer_driver_naks_sent_total` | The NAK messages that the media driver sent. |
| `kardamom_sealer_driver_naks_received_total` | The NAK messages that the media driver received. |
| `kardamom_sealer_driver_retransmits_total` | The retransmits that the media driver sent. |
| `kardamom_sealer_driver_short_sends_total` | The sends that the socket took only in part. |
| `kardamom_sealer_driver_sender_backpressure_total` | The back-pressure events of all network publications of the driver. |

### Ingress cluster status

The ingress reads the status frame of the sealer and exports it on port 9006. The frame carries the posted head, the sealed head,
the replay retention of the sealer, and the record-lag guard. See [l1-data-path.md](l1-data-path.md).

| Metric | Meaning |
| --- | --- |
| `kardamom_ingress_cluster_posted_head` | The last L2 block posted to L1. This is the block that `safe` names. |
| `kardamom_ingress_cluster_sealed_head` | The last sealed block. |
| `kardamom_ingress_cluster_retained_frames` | The egress frames that the sealer keeps for replay. The count is above the retention window while unposted blocks hold it there. |
| `kardamom_ingress_cluster_floor_block` | The oldest boundary block that the sealer still keeps. This is the replay floor. |
| `kardamom_ingress_cluster_recorded_head` | The best recorded cursor of the executors: the highest canonical index that one executor or more recorded. `-1` while no executor sent a cursor, and for a status frame of a sealer that sends no record-lag tail. |
| `kardamom_ingress_cluster_record_lag` | The canonical records past the best recorded cursor when the status frame arrived. The record-lag guard compares this value with its budget. `0` while no executor sent a cursor, because the guard then refuses nothing. |
| `kardamom_ingress_tx_rejected_total{reason="paused"}` | Submits that a paused ingress refused. |
| `kardamom_ingress_tx_rejected_total{reason="too-large"}` | Submits above the size cap, `--max-tx-bytes`. |
| `kardamom_ingress_tx_rejected_total{reason="nonce-too-low"}` | With `--ack validated`, submits with a nonce below the account view of the sender. |
| `kardamom_ingress_tx_rejected_total{reason="txpool-full"}` | With `--ack validated`, submits whose `tx_data` offer failed. |
| `kardamom_ingress_seal_latency_seconds` | A histogram: the time from the `tx_data` publish of a transaction to the egress frame that gives it a canonical index. Only the ingress that published the transaction records it. A seal later than 180 s gives no sample. The buckets are the shared service buckets of 100 µs to 5 s. Above 10 ms their bounds are 25, 50, 100, 250 and 500 ms, then 1, 2.5 and 5 s. |
| `kardamom_ingress_seal_stamps_expired_total` | The publish stamps that no seal matched within 180 s. Most are of refused transactions. A non-zero rate under load means that the seal latency histogram undercounts the tail. |
| `kardamom_ingress_seal_stamps_dropped_total` | The publish stamps that the submit path dropped because the channel to the seal clock (65,536 stamps) was full. Each one is a seal with no latency sample. |
| `kardamom_ingress_execute_latency_seconds` | A histogram: the time from the `tx_data` publish of a transaction to the first executor receipt for it. Only the ingress that published the transaction records it. A receipt later than 180 s gives no sample. The buckets are the shared service buckets. |
| `kardamom_ingress_execute_stamps_expired_total` | The publish stamps that no receipt matched within 180 s. A non-zero rate under load means that the execute latency histogram undercounts the tail. |
| `kardamom_ingress_execute_stamps_dropped_total` | The publish stamps that the submit path dropped because the channel to the executed clock (65,536 stamps) was full. |
| `kardamom_ingress_bus_lagged_total{bus}` | The items that a stream bus of the ingress (32,768 items) dropped because its watcher fell behind. `bus` is `tx_receipts`, `tx_verdicts`, `tx_errors`, `block_boundaries`, `quorum_watermark` or `local_fsync_watermark`. A dropped verdict leaves its submission to the ack timeout. A dropped receipt or error does not release its waiter, so that client waits until the pending timeout (30 s). A non-zero `tx_receipts` rate explains a 30 s tail in the submit latency. |

- The sealed head minus the posted head is the DA lag. The sealer refuses new transactions when it passes the DA-lag budget.
- The record lag is the canonical count minus the best recorded cursor plus one. The ingress takes the count from the records and boundaries that it observed before the status frame. The sealer refuses user transactions when the record lag passes `recordLagBudget`. A flat recorded head while the lag grows means that no executor records.

### Ingress pipeline

The ingress records these names of the pipeline metrics (see "Pipeline metrics"). A submit records its steps once for each request.

| Name | Metric | Meaning |
| --- | --- | --- |
| `ingress.submit` | step | The whole submit on the server, from the call to the answer, on each return path. With the receipt wait (no ack at admission), it includes the wait for the receipt. |
| `ingress.validate` | step | The shared head of the submit: the shed, drain, pause and rate checks, the decode, and the sender recovery. It includes `ingress.recover`. |
| `ingress.recover` | step | The sender recovery of one submit: the wait for a slot in the verifier intake, the batch window, and the recovery. |
| `ingress.publish_round_trip` | step | The `tx_data` publish: the hop to the blocking pool and the round trip to the Aeron thread up to its ack. |
| `ingress.recover_batch` | step | The recovery of one verifier batch on the blocking pool, up to the end of its last worker. |
| `ingress.recover_batch` | batch | The requests in each verifier batch. |
| `ingress.recover_intake` | queue | The requests that wait in the verifier intake after the flush task takes a batch. The intake holds four times the batch depth; a full intake makes a submit wait. |
| `ingress.ack_waiters` | queue | The submissions that wait for the verdict of the sequencer (ack at admission). The shed check counts them with `kardamom_ingress_queue_depth`. |
| `ingress.watermark` | loop | The cluster egress thread. Idle while it waits for an egress event (the receive and the decode), busy while it handles the event. |
| `ingress.receipt_pump` | loop | The `tx_receipts` pump task. Idle while it waits for a frame, busy while it applies the rows and fans out the receipts. |
| `ingress.receipt_batch` | batch | The receipts in each `tx_receipts` batch frame. A boundary-only frame adds no sample. |

- A high `ingress.recover` with a low `ingress.recover_batch` step means that submits wait for the intake or the batch window, not for the recovery.
- A busy share of `ingress.watermark` near 1 means that the egress frames queue in the cluster client before the thread reads them.

### Aeron thread

Each process has one Aeron thread. It carries the publishes and the subscriptions of every service in the process. The `service` label tells the processes apart.

| Name | Metric | Meaning |
| --- | --- | --- |
| `log.aeron` | loop | One pass: the drain of the command queue, one retry of each queued publish, and one poll of each subscription. Busy when the pass handled a command or polled a fragment. Blocked when the pass did no work and a refused offer waits in the retry queue. Idle otherwise. The wait for the next command is blocked while a refused offer waits, idle otherwise. |
| `log.aeron_commands` | queue | The commands that wait for the thread at the start of a pass: publishes, opens and closes from all callers. |
| `log.aeron_pending` | queue | The publishes in the retry queue after a pass: offers that Aeron refused, and the frames behind them on the same publication. |

- `kardamom_log_offer_retries_total` counts the offers that Aeron refused with a transient status (`BACK_PRESSURED`, `NOT_CONNECTED`, `ADMIN_ACTION`) and that stay queued for the next pass. A pass counts at most one refusal for each publication.
- A frame that stays refused until its deadline fails. A best-effort frame then counts in `kardamom_log_best_effort_dropped_total`.

### Receipt cache and lookups

`kardamom_cache_lookups_total{layer,outcome}` counts the cache lookups.

| Layer | Outcomes |
| --- | --- |
| `receipt` | `hit`, `state_hit`, `miss`, `shed`, `state_error` |
| `live` | `hit`, `miss` |
| `redis` | `hit`, `miss`, `error`, `timeout` |

- On the `receipt` layer, a lookup first reads the receipt cache of the ingress. That read is a `hit`.
- A miss asks an executor for the receipt. The outcome is `state_hit` when the executor has it, `miss` when it has not,
  and `state_error` when the query fails.
- A client that is over its rate limit gets `shed`, and the ingress answers `null` without a query.
- The receipt cache keeps the newest 131072 receipts and evicts the oldest first.
- `kardamom_cache_reconnects_total` counts the Redis reconnects that a failed command started. One failure burst starts one reconnect.
- `kardamom_cache_live_evictions_total` counts the entries that the local account layer removed, for the TTL or for the capacity. A high rate means that the layer is too small for the active accounts.
- `kardamom_cache_reconnects_total` counts the reconnects that a failed Redis command starts. One failure burst starts one reconnect.
  The readers skip the cache while the reconnect runs.
- `kardamom_cache_live_evictions_total` counts the entries that the `live` layer removes, for the TTL or the capacity.
  A high rate with many `live` misses means that the layer is too small for the active accounts.

### Transaction canary

All canary metrics start with `kardamom_canary_`. The dashboard is `kardamom-canary`. The canary uses the chain as a user does. See "Transaction canary" in [failure-modes.md](failure-modes.md#transaction-canary).

| Metric | Meaning |
| --- | --- |
| `probe_total{probe,endpoint,outcome}` | Probe runs. `receipt_status_0` is a revert: the nonce is used. `skipped` is a transaction that the chain skipped: the nonce is free. A failure outcome adds one detail label: `code` (`rpc_error`), `reason` (`rejected`), `stage` (`timeout`) or `field` (`fee_mismatch`). |
| `stage_seconds{probe,endpoint,stage}` | Stage latencies. `transfer`: `submit` (submit to hash), `offered`, `sealed`, `executed` and `receipt` (hash to each). `contract` and the market probes: `receipt` (submit to receipt); `contract` also `read`. `safe`: `safe` (receipt to the safe head). `deposit`: `l1_inclusion`, `l1_finality`, `l2_credit`. The buckets go from 5 ms to one hour. |
| `last_success_timestamp_seconds{probe}` | The unix time of the last success. |
| `balance_wei{layer,account}` | The balance of each canary account. |
| `balance_floor_wei{layer}` | The balance under which an account is unfunded. |
| `account_stalled{account}`, `account_stalled_nonce{account}` | 1, and the nonce, while a ring account holds a transaction that the canary cannot resolve. |
| `feed_gaps_total{kind}` | Status feed gaps: `lagged`, `disconnect`, or `missing_<stage>` for a stage that never came though the receipt did. A gap is not a transaction failure. |
| `feed_connected` | 1 while the status feed session is open. |
| `pool_reserve{asset}`, `pool_price_eth` | The canary pool's reserves (`eth`, `kca`, in wei) and its price of one KCA in ETH, after the canary's last call. |
| `pool_external_total` | Pool activity of another account between two canary calls. It is not a fault. |
| `rwa_supply_tokens` | The test RWA token's total supply. |
| `topups_total{outcome}` | Top-ups of ring accounts from the first one. |

### Notifier

All notifier metrics start with `kardamom_notifier_`. The dashboard is `kardamom-notifier`.
See [tx-status-events.md](tx-status-events.md) for the feed and the webhooks.

| Metric | Meaning |
| --- | --- |
| `events_total{stage}` | Status events that the ring stored. |
| `duplicates_total` | Events dropped because the transaction already held the stage. |
| `unresolved_errors_total` | `tx_errors` records that the ring could not join to a hash. |
| `evicted_total` | Events that the ring evicted by age or by count. |
| `ring_events`, `ring_transactions` | Events and transactions in the ring. |
| `ws_subscriptions` | Open WebSocket subscriptions. |
| `ws_events_total` | Events sent to WebSocket subscribers. |
| `ws_lagged_total` | Lag markers sent to slow WebSocket subscribers. |
| `webhook_subscriptions{owned}` | Webhook subscriptions that this instance holds. |
| `webhook_feed_lagged_total` | Live events that the webhook fan-out missed. |
| `webhook_queue_full_total{subscription}` | Events dropped by a full worker queue. |
| `outbox_appended_total{subscription}` | Events appended to an outbox. |
| `outbox_delivered_total{subscription,outcome}` | Events finished by a delivery loop. The outcomes are `delivered` and `gave_up`. |
| `outbox_backlog_bytes{subscription}` | Bytes of an outbox that are not delivered. |
| `webhook_attempts_total{outcome}` | Webhook POST attempts. The outcomes are `ok`, `status`, and `error`. |
| `webhook_delivery_seconds` | Histogram of the time from event arrival to delivery. |

### Egress

The egress (`kardamom-egress`) exports these metrics on `--metrics-addr` (`KARDAMOM_METRICS_ADDR`, default `127.0.0.1:9013`). The cluster job binds `0.0.0.0:9013` and registers it as the Consul service `kardamom-egress-metrics`. Every metric carries `host_id` (`egress-<alloc index>` in the cluster job).

| Metric | Meaning |
| --- | --- |
| `kardamom_egress_copy_mismatch_total` | Copies of a canonical frame whose bytes differ from the first copy. The egress does not stop. A difference means that one executor executed other bytes than the others: the validator decides which copy is wrong. |
| `kardamom_egress_waiting_dropped_total` | Times that every copy missed a frame and the merge dropped the frames that waited behind it. |
| `kardamom_egress_ref_frame_max_index` | R: the highest canonical index of a stored transaction reference (`RT_TXREF`). 0 when the store holds none. |
| `kardamom_egress_ref_frame_max_block` | The block of R. 0 when the store holds none. |

- Any increment of `kardamom_egress_copy_mismatch_total` needs an operator.
- R is durable. The store keeps it in the side file `ref-mark` of its directory (16 bytes: the index, then the block, little endian), and reads it at start. A restart does not lower R.
  - The store writes the mark of a batch before the frames of the batch. After a crash, R is at or above every reference that the segments hold.
  - A start at the floor of a peer keeps the higher of the two marks. The peer sends its mark in the headers `x-egress-ref-mark-index` and `x-egress-ref-mark-block` of its `410` answer.
  - R can stay above the stored references. That only delays the move of a reader to the egress.
  - A corrupt or hostile mark of a peer, for example `u64::MAX`, raises R for good: R never goes down. To recover, stop the egress, then copy `ref-mark` from the store directory of a healthy peer, then start the egress. Delete `ref-mark` only when the store holds no `RT_TXREF` frame.
- A reader below R can meet a reference in the egress range that it cannot join. The readers after the executors export their cursor on the egress: `kardamom_batcher_egress_cursor_index` and `kardamom_validator_egress_cursor_index`, the canonical index of the newest record that the reader took. A reader is past every reference when its cursor is above the highest R of all egress instances.
- The egress answers `GET /health` on its HTTP port (`:8548` in the cluster job). The Consul check of the service `kardamom-egress` reads it.

### State mirror

The state mirror exports `kardamom_state_mirror_serving`, `_batches_applied_total`, `_producer_disagreement_total`,
`_head_tx_idx`, `_rebuilds_total`, `_rebuild_seconds`, `_write_retries_total`, and `_wait_replica_zero_total`.

The mirror writes the waiting receipt batches as a group. It records these [pipeline metrics](#pipeline-metrics), once per group:

| Name | Kind | Meaning |
| --- | --- | --- |
| `state_mirror.group` | step | One whole group: the write with its retries, the head and the replica wait. |
| `state_mirror.write` | step | The write of one group: a `SCRIPT LOAD`, then one pipeline with the rows and the receipts of every batch. |
| `state_mirror.head_set` | step | The publish of the head. |
| `state_mirror.wait_replica` | step | The Redis `WAIT` for a replica. |
| `state_mirror.group_batches` | batch | The receipt batches in one group. |
| `state_mirror.group_receipts` | batch | The receipts in one group. |

### Validator replica checks

Every executor replica publishes its own BAL on `tx_bal` and its own receipts on `tx_receipts`.
The validator compares the BAL and the receipts of every replica with its own re-execution.
The Aeron session id of the publication names the replica.

| Metric | Meaning |
|---|---|
| `validator_replica_divergence_total{replica, check}` | Proven divergences, once for each session a divergence names. `replica` is the session id. `check` is `bal`, `receipt`, or `rows`. |
| `validator_replica_results_checked_total{check}` | Replica results that matched the re-execution. With N replicas, this grows about N times as fast as `validator_blocks_verified_total` (`check="bal"`). |
| `validator_replica_results_unchecked_total{check, reason}` | Replica results that do not count as checked. Not a fault. |

- The `reason` label of the unchecked counter:
  - `late`: the result arrived below the check window (64 blocks for a BAL, 4096 canonical records for a receipt), or its key has no checked result left.
  - `repeat`: the session already published the same result for the key.
  - `bound`: the key already holds 8 distinct results, or the result already names 32 sessions.
  - `evicted`: the buffer was full. The highest key goes first.
  - `ahead`: the key is more than the reach above the cursor (2^20 blocks, 2^32 records).
- Steady growth of `late` means a replica lags the validator by more than the window. Growth of `ahead` means a publisher sends wrong keys.
- `validator_rows_verified_total` and `validator_rows_unverified_total` count one replica batch each.
  Rows that arrive after the validator passed their position count as unverified.
- The executor logs the session ids at start: `tx_bal publication open` and `tx_receipts publication open`, with the field `session`.
  With discovery, the publisher record in the catalog carries the same id in its `session_id` meta.

## Log lines for diagnosis

Some services have no metrics for their key events. Their log lines are the diagnosis tool.

### Sealer

The sealer writes to stdout. Each line starts with a UTC instant.
The state machine runs on each member and on replay, so each member prints the lines of the log events.

| Line | Meaning |
| --- | --- |
| `cluster SESSION open memberId=M session=N` | A client session opened. |
| `cluster SESSION close memberId=M session=N reason=R` | A client session closed, with the close reason. A client that the cluster dropped offers into nothing. This line is the record of when and why. |
| `cluster TERM memberId=M leadershipTermId=T leaderMemberId=L logPosition=P role=R block=B` | A leadership term began. It gives the order of the elections. |
| `cluster boundary-clock TICK memberId=M block=B role=R` | The boundary clock runs. The sealer prints it every 30 ticks. A stall with a leader and no new `TICK` line is a dead clock. A stall with `TICK` lines is a block that does not reach the consumers. |
| `cluster PAST-DEADLINE memberId=M nonce=N maxInclusionBlock=D atBlock=B totalPastDeadline=C` | The sealer refused an offer whose inclusion deadline passed. It prints at count 1, 2, 4, 8, and so on. |
| `cluster WINDOW-FULL memberId=M nonce=N windowSize=S capacity=C totalWindowFull=T` | The dedup window had no room for an offer. The sealer prints it at powers of two. The sequencer republishes the offer. |
| `cluster REPLAY memberId=M session=S from=(I,B) SKEWED block B spans (L,U)` | The sealer refused a replay request. The index and the block do not name one point of the stream. The consumer takes its repair path. |
| `cluster VOID-VOTE memberId=M voter=V index=I result=R votes=N/T` | A consumer asked to void an entry. The line shows the vote result and the count of votes against the configured voters. |
| `cluster ORIGIN-GAP memberId=M offered=O expected=E totalOriginGaps=C` | The sealer refused an epoch for L1 block `O`, because it expects the epoch for block `E`. It prints at powers of two. The offering sequencer offers its epochs again from `E`. |
| `cluster LAUNCH REPAIR memberId=M attempt=A …` | A launch failed because a hard kill left a torn last fragment in the archive. The sealer truncates it and launches again. |

### Cluster client

- `cluster session: ingress re-pointed at the leader` is a log line of the Rust cluster adapter.
  A service prints it when its ingress session moves to a new leader.
- The cluster adapter also watches the egress. A session with no egress for 10 s resets.
  The window doubles after each reset that did not help, up to 60 s.

### Join timeout

An executor, a validator, or a batcher that cannot join a `TxRef` to its envelope logs this warning:

```text
join timeout: TxRef has no envelope on tx_data (archive refetch exhausted); aborting
```

The line has the fields `sequencer_id`, `session_id`, `tx_data_position`, `timeout_ms`, and `every_archive_refused`.

- `every_archive_refused=true`: each archive refused the range. The data is gone. A restart meets the same entry again.
  A consumer with a voter id asks the sealer to void the entry.
- `every_archive_refused=false`: an archive was not reachable. A restart can still recover.

## Bench and profiling

### Flamegraph and CPU profile: the in-process harness

`kardamom-bench-harness` runs the bench against an in-process ingress stand-in.

- The stand-in is a real ingress: batched secp256k1 recovery, sender routing, JSON-RPC framing, and parked-receipt release.
- It uses in-memory channels and a fake executor that answers each submitted transaction with a success receipt.
- No Aeron media driver, sequencer, executor, or sealer runs. The recording stays on the dispatch window.

```sh
cargo build --release --bin kardamom-bench-harness

./target/release/kardamom-bench-harness \
  --timeout 10s --concurrency 128 --max-in-flight 30 \
  --pprof-out /tmp/cpu.svg \
  transfers

open /tmp/cpu.svg
```

- `--pprof-out` samples the on-CPU time at 999 Hz with [`pprof-rs`](https://crates.io/crates/pprof).
  - The sampling covers the dispatch window only.
  - The report keeps the stacks that contain a `kardamom_ingress::*` frame.
    The harness runs the ingress and the bench client on one runtime. The filter removes the client work.
  - The harness logs the number of kept and dropped samples.
- `--flame-out` (default `flame.svg`) records `tracing-flame` spans over the same window.
  The stand-in emits no `tracing` spans, so the SVG is skipped when it is empty. The harness logs a warning.
  Use the `pprof` output for this stand-in.
- `pprof-rs` resolves the symbols in the process. You do not need `dsymutil`.
- Neither output covers the presigning, the warmup queue, the server start, or the shutdown.
- The harness supports only the `transfers` workflow. `calls` and `mixed` need `eth_call`, which the stand-in does not serve.

### Workflows and signer prefunding

A workload is a Rust `BenchWorkflow` in `crates/bench/src/workflows/`. Three workloads are built in.
Both binaries take them as subcommands.

| Subcommand | Workflow type | Content |
| --- | --- | --- |
| `transfers` | `TransfersWorkflow` | `eth_sendRawTransaction` only. It stresses the write path. |
| `calls` | `CallsWorkflow` | `eth_call` only, against the `PUSH1 0x42 ... RETURN` contract. The ingress does not serve `eth_call`. |
| `mixed` | `MixedWorkflow` | Transfers and calls at the ratio 1:4. |

- The bench flags that both binaries share: `--timeout` (default `10s`), `--concurrency` (default 16),
  `--txs-per-task` (default 10000), and `--max-in-flight` (default 5).
- `kardamom-bench` also takes `--rpc <URL>` and `--output <path>`.
- The signers come from the Anvil test mnemonic. The path is `m/44'/60'/0'/0/i` for `i = 0..concurrency`.
- `kardamom-bench-harness` needs no prefunding. The fake executor accepts each signed transaction.
- `kardamom-bench` needs a prefunded chain. The target chain must prefund the first N Anvil accounts.
  - `chains/dev.toml` prefunds account #0 only.
  - To bench transfers against a cluster that starts from it, add one `[[alloc]]` for each Anvil account `0..N`.
  - Seed the genesis of the executors from that file before you start the bench.

```toml
chain_id = 412346

[[alloc]]
address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"  # Anvil #0
balance = "1000000000000000000000"

# ... one for each signer 1..N-1 ...
```

### Custom workloads

Implement `BenchWorkflow` for your own type. Drive it with the generic `Benchmark<W>` or `Harness<W>`.
The example `crates/bench/examples/custom_workflow.rs` benches `eth_blockNumber` from outside the crate.

```sh
cargo run --release --example custom_workflow -p kardamom-bench
```

`cargo run --example gen_mnemonic -p kardamom-bench` prints a new BIP-39 phrase for your own signer set.

#### Workflow API

`BenchWorkflow::prepare` returns `Prepared<Item> { warmup, main }`:

- `warmup: Vec<Item>` is one queue. The harness drains it one request at a time, without metering, with the flame and pprof gates off.
  - `--timeout` bounds it.
  - Use it to warm the code paths and the buffers before the metered window.
  - The built-in workflows make `WARMUP_PER_TASK = 100` items for each task.
- `main: Vec<Vec<Item>>` is the metered work for each task (`n_tasks` by `txs_per_task`).
  The harness dispatches it concurrently in the recording window.

A workflow that keeps state across the phases must lay out its warmup queue to match.
Transfers use nonces. `TransfersWorkflow::prepare` presigns the warmup round-robin across the signers for the nonces `0..WARMUP`.
It then presigns the main work for each signer from the nonce `WARMUP`.
