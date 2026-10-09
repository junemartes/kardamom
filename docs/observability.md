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

### Labels and the sealer series

- Each binary takes `--host-id <STRING>` (env `KARDAMOM_HOST_ID`, default `local`).
  It sets the `host_id` label of each metric. `kardamom_obs::init` adds a `service` label.
- The `Kardamom Overview` dashboard has a `host` template variable. The service dashboards inherit it.
- The sealer is a Java service in an Aeron Cluster. It has no Prometheus endpoint.
  Each executor re-exports the output of the sealer as it decodes the cluster egress:
  - `kardamom_sealer_boundaries_emitted_total` counts the boundary frames.
  - `kardamom_sealer_block_number` is the block that the sealer declared.
  - The series are on the executor port with the `host_id` of the executor.
    They show the boundary stream as one executor sees it.
  - For the sealer itself, use the admin port and the log lines (see below).

### Cluster client metrics

Every service that drives a cluster session exports the state of that session under its own `service` label. These services are the sequencer, the executor, the ingress, the validator and the batcher.

| Metric | Meaning |
| --- | --- |
| `kardamom_cluster_client_connected` | 1 while the session is open. |
| `kardamom_cluster_client_leader_member_id` | The leader that the ingress publication points at. |
| `kardamom_cluster_client_leader_changes_total` | Redirects and new-leader events. |
| `kardamom_cluster_client_sessions_total` | Sessions opened. |

- A leader change rate above zero in steady state is an election.
- A client that stays disconnected finds no member that answers.
- These series observe the Raft set from its clients, not from inside the JVM.

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

- A dashboard queries `kardamom`-scoped metrics of its service.
- The test `crates/obs/tests/dashboards.rs` checks that each dashboard in `EXPECTED_DASHBOARDS` parses, uses schema 38,
  and queries only `kardamom`-scoped metrics.
  Add a dashboard to that list when you add the dashboard.

## Alerts

`deploy/alerts.yml` holds 17 Prometheus alert rules. The monitoring job loads them as `rule_files`.
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
| `KardamomL1FollowerLag` | critical | The newest `l1_blocks` record is more than 64 blocks (two finality steps) behind the finalized tip for 5 minutes. Runbook: [`l1_follower_lag`](runbooks/l1_follower_lag.md). |
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
  - `peer-checkpoint`: the node fell behind the retention window and repaired itself from a peer checkpoint.
  - `unrecoverable`: the node could not repair itself and waits for an operator.
  - See "Replay-window overrun" in [failure-modes.md](failure-modes.md).
  - The validator counter is `validator_resync_total`. A `peer-checkpoint` increment there means the validator did not verify the blocks up to the adopted checkpoint.
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
| `kardamom_batcher_last_post_age_seconds` | Age of the last `BatchPosted` block as L1 serves it. The batcher reads it from L1 every 10 seconds, not from its memory. It grows when the batcher stops posting and when the L1 endpoint hides its posts. |
| `kardamom_batcher_idle_flush_seconds` | The idle flush wait. The alert compares the post age with twice this value. |
| `kardamom_batcher_resume_failures_total` | Starts whose L1 read failed. The start retries in the process, so the counter stays scrapeable. |
| `kardamom_batcher_rebuilt_blocks_total` | Blocks rebuilt from references after the sealer refused a replay. |
| `kardamom_batcher_spool_dropped_total{reason}` | Spools dropped at start. `other-version`: a spool of another release. `unreadable`: a block file does not decode. `discontinuous`: the spool does not continue the confirmed cursor. The sealer serves the range again. One after a deploy is expected. Growth means the spool disk is bad. |
| `kardamom_batcher_feed_running` | 1 when the feed loop runs over the restored spool. The readiness rule needs it. |

### L1 sources

The da-watcher and the indexer export these metrics. See "Two L1 sources for the followers" in [l1-data-path.md](l1-data-path.md).

| Metric | Meaning |
| --- | --- |
| `kardamom_l1_source_disagreement_total` | Two sources answered differently for one block or one log query. Any increase means an endpoint lies. |
| `kardamom_l1_source_rotations_total{source,reason}` | Rotations of a source out of its set for the backoff. The reasons are `error`, `rate_limited`, and `disagreement`. |

### DA-watcher

| Metric | Meaning |
| --- | --- |
| `kardamom_da_watcher_tick_total{outcome}` | Loop ticks. The outcomes are `ok`, `chain_break`, `parse_error`, and `rpc_error`. |
| `kardamom_da_watcher_l1_finalized_block_number` | Newest finalized L1 block that the watcher saw. |
| `kardamom_da_watcher_epoch_origin_block_number` | Newest L1 block with a published epoch. The difference to the finalized block is the origin lag. |
| `kardamom_da_watcher_epochs_published_total` | Epochs published. One for each finalized L1 block. |
| `kardamom_da_watcher_deposits_detected_total` | Deposit publishes. A range that is retried after back-pressure counts again. |
| `kardamom_da_watcher_last_tick_unix_seconds` | Unix time of the last tick. The readiness rule uses it. |
| `kardamom_da_watcher_l1_confirmed_origin` | The L1 origin of the sealer, as the boundaries carry it: the last epoch that the sealer committed. The cursor file holds it. |
| `kardamom_da_watcher_epochs_unconfirmed` | Published epochs that no boundary confirmed yet. It stays near 0 while the sealer commits. At 2048, the watcher publishes no new epoch. |
| `kardamom_da_watcher_epochs_republished_total` | Epochs published again, because no boundary confirmed them within 30 s. |
| `kardamom_da_watcher_l1_cursor_persist_failures_total` | Failed writes of the L1 cursor file. A failure is not fatal: a restart publishes epochs again, and the sealer drops them. A growing count moves the restart point further back. |

- A `chain_break` outcome means a block did not descend from the block before it. The watcher halts at that block.
- The interop watcher exports `kardamom_da_watcher_remote_*` counters with the label `origin` (the peer chain id).

### Executor stream

Each executor exports these metrics for its executor stream (`exec_txs`). See "The executor stream" in [failure-modes.md](failure-modes.md#the-executor-stream-the-executor-records-what-it-joins).

| Metric | Meaning |
| --- | --- |
| `kardamom_executor_exec_stream_recorded_index` | The recorded cursor: the highest canonical index whose records the local archive has written. It never passes the recording position. It moves with the canonical order, also with no transaction load. A flat value on a chain that progresses means that the archive of the node takes no records. |
| `kardamom_executor_exec_stream_session_id` | The Aeron session id of the recorded publication. A restarted executor shows a new value. |
| `kardamom_executor_exec_stream_publish_blocked_ms_total` | Milliseconds that the publisher waited for the archive to take a record. The executor stalls while it grows. |

- The live publication counts its dropped records in `kardamom_log_best_effort_dropped_total{stream_id="1005"}`. A drop is normal while no consumer subscribes.

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

The epoch lane exports these metrics. See "Sequencer" in [failure-modes.md](failure-modes.md).

| Metric | Meaning |
| --- | --- |
| `kardamom_sequencer_l1_origin` | The highest L1 origin that a boundary carried: the last L1 block whose epoch the sealer ordered. The sealer accepts the epoch of the next block only. An operator resumes the da-watcher after this block. |
| `kardamom_sequencer_epochs_unconfirmed` | The epochs that the lane relayed, or took to relay, and that no boundary confirmed yet. The lane holds at most 4096. |
| `kardamom_sequencer_origin_gap_total` | The `ORIGIN_GAP` rejects that the sealer sent to this replica. Each one makes the lane offer its unconfirmed epochs again from the expected block. |

### Ingress cluster status

The ingress reads the status frame of the sealer and exports it on port 9006. The frame carries the posted head, the sealed head,
and the replay retention of the sealer. See [l1-data-path.md](l1-data-path.md).

| Metric | Meaning |
| --- | --- |
| `kardamom_ingress_cluster_posted_head` | The last L2 block posted to L1. This is the block that `safe` names. |
| `kardamom_ingress_cluster_sealed_head` | The last sealed block. |
| `kardamom_ingress_cluster_retained_frames` | The egress frames that the sealer keeps for replay. The count is above the retention window while unposted blocks hold it there. |
| `kardamom_ingress_cluster_floor_block` | The oldest boundary block that the sealer still keeps. This is the replay floor. |
| `kardamom_ingress_tx_rejected_total{reason="paused"}` | Submits that a paused ingress refused. |

- The sealed head minus the posted head is the DA lag. The sealer refuses new transactions when it passes the DA-lag budget.

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

### State mirror

The state mirror exports `kardamom_state_mirror_serving`, `_batches_applied_total`, `_producer_disagreement_total`,
`_head_tx_idx`, `_rebuilds_total`, `_rebuild_seconds`, `_write_retries_total`, and `_wait_replica_zero_total`.

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
