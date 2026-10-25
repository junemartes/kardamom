# Transaction status events

The notifier tells a client how far a transaction has gone through the pipeline. It serves a
WebSocket feed. The service is `kardamom-notifier`. It runs apart from the ingress, on its own port.

The webhooks are removed (2026-10-25). Offline delivery to a server that is not connected comes later
through a task queue, such as SQS. The notifier keeps no state on disk.

- The feed is best effort. It tells you early. The receipt is the truth.
- The notifier is off the hot path. A slow client costs only that client.
- For the client JSON-RPC API, see [Client JSON-RPC API](json-rpc.md).

## Stages

A transaction goes through up to three stages. Or it ends at `rejected`.

| Stage | Published by | What it proves |
|---|---|---|
| `offered` | The sequencer | The sequencer accepted the transaction and offered it to the sealer. The order is not yet fixed. The sealer can still refuse it. |
| `sealed` | The ingress (the egress tap) | The sealer gave the transaction a canonical index. Its place in the order is final. |
| `executed` | The notifier, from `tx_receipts` | An executor ran the transaction and published a receipt. `status` is the receipt status. |
| `rejected` | The sequencer | The sequencer, or the sealer through the sequencer, did not order the transaction. It never gets a receipt. |

- The order for one transaction is `offered`, `sealed`, `executed`. Events can arrive in another order. Sort by stage, not by arrival.
- `executed` with `status` `0` is a revert, a halt, or a skipped transaction. A skipped transaction has `gasUsed` `0` in its receipt, and it did not run. After the activation of `FEATURE_SEALER_V3`, a skip at the chain nonce uses its nonce.
- A transaction that the ingress refuses (rate limit, bad signature, and so on) never enters the pipeline. It has no event.
- The stream is `tx_status` (stream id 1018). It is RAM only. It has no archive.
- Each executor publishes its own copy of a receipt, and each ingress replica publishes a `sealed`. The notifier keeps each stage of a transaction once.
  - If a client resubmits the same signed bytes, the stages that the ring already holds do not repeat while the ring holds them.

## WebSocket API

Connect to the notifier address with a WebSocket client. The default address is `127.0.0.1:8547`. The deploy uses port 8547 on each notifier instance.

| Method | Params | Result |
|---|---|---|
| `kardamom_subscribeTxStatus` | one `filter` object | A subscription. |
| `kardamom_unsubscribeTxStatus` | the subscription id | `true` |

- On subscribe, the notifier first replays the events in its ring that match the filter, oldest first. Then it streams live events.
  - The replay and the live stream do not lose or repeat an event.
  - A new subscriber gets the history of the ring: by default the last 10 minutes, and at most 1000000 events. On the notifier nodes (`KARDAMOM_NOTIFIER_NODES=on`), the ring keeps at most 250000 events.
- The `filter` is one of these:

| Filter | Selects |
|---|---|
| `{"all": true}` | Every event. `{"all": false}` is not valid. |
| `{"sender": "0x…"}` | The events of one sender. |
| `{"tx_hash": "0x…"}` | The events of one transaction. |

- The feed is public, as the blocks of the chain are. A filter narrows the feed. It does not protect it.
- A `sealed` event has no sender on the wire. The notifier fills the sender from an earlier `offered` event of the same hash.
  - If the `sealed` event arrives first, it has no sender. A `sender` filter does not select it. The `tx_hash` filter and the `all` filter do.
- Open the subscription:

```json
{"jsonrpc":"2.0","id":1,"method":"kardamom_subscribeTxStatus","params":[{"sender":"0x1111111111111111111111111111111111111111"}]}
```

- Each event arrives as a notification. The `result` field holds the event:

```json
{"jsonrpc":"2.0","method":"kardamom_subscribeTxStatus","params":{"subscription":"<id>","result":{"tx_hash":"0x…","sender":"0x1111111111111111111111111111111111111111","nonce":7,"stage":"executed","status":1}}}
```

### Event JSON

The keys are `snake_case`. An optional field that has no value is left out. It is never `null`.

| Field | Present | Meaning |
|---|---|---|
| `tx_hash` | always | The transaction hash. |
| `stage` | always | `offered`, `sealed`, `executed` or `rejected`. |
| `sender` | when known | The sender address. |
| `nonce` | when known | The transaction nonce. |
| `status` | `executed` | The receipt status: `1` success, `0` failure. |
| `reason` | `rejected` | A reason word. See the next table. |
| `expected_nonce` | `rejected`, for three reasons | The next nonce that the sequencer expects from the sender. |

| `reason` | `expected_nonce` | Meaning |
|---|---|---|
| `duplicated-tx` | set | The nonce is below the next nonce of the sender. |
| `evicted` | set | The sequencer shed the transaction under overload. Resubmit when the nonce is within the reorder window. |
| `expired` | set | The transaction waited on a nonce gap for longer than the sequencer lifetime. Resubmit when the gap is filled. |
| `past-deadline` | left out | The sealer refused the transaction because its block passed the inclusion deadline. Resubmit at once. |
| `fee-invalid` | left out | The tip rate is above the fee cap. Sign again. |
| `fee-too-low` | left out | The fee cap is below the base fee. Sign again with a higher cap. |
| `insufficient-funds` | left out | The balance does not cover the worst-case cost. |
| `da-lag` | left out | The sealer refused the transaction because the sealed head is too far past the posted head. Submit again after the batcher posts. |
| `record-lag` | left out | The sealer refused the transaction because no executor recorded the chain within the record-lag budget. Submit again after an executor records. The guard is off by default until a later release. |
| `sealer-nonce-ahead` | left out | The sealer nonce guard expects a nonce above the chain nonce after a skipped transaction. The sender is stuck until an operator acts. See [Failure modes](failure-modes.md). |
| `nonce-in-flight` | left out | The sealer refused the nonce, and an earlier transaction of the sender can still be in flight. Wait for its receipt, then submit again. |

- The reason words are the same as in the `txError` frames of `kardamom_subscribeReceipts`. See [Client JSON-RPC API](json-rpc.md#receipt-subscription).
- The sequencer fee gate gives `fee-invalid`, `fee-too-low` and `insufficient-funds`. The gate runs only when priority fees are on. See [Priority fees](priority-fees.md).

Example events of one transaction:

```json
{"tx_hash":"0xab…","sender":"0x1111111111111111111111111111111111111111","nonce":7,"stage":"offered"}
{"tx_hash":"0xab…","sender":"0x1111111111111111111111111111111111111111","nonce":7,"stage":"sealed"}
{"tx_hash":"0xab…","sender":"0x1111111111111111111111111111111111111111","nonce":7,"stage":"executed","status":1}
```

Example of a rejection:

```json
{"tx_hash":"0xcd…","sender":"0x1111111111111111111111111111111111111111","nonce":7,"stage":"rejected","reason":"duplicated-tx","expected_nonce":9}
```

### Lag marker

- Each subscriber has a buffer of 65536 events (`KARDAMOM_NOTIFIER_FEED_BUFFER`). The notifier never waits for a slow subscriber.
- A subscriber that falls further behind gets a lag marker in place of the dropped events:

```json
{"stage":"lagged","skipped":120}
```

- After the marker, the stream continues from the present.
- To fill the gap, read the receipt, or subscribe again. A new subscription replays the ring.

## Instances

- Run one or more instances. Each instance reads the three streams itself and keeps its own ring. A WebSocket client can connect to any instance.
- An instance keeps no state on disk and needs no peers. A change of the instance count only starts or stops instances.
- `KARDAMOM_NOTIFIER_INSTANCE_INDEX` (from 0) names the stream plane of each instance.
- The deploy job `notifier` runs 2 instances, one on each ingress node. With `KARDAMOM_NOTIFIER_NODES=on`, it runs `KARDAMOM_NOTIFIER_COUNT` instances, one on each node of role `notifier`. See `deploy/cluster/nomad/notifier.nomad.hcl`, and "The notifier nodes" in `deploy/cluster/README.md`.

## Configuration

Each setting is a flag or an environment variable. The flag overrides the variable.

| Flag | Environment variable | Default | Meaning |
|---|---|---|---|
| `--bind` | `KARDAMOM_NOTIFIER_BIND` | `127.0.0.1:8547` | The address of the WebSocket feed. |
| `--max-connections` | `KARDAMOM_NOTIFIER_MAX_CONNECTIONS` | `10000` | The connection limit of the listener. |
| `--ring-minutes` | `KARDAMOM_NOTIFIER_RING_MINUTES` | `10` | The age bound of the ring, in minutes. |
| `--ring-max-events` | `KARDAMOM_NOTIFIER_RING_MAX_EVENTS` | `1000000` | The count bound of the ring. An event takes about 300 bytes, so the default is about 300 MB. |
| `--feed-buffer` | `KARDAMOM_NOTIFIER_FEED_BUFFER` | `65536` | The live buffer of each subscriber. It is also the ingest queue. |
| `--replay-page` | `KARDAMOM_NOTIFIER_REPLAY_PAGE` | `1000` | The number of events in one replay page. |
| `--instance-index` | `KARDAMOM_NOTIFIER_INSTANCE_INDEX` | `0` | The index of this instance. It names its stream plane. |
| `--metrics-addr` | `KARDAMOM_METRICS_ADDR` | `127.0.0.1:9008` | The address of the Prometheus `/metrics` listener. |
| `--host-id` | `KARDAMOM_HOST_ID` | `local` | The host label on each metric. |
| `--log-config` | `KARDAMOM_LOG_CONFIG` | none | The `LogConfig` file with the Aeron streams. |
| `--aeron-dir` | none | none | The Aeron media-driver directory. |
| `--executor-count` | `KARDAMOM_EXECUTOR_COUNT` | from `channels.tx_receipts_executor_count` | The number of executors whose receipts the notifier reads. |

- The deploy sets `ring_minutes` and `ring_max_events` as Nomad variables. The defaults are the same as in the table.
- The deploy runs the notifier with a 1024 MB memory limit, or 512 MB on the notifier nodes. A full ring at the default count bound takes about 300 MB of it.

## Metrics

The notifier serves these metrics on `--metrics-addr`. All names start with `kardamom_notifier_`.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `events_total` | counter | `stage` | Events that the ring stored. |
| `duplicates_total` | counter | none | Events dropped because the transaction already held the stage. |
| `unresolved_errors_total` | counter | none | `tx_errors` records with no known transaction hash. The notifier cannot name the transaction. |
| `evicted_total` | counter | none | Events that the ring removed, by age or by count. |
| `ring_events` | gauge | none | Events in the ring. |
| `ring_transactions` | gauge | none | Transactions with an event in the ring. |
| `ws_subscriptions` | gauge | none | Open WebSocket subscriptions. |
| `ws_events_total` | counter | none | Events sent to WebSocket subscribers. |
| `ws_lagged_total` | counter | none | Lag markers sent. |

- The publishers count the drops of `tx_status` records in `kardamom_log_best_effort_dropped_total{stream_id="1018"}`.
- The Grafana dashboard is `deploy/grafana/provisioning/dashboards-json/kardamom-notifier.json`.

## Caveats

- `sealed` needs an egress channel on the ingress.
  - The ingress publishes `sealed` from its tap on the cluster egress. The tap runs only if the ingress has a cluster egress channel: the `--cluster-egress-endpoint` flag or `egress_channel` in `[cluster]`.
  - Without a channel, the ingress logs a warning and no `sealed` event exists.
  - If the ack policy needs a quorum, a tap that cannot start stops the ingress. In other cases it logs a warning.
- The stream is best effort. A publisher drops a status if the channel is back-pressured. An event can be missing. A missing `offered` or `sealed` is normal.
- The receipt is the truth. Use `eth_getTransactionReceipt` to confirm a result. Do not act on a missing event.
- A restart of an instance loses its ring. A new subscriber then gets no history from before the restart. The stream has no archive.
- A `tx_errors` record names a transaction by sender and nonce. The notifier needs an earlier event with that sender and nonce to find the hash. Without it, the error is dropped (`unresolved_errors_total`). The `rejected` status on `tx_status` carries the hash.
- The notifier has no event for a drop on `tx_drops`. When the sealer refuses a reference without the bytes of its transaction, the sequencer publishes only a drop. The egress status (`kardamom_getTransactionStatus`) then answers `dropped` with the reason `transaction dropped by the sealer, send it again` (code -32000). This status is provisional: safe to send again; the status can still become executed. The feed can still show `sealed` and `executed` for the copy of the twin.
- Filters do not protect data. Every client can read the full feed.
