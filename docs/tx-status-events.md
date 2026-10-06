# Transaction status events

The notifier tells a client how far a transaction has gone through the pipeline. It serves a
WebSocket feed and webhooks. The service is `kardamom-notifier`. It runs apart from the ingress, on its own port.

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
- `executed` with `status` `0` is a revert, a halt, or a skipped transaction. A skipped transaction has `gasUsed` `0` in its receipt, and it did not happen.
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
  - A new subscriber gets the history of the ring: by default the last 10 minutes, and at most 1000000 events.
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

## Webhooks

A webhook posts each matching event to a URL that you give. Delivery is at least once.

### Register

`POST /webhooks` on the notifier port.

```json
{"url":"https://example.org/hook","filter":{"sender":"0x1111111111111111111111111111111111111111"},"secret":"a-long-random-string"}
```

- `url` must be an `http` or `https` URL. `secret` must not be empty. `filter` is the same object as for the WebSocket feed.
- An unknown key in the body is an error. The body can have at most 64 KiB.
- The answer is `201` with the subscription `id` and the `owner`:

```json
{"id":"0x…","owner":0}
```

- `id` is the keccak256 hash of the request. The same request gives the same `id`. A repeated registration is the same subscription.
- `owner` is the index of the instance that delivers the subscription.
- A bad request gets `400` with `{"error":"…"}`. A notifier that is shutting down answers `503`.
- The notifier stores each subscription, with its secret, in a file in its directory. A restart loads the subscriptions again.
- The API has no call to list or remove a subscription.

### Delivery

For each event, the notifier sends an HTTP `POST` to the URL. The body is the event JSON, as in the WebSocket feed. The content type is `application/json`.

| Header | Value |
|---|---|
| `X-Kardamom-Signature` | `sha256=` and the hex HMAC-SHA256 of the body bytes, with the `secret` as the key. |
| `X-Kardamom-Idempotency-Key` | `<tx_hash>:<stage>`, for example `0xab…:sealed`. |
| `X-Kardamom-Subscription` | The subscription `id`. |

- Answer with any 2xx status to confirm. Any other answer, and a timeout, is a failed attempt.
- Verify the signature on the raw body, before you parse it. Example in Python:

```python
import hashlib
import hmac

def verify(secret: bytes, body: bytes, header: str) -> bool:
    expected = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)
```

- Delivery is at least once. The same event can arrive more than once, for example after a crash of the notifier.
  - Use the idempotency key to find a repeat. A transaction has at most one event for each stage.
- Events of one subscription go out one at a time, in the order of its outbox.

### Retries and give-up

- The notifier makes up to 10 attempts for each event.
- The wait after a failed attempt doubles from 1 second and stops at 60 seconds: 1, 2, 4, 8, 16, 32, 60, 60, 60 seconds.
- Each attempt has a timeout of 5 seconds (`KARDAMOM_NOTIFIER_WEBHOOK_TIMEOUT_MS`).
- After the 10th failure, the notifier gives up on that event and goes on to the next one. It counts the event as `gave_up`. A dead endpoint does not block the events behind it for ever.
- A notifier that stops during a retry keeps the event. It sends the event again at the next start.

### Outbox

- Each subscription has an outbox file and a cursor file in the notifier directory. The notifier appends each matching live event to the outbox, then delivers from the cursor.
- The cursor is stored after every 64 deliveries and when the loop is idle. A crash can repeat up to that many events.
- If every event is delivered and the outbox is at least `KARDAMOM_NOTIFIER_OUTBOX_RETAIN_BYTES` long, the notifier cuts it to zero.
- An event enters the outbox only if the owner instance is running and its queue has room.
  - The queue of one subscription has room for `KARDAMOM_NOTIFIER_FEED_BUFFER` events. If it is full, the notifier drops the event. The drop shows in `kardamom_notifier_webhook_queue_full_total`.
  - An event that arrives while the owner instance is down is not in its outbox.

## Sharding and instances

- Run one or more instances. Each instance reads the three streams itself and keeps its own ring. A WebSocket client can connect to any instance.
- Webhook subscriptions are sharded. The owner of a subscription is the instance with the highest rendezvous hash of the subscription `id`. Every instance computes the same owner from the instance count.
  - Only the owner delivers.
  - A change of the instance count moves only the subscriptions whose owner changes.
- A registration goes to one instance. That instance stores it and forwards it to every peer in `KARDAMOM_NOTIFIER_PEERS`, so that a peer can take over a shard.
  - The forward is best effort: up to 3 attempts, one second apart.
  - A peer that gets a forwarded registration stores it. It does not forward it again.
  - The peer list can include the instance itself. That forward is a repeat of the same subscription, and it changes nothing.
- Set `KARDAMOM_NOTIFIER_INSTANCE_INDEX` (from 0) and `KARDAMOM_NOTIFIER_INSTANCE_COUNT` on each instance. The index must be below the count.
- The deploy job `notifier` runs 2 instances, one on each ingress node. See `deploy/cluster/nomad/notifier.nomad.hcl`.

## Configuration

Each setting is a flag or an environment variable. The flag overrides the variable.

| Flag | Environment variable | Default | Meaning |
|---|---|---|---|
| `--bind` | `KARDAMOM_NOTIFIER_BIND` | `127.0.0.1:8547` | The address of the WebSocket feed and `POST /webhooks`. |
| `--max-connections` | `KARDAMOM_NOTIFIER_MAX_CONNECTIONS` | `10000` | The connection limit of the listener. |
| `--ring-minutes` | `KARDAMOM_NOTIFIER_RING_MINUTES` | `10` | The age bound of the ring, in minutes. |
| `--ring-max-events` | `KARDAMOM_NOTIFIER_RING_MAX_EVENTS` | `1000000` | The count bound of the ring. An event takes about 300 bytes, so the default is about 300 MB. |
| `--feed-buffer` | `KARDAMOM_NOTIFIER_FEED_BUFFER` | `65536` | The live buffer of each subscriber. It is also the queue of each webhook subscription and the ingest queue. |
| `--replay-page` | `KARDAMOM_NOTIFIER_REPLAY_PAGE` | `1000` | The number of events in one replay page. |
| `--dir` | `KARDAMOM_NOTIFIER_DIR` | `/opt/kardamom/notifier` | The directory of the subscriptions, outboxes and cursors. |
| `--outbox-retain-bytes` | `KARDAMOM_NOTIFIER_OUTBOX_RETAIN_BYTES` | `268435456` (256 MiB) | The size at which a fully delivered outbox is cut. |
| `--webhook-timeout-ms` | `KARDAMOM_NOTIFIER_WEBHOOK_TIMEOUT_MS` | `5000` | The timeout of one webhook POST. |
| `--instance-index` | `KARDAMOM_NOTIFIER_INSTANCE_INDEX` | `0` | The index of this instance. |
| `--instance-count` | `KARDAMOM_NOTIFIER_INSTANCE_COUNT` | `1` | The number of instances that share the subscriptions. |
| `--peers` | `KARDAMOM_NOTIFIER_PEERS` | none | The base URLs of the instances, separated by commas. |
| `--metrics-addr` | `KARDAMOM_METRICS_ADDR` | `127.0.0.1:9008` | The address of the Prometheus `/metrics` listener. |
| `--host-id` | `KARDAMOM_HOST_ID` | `local` | The host label on each metric. |
| `--log-config` | `KARDAMOM_LOG_CONFIG` | none | The `LogConfig` file with the Aeron streams. |
| `--aeron-dir` | none | none | The Aeron media-driver directory. |
| `--executor-count` | `KARDAMOM_EXECUTOR_COUNT` | from `channels.tx_receipts_executor_count` | The number of executors whose receipts the notifier reads. |

- The deploy sets `ring_minutes`, `ring_max_events` and `outbox_retain_bytes` as Nomad variables. The defaults are the same as in the table.
- The deploy runs the notifier with a 1024 MB memory limit.

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
| `webhook_subscriptions` | gauge | `owned` | Subscriptions that this instance holds. `owned` is `true` for the ones it delivers. |
| `webhook_feed_lagged_total` | counter | none | Live events that the webhook fan-out missed because it fell behind. |
| `webhook_queue_full_total` | counter | `subscription` | Live events that a full subscription queue dropped. |
| `outbox_appended_total` | counter | `subscription` | Events appended to an outbox. |
| `outbox_delivered_total` | counter | `subscription`, `outcome` | Events that delivery finished. `outcome` is `delivered` or `gave_up`. |
| `outbox_backlog_bytes` | gauge | `subscription` | Outbox bytes not yet delivered. |
| `webhook_attempts_total` | counter | `outcome` | POST attempts. `outcome` is `ok`, `status` (a non-2xx answer) or `error` (no answer). |
| `webhook_delivery_seconds` | histogram | none | Seconds from the arrival of an event at the notifier to its delivery. |

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
- Filters do not protect data. Every client can read the full feed.
- A webhook endpoint must be reachable from the notifier. The notifier does not retry for longer than the schedule above.
