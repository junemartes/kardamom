# Transaction status events for clients

Status: designed, not built.

## 1. Problem

A client learns one thing about its transaction today: the receipt, when `eth_sendRawTransaction`
returns or when `kardamom_subscribeReceipts` delivers it. Between the submit and the receipt
there are three steps a trader wants to see: the transaction was accepted into the sequencer's
order (its nonce run is contiguous and it was offered to the sealer), the sealer gave it a
canonical index in a block, and the executor ran it. Each step is observable inside the
pipeline today, none is published to clients.

Constraints:

- The hot path (ingress, sequencer, sealer, executor) must not wait for a subscriber. A slow
  client costs the client, never the chain.
- Volume: the chain sustains 3,800 tx/s; three events per transaction is above 10,000 small
  events a second.
- Subscribers: many (one per connected trader), short-lived, each interested in its own
  transactions; and a few integrators who want every event by webhook.

## 2. What exists

| Step | Where it is observable | Published today |
|---|---|---|
| accepted by the sequencer | the sequencer's offer to the cluster ingress (stream 101), its metrics | no; rejections go to `tx_errors` (1015) |
| sealed (canonical index, block) | cluster egress (102): `RELAYED [index]`, `BOUNDARY` | to egress consumers only |
| executed | `tx_receipts` (1002): `ReceiptBatch` from every executor, deduplicated by the readers | yes, to the ingress's receipt feed |
| rejected | `tx_errors` (1015): expired, duplicate, contiguity, past deadline | yes, through `kardamom_subscribeReceipts` |

Two facts shape the design. First, every stream is an Aeron publication with
multi-destination cast: a new reader costs the publisher nothing, and a lagging reader
is paced as "fastest receiver" and recovers through refetch. `tx_receipts` and `tx_errors`
are safe to tap. Second, cluster egress is not free: every subscribed session gets a leader
unicast, a wedged session can hold the sealer's single service thread for up to a second
per frame, and each session costs retention and replay. Egress taps stay few and fixed.

The ingress already runs a WebSocket feed (`kardamom_subscribeReceipts`, with a sender
filter and `Lagged` markers) and an egress tap for its quorum watermark. The interop feed
(`crates/interop-feed`) defines the WebSocket DTO conventions.

## 3. Design

### 3.1 One stream of status events

A new Aeron stream `tx_status` (id 1018) carries one small record per step:

```
TxStatus {
    tx_hash:  B256,
    sender:   Address,
    nonce:    u64,
    stage:    Offered | Sealed | Executed | Rejected,
    lane:     u16,          // Offered
    index:    u64,          // Sealed: the canonical index
    block:    u64,          // Sealed, Executed
    status:   u8,           // Executed: receipt status; Rejected: the TxError kind
    at_ms:    u64,          // the publisher's clock, informational
}
```

About 80 bytes. The stage order is fixed per transaction; a reader that sees `Sealed` before
`Offered` (two publishers, two streams) orders by stage, not by arrival.

Who publishes what:

| Stage | Publisher | Cost on the hot path |
|---|---|---|
| Offered | the sequencer, when it offers a `RefOffer` to the cluster ingress | one non-blocking `offer` per transaction on a side publication; back pressure drops the event and counts it (`tx_status_dropped_total`) |
| Rejected | the sequencer, where it publishes `tx_errors` today | the same; `tx_errors` stays as is for the receipt feed |
| Sealed | the ingress's existing egress tap, which already decodes every `RELAYED` frame for the watermark | one `offer` per relayed record on the ingress's side publication; the ingress does not touch the sealer |
| Executed | derived by the notifier from `tx_receipts` | none |

The sequencer and the ingress publish the status stream with the same "best effort, never
block" rule the sequencer uses for `tx_errors`: the publication is a side channel, and the
receipt stream stays the truth. The `Sealed` publisher is the ingress and not the executor
because the executor's commit thread is the hot path's tail; the ingress decodes the egress
frames already and its watermark tap is the only egress session that exists for this purpose.

### 3.2 The notifier: fan-out off the hot path

A new service, `kardamom-notifier`, with `count = 2` behind the load balancer:

- It subscribes to `tx_status`, `tx_receipts` and `tx_errors` (multi-destination cast, no
  session on the sealer), deduplicates the executors' receipt copies, and derives `Executed`.
- It keeps a ring of the last `N` minutes of events per transaction hash and per sender in
  memory (a bounded map; 10,000 events a second for 10 minutes is a few hundred megabytes
  at 80 bytes, bounded by eviction), so a client that subscribes after the submit still gets
  the steps it missed.
- WebSocket: `kardamom_subscribeTxStatus(filter)` with `filter = { sender } | { tx_hash }`;
  on subscribe it replays the ring for the filter, then streams. A slow client gets a
  `Lagged` marker and the stream continues from the present, as the receipt feed does.
- Webhooks: a subscription is `{ url, filter, secret }` registered by `POST /webhooks`;
  the notifier posts each event as JSON with an HMAC signature header and an idempotency key
  (`tx_hash` + stage). Delivery is at least once with bounded retries (exponential, one
  minute cap, ten attempts); the subscriber deduplicates by the key.

Every notifier instance taps the streams itself, so there is no broker between the pipeline
and the fan-out. The streams are the queue: Aeron multi-destination cast delivers to every
instance, and the archive refetch covers a lagging instance. The load balancer spreads the
WebSocket clients; a client's session lives on one instance, which holds its filters.

### 3.3 Webhook durability

A WebSocket client that drops reconnects and replays the ring. A webhook subscriber that is
down for longer than the ring must still get its events, which needs a durable outbox.
Options:

| Option | Verdict |
|---|---|
| 1. A per-subscription append-only outbox on the notifier's disk, consumed by a delivery loop; subscriptions sharded across instances by consistent hashing of the subscription id | chosen: no new component; the outbox is a file per subscription; an instance loss moves its shard to the twin, which replays from the archive for the gap |
| 2. Redis Streams on the existing Redis | the deployed Redis is a cache without persistence by design; making it durable changes its role |
| 3. NATS JetStream or Kafka | a durable queue with consumer groups solves delivery cleanly, but is a new stateful cluster to run; worth it when webhook subscribers are many; not now |

Option 1 keeps the first version to two binaries and one stream. The outbox format is the
archive's length-prefixed rkyv record, so the tooling that reads archives reads outboxes.

### 3.4 What a client sees

```
{"tx_hash":"0x…","stage":"offered","lane":3,"at":"…"}
{"tx_hash":"0x…","stage":"sealed","index":128881,"block":4412,"at":"…"}
{"tx_hash":"0x…","stage":"executed","block":4412,"status":1,"at":"…"}
```

or `{"stage":"rejected","reason":"expired"}` in place of the last two. The stages are
monotonic per transaction; a client treats a missing earlier stage as implied.

## 4. Order and estimate

| Step | Work | Proof |
|---|---|---|
| 1 | `TxStatus` type and the `tx_status` stream in `channels.toml.tpl`; the sequencer publishes `Offered` and `Rejected` | a unit test on the sequencer's outbound; the load shard shows no change in receipt latency with the publication on |
| 2 | the ingress's egress tap publishes `Sealed` | the ingress's watermark test extended; the load shard again |
| 3 | `kardamom-notifier`: the taps, the ring, the WebSocket subscription; the job (count 2) and the load balancer route | a chain-semantics case: a client subscribes, sends, and sees the three stages in order within the receipt's latency |
| 4 | webhooks with the outbox and retries | a case with a subscriber that is down for the ring's length and gets every event once |
| 5 | dashboards: events per stage, dropped events, delivery lag, outbox depth | the monitoring job |

## 5. Open questions

1. Whether `Sealed` should carry the block's timestamp: the boundary follows the records,
   so the notifier would hold `Sealed` events until the boundary, which adds up to one tick
   (2 s) of delay. First version: no timestamp, no delay.
2. Authentication of WebSocket subscriptions by sender: today the receipt feed filters by
   sender without proof of ownership. The status feed inherits that; a signed challenge is
   a later step for both feeds.
3. The ring's size and the outbox retention as deploy values.
