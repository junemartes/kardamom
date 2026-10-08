# Client JSON-RPC API

The ingress serves the client JSON-RPC API to wallets and apps. This page lists the methods, the
receipt fields, the errors and the limits.

- Endpoint: one HTTP and WebSocket port. The `--jsonrpc-bind` flag sets it. The default is `127.0.0.1:8545`.
- The server is `jsonrpsee`. HTTP serves the request-response methods. WebSocket serves the same methods and the subscription.
- Health: `GET /health` on the same port answers `200 ok`. It answers `503 draining` from the start of a shutdown drain. A load balancer reads it.
- The method set is a subset of the Ethereum API, plus the `kardamom_*` methods.
- The notifier feed for transaction status is a separate service on a separate port. See [Transaction status events](tx-status-events.md).

## Methods

| Method | Params | Result |
|---|---|---|
| `eth_chainId` | none | The chain id as a hex quantity. The `--chain-id` flag sets it (default `1`). It must equal the executor chain id. |
| `eth_blockNumber` | none | The newest closed block number, as a hex quantity. It comes from the block boundaries on the receipt stream. It is `0x0` before the first boundary. |
| `eth_getBalance` | `address`, `block` | The balance in wei, as a hex quantity. |
| `eth_getTransactionCount` | `address`, `block` | The committed nonce of the account, as a hex quantity. |
| `eth_sendRawTransaction` | `bytes` | The transaction hash. The call waits until the receipt arrives. |
| `eth_getTransactionReceipt` | `hash` | The receipt, or `null` if the transaction is not committed. |
| `eth_feeHistory` | `blockCount`, `newestBlock`, `rewardPercentiles` (optional) | An Ethereum `FeeHistory` object. |
| `eth_maxPriorityFeePerGas` | none | A tip rate that a wallet can bid, in wei per gas. |
| `eth_gasPrice` | none | The next block base fee plus the suggested tip rate, in wei per gas. |
| `kardamom_sendRawTransactionAsync` | `bytes` | The transaction hash. The call returns when the transaction is published. |
| `kardamom_subscribeReceipts` | `senders` (optional) | A subscription. WebSocket only. See [Receipt subscription](#receipt-subscription). |
| `kardamom_blockNumberByTag` | `tag` | The block number that the tag names, as a hex quantity. See [Block tags](#block-tags). |
| `kardamom_chainStatus` | none | The chain status: the heads, the DA-lag budget, the live halts and the state of each service. See [Chain status](#chain-status). |

### Submitting a transaction

- `eth_sendRawTransaction` validates the transaction and publishes it. Then it waits.
  - It returns the hash when the receipt for `(sender, nonce)` has arrived and the ack policy gate has passed.
  - It holds the connection while it waits. Each held call uses one connection.
  - The wait ends with an error if the sequencer rejects the transaction, or if the timeout passes.
  - A DA-lag reject from the sealer also ends the wait. The error is `-32010`, with the cause `da_lag` at the service `sealer`.
  - A record-lag reject from the sealer also ends the wait. The error is `-32010`, with the cause `record_lag` at the service `sealer`. The record-lag guard is off by default until a later release.
  - The timeout is 30 seconds. The `--pending-receipt-timeout-ms` flag (env `KARDAMOM_PENDING_RECEIPT_TIMEOUT_MS`) sets it.
- `kardamom_sendRawTransactionAsync` validates the transaction and publishes it. It returns the hash at once.
  - Use it to keep many transactions in flight on one connection.
  - A rejection by the sequencer does not reach this call. It reaches the client through the receipt subscription or the status feed.
- Both methods check the transaction in this order:
  1. Overload: refuse if too many submissions wait.
  2. Drain: refuse if the ingress is in shutdown drain.
  3. Pause: refuse if the ingress is paused. The error is `-32010`. See [Chain halts](#chain-halts).
  4. Rate limit: one token from the bucket of the client IP.
  5. Decode: the bytes must be an EIP-2718 transaction.
  6. Protocol limits: refuse a blob transaction (type `0x03`). Refuse a gas limit above 16777216 (the EIP-7825 cap).
  7. Signature: recover the sender.
  8. Known receipt: a resubmission of a transaction with a receipt answers with its hash. A different transaction at a nonce with a receipt is a `Duplicate` error.
  9. Balance: if the ingress knows a fresh balance below `gas_limit * max_fee_per_gas + value`, it refuses with `insufficient funds`.
- The ingress publishes the transaction with an inclusion deadline: the newest block it has seen plus `--inclusion-horizon-blocks` (default 64).
  - The sealer refuses the transaction after that block. See `past deadline` in [Errors](#errors).
- Blob (EIP-4844) transactions are not supported.

### Account reads

- `eth_getBalance` and `eth_getTransactionCount` serve the head block only. The `block` param is required.
  - `latest` and `pending` name the head. The L2 has no reorg.
  - `safe` and `finalized` name the posted head: the last block that the batcher confirmed on L1.
    - The ingress serves them only when the posted head equals the newest block.
    - Behind the head, the call fails with `-32602` and `not served: the ingress answers only the head`.
  - A block number is valid only if it equals the newest block. Any other number, and `earliest`, gives error `-32602`.
- The ingress reads the local account layer first. On a miss it reads Redis, if the `[cache]` section is on. Then it asks one executor.
- If no layer can answer, the call fails with `account state unavailable` (`-32000`).

### Block tags

`kardamom_blockNumberByTag` turns a tag into a block number.

| Tag | Result |
|---|---|
| `latest`, `pending` | The newest closed block (the head). |
| `safe` | The posted head: the last block that the batcher confirmed on L1. |
| `finalized` | The posted head. The batcher does not observe L1 finality, so `finalized` equals `safe`. |
| `earliest` | `0x0` |
| a block number | The number itself. A number above the head gives `-32602`. |

- A client that cannot accept a revert of the L1 data waits until `safe` reaches its block.

### Chain status

`kardamom_chainStatus` returns the data-availability heads and the halt state that this ingress observes.

| Field | Meaning |
|---|---|
| `posted_head` | The last block that the batcher confirmed on L1. |
| `sealed_head` | The last block that the sealer closed. |
| `da_lag_budget_blocks` | The DA-lag budget. `0` means the guard is off. |
| `roots` | The live halts that a pause waits on. Each entry has `service`, `instance`, `cause` and `runbook`. |
| `sealer` | The sealer as this ingress observes it. A `/halt` record with `state`, `halted` and `pause`. |
| `ingress` | The state of this ingress. The same shape as `sealer`. |
| `batcher_halted` | A root entry if a batcher is halted. Else `null`. |
| `deposits_delayed` | `true` if the da-watcher is halted. |
| `services` | The latest state of each service on the `events` stream. |

- Each `services` entry has `service`, `instance`, `state`, `seq`, `age_ms`, `halt` and `pause`.
- `halt` and `pause` are `null` when the service has none.
- The same record shape is on the `/halt` route of each service. See [Failure modes](failure-modes.md#halts-and-service-events).

Example, abridged. The sealer is halted on a DA lag.

```json
{
  "posted_head": 1200,
  "sealed_head": 11300,
  "da_lag_budget_blocks": 10000,
  "roots": [{"service":"sealer","instance":"cluster","cause":"da_lag","runbook":"docs/runbooks/da_lag.md"}],
  "sealer": {"cause":"da_lag","state":"halted","halted":true,"pause":null},
  "ingress": {"state":"paused","halted":false,"pause":{"reason":"upstream"}},
  "batcher_halted": null,
  "deposits_delayed": false,
  "services": []
}
```

### Fee methods

- The ingress keeps a ring of the last 1024 blocks. Each block has its base fee, its gas used and the tip rate of each receipt.
- `eth_feeHistory`:
  - `newestBlock` is a block number, or any tag. A tag names the newest closed block.
  - The answer has `oldestBlock`, `baseFeePerGas` (one more value than blocks: the base fee of the next block), `gasUsedRatio` and `reward`.
  - `gasUsedRatio` is `gas_used / 30000000`.
  - `reward` is present only if you ask for percentiles. A percentile is a rank over the tip rates of the block. An empty block gives zero.
  - If `blockCount` is larger than the ring, the answer starts at the oldest block that the ring holds.
  - If `newestBlock` is not a closed block in the ring, the call fails with `-32000` and `block N is not in the fee history`.
- `eth_maxPriorityFeePerGas`: the median tip rate of the receipts in the newest 20 closed blocks. It is `0x0` if there are none. There is no floor, so zero is always a valid bid.
- `eth_gasPrice`: the base fee of the next block plus the value of `eth_maxPriorityFeePerGas`.
- The ring is in memory. A restart of the ingress empties it.
- For the fee rules, see [Priority fees](priority-fees.md).

## Receipts

A receipt is an Ethereum receipt with two more fields.

| Field | Meaning |
|---|---|
| `priorityFeePerGas` | The tip rate that the transaction paid, in wei per gas. |
| `priorityFeePaid` | The tip that the transaction paid, in wei. It is the tip rate times the gas limit, also for gas that the transaction does not use. |

- Both fields are hex quantities. They are `0x0` for a transaction that pays no tip: a skipped transaction, a deposit, or any transaction on a chain with no fee schedule.
- `effectiveGasPrice` is the base fee of the block when the chain has a fee schedule.
- A client that ignores unknown fields reads the receipt as an Ethereum receipt.
- `blockHash` is always `null`. The chain has no block hash in the receipt.
- A deposit receipt has the legacy receipt type.
- A skipped transaction has `status` `0x0` and `gasUsed` `0x0`. The transaction did not happen and used no nonce.

### Receipt lookup

`eth_getTransactionReceipt` reads in this order:

1. The receipt cache of the ingress. The ingress fills it from the receipt stream. It keeps the newest 131072 receipts and removes the oldest first.
2. On a miss, one query to an executor state database. This is the durable copy, so a receipt survives an ingress restart.
   - A receipt that the query finds enters the cache.
   - The query takes one token from the rate-limit bucket of the client IP. It is also bounded by a limit on queries in flight and by a timeout.
3. If there is no receipt, the answer is `null`.

- `null` means "not committed yet". It also means that the query was shed or failed. Poll again.
- A transaction that the sequencer rejected never gets a receipt. The call returns `null` for it for ever.
- The receipt is the truth. Use the status feed only for early information.

## Receipt subscription

`kardamom_subscribeReceipts` pushes receipts and rejections over WebSocket. Unsubscribe with `kardamom_unsubscribeReceipts`.

- The `senders` param is a list of addresses. `null` or `[]` means all senders.
- The ingress sends one copy of each receipt, also when several executors publish it.
- Each notification has a `type`. There are three frame kinds.

| `type` | Fields | Meaning |
|---|---|---|
| `receipt` | `receipt` | A transaction executed. `receipt` has the same fields as the `eth_getTransactionReceipt` result. |
| `txError` | `sender`, `nonce`, `reason`, `expectedNonce` | The sequencer rejected the transaction. It never gets a receipt. |
| `lagged` | `skipped` | The subscriber fell behind the feed buffer. The ingress dropped `skipped` items. |

- `reason` is one of these words:

| `reason` | `expectedNonce` | Meaning |
|---|---|---|
| `duplicated-tx` | the next nonce of the sender | The nonce is below the next nonce of the sender. |
| `evicted` | the next nonce of the sender | The sequencer shed the transaction under overload. |
| `expired` | the next nonce of the sender | The transaction waited on a nonce gap for longer than the sequencer lifetime. |
| `past-deadline` | `null` | The sealer refused the transaction after its inclusion deadline. |
| `fee-invalid` | `null` | The tip rate is above the fee cap. |
| `fee-too-low` | `null` | The fee cap is below the base fee. |
| `insufficient-funds` | `null` | The balance does not cover the worst-case cost. |
| `da-lag` | `null` | The sealer refused the transaction because the sealed head is too far past the posted head. Submit again after the batcher posts. |
| `record-lag` | `null` | The sealer refused the transaction because no executor recorded the chain within the record-lag budget. Submit again after an executor records. |

- After a `lagged` frame, the client must poll `eth_getTransactionReceipt` for the transactions it waits for. Then it can continue to read the stream.
- The subscription ends if the sink closes or a feed closes.

Example `txError` frame:

```json
{"type":"txError","sender":"0x1111111111111111111111111111111111111111","nonce":7,"reason":"duplicated-tx","expectedNonce":9}
```

## Errors

An error has a JSON-RPC `code` and a `message`. Most errors have no `data` field. The `-32010` errors carry `data`.

| Code | Class | Retry |
|---|---|---|
| `-32005` | Limit exceeded: rate limit, overload, drain. | Yes, with a back-off. |
| `-32602` | Invalid params: the request can never succeed as sent. | No. Change the request. |
| `-32000` | Server error: the request failed for a reason of the chain state or the pipeline. | Depends on the message. See the table. |
| `-32603` | Internal error. | Yes, then report it if it stays. |
| `-32010` | Chain halted or ingress paused: the ingress refuses submits until the root clears. | Yes, after the root clears. See [Chain halts](#chain-halts). |

| Code | Message starts with | Cause | Retry |
|---|---|---|---|
| `-32005` | `rate limit exceeded for client` | The bucket of the client IP is empty. | Yes, after a short wait. |
| `-32005` | `ingress overloaded: N submissions pending` | The ingress has 16384 or more waiting submissions. | Yes, with a back-off. |
| `-32005` | `ingress draining for shutdown` | The ingress is stopping. | Yes, on another replica. |
| `-32602` | `failed to decode transaction` | The bytes are not a valid transaction. Also a block that the ingress does not serve. | No. |
| `-32602` | `signature verification failed` | The signature is not valid. | No. |
| `-32602` | `duplicate (sender, nonce)` | The nonce has a receipt for another transaction, or it is below the next nonce of the sender. | No. Use a new nonce. |
| `-32602` | `transaction gas limit N exceeds the EIP-7825 per-tx cap` | The gas limit is above 16777216. | No. Lower the gas limit. |
| `-32602` | `unsupported transaction type 0x03` | A blob transaction. | No. |
| `-32000` | `evicted by sequencer overload shed` | The sequencer shed the transaction. | Yes, when the nonce is within the reorder window. |
| `-32000` | `expired: the sequencer dropped` | The transaction waited on a nonce gap for the sequencer lifetime. | Yes, when the gap is filled. |
| `-32000` | `past deadline: the sealer refused` | The sealer block passed the inclusion deadline. The message has the block numbers. | Yes, at once. The new submission gets a new deadline. |
| `-32000` | `timed out waiting for receipt or watermark` | No receipt arrived in the timeout. The transaction can still land. | Check the receipt first. |
| `-32000` | `sequencer partition unavailable` | The ingress could not publish. | Yes, with a back-off. |
| `-32000` | `insufficient funds for gas * price + value: address A have H want W` | The balance is below the worst-case cost. | No, until the account has funds. |
| `-32000` | `max priority fee per gas higher than max fee per gas` | The tip rate is above the fee cap. | No. Sign again with a tip rate within the cap. |
| `-32000` | `max fee per gas less than block base fee` | The fee cap is below the base fee. | Sign again with a higher cap. |
| `-32000` | `account state unavailable` | No read layer could answer. | Yes. |
| `-32000` | `block N is not in the fee history` | `eth_feeHistory` asked for a block that the ring does not hold. | Ask for a newer block. |
| `-32010` | `chain halted: <cause> at <service> (<detail>)` | A halt upstream (the sealer or an executor) stops the chain. The ingress pauses submits. The sealer refuses user records on a DA lag. | Yes, after the root clears. The ingress resumes by itself. |
| `-32010` | `ingress paused by an operator (<note>)` | An operator paused this ingress. | Yes, after the operator resumes it. |
| `-32603` | `internal server error` | An internal fault. | Yes. |

- The fee and funds messages use the shape of the geth messages, so a wallet handles them as on Ethereum.
- The sequencer errors reach `eth_sendRawTransaction` as an error response. Every such error also appears as a `txError` frame and as a `rejected` status event.
- `past deadline`: the sealer did not order the transaction. A new submission of the same signed bytes gets a new deadline.
- A resubmission of a transaction that already has a receipt is not an error. The call returns its hash.

### Chain halts

An error `-32010` means that the chain, or this ingress, does not take submits now.

- The full message of a halt is `chain halted: <cause> at <service> (<detail>); the ingress pauses submits until it clears; cause and recovery at /halt and kardamom_chainStatus, runbook docs/runbooks/<cause>.md`.
- The `data` of a halt has these fields:
  - `cause`: the cause id, for example `da_lag`
  - `root_service`: the service that raised the halt
  - `root_instance`: the instance of that service
  - `halt`: the string `/halt`, the route that holds the record
  - `runbook`: the path of the runbook file
- The `data` of an operator pause is `{"cause":"operator","halt":"/halt"}`.
- The pause check runs in the submit order after the drain check and before the rate limit. A refused call uses no token.
- A DA-lag reject from the sealer reaches a waiting `eth_sendRawTransaction` as the same error, with `da_lag` at `sealer`. The `txError` frame has the reason `da-lag`.
- A record-lag reject from the sealer reaches a waiting `eth_sendRawTransaction` as the same error, with `record_lag` at `sealer`. The `txError` frame has the reason `record-lag`.
- Wait for the root to clear, then submit again. The ingress resumes by itself. An operator pause ends only on the operator resume.
- The cause table and the halt model are in [Failure modes](failure-modes.md#halts-and-service-events). The recovery steps are in the [runbooks](runbooks/README.md).

## Rate limits

- The ingress has a token bucket for each client IP.
  - The rate is 10000 tokens each second. The burst is 1000 tokens.
  - These values are fixed in the ingress build. A flag does not change them.
  - The IP is the peer address of the connection. The ingress does not read forwarding headers. Behind a proxy, all clients share the IP of the proxy.
- These calls take one token: `eth_sendRawTransaction`, `kardamom_sendRawTransactionAsync`, `eth_getBalance`, `eth_getTransactionCount`, and a receipt lookup that misses the cache.
- A call with no token left fails with `-32005`. A receipt lookup answers `null` instead.
- `eth_chainId`, `eth_blockNumber`, the fee methods and the subscription take no token.
- Other limits:
  - Connections: 8192 at the same time. The `--rpc-max-connections` flag (env `KARDAMOM_RPC_MAX_CONNECTIONS`) sets it. A held `eth_sendRawTransaction` uses a connection, so keep the limit above the submit rate times the receipt latency.
  - Waiting submissions: 16384. More than this gives `-32005`.
- Metrics: `kardamom_ingress_tx_rejected_total` has a `reason` label for each refusal. See [Observability](observability.md).

## Further reading

- [Transaction status events](tx-status-events.md): the notifier feed and the webhooks.
- [Priority fees](priority-fees.md): the fee rules, the admission checks and the settlement.
- [Failure modes](failure-modes.md): how the ingress behaves when other actors fail. The section [halts and service events](failure-modes.md#halts-and-service-events) has the halt model.
- [Runbooks](runbooks/README.md): one runbook for each halt cause.
