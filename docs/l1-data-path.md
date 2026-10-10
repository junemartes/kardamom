# L1 and data availability

This document is the operator reference for the data path to L1: the batcher, the EigenDA proxy,
the settlement contract, the L1 follower (the inbox indexer), and the tools that read them back. The failure behavior
is in [failure-modes.md](failure-modes.md). The deploy switches are in
[../deploy/cluster/README.md](../deploy/cluster/README.md).

## The data path

L1 holds the ordering and a certificate for each batch. EigenDA holds the bytes.

```text
sealer egress ─▶ kardamom-batcher ──payload──▶ EigenDA proxy ──▶ EigenDA
                       │                            │
                       │◀────── certificate ────────┘
                       ▼
        KardamomL2Settlement.postBatch(prevBatchIndex, daCert,
                                       l2BlockStart, l2BlockEnd, recordsCommitment)
                       │  emits BatchPosted
                       ▼
        kardamom-l1-indexer ──▶ archive (batches, payloads, epochs, blocks)
          (the L1 follower)  ──▶ l1_blocks stream (one record per finalized block)
```

- The batcher packs closed blocks into a KAR1 payload.
- The batcher sends the payload to the EigenDA proxy. The proxy returns a certificate.
- The batcher posts the certificate to `KardamomL2Settlement.postBatch`.
  - The call reverts unless the sender is the contract's `l1Batcher`.
  - The call reverts unless `prevBatchIndex` equals `lastBatchIndex`. This rule stops a double post.
  - The call reverts with `EmptyCert` for an empty certificate.
  - The contract does not check the certificate. The proxy checks it.
- The `BatchPosted` event carries the certificate. It is the only place L1 keeps it.
- The indexer follows finalized L1 and keeps a copy of each payload.
  - EigenDA keeps a payload for 14 days. The archive keeps it for as long as the operator says.
- Readers take a payload by certificate from the proxy, or from the indexer after the 14 days.

## `kardamom-batcher`

The live batcher is a cluster-egress consumer. It starts with `--live`. It needs `--dry-run=false`,
`--config`, `--cursor-file`, and the L1 flags (`--l1-rpc`, `--l1-key`, `--settlement`, `--da-proxy`).

### Flags

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `--live` | — | off | Run the live service. Without it, the binary runs the offline archive scan. |
| `--dry-run` | — | `true` | Skip the L1 broadcast. Live mode needs `--dry-run=false`. |
| `--config` | `KARDAMOM_BATCHER_CONFIG` | none | TOML file with the `[cluster]` section. Live mode needs it. |
| `--log-config` | `KARDAMOM_LOG_CONFIG` | none | Channel layout of `tx_data` and `tx_deposits`. |
| `--aeron-dir` | `KARDAMOM_AERON_DIR` | none | Directory of the Aeron media driver. |
| `--cluster-egress-endpoint` | `KARDAMOM_CLUSTER_EGRESS_ENDPOINT` | none | `ip:port` of this node for the cluster egress. It overrides `egress_channel` of the config. |
| `--replay-destination-endpoint` | `KARDAMOM_REPLAY_DESTINATION` | none | UDP endpoint for refetched `tx_data` and `tx_deposits`. Without it, refetch is off. |
| `--archive-control-response-endpoint` | `KARDAMOM_ARCHIVE_CONTROL_RESPONSE` | none | UDP endpoint for the archive-control responses. Required with the replay destination. |
| `--void-voter-id` | `KARDAMOM_VOID_VOTER_ID` | none | Voter id at the sealer. Without an id, the batcher stops at an entry that no archive serves. |
| `--chain-id` | `KARDAMOM_CHAIN_ID` | `1` | L2 chain id. The records commitment contains it. |
| `--l1-rpc` | `KARDAMOM_L1_RPC` | none | L1 endpoints. Repeat the flag, or separate the values with commas. |
| `--l1-key` | `KARDAMOM_L1_KEY` | none | Hex key of the batcher account. It must equal `l1Batcher` of the settlement. |
| `--settlement` | `KARDAMOM_SETTLEMENT` | none | Address of the `KardamomL2Settlement` proxy. |
| `--da-proxy` | `KARDAMOM_DA_PROXY` | none | URL of the EigenDA proxy. |
| `--l1-retries` | — | `5` | Retries for each L1 post. When all fail, the batcher halts with `l1_unreachable`. |
| `--cursor-file` | `KARDAMOM_BATCHER_CURSOR` | none | Durable cursor: the ordering position of the last confirmed post. Live mode needs it. |
| `--spool-dir` | `KARDAMOM_BATCHER_SPOOL` | `spool` beside the cursor file | Root of the spool: the consumed blocks that are not yet posted. The block files of layout version N live in `v<N>` under it. |
| `--indexer-url` | `KARDAMOM_INDEXER_URL` | none | API of the L1 follower (the inbox indexer). |
| `--da-lag-budget-blocks` | `KARDAMOM_DA_LAG_BUDGET_BLOCKS` | `10000` | The sealer's DA-lag budget. A group is due at half of it. `0` matches a sealer with the guard off. |
| `--l1-silence-secs` | — | `1152` | Seconds with no `l1_blocks` record before the batcher pauses with the follower as its root. |
| `--block-refs-source` | `KARDAMOM_BLOCK_REFS_SOURCES` | none | Query endpoints of the executors and the validator. Repeat the flag, or separate the values with commas. |
| `--blocks-per-batch` | — | `1` | A group posts when it holds this many blocks. |
| `--flush-ms` | — | `2000` | A group with a transaction or a remote-epoch record posts when its oldest block waited this long. Must not be zero. |
| `--idle-flush-ms` | — | value of `--flush-ms` | The same wait for a group of empty blocks. |
| `--target-payload-bytes` | — | 14 MiB | A group posts when its raw bytes reach this value. |
| `--no-compress` | — | off | Turn off zstd on the payload. |
| `--metrics-addr` | `KARDAMOM_METRICS_ADDR` | `127.0.0.1:9002` | Address of the `/metrics` and `/ready` listener. |
| `--host-id` | `KARDAMOM_HOST_ID` | `local` | Value of the `host_id` label. |

- The offline mode takes `--channel-b-segment` and `--channel-a-archive sid=path,...`.
  It scans archive files and, with `--dry-run=false`, posts what it finds.
- The payload ceiling is 15 MiB. It stays under the 16 MiB blob of EigenDA.
- Each endpoint of `--l1-rpc` is a fallback for the others.
  - A request goes to the best endpoint first.
  - An error or an HTTP 429 moves the request to the next endpoint.
  - The batcher does not compare the answers of the endpoints. The L1 follower does (see below).
- The batcher uses `--l1-rpc` for its own writes only: the nonce, the fees, `eth_sendRawTransaction`,
  the receipt of its own transaction, the contract read at start, and the read that checks whether a
  failed send landed. It reads no `BatchPosted` log: it learns its posts from the `l1_blocks` stream.

### Posting cadence

A group of closed blocks posts when the first of these conditions holds:

- The raw bytes of the group reach `--target-payload-bytes`.
- The group holds `--blocks-per-batch` blocks.
- The oldest block waited `--flush-ms`, and the group has a transaction or a remote-epoch record.
- The oldest block waited `--idle-flush-ms`, and the group has neither.
- The group's last block is half the sealer's DA-lag budget (`--da-lag-budget-blocks`) past the posted head.
  The deploy passes the batcher the same value as the sealer (`KARDAMOM_DA_LAG_BUDGET_BLOCKS`), so the guard
  stays a backstop and never halts an idle chain between two posts.
- The deploy refuses an idle flush longer, in seconds, than the budget in blocks: an idle chain seals about one
  block a second.

The batcher checks the timers at every boundary and once each second.

- A real L1 needs a long `--idle-flush-ms`. An idle chain still closes a block on every sealer tick (`kardamom.cluster.tickMs`, default 2 s), and each post costs gas.
- The alert `KardamomBatcherLastPostStale` fires at twice the idle wait.
  The idle wait therefore sets the paging threshold. See [observability.md](observability.md).
- The cluster job sets `blocks_per_batch` to 5 and `flush_ms` to 3000. The idle wait follows `flush_ms` when it is empty.

### Start sources

At start, the batcher reads the contract. It does not scan events.

- It calls `lastBatchIndex`, and then `batches(index)` for `l2BlockEnd`.
- A failed read counts in `kardamom_batcher_resume_failures_total`.
  The batcher retries in the process, with a backoff of 2, 4, 8, 16, then 32 seconds.
- With a cursor file, the cursor is the position of the last confirmed post.
  - A cursor behind L1 (a crash between the post and the cursor write) replays from the cursor.
    The batcher drops the blocks that L1 already covers and does not post them again.
  - A cursor ahead of L1 stops the batcher. The L1 chain went back under a cursor that survived.
    The operator decides which side is real. Delete the cursor file to derive the state from this L1.
- Without a cursor file and with posted batches, the batcher rebuilds the cursor from the last batch.
  Each block in the payload carries its own cursor. The batcher reads the payload from the L1 follower's archive
  (`--indexer-url`).
- The archive holds a batch once its block is finalized. Until then the start fails, counts a resume failure,
  and retries. This case needs a lost cursor file and a batch posted in the last finality window.
- A batch with a payload that has no block cursor gives a replay from genesis, with a warning.

### Posted cursor and halts

The batcher tells the sealer how far L1 holds the chain.

- The batcher publishes the confirmed cursor to the sealer as the posted head (the last L2 block in a confirmed post).
  - It publishes at start, before the first post, and after each change of the confirmed cursor.
  - A refused publish is logged. The next confirmed post publishes again.
- The sealer keeps its replay frames above the posted head (see "Resume sources").
  It also refuses new transactions when the sealed head is too far past the posted head (the DA-lag guard).
  The ingress exports the heads as `kardamom_ingress_cluster_posted_head` and `kardamom_ingress_cluster_sealed_head`.
- The batcher holds a halt when it cannot go on. The halt shows on `/halt` and in `kardamom_halt`.
  See [failure-modes.md](failure-modes.md#halts-and-service-events) and [runbooks/README.md](runbooks/README.md).

| Halt cause | Raised when | Clears |
| --- | --- | --- |
| `l1_unreachable` | L1 does not answer at start, or every retry of a post fails. | By itself, when L1 answers. |
| `replay_unavailable` | The sealer refuses a replay and the rebuild cannot fill the gap. | By an operator. |

### Resume sources

After the start, the batcher needs the blocks that follow the cursor. It takes them from three sources, in order:

1. The spool.
   - The batcher writes each closed block to the spool before the block joins the pending group.
     Each block is one file, written whole and renamed into place.
   - A confirmed post removes its blocks.
   - A restart reloads the pending group from the spool. The age of the group counts from the oldest write.
   - The batcher drops a spool that does not continue the confirmed cursor.
     It first drops the blocks that L1 already covers.
   - Keep the spool on a persistent mount.
2. The replay of the sealer from the cursor.
   - The sealer keeps a minimum window of egress frames (`kardamom.cluster.retention`).
   - A frame past the window leaves only when its block is at or below the posted head.
   - The replay floor therefore stays at or below the posted head, and the batcher can replay from its cursor.
3. The rebuild from references.
   - The sealer answers `REPLAY_UNAVAILABLE` when the cursor is below its replay floor.
   - The batcher then reads each missing block from `--block-refs-source`. The method is `kardamom_getBlockRefs`.
     The answer lists the references of the transactions of the block.
   - The batcher fetches the bytes from the `tx_data` archives. It checks each hash, closes the blocks, and fills the spool.
   - It resumes at the floor of the sealer. The metric `kardamom_batcher_rebuilt_blocks_total` counts the blocks.
   - A block with a cross-chain message cannot be rebuilt this way. The endpoint refuses it.
   - Without an endpoint, a refused replay raises the `replay_unavailable` halt.
   - A second refusal after the rebuild also raises it. The halt clears only by an operator.

### KAR1 payload versions

- The payload starts with the magic `KAR1`, and a version byte.
- Version 5 is the version the batcher writes. Each block header carries a cursor: `end_tx_idx` and `l1_origin`.
- Version 4 has no cursor. A reader accepts it. A state rebuilt from it is correct but is not resumable.
- Versions 2 and 3 are not valid.
- The flag byte has bit 0 for zstd compression. The batcher compresses by default.

## The EigenDA proxy

The proxy is the single API to EigenDA. It runs beside the batcher. It listens on port 3100.

- The batcher sends a payload with `POST /put` and receives a certificate.
- A reader sends `GET /get/<cert>` and receives the payload.
- The proxy checks each certificate against the verifier contract, on `put` and on `get`.
- On `get`, it also checks the bytes against the KZG commitment of the certificate.
  A reader therefore trusts its proxy and not the disperser.
- The proxy holds no state that a deployment depends on. EigenDA keeps the bytes for 14 days.

### Settings

The job is `deploy/cluster/nomad/da-proxy.nomad.hcl`. It runs the image `ghcr.io/layr-labs/eigenda-proxy:2.7.1` with the V2 client.

| Job variable | Deploy env var | Meaning |
| --- | --- | --- |
| `eigenda_network` | `EIGENDA_NETWORK` | `sepolia_testnet` or `mainnet`. The proxy fills the disperser and the contract addresses from it. |
| `eigenda_cert_verifier` | `EIGENDA_CERT_VERIFIER` | Address of the `EigenDACertVerifierRouter` of the network. Empty gives the known address for `sepolia_testnet` only. Other networks need a value. |
| `eigenda_ledger_mode` | none | Payment mode of the signer. Default `on-demand-only`. Other values: `reservation-only`, `reservation-and-on-demand`. |

- The signer key (`BATCHER_KEY`) and the L1 endpoint that checks certificates (`L1_RPC`) are secrets.
  They are not job variables. The task reads them from the Nomad Variable `nomad/jobs/da-proxy`.
  See "Secrets" in [`deploy/cluster/README.md`](../deploy/cluster/README.md#secrets).
- The deploy role does not set `eigenda_ledger_mode`. Change it as a job variable.
- The default ledger mode pays from an on-demand deposit.
  - The batcher account must hold a deposit in the EigenDA PaymentVault.
  - Without a deposit, the proxy exits with `no reservation found for account`.
- The proxy answers `/health` only after it loads its SRS points. This takes tens of seconds.
  The deploy waits for `/health` before it starts the batcher.

### The `da-store` stand-in

A deployment without `EIGENDA_NETWORK` runs `kardamom-da-store` (`nomad/da-store.nomad.hcl`).

- It serves the same API on port 3100 under the same Consul service. The batcher, the indexer, and `kardamom-reconstruct` use it without a change.
- It keeps the payloads in a directory (`--dir`, env `KARDAMOM_DA_STORE_DIR`). The payloads survive a restart.
- It checks no certificate. Use it for the local profile and the end-to-end suites only.

## `kardamom-l1-indexer`

The indexer is the L1 follower: the one service that reads L1 data. It reads the finalized L1 once per
finality step, archives the batches, their payloads, and the epoch record of each block, and publishes
one `L1Block` record for each finalized block on the `l1_blocks` Aeron stream (id 1020).

- Each tick reads the finalized tip, then the headers of the range after the cursor in one JSON-RPC batch
  request, then the settlement's and the lockbox's logs in one query with both addresses for each chunk of
  `--max-log-range` blocks.
- Each header must name the one before it as its parent, and the first must name the cursor's block.
- With a light client, the last header of a range that ends at the finalized tip must be the light client's
  header for that number. A mismatch is the operator halt `l1_light_client_mismatch`. While the follower
  catches up, the ranges in the middle have the two-source rule alone; the anchor of the range that reaches
  the tip then covers them through the parent chain.
- The order of a range: the archive, then the records on the stream, then the cursor. A failure before the
  cursor write reads the range again; the consumers drop the copies.
- A record on the stream is a block that two sources agreed on and that descends from the record before it.
  A consumer checks only the parent link against its own cursor.
- Two instances run on two nodes, and both publish. A consumer keeps the first record of each number
  (`kardamom_types::L1BlockDedup`). Two records of one number with different hashes halt the consumer:
  `l1_follower_disagreement`.

### The poll follows the finality schedule

- With `--beacon-api`, the follower reads the genesis time, `SECONDS_PER_SLOT` and `SLOTS_PER_EPOCH` once.
  After a range that reaches the finalized tip, it sleeps until the next epoch boundary, then reads the tip
  every `--poll-interval-secs` (one slot) until the tip moves. A range behind the tip reads the next at once.
- A failed tick, a halt included, reads again after one slot.
- Without `--beacon-api` (anvil), the follower reads every `--poll-interval-secs`.
- Before each sleep, the follower exports the wake time, `kardamom_l1_follower_next_wake_seconds`. It is
  ready while now is before that time plus one slot and 10 s.

### Retention of the stream

The archive of each follower node records `l1_blocks`, and nothing purges the recording. A consumer that
restarts after a sealer fleet rebuild resumes at the L1 origin M of the batcher's posted head, so a future
purge of the recording keeps every block at or above M: the same floor as the other retentions of the
cluster. The follower's own archive (`blocks/`, `epochs/`) keeps every block since its start block, and is
the backstop below M.

- The archive is not a source of truth. L1 and the DA layer within its retention can rebuild it.
- A lost archive costs a re-index from the start block. It does not cost the chain.

### Flags

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `--l1-rpc` | none | none | L1 endpoints. Repeat the flag, or separate the values with commas. |
| `--l1-light-client-rpc` | none | none | Endpoint of the L1 light client. It settles a read that it serves. |
| `--da-proxy` | `KARDAMOM_DA_PROXY` | none | EigenDA proxy for the payloads. Required. |
| `--settlement` | none | none | Address of `KardamomL2Settlement`. Required. |
| `--lockbox` | none | none | Address of the `ETHLockbox` proxy. Required. |
| `--start-block` | none | finalized block at first start | First L1 block to index on an empty archive. Use the block of the contract deploy. |
| `--data-dir` | none | none | Archive directory. Required. |
| `--listen` | none | `0.0.0.0:8549` | Address of the JSON-RPC API. |
| `--beacon-api` | none | none | A beacon API. With it, the follower reads on the finality schedule. |
| `--poll-interval-secs` | none | `12` | One slot: the read cadence while the tip does not move or the follower is halted, and the whole cadence without `--beacon-api`. |
| `--blocks-per-tick` | none | `64` | The most blocks that one tick indexes and one header batch holds. |
| `--max-log-range` | none | `10` | The most blocks one log query spans. Alchemy's free plan caps it at 10. |
| `--log-config` | `KARDAMOM_LOG_CONFIG` | IPC defaults | The Aeron channels and discovery. |
| `--aeron-dir` | none | Aeron's default | The media driver directory. |
| `--archive-durability` | `KARDAMOM_ARCHIVE_DURABILITY` | off | Record `l1_blocks` on the node's archive, and publish nothing before the recording is live. It needs `[discovery]`. The job sets it. |
| `--metrics-addr` | `KARDAMOM_METRICS_ADDR` | `127.0.0.1:9549` | Address of the `/metrics` and `/ready` listener. |
| `--host-id` | `KARDAMOM_HOST_ID` | `local` | Value of the `host_id` label. |

The cluster job binds the metrics listener to `0.0.0.0:9009`.

### JSON-RPC methods

| Method | Result |
| --- | --- |
| `indexer_status()` | The cursor of the archive. |
| `indexer_batch(index)` | The descriptor of a batch, or `null`. |
| `indexer_payload(daCert)` | The payload as `0x` hex, or `null`. |
| `indexer_epoch(l1Block)` | The epoch record of an L1 block as `0x` hex, or `null`. |
| `indexer_l1_block(l1Block)` | The `l1_blocks` record of an L1 block as `0x` hex (rkyv `L1Block`), or `null`. |
| `indexer_halt()` | The lifecycle record: `service` (`l1-indexer`), `state`, `halted`, `pause`, and the halt fields while a halt stands. |

- The indexer publishes its state on the `events` stream. A tool without an Aeron runtime calls `indexer_halt()`.
- `kardamom-reconstruct` calls `indexer_halt()` before it reads from an indexer. It refuses a halted or paused indexer.
  The error is `the indexer is <state> (cause <cause>, runbook <runbook>); refuse to rebuild from it`.

### Archive layout

```text
<data-dir>/
  batches/<index>.json            descriptor of one batch
  epochs/<l1_block>.rkyv          epoch record of one L1 block: the bytes of the record's epoch on l1_blocks
  blocks/<l1_block>.rkyv          the l1_blocks record of one L1 block
  payloads/<keccak(cert)>.bin     payload, named by the hash of its certificate
  cursor.json                     last indexed block and its hash; written last
```

- The indexer writes `cursor.json` after the items of a block. A crash re-indexes the block and never skips it.
- The indexer checks that each block descends from the previous one. The check also holds across a restart.
- The cluster job keeps the archive in `/opt/kardamom/l1-indexer`. A volume snapshot is a backup.

### Ports and observation

| Port | Use |
| --- | --- |
| 8549 | JSON-RPC API. The Consul service is `kardamom-l1-indexer`. |
| 9549 | Metrics, by default. The cluster job uses 9009. |

- Metrics, readiness, and the alert are in [observability.md](observability.md).

## Two L1 sources for the followers

The indexer (the L1 follower) and the validator's check read L1 through a set of sources. The da-watcher has no L1
access: it reads the follower's `l1_blocks` stream.

- `--l1-rpc` is a list of public endpoints. `--l1-light-client-rpc` is the light client.
- The set accepts the ids of a block, or the result of a log query, in these cases:
  - Two public sources agree.
  - The light client serves the answer.
  - Only one public source is configured. The set trusts it alone.
- The finalized tip is the lowest tip that the agreeing sources report.
- A majority never settles a disagreement. Two public endpoints can share a backend.
- With a light client, the source that disagrees with it is the liar. That source rotates out.

### Rotation and backoff

- A source that fails or answers HTTP 429 rotates out for 30 seconds. The set goes on with the other sources.
- When all sources are out, or fewer sources answer than the rule needs, the read fails. The follower tries again at the next tick.
- The counter `kardamom_l1_source_rotations_total` counts the rotations. Its labels are `source` and `reason`.
  The reasons are `error`, `rate_limited`, and `disagreement`.

### Halt causes

A read that the set cannot serve, or a chain that does not link, halts the follower. The halt has one typed cause.
The log line, the error, the `/halt` record, and the `kardamom_halt` gauge carry it.

| Cause | Meaning | Clears when |
| --- | --- | --- |
| `l1_source_disagreement` | Two sources gave different answers, and no light client settles it. Both answers are in the log. | The sources agree again. Remove the lying endpoint. |
| `l1_unreachable` | No source answers, or fewer sources answer than the rule needs. | A source returns after its backoff. |
| `l1_chain_break` | A finalized block does not descend from the block that the follower holds. | The follower reads a chain that links. It retries on each tick. |
| `l1_light_client_mismatch` | The last header of the indexer's step is not the light client's header. | An operator clears it after the runbook. |

- Each halt of a follower but `l1_light_client_mismatch` clears by itself. The follower retries the same range every slot.
- `l1_sources_out` is the label of the error when fewer sources answer than the rule needs. The halt cause for that error is `l1_unreachable`.
- A halted follower serves `/halt` on its metrics port, and its `/ready` answers 503.
  See [observability.md](observability.md).
- The counter `kardamom_l1_source_disagreement_total` counts each disagreement.
  A disagreement that the light client settles also counts. The liar rotates out, and the follower does not halt.
- The alert `KardamomL1SourceDisagreement` fires on any increase. One alert for each cause (`KardamomHalt*`) fires on the halt.
- The da-watcher pauses with the follower as its root while every follower instance is halted.
  See [failure-modes.md](failure-modes.md) for the effect.

### Why the followers do not walk the light client alone

The light client serves only the blocks it saw, and the finalized checkpoints.

- A block outside this window needs a historical `eth_getProof` from the execution endpoint.
  No free public endpoint serves that.
- A follower behind the light client alone stalls after any gap. The error is
  `distance to target block exceeds maximum proof window`.
- The deploy therefore gives the followers the public endpoints (`L1_FOLLOWERS_RPC`), and gives the light client as the tie-breaker.
  The validator keeps its view through the light client.
- Set the two endpoints with the deploy switches in [../deploy/cluster/README.md](../deploy/cluster/README.md).

## `kardamom-reconstruct`

The tool rebuilds the L2 state from L1 data alone. It reads the `BatchPosted` log, takes each payload by certificate,
and runs the blocks through the same engine as the executor.

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `--l1-rpc` | `KARDAMOM_L1_RPC` | none | L1 endpoint with the `BatchPosted` log. Required. |
| `--settlement` | none | none | Address of `KardamomL2Settlement`. Required. |
| `--da-proxy` | `KARDAMOM_DA_PROXY` | none | EigenDA proxy. Use it while EigenDA keeps the payloads. |
| `--indexer-url` | `KARDAMOM_INDEXER_URL` | none | Indexer API. Use it after the EigenDA retention. It conflicts with `--da-proxy`. |
| `--chain` | none | none | Genesis TOML. Required. |
| `--lockbox` | none | none | Address of the `ETHLockbox` proxy. The rebuild derives the L1 deposits from its logs. Without it, the rebuild leaves deposits out. |
| `--state-dir` | none | none | Output directory of the state database. Required. |
| `--from-block` | none | `0` | First L1 block of the log scan. |
| `--through-block` | none | last block | Last L2 block to run. The posted batches must reach it. |
| `--expect-root` | none | none | Expected state root. A mismatch gives a non-zero exit. |
| `--executor-image` | none | off | Write an image that an executor resumes on. |
| `--sealer-seed` | none | none | Write the seed file that a sealer cluster with no state starts from. |
| `--no-sync` | none | off | Skip the fdatasync of each block. Use it for a check only. It conflicts with `--executor-image`. |

One of `--da-proxy` and `--indexer-url` is required.

- The output line is one line on stdout:

  ```text
  reconstructed head=<block> blocks=<n> txs=<n> state_root=<0x…> end_tx_idx=<n|none>
  ```

- `end_tx_idx` is the canonical end index of the head block. It is `none` for a payload without a cursor.
- `--executor-image` removes the trie, the hashed mirror, and the stored root after the root check.
  It needs a cursor through the last block. Without one, the tool exits with an error.
- `--sealer-seed <file>` writes the head `H` of the rebuilt state for a new sealer cluster.
  - The file holds the chain id, `H`, the canonical end of `H`, the timestamp and L1 origin of `H`, the state root and the next nonce of each sender.
  - It needs a payload that carries the canonical cursor through the last block. The tool refuses a head that it did not rebuild from L1.
  - The sealer reads the file in `-Dkardamom.cluster.seedSnapshot`. See [the sealer README](../cluster/sealer-service/README.md#seeded-start).
- The tool covers L2 transactions, interop deliveries and L1 deposits.
  - A deposit is not in the payload. With `--lockbox`, the tool derives each deposit from the lockbox logs of the L1 block that the L1 origin of a block names. It reads those logs through `--l1-rpc`.
  - Without `--lockbox`, the tool logs a warning. A chain with deposits then rebuilds to a wrong root.
  - A missing deposit shows as a mismatch at `--expect-root`.
- The recovery procedure is in [failure-modes.md](failure-modes.md).

## The L1 fault proxy

`kardamom-l1-fault-proxy` is a JSON-RPC proxy that lies on command. It sits in front of the in-cluster anvil.
Only the `chaos-l1` shard uses it. The fault kinds, the HTTP API and the deploy switch are in
[chaos-suite.md](chaos-suite.md#the-l1-fault-proxy).
