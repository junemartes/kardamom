# Batcher live L1 broadcast (issue #39)

Status: implemented in this PR. Companion to `docs/failure-modes.md` §"Batcher"
and `docs/agents/log-config-and-recorder-spec.md` (whose "the batcher opens no
Aeron channels" non-goal this spec deliberately reverses).

## Problem

`kardamom-batcher` was an offline archive reader: point it at segment files,
scan to EOF, pack KAR1 → zstd → blobs, and (since the settlement work) post the
collected batches to L1. Three things kept it from being the live DA service
issue #39 asks for:

1. **No live source.** There is no `tx_ordering` Aeron recording in cluster
   mode at all — canonical ordering lives in the Aeron Cluster (Raft) log and
   is served to consumers over **cluster egress** (executor and validator are
   the two existing consumers). The `--channel-b-segment` file format the
   offline reader parses is a synthetic test fixture, not production data.
2. **No streaming sender.** The binary scanned the whole archive through a
   `MockSender`, then posted the accumulated batches in one shot. No cursor,
   no retry, no restart story.
3. **No deployment.** The Nomad job was `type = "batch"` + hourly cron with
   placeholder args and `--dry-run`; the settlement contract was never
   deployed in the cluster.

## Design

### Ordering source: cluster egress, exactly like the validator

The live batcher is a third cluster-egress consumer, reusing the
`kardamom-engine` reader stack verbatim:

- `cluster_tx_ordering_subscription(rt, cluster.to_live(), cursor)` on a
  dedicated Aeron runtime, with `.suppress_sealer_metrics()` (the executor is
  the blessed `kardamom_sealer_*` emitter).
- M `tx_data` subscriptions (`bin_support::open_tx_data_subs`) + the
  `tx_deposits` subscription feeding `JoinBuffer` / `DepositJoinBuffer`.
- `archive_join_recovery` so a join miss (batcher was down while ingress kept
  publishing) refetches the envelope from the remote durability archives
  instead of dying.
- `spawn_tx_ordering_reader` → `Receiver<ReaderToExec>`; the feed loop maps
  `Tx { envelope, position }` → `BatchAccumulator::observe_tx`, `Boundary` →
  `observe_boundary` → close policy, and **skips `Deposit` records** —
  deposits are absent from DA by design (they are re-derivable from L1; see
  `docs/agents/l1-origin-deposit-derivation-spec.md`).

Replay across restarts and session loss is the cluster client's existing
contract: the replay request `(next_index, next_block)` is re-sent on every
session establishment. `EGRESS_KIND_REPLAY_UNAVAILABLE` (cursor aged out of
cluster retention) is a **fail-stop**: the batcher is the DA source — an
ordering gap it cannot replay is unpostable data, and restarting into the same
gap loudly is strictly better than silently skipping blocks. Operator recourse
is cluster-log retention, not batcher-side fallback.

### Sender: streaming posts with the CAS as the serializer

`AlloySender` implements the existing `Sender` trait over the existing
`post_batch` (EIP-4844 blobs + KZG sidecar + `postBatch(prev, hashes, start,
end)`), called via `Handle::block_on` from the feed thread. Per post:

- Blobs are written to the DA store **before** the L1 send (unchanged
  invariant — the DA store must never lack bytes for an on-chain hash).
- On success, `lastBatchIndex` advances by exactly 1 (contract CAS), the
  durable cursor is persisted, and `kardamom_batcher_batches_posted_total` /
  `_blobs_posted_total` now count **confirmed L1 posts**, not packed batches.
- On `StaleBatchIndex` revert or a receipt timeout, re-read chain state: if
  `lastBatchIndex == ours + 1` **and** the latest `BatchPosted` event matches
  our exact block range, our transaction landed (duplicate send after a
  timeout) → treat as success. Any other advance means a second writer —
  fail-stop; the batcher is single-instance by design and the CAS is what
  makes the race loud instead of corrupting.
- Transport errors retry with bounded backoff (`--l1-retries`, default 5);
  exhaustion is fatal (the supervisor restarts us and the cursor replays).

### Durable cursor: L1 is the truth, the file is the position hint

Two pieces of state survive a restart:

1. **`lastBatchIndex` + its `BatchPosted` event on L1** — the authoritative
   "what has been posted" (gives `skip_through_block = l2BlockEnd`).
2. **The cursor file** (`--cursor-file`, atomic tmp+rename, JSON
   `{next_index, next_block, last_batch_index}`) — the ordering-stream
   position matching that truth. Written **only after a confirmed post**
   (at-least-once, like the da-watcher's cursor).

Startup reconcile: read both; start the egress replay from the cursor (genesis
`(0, 1)` if the file is missing); the feed loop **drops closed blocks with
`block_number <= skip_through_block` without posting** — so a stale or lost
cursor causes re-observation, never a double post (the CAS would revert it
anyway) and never a gap. A lost cursor file on a long-lived chain degrades to
a genesis replay, which either succeeds (retention permitting) or fail-stops
on `REPLAY_UNAVAILABLE` — see above.

Blocks are only batch-boundary-atomic: the cursor after posting through block
B with boundary `end_tx_idx` E is `(E.as_index(), B + 1)`.

### Close policy

`--blocks-per-batch` (default 1) as before, plus `--flush-ms` (default 2000):
if pending blocks exist and no boundary arrives within the deadline, post the
partial group. The 6-blob ceiling stays a hard error — at `blocks_per_batch`
defaults it is unreachable (a 1s block at 800 tps ≈ 2 blobs); raising
`blocks_per_batch` toward the ceiling is an operator concern.

### Deployment

- `deploy.sh` gains a settlement phase after anvil is up: `kardamom-deploy
  deploy --rpc http://<control>:8546 --l2-chain-id <id> --l2-minter <batcher
  EOA> KardamomL2Settlement`, run from the host (the deployer binary joins
  `LOAD_BIN`/`REREP_BIN` in the CI release stage); the deterministic proxy
  address is passed to the batcher job as `-var settlement_address=…`.
- `batcher.nomad.hcl` becomes `type = "service"` (restart/reschedule/update
  stanzas modeled on da-watcher), with the `channels.toml` template, `--live`,
  `--dry-run=false`, `--cursor-file` + `--da-store` on the `/opt/kardamom`
  volume, and the batcher EOA key injected via job var (anvil dev account #2;
  first key plumbing in `deploy/cluster` — acceptable for the dev cluster,
  flagged for real deployments).
- CLI additions: `--live`, `--log-config`, `--aeron-dir`, `--shards`,
  `--cursor-file`, `--flush-ms`, `--l1-retries`, env `KARDAMOM_SETTLEMENT`.
  `--channel-b-segment` is now required only in offline mode, which is
  unchanged (rereplicate/heal tooling and the offline tests keep working).

### Verification

- Unit: cursor round-trip + reconcile matrix (fresh, stale-cursor re-observe,
  lost-file genesis, foreign-writer fail-stop), close policy, sender CAS
  reconcile against a mock provider.
- Cluster: new `kardamom-semantics` case `l1-batch` in the semantics shard —
  polls `lastBatchIndex > 0`, then asserts `BatchPosted` ranges are **dense
  from block 1** and catch up to within a slack of the executed head. Root
  parity (reconstruct-from-L1 == validator root) stays in the existing
  `chain-semantics-e2e` drill (`s8_da_parity_batcher_matches_validator`).
