# Incremental checkpoints

- Status: proposed on 2026-10-22. The user decided to use incremental checkpoints, and to make
  the receive window of the sealer log channel larger (a separate change). This spec covers only
  the checkpoints.
- Cause: the seal p99 waves of the staging spike of 2026-10-21 (see section 1).

## 1. The problem

The executor writes a full compacted copy of its state database every 20 s
(`--checkpoint-interval-secs 20`, `create_checkpoint` in `crates/state/src/checkpoint/mod.rs`).

- On staging the copy is about 550 MB, and it grows with the chain.
- On the box with spinning disks (ded-3), one copy took 2.7 s at the start of the spike and 16 s
  near its end. At the end, the copies ran almost all the time.
- sealer-0 shares that disk. Its archive syncs each write, and these syncs wait behind the copy.
  With the default flow control of the log channel, the leader then stops too. 35 of the 44
  seconds with a seal p99 above 1 s were inside a checkpoint of executor-0.
- The NVMe boxes also write about 48 MB/s for the checkpoints, and are 60-90 % busy.

A full copy of the whole state at a fixed short period cannot scale with the state. The cost must
follow the rate of change, not the size of the state.

## 2. What the checkpoints do today

| Use | Code | What it needs |
|---|---|---|
| Cold start of a wiped executor | `restore_if_fresh` (executor `state.rs`), `bin_support::restore_or_fetch_checkpoint` | The newest checkpoint, local or of a peer |
| Rollback after a divergence | `RollbackRepair` (executor `rollback.rs`) | The newest checkpoint below the divergence block |
| Resync after a retention overrun | `bin_support`, `fetch_best_checkpoint` | A peer checkpoint at or above the retention floor |
| Validator adoption | `adoption.rs` (validator) | A peer checkpoint, then a trie bootstrap |
| State mirror rebuild | `state-mirror/src/rebuild.rs` | A checkpoint at or above the first live batch |
| Peer transfer | `checkpoint_transfer.rs` | `GET /checkpoint/latest[?max_block=]` serves one full image |

The short period exists for two of these uses:

- A rollback needs a checkpoint just below the divergence block. A double vote voids the last
  seconds of the log, so the checkpoint must be recent.
- A state mirror rebuild waits until a checkpoint reaches its first live batch.

## 3. The design

### 3.1 A checkpoint is a base and a block log

- A **base** is a full compacted image, as today: a `checkpoint-<block>` directory with
  `mdbx.dat` and a manifest. Each existing checkpoint is a valid base. The format of the base
  does not change.
- The **block log** holds the write batch of each committed block: the boundary, the
  `BlockDelta`, and the transaction references. This is the `WriteBatch` that the state writer
  commits (`crates/state/src/writer/mod.rs`).
- A **restore point** is any block `n` that has a base at block `b <= n` and a log record for
  each block from `b + 1` to `n`, with no gap.
- A restore copies the base, then applies the log records from `b + 1` to `n` through the same
  `apply` function as the live writer. So the restored state is the state that the writer made.
  The executor writer runs with `TrieMode::Off`, and the restore applies with the same mode.

### 3.2 The block log on disk

- The log is in `<checkpoint_dir>/log/`, in segment files. The name of a segment is its first
  block, in 20 digits, with the suffix `.blocks`. So the names sort in block order.
- A segment starts with a head: a magic value, the format version, the genesis digest, the state
  schema version, and the first block.
- Then each record follows: `[len:u32][block:u64][crc32c:u32][rkyv bytes of the batch]`, little
  endian.
- A reader stops at the first torn or bad record. At start, the log writer cuts the last segment
  after its last good record.
- A segment ends at a size limit (default 64 MiB). The next segment starts at the next block.
- A gap is a block with no record. After a gap, a new segment starts at the first block after
  the gap. So the start of each segment shows the gap, and a reader needs no other mark.
- The log writer syncs the segment once a second. The state database stays the source of truth.
  The log needs to be durable only so that a restore after a crash finds it.

### 3.3 The log thread

- The writer thread commits a batch, publishes the snapshot, and then sends the owned batch to
  the log thread. It sends no copy of the batch, so the hot path gets no new allocation.
- The channel is bounded (default 4,096 blocks, about 2 hours of 2 s blocks).
- The writer never waits for the log. When the channel is full, the writer drops the batch, and
  the log has a gap. The executor counts `kardamom_checkpoint_log_dropped_total`, and the next
  base starts at once. A slow disk then costs restore points, but never slows the state commit.
- At start, the log thread compares the end of the log with `last_committed_block` of the state
  database:
  - The log ends at the database block: the log continues.
  - The log ends below the database block (a crash between two syncs): the blocks between are a
    gap. A new base starts at once.
  - The log ends above the database block: the log thread cuts the records above the database
    block. The executor executes those blocks again.

### 3.4 When a new base starts

- A new base starts when the log after the newest base is larger than that base (the ratio is a
  setting, default 1.0), or when the newest base is older than a limit (default 6 h), or after a
  gap.
- The checkpointer keeps the newest `--checkpoint-keep` bases (default 2) and the log segments
  that they need. It deletes a segment only when every block in it is at or below the oldest
  kept base.
- **The copy of a base has a rate limit** (default 16 MiB/s). The copy uses
  `mdbx_env_copy2fd` with `MDBX_CP_COMPACT` into a pipe. A thread reads the pipe and writes the
  image file at the limit. The copy still reads from an online read-only snapshot, so it does
  not block the writer. libmdbx refuses a copy into a pipe when the database has a page leak.
  Then the copy writes the file directly with no limit, and counts
  `kardamom_checkpoint_copy_unlimited_total`. A long read transaction keeps old pages alive in the database, so the
  rate limit has a lower bound: the copy of a base must end before the next base starts.

Estimate for staging at 1,000 tx/s:

| | Today | With the log |
|---|---|---|
| Write rate of the checkpoints | 550 MB every 20 s, about 27 MB/s | log about 0.6 MB/s, base about 0.5 MB/s on average |
| Largest burst | the full copy at disk speed | 16 MiB/s during a base copy |
| Restore points | one each 20 s, 3 kept | every block since the oldest kept base |

The log size uses `WriteBatch::approx_size_bytes`: about 550 bytes for each transfer, most of it
the receipt. At 1,000 tx/s a new base starts about every 17 minutes.

### 3.5 Restore

`restore_point(n)` replaces the checkpoint lookup. It returns the newest base at or below `n` that
has a gap-free log up to `n`, or up to the end of the log when `n` is above it.

- **Cold start:** the newest restore point.
- **Rollback at block `d`:** the restore point `d - 1`. The rollback then cuts the log at block
  `d`, so the log continues with the blocks of the new stream. Today the rollback goes back to
  the newest checkpoint below `d`, up to 20 s and more. With the log it goes back exactly one
  block.
- **Retention overrun and validator adoption:** a peer restore point at or above the floor.
- **State mirror rebuild:** the newest restore point. The log ends close to the head, so the
  rebuild does not wait for a new checkpoint. The mirror reads the executor's directory and
  never writes to it. It reads the log up to the last whole record.

The restore applies the log in a temporary directory with no sync for each block, then syncs
once and renames the directory into place. A crash during the restore leaves no partial state.

### 3.6 Peer transfer

- `GET /checkpoint/latest[?max_block=]` does not change. It serves a full base image. A client of
  an older release gets a base. That base is older than the newest checkpoint of today, so the
  client replays a longer tail.
- A new request, `GET /checkpoint/log?from_block=<b>&to_block=<n>`, streams the log records from
  `b` to `n`, or to the end of the log of the server. The server answers `404` when its log does
  not hold block `b`. A server of an older release answers `404` for the unknown path.
- A new client fetches a base, then the log after it from the same peer. With a `404` for the log,
  the client uses the base alone.
- The resync fallback needs a restore point at or above the retention floor. The client asks for
  the log up to the floor at least, and refuses the peer when the log ends below the floor.

### 3.7 Settings

| Flag | Default | Meaning |
|---|---|---|
| `--checkpoint-log` | `false` | Write the block log and make bases by the rule of section 3.4. `false` keeps the full copy each `--checkpoint-interval-secs`. |
| `--checkpoint-base-ratio` | `1.0` | A new base when the log after the newest base is this many times the size of that base. |
| `--checkpoint-base-max-age-secs` | `21600` | A new base when the newest base is older than this. |
| `--checkpoint-copy-mib-per-sec` | `16` | The rate limit of a base copy. |
| `--checkpoint-log-segment-mib` | `64` | The size of one log segment. |
| `--checkpoint-keep` | `2` with the log | The bases to keep, with their log. |

## 4. Upgrade and rollout

- No chain rule changes, and no wire format of the chain changes. The block log is a new local
  file format and goes into `formats.toml`. The base keeps the format of today's checkpoint.
- The flag is off by default. An executor of an older release still reads every base, because a
  base is a checkpoint of today.
- Rollout:
  1. Deploy with the flag off.
  2. Turn on the flag on staging, one executor at a time.
  3. Measure with a 10-minute spike at 1,000 tx/s, and compare the seal p99 with the spike of
     2026-10-21.
  4. Make the flag the default.
- A downgrade ignores the log. It uses the newest base as a full checkpoint.

## 5. Tests

- Unit tests in `crates/state`:
  - a restore from a base and its log gives the same database as the live writer, table by
    table (`deep_compare` in `crates/state/src/integrity`);
  - a torn last record is cut at start;
  - a gap ends the restore points before it, and the next base starts at once;
  - a full channel drops a batch, and the writer does not wait;
  - a rollback cuts the log at the divergence block;
  - the log after a crash: the three cases of section 3.3;
  - the rate limit of the base copy.
- Peer transfer: an old client with a new server, a new client with an old server, and the
  retention floor rule.
- The chaos cases that use checkpoints run with the flag on: `state-checkpoint-restore`,
  `replay-window-resync`, `retention-overrun`, the rollback cases, the state mirror rebuild and
  the validator adoption. A new case rolls back to the block before the divergence, and asserts
  that the restore point is that block.

## 6. Implementation plan

Each item is one pull request. Each passes `just style` and CI alone.

| PR | Title | Depends on |
|---|---|---|
| I1 | state: the block log format, its writer and its reader, and the `formats.toml` entry | none |
| I2 | state: the log thread behind the state writer, the gap rule and the start check | I1 |
| I3 | state: restore points: a base and its log; the rollback cuts the log | I2 |
| I4 | state: the base rule and the rate-limited base copy | I2 |
| I5 | state: the log in the peer transfer | I3 |
| I6 | executor, validator, state mirror: the settings and the use of restore points; the deploy flag, off | I3, I4, I5 |
| I7 | chaos: the cases of section 5 with the flag on; staging on; the spike | I6 |

## 7. Not in this spec

- The larger receive window of the sealer log channel (`rcv-wnd`, `so-rcvbuf`). It is a separate
  change in the sealer configuration.
- The move of sealer-0 to another machine. The user does it.
- Compression of the log. The receipts take most of the bytes and compress well. Add it when the
  log size matters.
