# kardamom-log

S3 canonical-log subsystem. See `` §2.3 and §2.5, and `` / / /.

## Owned components

- **TxOrdering** (canonical tx log, recorded, fsync-quorum durable)
- **TxReceipts** (receipts + block boundaries, RAM only)
- **Receipt-cache channel** (`CachedReceipt` stream, RAM only)
- **Per-recorder fsync-watermark stream** (published from the Aeron Archive's recording position, which is byte-durable when the archive runs with `fileSyncLevel=1`)
- **Quorum fsync-watermark aggregator** (Q-of-N largest position)
- **`testing` feature** — in-memory pub/sub fakes for other crates' unit tests
- **`docker-e2e` feature + `docker/aeron/`** — testcontainers-driven Aeron Docker harness; reusable by other crates' e2e tests
- **`aeron-live` feature** — gates the real rusteron-backed publishers / subscribers / recorder

## Durability model

TxOrdering durability comes from two things working together:

1. The Aeron Archive daemon is launched with `aeron.archive.file.sync.level=1` (and the same for the catalog file). Every recorded frame is `fdatasync`'d to local storage before the recording position advances past it. The defaults live in [`config::AeronConfig`]; the supervisor exports the value via both the `AERON_ARCHIVE_FILE_SYNC_LEVEL` env var (C archive) and the `-Daeron.archive.file.sync.level` system property (Java archive).
2. The per-recorder watermark loop polls the archive's recording position and republishes it. N recorders feed the quorum aggregator, which publishes the Q-th largest position as the durability watermark proxies use for the I2 ack guarantee.

For correlated power-loss survival, point `archive_dir` at enterprise NVMe with PLP — without it, `fdatasync` only flushes to the device cache.

## Feature matrix

| feature       | what it enables                                                   | requires at compile time            |
|---------------|-------------------------------------------------------------------|-------------------------------------|
| (default)     | codec, config, supervisor, watermark, types                       | rust                                |
| `testing`     | adds `kardamom_log::testing::Fake*`                               | (none extra)                        |
| `aeron-live`  | adds `publisher`, `subscriber`, `recorder`                        | cmake + JDK + Aeron C build         |
| `docker-e2e`  | implies `testing`, adds `AeronTestCluster`                        | docker at *runtime*                 |

## Shared types

Wire types live in `kardamom-types`; this crate re-exports them via `kardamom_log::types::*` for convenience. Do not add new wire types here.

## Wire codec

`rkyv` v0.8 zero-copy archival serialization. Hot-path consumers use `codec::access` for an `&Archived<T>` view; callers needing an owned value call `codec::materialize`.

## Replay

We do **not** ship a custom tx_ordering replay API. Aeron Archive already exposes the standard replay protocol; offline consumers (S7 L1 batcher) read segment files directly or use Aeron Archive's built-in replay.

## Runtime dependencies

- Aeron Media Driver and Aeron Archive binaries (Java or C) installed on each host for production. The archive must support `fileSyncLevel`.
- For e2e tests (`docker-e2e` feature): a working Docker daemon (`docker info` must succeed). The testcontainers harness builds and runs the Aeron image on demand.
- Recommended for production: enterprise NVMe with PLP for `archive_dir`, separate from the OS disk. Without PLP, `fdatasync` only reaches the device write cache.
