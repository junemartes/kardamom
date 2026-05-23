# kardamom-log

S3 canonical-log subsystem. See `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §2.3 and §2.5, and `docs/plans/2026-05-23-S0-shared-decisions.md` D-Sh1 / D-Sh2 / D-Sh8 / D-Sh10.

## Owned components

- **Channel B** (canonical tx log, recorded, fsync-quorum durable)
- **Channel C** (receipts + block boundaries, RAM only)
- **Receipt-cache channel** (`CachedReceipt` stream, RAM only)
- **Per-recorder fsync sidecar** (io_uring + O_DIRECT mirror)
- **Per-recorder fsync-watermark stream**
- **Quorum fsync-watermark aggregator** (Q-of-N largest position)
- **`testing` feature** — in-memory pub/sub fakes for other crates' unit tests
- **`docker-e2e` feature + `docker/aeron/`** — testcontainers-driven Aeron Docker harness; reusable by other crates' e2e tests
- **`aeron-live` feature** — gates the real rusteron-backed publishers / subscribers / recorder / Aeron position source

## Feature matrix

| feature       | what it enables                                                   | requires at compile time            |
|---------------|-------------------------------------------------------------------|-------------------------------------|
| (default)     | codec, config, fsync sidecar, supervisor, watermark, types        | rust + io-uring (Linux)             |
| `testing`     | adds `kardamom_log::testing::Fake*`                               | (none extra)                        |
| `aeron-live`  | adds `publisher`, `subscriber`, `recorder`, `AeronPositionSource` | cmake + JDK + Aeron C build         |
| `docker-e2e`  | implies `testing`, adds `AeronTestCluster`                        | docker at *runtime*                 |

## Shared types

Wire types live in `kardamom-types`; this crate re-exports them via `kardamom_log::types::*` for convenience. Do not add new wire types here.

## Wire codec

`rkyv` v0.8 zero-copy archival serialization. Hot-path consumers use `codec::access` for an `&Archived<T>` view; callers needing an owned value call `codec::materialize`.

## Replay

We do **not** ship a custom channel-B replay API. Aeron Archive already exposes the standard replay protocol; offline consumers (S7 L1 batcher) read segment files directly or use Aeron Archive's built-in replay (D-Sh10).

## Runtime dependencies

- Aeron Media Driver and Aeron Archive binaries (Java) installed on each host for production.
- For e2e tests (`docker-e2e` feature): a working Docker daemon (`docker info` must succeed). The testcontainers harness builds and runs the Aeron image on demand.
- Mirror file must be on an ext4/xfs/etc. filesystem that supports `O_DIRECT`. tmpfs returns `EINVAL` for `O_DIRECT` opens.
- Recommended for production: enterprise NVMe with PLP, separate from the OS disk.
