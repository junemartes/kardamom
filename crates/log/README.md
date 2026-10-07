# kardamom-log

`kardamom-log` is the Aeron transport layer of the pipeline. It has the application channels, the archive recorders, the archive refetch client and the publisher discovery.

- The canonical order is not in this crate. The Aeron Cluster (the Java sealer) holds it. The ingress gates its ack on the cluster egress progress.
- This crate owns the transport only. The wire data types live in `kardamom-types`. The crate re-exports them as `log::types::*`. Do not add wire types here.

## Owned components

| Component | Module | What it does |
|---|---|---|
| Channel handles | `aeron_live` | Typed publisher and subscriber handles for each application channel. One dedicated Aeron thread runs behind each `AeronRuntime`. |
| Offer retry | `aeron_live` | Retries a parked offer until it succeeds or 5 s pass. A best-effort publish drops the record instead. `kardamom_log_best_effort_dropped_total{stream_id}` counts the drops. |
| Recorders | `recorder` | Record a stream in the local Aeron Archive: `tx_data` (`RecorderKind::TxData`) and `tx_deposits` (`RecorderKind::TxDeposits`). |
| Archive refetch | `refetch` | Fetches a missed range of `tx_data` or `tx_deposits` from a remote archive. |
| Discovery | `discovery` | The Consul-based discovery of the dynamic MDC transport. |
| Archive catalog paging | `archive_catalog` | Pages the recording catalog of an archive for one stream id. Internal. |
| Configuration | `config` | `LogConfig`, loaded from TOML. |
| Codec | `codec` | The `rkyv` helpers: `codec::access` and `codec::materialize`. |
| Test support | `testing` | In-memory fakes, and a Docker-driven real-Aeron cluster. |

### Channels

The `[channels]` section of `LogConfig` names the channels. Every default is an IPC channel for a single host.

| Channel | Publisher | Notes |
|---|---|---|
| `TxData` (one per sequencer lane) | Ingress | Full `TxEnvelope` bytes. Recorded by the archive of each ingress node. |
| `TxReceipts` and its boundary stream | Executor | Receipts and block boundaries. RAM only. With `tx_receipts_control_channel` set, the receipts use a multi-destination fan-in: each executor replica has its own UDP endpoint. |
| `TxErrors` | Sequencer | Rejection signals. RAM only. |
| `TxStatus` | Sequencer, ingress | One record for each step of a transaction. Best effort. RAM only. |
| `TxDeposits` | DA watcher | Full `Deposit` envelopes. Recorded by the archive of the DA watcher node. |
| `TxRemoteEpochs` | Interop watcher | One record for each peer-chain origin block with cross-chain messages. RAM only. |
| `TxBal` | Executor | The block access list (`BlockDelta`) of each block. RAM only. |
| Per-recorder fsync watermark | A recorder | Typed handles exist. The ingress subscribes to it for the local-fsync ack policies. |

- `LogConfig` accepts any subset of the keys. A missing key takes the built-in default.
- An unknown key is an error. The loader (`LogConfig::from_toml_path`) also checks the cross-field rules of `[channels]` and `[discovery]`.
- Service binaries use `LogConfig::resolve` behind the `--log-config` flag.

### Discovery

With discovery on, Consul is the control plane of the dynamic MDC transport.

- A publisher binds a dynamic MDC publication and registers its control endpoint.
- A subscriber watches the catalog. It attaches one destination for each publisher to its multi-destination subscription.
- Messages travel directly between the Aeron media drivers. Consul never relays or orders a message.
- The `[discovery]` section of `LogConfig` sets the Consul agent, the scope and the timing.
- The instance identity comes from `NOMAD_ALLOC_ID`. The media driver binds the control port of each publication, and the publisher record carries the bound address.
- The discovery-driven recorder records every publisher of a topic that the catalog lists. Each publisher gets its own recording.
- With discovery off, every handle opens on its static `[channels]` URI.
- See [`docs/aeron-discovery.md`](../../docs/aeron-discovery.md) for the contract.

### Archive refetch

The multicast side streams (`tx_data`, `tx_deposits`) are lossy for a subscriber that missed frames. The durable copies are on the archives of other nodes. `ArchiveRefetcher` reads them.

- `fetch_tx_data` and `fetch_deposits` run a bounded replay of `[from, recorded position)` from a remote archive. They deliver the records to the caller.
- The replay is session-keyed. It pins the session id of the original publisher. A restarted publisher is never confused with its predecessor.
- The refetcher has its own `AeronRuntime` and its own archive control session. A slow refetch never starves the live polling.
- All resources are lazy. Nothing exists until the first miss.
- The archive endpoints come from a static list (`tx_data_archive_endpoints`, `tx_deposits_archive_endpoints` in `[aeron]`) or from the discovered archive records. An empty static list disables the refetch.
- The refetcher rotates through the endpoints on a failure.

## Durability model

The recording position of the Aeron Archive is byte-durable only when the archive syncs each frame.

1. The cluster deploy runs the archive daemon with `aeron.archive.file.sync.level` and `aeron.archive.catalog.file.sync.level` at 1. At level 1 the daemon runs `fdatasync` on each recorded write batch before the recording position advances past it.
   - The crate does not start the archive daemon. The `aeron` Nomad job sets both levels from its variable `archive_file_sync_level` (default 1). The operator sets it with `KARDAMOM_ARCHIVE_FILE_SYNC_LEVEL`.
   - Level 0 leaves a recording in the page cache. Level 2 also syncs the metadata.
   - `AeronConfig::file_sync_level` and `catalog_file_sync_level` hold the intended levels. Both default to 1. No code reads them to start a daemon.
2. The ingress archives record `tx_data`. The DA watcher archive records `tx_deposits`. The executors use these recordings to rebuild a missed range (see Archive refetch).

For survival of a correlated power loss, point `archive_dir` at enterprise NVMe with power-loss protection (PLP). Without PLP, `fdatasync` only flushes to the cache of the device.

## Feature matrix

| Feature | What it enables | Requires at compile time |
|---|---|---|
| (default) | Everything: publishers, subscribers, recorders, discovery and refetch | cmake, JDK 17 and the Aeron C build |
| `testing` | Adds `log::testing::Fake*`, the in-memory fakes | Nothing extra |
| `docker-e2e` | Implies `testing`. Adds `AeronTestCluster`. | Docker at runtime |

- The `rusteron-client` and `rusteron-archive` dependencies are unconditional.
- There is no `aeron-live` feature in this crate.
- The `docker-e2e` harness uses `docker/aeron/`. A testcontainers setup builds and runs the Aeron image on demand. Other crates reuse the harness in their e2e tests.

## Wire codec

The codec is `rkyv` v0.8 with zero-copy archived access.

- A hot-path consumer calls `codec::access` to get an `&Archived<T>` view with no allocation.
- A caller that needs an owned value calls `codec::materialize`.

## Replay

This crate has no custom replay API for the canonical order. Two replay paths exist:

- The refetch client replays `tx_data` and `tx_deposits` ranges. See Archive refetch.
- An offline consumer, such as the batcher, reads the archive segment files directly. It can also use the built-in replay of the Aeron Archive.

## Runtime dependencies

- Production hosts need the Aeron Media Driver and the Aeron Archive (Java or C). The archive must support `fileSyncLevel`.
- The `docker-e2e` tests need a working Docker daemon (`docker info` must succeed).
- For production, use enterprise NVMe with PLP for `archive_dir`, on a disk separate from the OS disk.
