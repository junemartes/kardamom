# Empty-poll parking of the Aeron command wait (2026-10-28)

This bench measures `ParkIdle` in `crates/log/src/aeron_live/thread/command_wait.rs`. `ParkIdle` is the command wait of the Aeron runtime thread when the idle cap is 1000 µs or more.

- Baseline source: `b9bf2222d2ca07df97a9268b677512f590547374`. Every wait uses `Receiver::recv_timeout`.
- Candidate source: `03acbe82482e5cfce2090daadaa37945a2939f1f`. An empty wait under a cap of 1000 µs or more uses `Select::select_timeout`. A wait after useful work, and every wait under a shorter or unset cap, keeps `recv_timeout`.

## Mechanism

- In crossbeam-channel 0.5.15, `Receiver::recv_timeout` spins with `Backoff::snooze` before it parks (`flavors/array.rs` and `flavors/list.rs`).
- `Select::select_timeout` tries the receiver once, and then parks at once. `Context::wait_until` has no spin.
- So an empty wait under `ParkIdle` does not spin.

## Setup

- The test is `unavailable_outputs_preserve_live_delivery_and_recover` in `crates/log/tests/transport_pressure.rs`. It uses one Aeron media driver in Docker and some Rust Aeron runtimes in one process.
- The test runs these phases in order: `idle` (3 s with no traffic), `quiet_arrivals_connected`, `connected`, `outputs_unavailable_idle`, `quiet_arrivals_outputs_unavailable`, `outputs_unavailable`, `output_rejoin`, and `recovered`.
- Idle caps: `KARDAMOM_IDLE_CAP_US=1000` and `KARDAMOM_IDLE_CAP_US=50`.
- Three alternating pairs (baseline, candidate) for each cap.
- Host: a shared 12-CPU workstation. The Rust process is pinned to CPUs 0-3. The Java driver is not pinned. Other agents shared the host.
- CPU is the user and system time of the Rust process, from `/proc/self/stat`, at 100 clock ticks each second. It does not include the Java driver.
- The driver image is `sha256:5f091d6456a75476de0e26ca66c037969d87c32f1fa8b0ee692e4ea10b4ff2dc`.
- The record does not give the number of runtimes or frames. The defaults of the test are 8 runtimes and 256 frames for each traffic phase.

## Results: idle cap 1000 µs

Each value is the mean of three trials. CPU is in cores.

| Phase | Baseline CPU | Candidate CPU | Baseline msg/s | Candidate msg/s | Baseline p99 | Candidate p99 |
|---|---|---|---|---|---|---|
| idle | 0.56 | 0.17 | 0 | 0 | - | - |
| quiet_arrivals_connected | 0.59 | 0.19 | 82.1 | 82.2 | 2368 µs | 2372 µs |
| connected | 0.80 | 0.36 | 5947 | 5599 | 347 µs | 348 µs |
| outputs_unavailable_idle | 2.31 | 0.82 | 0 | 0 | - | - |
| quiet_arrivals_outputs_unavailable | 2.34 | 0.80 | 85.6 | 86.2 | 1883 µs | 1462 µs |
| outputs_unavailable | 2.07 | 1.01 | 4319 | 5548 | 675 µs | 459 µs |
| output_rejoin | 0.63 | 0.20 | 8.6 | 8.7 | 128 ms | 128 ms |
| recovered | 0.85 | 0.40 | 6047 | 5882 | 391 µs | 378 µs |

## Results: idle cap 50 µs

The candidate keeps `recv_timeout` under this cap, so the code path is the same. The table shows the noise of the host.

| Phase | Baseline CPU | Candidate CPU | Baseline msg/s | Candidate msg/s | Baseline p99 | Candidate p99 |
|---|---|---|---|---|---|---|
| idle | 0.24 | 0.24 | 0 | 0 | - | - |
| quiet_arrivals_connected | 0.26 | 0.26 | 86.1 | 86.3 | 1285 µs | 1596 µs |
| connected | 1.22 | 1.11 | 26739 | 25258 | 152 µs | 118 µs |
| outputs_unavailable_idle | 0.65 | 0.64 | 0 | 0 | - | - |
| quiet_arrivals_outputs_unavailable | 0.65 | 0.65 | 86.9 | 87.2 | 1328 µs | 1209 µs |
| outputs_unavailable | 1.15 | 1.15 | 28987 | 26751 | 84 µs | 160 µs |
| output_rejoin | 0.53 | 0.50 | 6.2 | 7.5 | 116 ms | 82 ms |
| recovered | 1.12 | 1.24 | 27564 | 29930 | 211 µs | 100 µs |

## Limits

- The host is shared, and the Java driver is not pinned.
- The messages are serialized 1 KiB transport frames. They are not a transaction load.
- Three pairs and 64 quiet-arrival samples do not prove a production p99 or statistical equivalence.
- These trials do not qualify production latency, rate, state or validator coverage.

## Reproduce

Run this command once with the baseline source and once with the candidate source. Alternate the order three times for each cap.

```sh
KARDAMOM_IDLE_CAP_US=1000 taskset -c 0-3 \
  cargo test --release -p kardamom-log --features docker-e2e --test transport_pressure \
  -- --ignored --exact unavailable_outputs_preserve_live_delivery_and_recover --nocapture
```

- Each phase prints one `TRANSPORT_SAMPLE` JSON line. It gives the seconds, the CPU ticks, the message count, and p50, p95 and p99.
- CPU in cores is `process_cpu_ticks / 100 / seconds`.
- `KARDAMOM_PROFILE_RUNTIMES` and `KARDAMOM_PROFILE_FRAMES` set the number of runtimes and frames.
