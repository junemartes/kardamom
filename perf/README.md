# Performance and allocation tracking

This directory holds the allocation ceilings (`alloc-baselines.env`) that CI enforces. The numbers below are the last measured values.

## Numbers at a glance

| Path | allocs/op | bytes/op | Wall time | Notes |
|:--|--:|--:|--:|:--|
| Engine `execute_tx` (DeFi mix) | 16.5 | 3,672 B | ~55-60 us | One core runs ~806 Mgas/s. The remaining bytes are revm internals. |
| Sequencer `run_once` | 3.0 | 320 B | ~6 us | The publish vector has its exact size. |
| Ingress `submit_raw` | 7.3 | ~4,900 B | ~44-48 us | Bytes vary from 4,863 to 4,939 B across CI runs. |

- The allocation counts are deterministic. The wall times depend on the machine.
- For cluster-level numbers, see [`docs/agents/2026-08-01-bal-phase1-measurement.md`](../docs/agents/2026-08-01-bal-phase1-measurement.md) and the DeFi and gigagas runs in [`docs/agents/2026-08-03-allocation-report.md`](../docs/agents/2026-08-03-allocation-report.md).
- The cluster shard of CI logs Mgas/s for each ramp step (the transfer and DeFi stages).

## How tracking works

- **Harnesses.** The DHAT harnesses run in process and are deterministic:
  - `crates/bench/tests/alloc_profile.rs` measures the engine. `KARDAMOM_PROFILE_OPS` selects the operation family (the gate uses `mix`).
  - `crates/sequencer/tests/alloc_profile.rs` measures the sequencer.
  - `crates/bench/tests/alloc_profile_ingress.rs` measures the ingress.
  - Run one with `cargo test -p <crate> --test <name> --release -- --ignored --nocapture`.
- **CI gate.** `just alloc-gate` runs all three harnesses. The `alloc-gate` job in `.github/workflows/ci.yml` runs the same recipe.
  - The gate fails when allocs/op or bytes/op exceeds a ceiling in `perf/alloc-baselines.env`.
  - The ceilings are the measured steady state plus about 15% headroom.
  - The gate prints the wall time. It never gates on it.
- **Changing a baseline.** A change that moves an allocation profile updates `perf/alloc-baselines.env` in the same pull request. The description says why.
- **Per-callsite attribution.** Each harness writes a `dhat-heap*.json` file. Open it with `dh_view.html` of DHAT. The analysis recipe is in [`docs/agents/2026-08-03-allocation-report.md`](../docs/agents/2026-08-03-allocation-report.md).
