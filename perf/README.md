# Performance & allocation tracking

## Numbers at a glance (updated per change; history in git)

| path | allocs/op | bytes/op | measured wall | notes |
|:--|--:|--:|--:|:--|
| engine execute_tx (DeFi mix) | 16.5 | 3,672 B | ~55-60 us | 1-core ~806 Mgas/s; remaining bytes are revm internals |
| sequencer run_once | 3.0 | 504 B | ~6 us | post metrics-handles + nonce-peek |
| ingress submit_raw | 7.3 | 4,254 B | ~44-48 us | post parked-index + Arc cache + scratch reuse |

Cluster-level: see `docs/agents/2026-08-01-bal-phase1-measurement.md` and
the DeFi/gigagas runs in `docs/agents/2026-08-03-allocation-report.md`.
CI's cluster shard logs Mgas/s per ramp step (transfer + DeFi stages).

## How tracking works

- **Harnesses** (DHAT, in-process, deterministic):
  `crates/bench/tests/alloc_profile.rs` (engine, per-op-family via
  `KARDAMOM_PROFILE_OPS`), `crates/sequencer/tests/alloc_profile.rs`,
  `crates/bench/tests/alloc_profile_ingress.rs`. Run any of them:
  `cargo test -p <crate> --test <name> --release -- --ignored --nocapture`.
- **CI gate**: `deploy/ci/alloc-gate.sh` (the `alloc-gate` job in
  `ci.yml`) runs all three and FAILS on any allocs/op or bytes/op above
  the ceilings in `perf/alloc-baselines.env`. Allocation counts are
  deterministic, so the gate is tight (~15% headroom); wall time is
  printed but never gated (machine noise).
- **Changing a baseline**: if a PR legitimately changes an allocation
  profile, update `perf/alloc-baselines.env` in the same PR and justify
  the change in the description.
- **Per-callsite attribution**: each harness writes a `dhat-heap*.json`
  viewable with DHAT's `dh_view.html`; the analysis recipe is in
  `docs/agents/2026-08-03-allocation-report.md`.
