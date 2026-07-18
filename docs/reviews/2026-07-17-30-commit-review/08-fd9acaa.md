# 08 fd9acaa — feat(obs): revive sealer observability via executor re-export from cluster egress (#69)

## Summary of change
The clustered Java sealer has no Prometheus endpoint, leaving the kardamom-sealer dashboard, the load-harness sealer-vs-executor gap checks, and the smoke/chaos probes pointed at a dead `:9003`. Instead of deleting them, the executor now re-exports the sealer's output as it decodes cluster egress: each Boundary frame bumps `kardamom_sealer_boundaries_emitted_total` and sets `kardamom_sealer_block_number`, riding the executor's `:9004` exporter. The load harness drops the dead `--sealer-node`/`:9003` scrape and reads sealer values off the executor scrape (max across nodes), smoke-load.sh and chaos.sh re-point their probes at executor exporters, and the dashboard swaps dead panels for an executor-lag-behind-sealer panel.

## Findings

### F08.1 [medium] [logic] — chaos.sh sealer_boundaries() returns the FIRST responding executor's counter, which may be a stalled replica's frozen value
- **Where**: `deploy/cluster/scripts/chaos.sh` at the commit (sealer_boundaries(), ~lines 141-151): loops `EXECUTOR_NODES` and returns on the first non-empty scrape. At HEAD: `deploy/cluster/scripts/chaos.sh:148-162` (rewritten).
- **What**: `assert_progress()` prefers this probe. An executor that is alive enough to answer `/metrics` but whose egress subscription is stalled or that just restarted (counter reset to 0/frozen — exactly the states chaos induces) satisfies the "first responder" condition and reports a non-advancing counter, failing the verdict as "pipeline NOT progressing" while the sealer and its peers are fine. The same reset-during-recovery flake was already known for the block gauge (the commit's own load.rs takes MAX across nodes for precisely this reason) but the shell probe didn't get the same treatment — an inconsistency that later manifested as chaos flakiness.
- **Still present at HEAD**: no (fixed by 3e6de2a, which takes the MAX across all responding executors with an explicit comment giving this rationale)
- **Suggested fix**: Already fixed; nothing further.

### F08.2 [low] [quality] — "Emission is executor-side only" is inaccurate: the validator shares the emitting subscription and also publishes kardamom_sealer_* series
- **Where**: commit message + `crates/engine/src/metrics.rs:11-16` comment ("the executor re-exports…") and emission in `crates/engine/src/reader/cluster.rs` (at the commit :40-47; at HEAD inside `try_deliver`, ~:96-99).
- **What**: The commit deliberately avoided emitting in the shared cluster-adapter to prevent double-counting with the ingress watermark observer, but placed the emission in `ClusterTxOrderingSubscription` in the shared `engine` crate — which the validator also uses for its canonical stream. Every validator therefore exports `kardamom_sealer_block_number`/`_boundaries_emitted_total` on its own exporter too, with lagging values. Current consumers are unaffected (chaos/smoke/load probe executor nodes only; dashboard joins are per-`host_id` or max-based), but any future `sum()` over the series across scraped hosts double-counts, and the docs/comments describing the observation point are wrong about who emits.
- **Still present at HEAD**: yes (validator still uses the shared subscription; emission still in the engine reader)
- **Suggested fix**: Either gate the emission on the consumer role (e.g. a constructor flag on `ClusterTxOrderingSubscription`), or update docs/observability.md and the metrics.rs comment to state that every canonical-stream consumer (executor and validator) re-exports the series and that queries must be per-host or max-based.

### F08.3 [nit] [logic] — Counter bumps on every decoded Boundary frame, including reconnect-overlap duplicates
- **Where**: `crates/engine/src/reader/cluster.rs:40-47` at the commit.
- **What**: At this commit the subscription had no dedup (the module header explicitly notes reconnect overlap can re-deliver frames), so `kardamom_sealer_boundaries_emitted_total` over-counts across reconnect overlaps — a liveness signal, so harmless in practice, but the counter's name promises "emitted" semantics it doesn't quite have.
- **Still present at HEAD**: no (fixed by 52a32a2, which moved emission into `try_deliver` so only in-order, deduplicated deliveries count)
- **Suggested fix**: Already fixed.

## Verdict
A sensible, well-scoped observability revival: re-exporting the boundary stream at the egress subscription is the right observation point given the Java service has no exporter, and the commit consistently converts every dead `:9003` consumer (load harness, smoke, chaos, dashboard, docs) rather than leaving half of them stale; the scrape.rs refactor also reduces per-node fetches. The one real defect was the first-responder chaos probe (F08.1), which reintroduced the stalled-replica flake the commit itself fixed on the Rust side with max-across-nodes — later corrected. The only actionable residue at HEAD is documentation/emission-placement drift (F08.2): the series are not, in fact, executor-only.
