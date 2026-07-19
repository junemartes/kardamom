# Fixes — WP-SEQ (sequencer racing replicas)

Findings F02.1, F02.2, F02.4, F02.6, F02.7 from `02-f2accb1.md`.

## Design choice for F02.1/F02.2 (nonce-floor hydration)

Options considered per instructions:
1. **Hydrate floors from a canonical/receipt stream the binary already subscribes to** — not
   available: the deployed binary subscribes only to per-shard `tx_data` and `tx_deposits`;
   there is no receipt/canonical subscription in the sequencer process, so this would add a
   new stream dependency (and its own catch-up/consistency problem: receipts also lag what
   the twin has *ordered*, reintroducing F02.2's race at a different offset).
2. **Fail-fast + degraded-mode alert** — strictly worse operationally: a restarted replica
   that refuses to start leaves the shard at P=1 anyway, which is the exact outage mode the
   commit exists to prevent.
3. **Stream-adaptive floor fast-forward** (chosen; it is also the finding's own suggested
   fix for F02.2): hydration is treated as a *lower bound* only. When a sender's pending
   buffer has held a run strictly above the floor, unchanged, for longer than a configured
   lag bound (`nonce_floor_lag_ms`, default 5000 ms >> ordering/commit latency), the gap
   provably isn't in flight — live-join has no replay and the twin already ordered the
   missing nonces — so the floor adopts the lowest buffered nonce and the run is published.

Why it is safe:
- The floor only ever skips **forward**, so per-publisher nonce order (the property the
  first-seen dedup merge relies on) is preserved.
- Refs the twin already offered are absorbed by the cluster's first-seen dedup — the same
  mechanism that makes racing replicas safe at all. A rejoiner's re-offers are refs it
  observed on live tx_data after subscribing, i.e. at most ~`nonce_floor_lag_ms` (+ buffer
  time) behind the twin's offers. **Interaction with WP-J's F02.3 fix**: with the sealer
  first-seen window raised 8192 → 1<<17, a 5 s worst-case rejoin lag stays inside the
  window up to ~26k unique canonical ids/s — comfortably above the deployment's envelope.
  The one corner outside that bound (a client retransmitting an *ancient* already-ordered
  nonce as the rejoiner's first observation of that sender) can cause one stale re-offer,
  backstopped by the executor's 1<<20 `DedupWindow` — the identical reliance F02.3 already
  documents for paused live replicas; noted here rather than "fixed" because a sequencer
  cannot locally distinguish that case (no freshness horizon was added in this pass).
- If the missing nonces were never ordered by anyone (both replicas of a shard down
  simultaneously — outside the P=2 design's stated failure envelope), the prefix is lost
  with or without fast-forward; fast-forwarding surfaces the gap at the executor instead of
  freezing the sender in a zombie replica forever.
- Floors can still only lag, never lead, the true next nonce, so no valid fresh tx is ever
  spuriously rejected (unchanged from before).

This single mechanism fixes both findings: F02.1 (EmptyStateDatabase floors of 0 now
converge to the live join point, so a restarted replica regains full coverage — the
commit's headline "restarts to full strength" becomes true) and F02.2 (a *real* state DB
whose committed nonce trails in-flight ordering converges the same way; the floor is now
refreshed whenever it stalls, not consulted once).

## Per-finding status

### F02.1 [H] — ~~FIXED~~ REVERTED (RE-OPENED)
> **REVERTED post-CI (run 29687514869)**: the stream-adaptive floor
> fast-forward adopted CLIENT-ABANDONED nonce holes (txs dropped at ingress
> under overload / chaos outages — never on tx_data, so ordered by NOBODY)
> and published canonical nonce gaps; every executor fail-stops on
> `NonceTooHigh`, killing all replicas in all five e2e shards. A sequencer
> cannot locally distinguish that case from the rejoin case this fix
> targeted (the doc below already conceded this). Removed wholesale; a
> rejoined replica again does not regain coverage of established senders
> until a global hydration signal exists. `nonce_floor_lag_ms` still parses
> (unused). See fixes-CI-replay-loop.md round 4. Original text kept below.
Files: `crates/sequencer/src/state.rs`, `crates/sequencer/src/pending.rs`,
`crates/sequencer/src/sequencer.rs`, `crates/sequencer/src/config.rs`,
`crates/sequencer/src/metrics.rs`, `crates/sequencer/src/bin/kardamom-sequencer.rs`,
`crates/sequencer/tests/replicated_shard_racing.rs`.
- `PartitionState::fast_forward_stalled(now, max_lag)`: stall marks keyed on
  `(expected, lowest-buffered)`; any progress (match, or a lower nonce arriving) re-arms
  the clock; on expiry the floor adopts the lowest buffered nonce and the contiguous run is
  drained for publishing. Zero lag fires immediately (test hook).
- `Sequencer::run_once` runs the fast-forward sweep between the retry-drain and the
  tx_data poll; adoptions emit a `warn!` and the new
  `kardamom_sequencer_nonce_floor_fastforward_total` counter (one per adopted sender) —
  this is the signal a chaos assertion can use to verify a *restarted* replica regained
  coverage (`tx_published_to_b_total` advancing works too now; chaos.sh itself is WP-OPS).
- New config knob `nonce_floor_lag_ms` (serde-defaulted 5000, so existing TOMLs including
  `deploy/cluster/config/sequencer.toml.tpl` parse unchanged).
- Bin: `EmptyStateDatabase` comment rewritten to state the actual contract (hydration = a
  lower bound; rejoin correctness comes from the fast-forward; a real committed-state
  reader is now a pure optimization), plus an explicit startup log line.
- Regression test `restarted_replica_with_empty_state_db_regains_coverage`: replica joins
  mid-stream with an **empty** state DB (the production wiring) and must emit exactly its
  twin's suffix; first-seen merge adds nothing. This is the precise zombie scenario.

### F02.2 [M] — ~~FIXED~~ mechanism REVERTED (see F02.1 note above); the adjacent flush_drained data-loss fix below STANDS (with the reinsert-overflow eviction fixed on top — see fixes-CI-replay-loop.md round 3)
Same mechanism (the floor is re-evaluated whenever it stalls, not one-shot). Regression
test `misaligned_hydration_floor_fast_forwards_to_the_join_point` pins the misaligned case
the finding called out as untested: DB floor `c` strictly below the live-join nonce `j`
(committed state trailing the twin's in-flight ordering) — the rejoiner must still emit
exactly the twin's suffix. The pre-existing perfectly-aligned test is kept as-is.

Adjacent data-loss bug fixed in the same function (same failure class as F02.2's
"gap that never heals", not separately numbered in the findings): all three publish paths
previously **dropped the un-published tail** of a drained batch when a mid-batch offer hit
backpressure (only the failed item was re-buffered; items after it had already been drained
out of the pending buffer and `next_nonce` advanced past them → refs permanently lost).
`run_once`'s drain-pending, fast-forward, and ingress-actions paths now share one
`flush_drained` helper that, on backpressure, re-buffers the failed item *and* the whole
remaining tail (in reverse, so each sender's floor rewinds to its lowest unpublished
nonce). Behavior pinned by the existing backpressure tests, which still pass.

### F02.4 [M] — FIXED
Files: `deploy/cluster/nomad/sequencer.nomad.hcl`,
`deploy/grafana/provisioning/dashboards-json/kardamom-sequencer.json`,
`deploy/prometheus.yml` (see boundary note).
- Nomad: both tasks now bind their exporters on `0.0.0.0` (`:9001` seq-a / `:9011` seq-b —
  seq-a previously inherited the loopback binary default too) and stamp
  `KARDAMOM_HOST_ID = node${meta.node_index}-seq-{a,b}`, so every series identifies its
  replica group (the finding's "stamp it via host_id" option; previously all four replicas
  exported `host_id="local"`). Job header documents the max-not-sum aggregation contract.
- Grafana: all stream-derived per-shard panels (`tx_ingested`, `tx_published_to_b`,
  `tx_buffered_future`, `tx_dropped_past`, `pending_evictions`) switched
  `sum by (partition)` → `max by (partition)` so per-shard semantics survive P=2 (both
  replicas count the same stream). Backpressure stays summed (real per-replica events) and
  is titled accordingly; the latency histogram legitimately pools both replicas'
  observations. Added a "Nonce-floor fast-forwards" panel
  (per `partition`+`host_id`, deliberately not deduped) — a rejoining replica shows a
  burst; sustained non-zero is the new chronic-lag alert signal.
- Prometheus (dev stack): `kardamom-sequencer` job now scrapes `:9011` alongside `:9001`,
  with `replica: a|b` target labels; comment documents the max-aggregation rule and that
  single-replica dev stacks just show the `:9011` target down.
- **Ownership boundary note**: `deploy/prometheus.yml` is not in any WP's "Owns:" list
  (WP-OPS owns `deploy/cluster/**` but not `deploy/prometheus.yml`). The finding assigned
  to WP-SEQ explicitly requires a `:9011` scrape target, and no other WP claims the file,
  so I made the 8-line job edit here and flag it for the coordinator. The Vagrant-cluster
  scripts scrape node IPs directly (`smoke-load.sh` — WP-OPS; its stale NODE_IP map is
  already tracked as F16.4).

### F02.6 [L] — DEFERRED (cross-WP), partially mitigated here
The doubled `DuplicatedTx` emissions cannot be deduped inside a sequencer: each replica
emits exactly once and cannot observe its twin's tx_errors stream. The finding's correct
fix — dedup at the consumer by `{sender, nonce, reason}` and let a success signal override
an earlier duplicate-rejection for the same correlation — lives in the ingress
(`crates/ingress/**`, owned by WP-LOG; ingress already dedups receipt copies by tx hash, so
the natural place and pattern exist there). Deferred to avoid an out-of-bounds edit;
recommend WP-LOG (or a follow-up) applies it.
Mitigation shipped in this WP: the F02.1/F02.2 fast-forward bounds the *divergent-floor*
half of the finding — twins' floors now converge within `nonce_floor_lag_ms` of a restart
instead of diverging indefinitely, shrinking the window in which a rejection can race a
success for the same tx to a few seconds per rejoin.

### F02.7 [N] — FIXED
Files: `crates/sequencer/src/config.rs`, `crates/sequencer/src/bin/kardamom-sequencer.rs`.
- Removed the `keep_sequencer_id` escape hatch: `rotate_partition(offset)` now
  unconditionally re-derives `sequencer_id = partition_index` (doc comment explains the
  subscribe-by-id vs filter-by-partition trap and the byte-identical-dedup argument).
- The binary rejects `--sequencer-id` combined with `--partition-offset` at startup
  (`anyhow::ensure!` with an explanatory message) instead of silently dropping every
  envelope.
- Replaced the test that enshrined the broken combination
  (`rotate_partition_keeps_explicit_sequencer_id`) with
  `rotate_partition_overrides_explicit_sequencer_id`.

## Follow-ups for other WPs (hand-off notes)
- **WP-OPS (docs/)**: `docs/agents/replicated-sequencer-shards-spec.md` ("Restart / rejoin
  semantics", "Observability") and `docs/failure-modes.md:92-93` still describe hydration
  from committed state as the whole restart story and the ":9011 twins are additional"
  claim; they should be updated to describe the stream-adaptive fast-forward
  (`nonce_floor_lag_ms`), the `max by (partition)` aggregation rule, and the
  `kardamom_sequencer_nonce_floor_fastforward_total` signal. `docs/**` is WP-OPS-owned.
- **WP-OPS (chaos.sh)**: F02.5's fix can now also assert the restarted replica's
  `tx_published_to_b_total` (or the fast-forward counter) advances post-restart — the
  metric exists and both replicas are scrapable off-node.
- **WP-LOG (ingress)**: F02.6 dedup as described above.

## Verification
- `cargo check -p kardamom-sequencer --all-targets` — pass (no warnings in owned crate).
- `cargo clippy -p kardamom-sequencer --all-targets` — pass (no lints in owned crate; only
  the pre-existing workspace-wide `proc-macro-error2` future-compat note).
- `cargo test -p kardamom-sequencer` — pass: 45 lib unit tests (incl. 4 new
  `fast_forward_*` state tests) + all integration suites, notably
  `replicated_shard_racing` now 5/5 (3 pre-existing invariants unchanged + 2 new rejoin
  regressions).
- `deploy/grafana/.../kardamom-sequencer.json` validated as JSON; `deploy/prometheus.yml`
  validated as YAML; `deploy/cluster/nomad/sequencer.nomad.hcl` passed `nomad fmt` (one
  unrelated alignment auto-change reverted to keep the diff minimal).
- No `Cargo.toml` changes were needed.
