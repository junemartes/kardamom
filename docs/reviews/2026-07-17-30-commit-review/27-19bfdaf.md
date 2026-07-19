# 27 19bfdaf — refactor(ingress): remove dead/legacy code and tech debt (#49)

## Summary of change
Dead-code cleanup of `kardamom-ingress`: removes `ReceiptCache::spawn_consumer` (leftover from the old self-consuming-broadcast design; the live proxy populates the cache via `spawn_tx_receipts_watcher` → `cache.insert`) together with its only, self-referential test, the zero-caller `PendingReceipts::policy()` accessor, and unused `serde`/`serde_json` dependencies. Updates the receipt_cache module doc to describe the actual population path.

## Findings

Dead-code verification: grep at the commit tree and at HEAD found zero references to `spawn_consumer` or the `.policy()` accessor (all `ack_policy` hits are the config field, not the removed method). `serde`/`serde_json` appear nowhere in ingress src/tests/benches at the commit — the only mention is a comment about a *future* `Deserialize` derive, so the dependency can be re-added when that lands. The deleted test `consumer_populates_from_broadcast` exercised only the deleted method; the surviving tests still cover `insert`/`lookup`/eviction, so live-path coverage is intact. The removal of `spawn_consumer` does not lose the broadcast-lag resilience it contained (`RecvError::Lagged` continue) because the live population path does not go through a broadcast receiver at all. The commit's "kept" list is accurate at HEAD: the on-quorum ack gate (`AckPolicy::OnQuorum`, quorum watcher) is still deployed and live, `secp256k1` is still declared for the feature-unification reason stated, and the module doc now matches the real data flow.

No findings — the commit is clean.

## Verdict
Clean, small, well-scoped removal commit. Both removed items were verifiably dead at the commit and remain unreferenced at HEAD; the dependency pruning is confirmed by source grep; the doc update fixes real drift. The commit message's kept/removed rationale (notably keeping the on-quorum gate because it is a deployed override) is careful and correct.
