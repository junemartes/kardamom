# 11 de25ccf — docs: sync README + observability docs with post-cluster architecture (#68)

## Summary of change
Docs-only audit after the kardamom-node removal (#57), libMDBX executor persistence (#61), and the Aeron Cluster sealer (#67). README: drops the removed `kardamom-sealer` crate, adds cluster-adapter/cluster-client/e2e, describes the Java sealer and its JDK 17 Gradle build, documents four previously-missing just recipes. observability.md: removes stale sealer binary/:9003/scrape claims, fixes host_id and block_number statements, marks the rename map historical, notes the empty sealer dashboard. chains/dev.toml header now points at `kardamom-executor --chain`; justfile header drops the removed `node` build script.

## Accuracy check against HEAD
- Just recipes `aeron-driver-up`/`aeron-driver-down`/`cluster-bootstrap`/`cluster-doctor` all exist (justfile:173, 225, 286, 374).
- `kardamom-executor --chain` is real (kardamom-executor.rs `chain: Option<PathBuf>` arg); dev.toml header fix is correct.
- README crate/workspace description matched `crates/` at the commit; later commits (#73) added validator/engine, keeping it in sync at HEAD.
- The "nothing emits kardamom_sealer_* / dashboard stays empty" claims were true at the commit and were properly superseded in the doc itself by fd9acaa (#69, executor re-export) — observability.md at HEAD describes the re-exported series. No drift left from this commit's sealer claims.
- Port table entries (sequencer 9001, batcher 9002, executor 9004, da-watcher 9005, ingress 9006) match every binary's `default_value` at HEAD — but see F11.1.

## Findings

### F11.1 [medium] [quality] — Docs "audit" missed the validator: absent from the port table, and its default port collides with ingress
- **Where**: docs/observability.md:13-18 (table; same lines at the commit and HEAD)
- **What**: The commit is framed as an audit of the docs against the code, but the port/service table omits `kardamom-validator`, which existed at the time (#63 predates #68). Worse, the omission hides a real code-level collision the table would have surfaced: `kardamom-validator` defaults to `127.0.0.1:9006` (crates/validator/src/bin/kardamom-validator.rs:124) — the same default as `kardamom-ingress` (crates/ingress/src/bin/kardamom-ingress.rs:94). Anyone running both locally with defaults gets the exact "race for one socket" failure the very next paragraph of the doc warns about (silently worse since #77: the losing exporter now only logs from a background thread — see F03.2). The cluster deploy avoids it only by node placement (validator.nomad.hcl puts the validator on the aux node).
- **Still present at HEAD**: yes
- **Suggested fix**: Add `kardamom-validator` to the table and change its default to an unused port (e.g. 9007) — or document the 9006 sharing explicitly if intentional.

### F11.2 [nit] [quality] — "port 9003 remains reserved ... kardamom-load still probes them opportunistically" left unverified
- **Where**: docs/observability.md:20-24 at the commit (rewritten by fd9acaa at HEAD)
- **What**: The parenthetical about the load harness opportunistically probing sealer metrics/port 9003 was a soft claim carried into the rewrite; nothing binds it to code and it will silently rot. Minor, and the fd9acaa rewrite already reworded the section.
- **Still present at HEAD**: partially (the reworded section no longer makes the probe claim in the same form)
- **Suggested fix**: None required; keep such behavioral claims pointing at the code that implements them.

## Verdict
A careful, genuinely accurate docs sync: every checkable claim (recipes, flags, crate lists, removed binaries, dashboard inventory) verifies against the code, and the sealer statements that later became stale were updated in the very next observability commit rather than left to rot. The one substantive miss is the validator: an audit that rebuilt the metrics port table without noticing an existing service whose default port collides with ingress's documented one (F11.1) — that gap is still open at HEAD and is the only actionable item.
