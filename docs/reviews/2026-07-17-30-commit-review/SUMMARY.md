# 30-commit review — consolidated actionable findings

Review of the 30 most recent commits on main (b02ee22..70d0823, newest = 01).
Per-commit detail lives in `NN-<hash>.md`. This file lists only findings **still present at HEAD**,
grouped by the fix work-package that owns them. Severity: C=critical H=high M=medium L=low N=nit.

## High-severity headlines
- F02.1 [H] restarted sequencer replica hydrates nonce floors from `EmptyStateDatabase` → shard silently degrades to P=1
- F07.1 [H] snapshot-restored cluster member answers pre-snapshot replay with bogus REPLAY_DONE → silent canonical gap
- F09.1 [H] incremental trie walker misses stored nodes under extensions → stale orphans, potential silent state-root divergence
- F10.1 [H] validator commit-thread retry consumes receipt-mismatch fail-stop → keeps committing after proven divergence
- F12.1/F12.2 [H] sealer snapshot restore lacks fragment assembler (truncation) / silently restarts at genesis on unreadable snapshot

## WP-J — Java sealer-service + repo hygiene
Owns: `cluster/sealer-service/**`, `.gitignore`, tracked build artifacts, `deploy/cluster/scripts/__pycache__/`
- F12.1 [H/logic] snapshot restore: no ImageFragmentAssembler → >~40 dedup ids truncate (SealerClusteredService.java:261-283)
- F12.2 [H/logic] unreadable/empty snapshot → silent genesis restart; onTakeSnapshot swallows non-retryable offer failures
- F07.1 [H/logic] snapshot-restored member leaves retention floors at genesis → bogus REPLAY_DONE instead of REPLAY_UNAVAILABLE (:112-147,302-303)
- F07.3 [M/logic] replay of ≤65536 frames served synchronously on the cluster service thread, 1s/frame deadline → leader egress stall
- F07.5 [L/logic] MAX_POSITION_EXCEEDED offer path returns without closing session (zombie session class)
- F12.6 [M/logic] CanonicalSealerState.load doesn't validate idCount vs capacity/remaining (:196-218)
- F02.3 [M/logic] first-seen dedup window 8192 FIFO vs unbounded replica lag → duplicate canonical ordering possible (ClusterNode.java:44)
- F01.2/F05.2/F06.2 [M/security] `cluster/sealer-service/.gradle/**`, `*/build/**` (~200KB) tracked at HEAD; gitignore missing
- F12.7 [L/security] `deploy/cluster/scripts/__pycache__/*.pyc` tracked; no `__pycache__` gitignore
- F12.12 [N/quality] malformed-frame drops unmetered (Java onSessionMessage side)

## WP-SEQ — sequencer racing replicas
Owns: `crates/sequencer/**`, `deploy/cluster/nomad/sequencer.nomad.hcl`, `deploy/grafana/**`
- F02.1 [H/logic] nonce floors hydrated from EmptyStateDatabase in deployed binary (bin:160,349-381)
- F02.2 [M/logic] one-shot cache-miss hydration races in-flight ordering; floor never refreshed (sequencer.rs:144-165)
- F02.4 [M/logic] seq-b metrics bound to 127.0.0.1:9011, never scraped; dashboards would double-count without replica label
- F02.6 [L/logic] tx_errors lacks dedup → 2x DuplicatedTx receipts per past-nonce tx (sequencer.rs:324-333)
- F02.7 [N/quality] `--partition-offset` + explicit `--sequencer-id` subscribes by id but filters by partition_index → drops everything

## WP-WD — withdrawals / contracts / deployer
Owns: `contracts/**`, `crates/validator/src/attester.rs`, `crates/types/src/withdrawals.rs`, `crates/deployer/**`, validator bin wiring for attester (coordinate with WP-VAL)
- F06.1 [M/logic] deleted output blocks same-range re-proposal (monotonicity counts deleted outputs); attester never re-attests stranded leaves
- F06.3 [M/quality] attester/OutputPoster never wired into kardamom-validator binary — production off-ramp inert
- F06.4 [L/logic] deployer index-out-of-bounds without `--l2-minter` (main.rs:239)
- F06.5 [L/security] Merkle verifier lacks leaf/node domain separation + proof-depth binding (ETHLockbox.sol:138)
- F06.6 [L/security] initialize accepts zero attester/challenger/window; no key rotation (WithdrawalOutputOracle.sol:60)
- F06.7 [N/quality] decode_message_passed trusts event-carried leaf hash; accepts trailing data (`< 64` not `!= 64`)
- F06.8 [N/quality] ETHLockbox initialize changed without reinitializer

## WP-TRIE — state trie + obs
Owns: `crates/state/**` (incl. schema.rs, genesis.rs), `crates/obs/**`
- F09.1 [H/logic] walker removals=visited−updated misses stored nodes under extensions → stale orphans (walker.rs:204-216)
- F09.3 [L/logic] `let _ = txn.del(...)` swallows all mdbx errors on hashed-mirror deletes (mod.rs:189,235)
- F09.4 [L/quality] differential test pool can't generate extension/collapse geometry where F09.1 lives
- F09.5 [L/quality] unchanged extension children re-walked; "~O(changed keys)" doc overstates
- F09.6 [N/quality] schema.rs:33 key-encoding comment wrong (ci-cluster.sh comment part → WP-OPS)
- F24.1 [N/quality] deleted roundtrip tests were only coverage of live encode_block_key/encode_header_value layouts
- F13.7 [L/logic] genesis seeding gated on presence-only flag; changed --chain file silently ignored
- F13.8 [N/quality] genesis.rs:108 comment misdescribes single-txn atomicity
- F03.2 [M/logic] exporter port-bind failure after init returns Ok — only background tracing::error (obs/lib.rs:88-101)
- F03.3 [N/logic] TOCTOU free-port test pattern can flake (obs/tests)

## WP-BENCH — load/chaos bench harness
Owns: `crates/bench/**`
- F15.1 [M/logic] must-deliver gate vacuous: Pending{accepted:true} never created → missing always 0 (load/engine.rs:133-149)
- F15.2 [M/logic] retry-on-any-error can resubmit landed tx; seq_dropped>0 hard-fails even in chaos mode
- F15.4 [M/logic] submit tasks never joined; drain exits with ≤256 submits parked in 30s timeouts
- F15.5 [L/logic] FROZEN false-positive on restarted executor gauge reset (remaining part)
- F15.7 [N/quality] "strict nonce order" doc claim is pop-order only (plan.rs part; chaos.sh part → WP-OPS)
- F14.1 [L/quality] harness bin advertises in-process node; `calls`/`mixed` subcommands fail at runtime
- F14.2 [N/quality] genesis_alloc doc claims validation that no longer happens

## WP-LOG — aeron log, replay/recovery, ingress/da-watcher bins
Owns: `crates/log/**`, `crates/ingress/**` (bin + cluster.rs metrics), `crates/da_watcher/**`
- F13.1 [M/logic] replay-merge hardcodes start_position=0, warns on session gaps → silent record loss after publisher restart (replay.rs:281)
- F13.2 [M/logic] no recorder barrier before serving RPC; recorder-startup failure swallowed (ingress bin:185-320, da-watcher:112-130)
- F13.4 [L/logic] fatal replay-merge error → warn + channel close → exit 0 (replay.rs:~150 + engine actor mapping — engine side owned by WP-VAL)
- F13.6 [L/logic] resolve_recording pages only first 100 catalog entries (replay.rs:398-404)
- F16.5 [L/logic] pending-publish queue: caller ack-timeout doesn't cancel queued frame; deadlines evaluated only at head (aeron_live.rs)
- F16.7 [N/quality] open_subscription_with_id duplicates open_subscription_merged deliver closure
- F21.1 [L/logic] silent-skip-on-missing-docker false-pass survives in crates/log tests (aeron_live_e2e.rs:40-43 etc.)
- F22.1/F28.1 [N/quality] da_watcher doc comments name removed kardamom_log::TxDepositsPublisher
- F12.12 [N/quality] malformed-frame drops unmetered (ingress/cluster.rs:55-60 Rust side)

## WP-VAL — validator + engine + executor bins
Owns: `crates/validator/**` (except attester.rs), `crates/engine/**`, `crates/executor/**`
- F10.1 [H/logic] commit-thread must-deliver retry consumes receipt-mismatch fail-stop error (engine/actor.rs:620 + validator/lib.rs:322)
- F10.3 [M/logic] cold-start catch-up: ReceiptBuffer lacks #78 aged-out skip → 5s block per historical receipt
- F10.4 [M/quality] ~200 lines of binary wiring duplicated between executor and validator bins (drifting)
- F10.5 [L/logic] receipt_consistent ignores `logs` — log-only divergence undetectable
- F10.6 [L/logic] BAL/receipt buffers unbounded; late arrivals past cursor leak (incl. F01.4 skip-then-arrive leak)
- F10.7 [L/quality] dead BalPublisher / Subscribers::bal()
- F10.8 [N/quality] engine docs describe never-built BlockSink seam
- F10.9 [N/quality] stale comments + state-root gauge mirrors committed-block gauge (crate parts; script parts → WP-OPS)
- F13.3 [M/logic] recovery gated on last_committed_block>0 → crash before first commit = crash-loop (executor bin:223,246)
- F13.5 [L/logic] receipts/Boundary published before durable ack; at-least-once contract undocumented (engine/actor.rs:549-556)
- F13.4b [L/logic] engine actor maps closed replay channel to Ok(()) → failed recovery exits 0 (actor.rs:354-356)
- F05.4 [N/quality] executor bin fresh-start/resume join-timeout comment reads backwards
- F11.1 [M/quality] validator metrics default port 9006 collides with ingress default (bin change here; docs table → WP-OPS)
- F07.2 [M/logic] boundary-only gap across reconnect undetected → replica canonical-order divergence (engine/reader/cluster.rs:103-150)
- F08.2 [L/quality] kardamom_sealer_* also emitted by validators — gate emission executor-side (engine/metrics.rs + reader/cluster.rs)

## WP-OPS — deploy scripts, CI, docs
Owns: `deploy/cluster/scripts/**`, `deploy/cluster/ansible/**`, `deploy/cluster/README.md`, `.github/workflows/**`, `justfile`, `docs/**`, `README.md`
- F03.1 [M/logic] chaos.sh bridge-probe fix never implemented; EXECUTOR_IPS dead; probes still via docker exec (:101-102)
- F01.1 [M/quality] chaos.sh comments describe discarded side-stream refetch design + nonexistent assertion (:174-182,595-598)
- F01.3 [L/logic] warm-up loop proceeds after 150s budget expires unmet (:196-206)
- F04.1 [L/logic] archive-driver-loss: aeron_base 0/empty makes assert_count trivially pass (~:465-480)
- F02.5 [L/logic] sequencer-replica-kill load not pinned to killed shard; name=sequencer kills arbitrary task (:418-456)
- F15.3 [M/logic] assert_count recovery SLO passes vacuously (old alloc reads running on first poll) (:281-288)
- F15.6 [L/quality] fixed sleep INJECT_DELAY with no load-flowing check (:436)
- F15.7b [N/quality] chaos.sh shell helpers duplicated from ci-cluster.sh; failed scrapes missing from service_up
- F07.6 [N/quality] ci-cluster.sh divergence grep reads only first validator alloc (:677-679)
- F09.6b/F10.9b [N/quality] ci-cluster.sh stale comments (shadow-check cadence; placement)
- F16.1 [M/logic] static-inventory/Vagrant path broken: templates need tier/node_ip inventories don't define; roles/IPs contradict contract
- F16.2 [L/logic] justfile:402 cluster-doctor greps old registry IP .11 → always MISS
- F16.3 [L/quality] cluster-e2e.yml failure diagnostics use container names that no longer exist (:95)
- F16.4 [L/logic] smoke-load.sh direct-scrape NODE_IP map uses removed r1..w2 topology (:260-263)
- F16.6 [N/quality] vestigial recorder_count/quorum config; tx_receipts_endpoint_base_port i32→u32 wrap
- F17.1 [N/quality] SMOKE_SENDER_OFFSET silently clamps negatives to 0 (smoke-load.sh:154-155)
- F18.1 [L/quality] deploy/cluster/README.md documents removed recorder topology as current
- F12.11 [N/quality] cluster-e2e.yml stale "must build WITH cluster feature" comments
- F11.1b [M/quality] docs/observability.md port table omits validator (+ collision note)
- F22.2 [N/quality] historical spec lacks "superseded" note for removed watermark API
- F04.2 [N/quality] ~1MB raster diagrams, no editable source (note/mitigate only)

## WP-CFG — cross-cutting cluster config (runs last, alone)
Owns: `crates/cluster-adapter/**`, ClusterConfig definitions in engine/sequencer/ingress, `deploy/cluster/scripts/check-contract.py`
- F12.8 [L/quality] ClusterConfig + defaults_applied + to_live defined 3× across crates
- F12.9 [L/quality] `enabled` field parsed and set in TOMLs but ignored; empty [cluster] fails only at runtime
- F12.10 [L/quality] check-contract.py excludes ingress [cluster] block and --cluster-egress-endpoint
- F12.4 [M/logic] initial open_leader_pub failure only logs and kills session thread; service runs with dead cluster session (cluster-adapter/live.rs:~330)
- F05.3 [L/logic] blocking publish_bytes (10s ack) on session loop can starve keep-alives; Result discarded (live.rs:317)

## Not addressed in the PR (deliberate)
- F05.1 [L/security] 2.5MB jar blob remains in git history — requires history rewrite (filter-repo) + force-push; own build artifact, no secrets. Flagged for a maintenance window decision, not a PR.
