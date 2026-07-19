# WP-WD fixes — withdrawals / contracts / deployer

Findings from `06-637f683.md`. Files owned: `contracts/**`, `crates/validator/src/attester.rs`,
`crates/types/src/withdrawals.rs`, `crates/deployer/**`.

## F06.1 [M/logic] — deleted output blocks same-range re-proposal; stranded leaves — FIXED
- **Files**: `contracts/src/L1/WithdrawalOutputOracle.sol`, `crates/validator/src/attester.rs`,
  `contracts/test/L1/WithdrawalOutputOracle.t.sol`, `contracts/test/L1/WithdrawalFlow.t.sol`
- Contract: `proposeOutput` now enforces monotonicity against the latest **non-deleted** output
  (backward scan bounded by the trailing run of deleted outputs — deletions are permissioned,
  rare, and window-limited). A corrected output for a challenged range (same `l2BlockNumber`)
  is re-proposable; `deleteOutput`'s dev comment updated accordingly. No storage-layout change.
- Attester: the new driver (see F06.3) only clears pending leaves after a **successful**
  `proposeOutput`, so a failed post retries with the accumulated set; on startup it resumes from
  the latest **non-deleted** on-chain output (`OutputPoster::latest_attested_block`), so a
  challenged-and-deleted latest output is re-attested from the leaves the validator re-collects
  on replay. Live (mid-run) deletion of an *older, already-cleared* range still needs an
  `OutputDeleted` event watcher + historical leaf storage — documented in the module docs as a
  follow-up; the contract-side deadlock that made it *unfixable* is gone.
- Regression tests: `test_repropose_same_block_after_delete`,
  `test_monotonicity_floor_is_latest_non_deleted` (oracle),
  `test_challenged_range_reattested_and_finalized` (end-to-end flow),
  `attest_state_accumulates_and_clears_only_on_success` (Rust carry-forward).

## F06.3 [M/quality] — attester never wired into the validator binary — PARTIAL
`crates/validator/src/bin/kardamom-validator.rs` is owned by WP-VAL (later wave); per
instructions I did **not** edit it. Everything needed is now exposed from
`crates/validator/src/attester.rs`:

- `AttesterConfig { l1_rpc_url, oracle, private_key, post_interval_blocks }`
- `spawn_attester(cfg) -> (AttesterHandle, JoinHandle<()>)` — background task; resumes from the
  latest non-deleted on-chain output; posts one output per `post_interval_blocks`; failed posts
  keep their leaves pending and retry at the next cadence point.
- `AttesterHandle::submit_leaves(block, leaves)` / `submit_root(block, state_root)` — non-blocking,
  Clone, safe from sync engine threads.
- `AttestingWriterQueue<Q: StateWriterQueue>` — drop-in wrapper that calls
  `collect_withdrawal_leaves` per submitted block, feeds the handle, forwards the delta.
- `AttestState` — the pure cadence/accumulation state, unit-tested.

**Exact wiring instructions for WP-VAL / coordinator** (all in
`crates/validator/src/bin/kardamom-validator.rs`):
1. Add CLI flags (all optional; attester enabled only when the first three are present):
   `--l1-rpc-url <url>` (env `KARDAMOM_L1_RPC_URL`), `--output-oracle <address>`
   (env `KARDAMOM_OUTPUT_ORACLE`), `--attester-key <hex-or-env:VAR>` (env `KARDAMOM_ATTESTER_KEY`;
   reuse the deployer's `env:VAR` convention by resolving it before building `AttesterConfig`),
   `--attester-post-interval <blocks>` default `1` (env `KARDAMOM_ATTESTER_POST_INTERVAL`).
2. After `let sw_queue = ValidatorWriterQueue::new(...)` (currently bin line ~367) and before
   `Executor::run` is spawned:
   ```rust
   use kardamom_validator::attester::{self, AttesterConfig, AttestingWriterQueue};
   let attester_handle = match (l1_rpc_url, output_oracle, attester_key) {
       (Some(url), Some(oracle), Some(key)) => {
           let (handle, _task) = attester::spawn_attester(AttesterConfig {
               l1_rpc_url: url,
               oracle,
               private_key: key,
               post_interval_blocks: args.attester_post_interval,
           })?;
           Some(handle)
       }
       _ => None, // milestone-1 default: no automatic attestation
   };
   ```
   (must run inside the tokio runtime — `main` already is; hold the handle so the task lives.)
3. Wrap the writer queue (both arms of the match keep the same `Executor::run` call because
   `AttestingWriterQueue` implements `StateWriterQueue`; if avoiding a `Box<dyn>` is preferred,
   wrap unconditionally with an `Option<AttesterHandle>`-aware handle — simplest is
   `Box<dyn StateWriterQueue + Send>` as the executor already takes the queue generically):
   ```rust
   let sw_queue = AttestingWriterQueue::new(sw_queue, handle.clone()); // when Some
   ```
4. In the existing background snapshot poller (bin lines ~374-393), where it already does
   `snap.state_root()` on block advance, add:
   ```rust
   if let (Some(h), Ok(Some(root))) = (attester_handle.as_ref(), snap.state_root()) {
       h.submit_root(block, root);
   }
   ```
   Note the poller currently only reads `state_root()` under `tracing::debug!` — hoist the call
   out of the debug branch.
5. Docs: note that a validator run without the three flags performs no automatic attestation
   (previous behavior), and that the attester key must be the oracle's permissioned `attester`.

## F06.4 [L/logic] — deployer index-out-of-bounds without `--l2-minter` — FIXED
- **Files**: `crates/deployer/src/main.rs`
- The `--l2-chain-id`/`--l2-minter` count check now applies to **every** id that consumes
  `l2_minters` (ETHLockbox *and* KardamomL2Settlement), so
  `deploy KardamomL2Settlement --l2-chain-id 42` fails with a clear error instead of panicking.
  The error names the offending ids. The minter→`_l1Batcher` reuse for settlement is now
  documented on the flag help and at the encode site (a dedicated `--l1-batcher` flag deferred
  until the roles diverge).

## F06.5 [L/security] — Merkle verifier: no leaf/node domain separation, leafIndex high bits ignored — FIXED
- **Files**: `contracts/src/L1/ETHLockbox.sol`, `crates/types/src/withdrawals.rs`,
  `contracts/test/L1/ETHLockbox.t.sol`, `contracts/test/L1/WithdrawalFlow.t.sol`
- The withdrawals tree is now domain-separated on **both sides** (byte-identically):
  tree leaf = `keccak(0x00 ++ withdrawalHash)`, internal node = `keccak(0x01 ++ l ++ r)`
  (`LEAF_DOMAIN`/`NODE_DOMAIN` constants in both languages). The raw withdrawal leaf hash
  (`keccak(abi.encode(nonce,sender,target,value))`) is unchanged, so the L2 predeploy, its
  genesis-pinned bytecode, the replay guard, and the events are all untouched.
- `_merkleRoot` additionally reverts if `leafIndex` has set bits beyond `proof.length`
  (binds the claimed position to the proof depth; previously many indices "verified" one proof).
  `withdrawals_root` / `withdrawal_proof` / `recompute_root` in `kardamom_types::withdrawals`
  mirror the change; empty-tree root stays `B256::ZERO`, single-leaf root is now the
  domain-hashed leaf.
- This is a commitment-format change: nothing is deployed beyond dev, and the always-run anvil
  e2e posts+verifies with both sides changed in lockstep. New tests:
  `test_raw_leaf_cannot_pose_as_internal_node`,
  `test_finalizeWithdrawal_leaf_index_beyond_proof_depth_reverts` (Solidity),
  `leaves_and_nodes_are_domain_separated` (Rust).

## F06.6 [L/security] — no init validation, no key rotation — FIXED
- **Files**: `contracts/src/L1/WithdrawalOutputOracle.sol`, `contracts/test/L1/WithdrawalOutputOracle.t.sol`
- `initialize` now rejects zero attester/challenger (`ZeroAddress`) and a zero window
  (`ZeroWindow`). Added factory-gated rotation: `setAttester` / `setChallenger` /
  `setFinalizationWindow` (same authority as UUPS upgrades, i.e. `KardamomUUPSBase.FACTORY`),
  each zero-validated and evented. A *minimum* window beyond non-zero was left as a deploy-time
  policy decision (dev/chaos environments legitimately use short windows); the deployer default
  remains 86 400 s. ETHLockbox's zero `_outputOracle` remains valid by design (documented
  deposit-only mode) — recoverable now via `initializeV2` (F06.8).

## F06.7 [N/quality] — `decode_message_passed` trusts event-carried hash; accepts trailing data — FIXED
- **Files**: `crates/types/src/withdrawals.rs`
- The decoder now recomputes `withdrawal_leaf(nonce, sender, target, value)` from topics+data and
  returns `None` unless it equals the event-carried hash (catches predeploy/bytecode drift), and
  requires `data.len() == 64` exactly. Tests: `decode_message_passed_recomputes_and_verifies_leaf`,
  `..._rejects_tampered_hash`, `..._rejects_trailing_data`.

## F06.8 [N/quality] — ETHLockbox initialize changed without reinitializer — FIXED
- **Files**: `contracts/src/L1/ETHLockbox.sol`, `contracts/test/L1/ETHLockbox.t.sol`
- Added `initializeV2(address _outputOracle) external reinitializer(2)`, factory-gated
  (`msg.sender == FACTORY`, preserved through the factory's `upgradeToAndCall` delegatecall), so
  proxies initialized with the one-arg V1 signature can be upgraded and wired to the oracle
  instead of staying deposit-only forever. One-shot (`reinitializer(2)`), covered by
  `test_initializeV2_sets_oracle_factory_only`. The `#[ignore]` on the flaky
  `full_withdrawal_finalize_and_challenge` e2e is unchanged (file owned by WP-VAL); the Solidity
  side remains covered by Foundry.

## Verification
- `forge test` (contracts/, forge 1.7.1): **61 passed, 0 failed** (was 48; +13 new tests).
- `forge fmt --check src test`: clean; `solhint --max-warnings 0 -c .solhint.json 'src/**/*.sol'`
  (5.0.5, the CI-pinned version): clean.
- `cargo check -p kardamom-types -p kardamom-validator -p kardamom-deployer`: **clean**.
- `cargo test -p kardamom-types withdrawals`: **8 passed** (incl. 5 new).
- `cargo test -p kardamom-validator --lib`: **11 passed** (incl. 3 new attester driver tests).
- `cargo test -p kardamom-deployer --lib` + `--tests`: **all passed** (24 lib; predeploy-pin,
  factory-address-sync, deploy e2e suites green — embedded bytecode regenerated by build.rs from
  the changed contracts; the FACTORY address is unchanged since KardamomFactoryV1 was untouched).
- `cargo test -p kardamom-validator --test withdrawal_e2e` (real anvil): **1 passed, 1 ignored**
  — the always-run cross-language test (Rust attester + new domain-separated tree + deployer
  against the freshly compiled contracts) passes in ~5 s. A `--include-ignored` attempt of
  `full_withdrawal_finalize_and_challenge` hung in exactly the alloy/anvil receipt-watcher flake
  its `#[ignore]` documents (pre-existing; test file owned by WP-VAL) and was aborted.

No Cargo.toml changes were needed (alloy-signer-local/tokio already validator dependencies).
Touched test files were reformatted with `forge fmt` / `rustfmt` to keep the repo lint-clean.
