//! EEST state-test conformance runner (W1 of
//! `docs/agents/l1-client-suite-port-spec.md`) — the `consume direct` analog.
//!
//! Walks the `state_tests` tree of a pinned execution-spec-tests fixture
//! release, and for every post entry of the pinned fork executes the
//! fixture's transaction through the SAME path the live executor and
//! validator share — `decode_alloy_envelope` → `tx_env_from_alloy` →
//! [`ExecScope::execute_tx`] — under the *fixture's* block/cfg env (via
//! [`ExecScope::new_with_envs`]), then checks the resulting state root and
//! logs hash against the fixture's expectation.
//!
//! Latest-fork-only policy: only `post.Osaka` entries run; every other fork
//! key is ignored. A fork bump changes [`FORK`] here alongside
//! `block_env::SPEC_ID` and the fixture tag in `scripts/fetch-eest-fixtures.sh`.
//!
//! Expected failures live in `tests/eest_expected_failures.json` — the
//! precise, versioned statement of where kardamom's execution deviates from
//! mainnet EVM. The runner fails on unexpected failures AND on unexpected
//! passes (stale entries must be removed, with their reason re-examined).
//!
//! Run: `KARDAMOM_EEST_FIXTURES=<dir> cargo test -p kardamom-exec-core \
//!       --release --test eest_state -- --ignored` (or `just eest`).

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use alloy_primitives::{Address, B256, Bytes as AlloyBytes, U256, keccak256};
use alloy_rlp::Encodable;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::executor::ExecScope;
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_exec_core::{TxIndex, WriteSet};
use kardamom_types::{BPosition, Receipt, TxEnvelope};
use revm::context::CfgEnv;
use revm::primitives::KECCAK_EMPTY;
use revm::primitives::hardfork::SpecId;
use revm::statetest_types::{SpecName, Test, TestSuite, TestUnit};

/// The one fork this chain executes — see `block_env::SPEC_ID`.
const FORK: SpecName = SpecName::Osaka;
const SPEC: SpecId = kardamom_exec_core::block_env::SPEC_ID;

// ---------------------------------------------------------------------------
// Expected-failures list

#[derive(serde::Deserialize)]
struct Xfail {
    /// Exact case id (`<unit name>[<post index>]`) or, with a trailing `*`,
    /// a prefix matching a whole family.
    id: String,
    reason: String,
}

fn load_xfails() -> Vec<Xfail> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/eest_expected_failures.json"
    );
    serde_json::from_str(&fs::read_to_string(path).expect("xfail list must exist"))
        .expect("xfail list must parse")
}

/// Exact-id entries are STRICT: a pass flags the entry as stale. Prefix
/// (`…*`) entries are LAX: they mark a family where *failures* are expected,
/// while cases in the family that pass keep counting as plain passes (many
/// families fail only on the parametrizations that hit the deviation).
fn xfail_match<'a>(xfails: &'a [Xfail], id: &str) -> Option<&'a Xfail> {
    xfails.iter().find(|x| {
        x.id == id
            || x.id
                .strip_suffix('*')
                .is_some_and(|prefix| id.starts_with(prefix))
    })
}

// ---------------------------------------------------------------------------
// Post-state oracle: a full alloc model + pure MPT root

#[derive(Default)]
struct Alloc {
    /// addr → (nonce, balance, code_hash)
    accounts: BTreeMap<Address, (u64, U256, B256)>,
    /// (addr, slot) → value
    storage: BTreeMap<(Address, B256), U256>,
}

impl Alloc {
    fn apply(&mut self, ws: &WriteSet) {
        for (addr, acct) in &ws.accounts {
            self.accounts.insert(*addr, *acct);
        }
        for ((addr, key), value) in &ws.storage {
            if value.is_zero() {
                self.storage.remove(&(*addr, *key));
            } else {
                self.storage.insert((*addr, *key), *value);
            }
        }
    }

    fn state_root(&self) -> B256 {
        let accounts = self
            .accounts
            .iter()
            .filter_map(|(addr, (nonce, balance, code_hash))| {
                // Kardamom's empty-code sentinel is `B256::ZERO` (normalized to
                // `KECCAK_EMPTY` at the snapshot boundary, #161); the trie always
                // encodes `KECCAK_EMPTY`.
                let code_hash = if *code_hash == B256::ZERO {
                    KECCAK_EMPTY
                } else {
                    *code_hash
                };
                // EIP-161: empty accounts do not exist in the state trie.
                if *nonce == 0 && balance.is_zero() && code_hash == KECCAK_EMPTY {
                    return None;
                }
                let storage_root = alloy_trie::root::storage_root_unhashed(
                    self.storage
                        .range((*addr, B256::ZERO)..=(*addr, B256::repeat_byte(0xff)))
                        .map(|((_, k), v)| (*k, *v)),
                );
                Some((
                    *addr,
                    alloy_trie::TrieAccount {
                        nonce: *nonce,
                        balance: *balance,
                        storage_root,
                        code_hash,
                    },
                ))
            });
        alloy_trie::root::state_root_unhashed(accounts)
    }
}

fn logs_hash(receipt: &Receipt) -> B256 {
    let logs: Vec<alloy_primitives::Log> = receipt
        .logs
        .iter()
        .map(|l| {
            alloy_primitives::Log::new_unchecked(
                l.address,
                l.topics.clone(),
                AlloyBytes::from(l.data.to_vec()),
            )
        })
        .collect();
    let mut buf = Vec::new();
    logs.encode(&mut buf);
    keccak256(&buf)
}

// ---------------------------------------------------------------------------
// One fixture case

enum Outcome {
    Pass,
    Skip(&'static str),
    Fail(String),
}

fn run_case(unit: &TestUnit, t: &Test) -> Outcome {
    let Some(txbytes) = &t.txbytes else {
        return Outcome::Skip("no txbytes");
    };
    // Kardamom carries no blob transactions: type-3 is rejected at ingress
    // and `max_blobs_per_tx = 0` in production cfg. Blob fixtures test
    // semantics the chain deliberately does not have.
    if txbytes.first() == Some(&0x03) {
        return Outcome::Skip("blob tx (type-3 unsupported)");
    }
    let Some(sender) = unit.transaction.sender else {
        return Outcome::Skip("no sender");
    };

    // Fixture-derived envs — NOT the production `ExecEnv` derivation. The
    // cfg starts from the same constructor production uses (pinned spec,
    // spec-derived gas table) and takes the fixture's chain id.
    let mut cfg = CfgEnv::new_with_spec(SPEC);
    cfg.chain_id = unit
        .env
        .current_chain_id
        .map(|id| id.try_into().unwrap_or(1))
        .unwrap_or(1);
    let block = unit.block_env(&mut cfg);

    let exec_env = ExecEnv {
        chain_id: cfg.chain_id,
        block_number: block.number.try_into().unwrap_or(0),
        l2_timestamp: block.timestamp.try_into().unwrap_or(0),
    };

    // Materialize `pre` into the mock state DB and the oracle alloc.
    let mut alloc = Alloc::default();
    let mut db = MockStateDatabase::builder();
    for (addr, info) in &unit.pre {
        let code_hash = if info.code.is_empty() {
            B256::ZERO
        } else {
            keccak256(&info.code)
        };
        db = db.account(*addr, info.balance, info.nonce, code_hash);
        if !info.code.is_empty() {
            db = db.code(code_hash, bytes::Bytes::from(info.code.to_vec()));
        }
        alloc
            .accounts
            .insert(*addr, (info.nonce, info.balance, code_hash));
        for (key, value) in &info.storage {
            let k = B256::from(*key);
            db = db.storage(*addr, k, *value);
            if !value.is_zero() {
                alloc.storage.insert((*addr, k), *value);
            }
        }
    }

    let mut scope = match ExecScope::new_with_envs(db.build(), None, exec_env, block, cfg) {
        Ok(s) => s,
        Err(e) => return Outcome::Fail(format!("scope construction: {e}")),
    };

    let envelope = TxEnvelope {
        correlation_id: 0,
        raw_tx: bytes::Bytes::from(txbytes.to_vec()),
        sender,
        tx_hash: keccak256(txbytes),
    };
    let (receipt, mut ws) = match scope.execute_tx(
        TxIndex(0),
        BPosition {
            term_id: 0,
            term_offset: 0,
        },
        &envelope,
        0,
        0,
        None,
        None,
    ) {
        Ok(r) => r,
        Err(e) => return Outcome::Fail(format!("executor error: {e}")),
    };
    ws.finish();
    alloc.apply(&ws);

    // A fixture that expects a validation exception must land on our
    // deterministic invalid-skip (total derivation, #92) — an executed
    // receipt here means a validation gap even if the root happens to match.
    if t.expect_exception.is_some() && !receipt.is_invalid_skip() {
        return Outcome::Fail(format!(
            "expected exception {:?} but tx executed (status={}, gas={})",
            t.expect_exception, receipt.status, receipt.gas_used
        ));
    }

    let root = alloc.state_root();
    if root != t.hash {
        // When the fixture ships its full post_state, name the differing
        // accounts — worth far more in triage than two root hashes.
        let mut diffs = Vec::new();
        for (addr, want) in &t.post_state {
            let got = alloc.accounts.get(addr);
            let want_tuple = (
                want.nonce,
                want.balance,
                if want.code.is_empty() {
                    B256::ZERO
                } else {
                    keccak256(&want.code)
                },
            );
            match got {
                None => diffs.push(format!("{addr}: missing (want {want_tuple:?})")),
                Some(g) => {
                    let norm = |h: B256| if h == KECCAK_EMPTY { B256::ZERO } else { h };
                    if (g.0, g.1, norm(g.2)) != (want_tuple.0, want_tuple.1, norm(want_tuple.2)) {
                        diffs.push(format!(
                            "{addr}: got (nonce={}, bal={}, code={}), want (nonce={}, bal={}, code={})",
                            g.0, g.1, g.2, want_tuple.0, want_tuple.1, want_tuple.2
                        ));
                    }
                }
            }
        }
        return Outcome::Fail(format!(
            "state root mismatch: got {root}, want {} (receipt status={} gas={}{}){}",
            t.hash,
            receipt.status,
            receipt.gas_used,
            if receipt.is_invalid_skip() {
                ", invalid-skip"
            } else {
                ""
            },
            if diffs.is_empty() {
                String::new()
            } else {
                format!("; account diffs: {}", diffs.join(" | "))
            },
        ));
    }
    let lh = logs_hash(&receipt);
    if lh != t.logs {
        return Outcome::Fail(format!("logs hash mismatch: got {lh}, want {}", t.logs));
    }
    Outcome::Pass
}

// ---------------------------------------------------------------------------
// The runner

#[test]
#[ignore = "needs KARDAMOM_EEST_FIXTURES (run via `just eest`)"]
fn eest_state_tests_conform() {
    let dir = std::env::var("KARDAMOM_EEST_FIXTURES").expect(
        "KARDAMOM_EEST_FIXTURES must point at an execution-spec-tests \
         fixture directory — run `just eest`",
    );
    let root = Path::new(&dir);
    // Prefer the state_tests subtree when handed a whole fixture release.
    let walk_root = ["state_tests", "fixtures/state_tests"]
        .iter()
        .map(|s| root.join(s))
        .find(|p| p.is_dir())
        .unwrap_or_else(|| root.to_path_buf());

    let xfails = load_xfails();

    let (mut pass, mut skip) = (0usize, BTreeMap::<&str, usize>::new());
    // Expected failures, keyed by the xfail entry's reason — the summary
    // shows reviewers what each accepted deviation currently costs.
    let mut expected_by_reason = BTreeMap::<String, usize>::new();
    let mut unexpected_failures: Vec<(String, String)> = Vec::new();
    let mut unexpected_passes: Vec<String> = Vec::new();
    let mut parse_failures: Vec<String> = Vec::new();

    for entry in walkdir::WalkDir::new(&walk_root)
        .sort_by_file_name()
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
    {
        let raw = match fs::read_to_string(entry.path()) {
            Ok(r) => r,
            Err(e) => {
                parse_failures.push(format!("{}: {e}", entry.path().display()));
                continue;
            }
        };
        let suite: TestSuite = match serde_json::from_str(&raw) {
            Ok(s) => s,
            Err(e) => {
                parse_failures.push(format!("{}: {e}", entry.path().display()));
                continue;
            }
        };
        for (name, unit) in suite.0 {
            let Some(posts) = unit.post.get(&FORK) else {
                continue;
            };
            for (i, t) in posts.iter().enumerate() {
                let id = format!("{name}[{i}]");
                match run_case(&unit, t) {
                    Outcome::Pass => match xfail_match(&xfails, &id) {
                        // Strict only for exact entries — see `xfail_match`.
                        Some(x) if !x.id.ends_with('*') => unexpected_passes.push(id),
                        _ => pass += 1,
                    },
                    Outcome::Skip(reason) => *skip.entry(reason).or_default() += 1,
                    Outcome::Fail(msg) => match xfail_match(&xfails, &id) {
                        Some(x) => *expected_by_reason.entry(x.reason.clone()).or_default() += 1,
                        None => unexpected_failures.push((id, msg)),
                    },
                }
            }
        }
    }

    let expected_fail: usize = expected_by_reason.values().sum();

    // Full failure list as JSON for triage / CI artifacts.
    if let Ok(report) = std::env::var("KARDAMOM_EEST_REPORT") {
        let body = serde_json::json!({
            "pass": pass,
            "expected_fail": expected_fail,
            "expected_by_reason": expected_by_reason,
            "skip": skip,
            "unexpected_failures": unexpected_failures
                .iter()
                .map(|(id, msg)| serde_json::json!({"id": id, "msg": msg}))
                .collect::<Vec<_>>(),
            "unexpected_passes": unexpected_passes,
            "parse_failures": parse_failures,
        });
        fs::write(&report, serde_json::to_string_pretty(&body).unwrap()).unwrap();
        println!("report written to {report}");
    }

    println!("== EEST state-test conformance ({FORK:?}) ==");
    println!("pass:            {pass}");
    println!("expected-fail:   {expected_fail}");
    let mut by_short = BTreeMap::<&str, usize>::new();
    for (reason, n) in &expected_by_reason {
        let short = reason.split([':', '(']).next().unwrap_or(reason).trim();
        *by_short.entry(short).or_default() += n;
    }
    for (short, n) in &by_short {
        println!("    {n:6}  {short}");
    }
    for (reason, n) in &skip {
        println!("skip ({reason}): {n}");
    }
    let total = pass + expected_fail + skip.values().sum::<usize>();
    assert!(total > 0, "no fixtures found under {}", walk_root.display());

    if !parse_failures.is_empty() {
        println!("-- unparseable files ({}):", parse_failures.len());
        for p in parse_failures.iter().take(10) {
            println!("   {p}");
        }
    }
    if !unexpected_failures.is_empty() {
        println!("-- UNEXPECTED FAILURES ({}):", unexpected_failures.len());
        for (id, msg) in unexpected_failures.iter().take(50) {
            println!("   {id}: {msg}");
        }
    }
    if !unexpected_passes.is_empty() {
        println!(
            "-- UNEXPECTED PASSES ({}) — remove stale xfail entries:",
            unexpected_passes.len()
        );
        for id in unexpected_passes.iter().take(50) {
            println!("   {id}");
        }
    }
    assert!(
        unexpected_failures.is_empty() && unexpected_passes.is_empty(),
        "{} unexpected failures, {} unexpected passes",
        unexpected_failures.len(),
        unexpected_passes.len()
    );
}
