//! EEST state-test conformance runner. This is the `consume direct`
//! analog.
//!
//! This walks the `state_tests` tree of a pinned execution-spec-tests
//! fixture release. For every post entry of the pinned fork, it executes
//! the fixture's transaction through the same path the live executor and
//! validator share: `DecodedTx::decode`, then `tx_env_from_alloy`,
//! then [`Executor::execute_tx`], under the fixture's block and cfg env
//! (through [`Executor::new_with_envs`]). It then checks the resulting
//! state root and logs hash against the fixture's expectation.
//!
//! Latest-fork-only policy: only `post.Osaka` entries run; every other
//! fork key is ignored. A fork bump changes [`FORK`] here, alongside
//! `block_env::SPEC_ID` and the fixture tag in
//! `scripts/fetch-eest-fixtures.sh`.
//!
//! Expected failures live in `tests/eest_expected_failures.json`. This
//! is the precise, versioned statement of where kardamom's execution
//! deviates from mainnet EVM. The runner fails on unexpected failures,
//! and on unexpected passes (stale entries must be removed, with their
//! reason re-examined).
//!
//! Run: `KARDAMOM_EEST_FIXTURES=<dir> cargo test -p kardamom-exec-core \
//!       --release --test eest_state -- --ignored` (or `just eest`).

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use alloy_primitives::{Address, B256, Bytes as AlloyBytes, U256, keccak256};
use alloy_rlp::Encodable;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::executor::Executor;
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_exec_core::{TxIndex, WriteSet};
use kardamom_types::{BPosition, Receipt, TxEnvelope};
use revm::context::CfgEnv;
use revm::primitives::KECCAK_EMPTY;
use revm::primitives::hardfork::SpecId;
use revm::statetest_types::{SpecName, Test, TestSuite, TestUnit};

/// The one fork this chain executes. See `block_env::SPEC_ID`.
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

/// Exact-id entries are strict: a pass flags the entry as stale.
/// Prefix (`...*`) entries are lax: they mark a family where failures are
/// expected, while cases in the family that pass still count as plain
/// passes. Many families fail only on the parametrizations that hit the
/// deviation.
fn xfail_match<'a>(xfails: &'a [Xfail], id: &str) -> Option<&'a Xfail> {
    xfails.iter().find(|x| {
        x.id == id
            || x.id
                .strip_suffix('*')
                .is_some_and(|prefix| id.starts_with(prefix))
    })
}

// ---------------------------------------------------------------------------
// Post-state oracle: a full alloc model, plus a pure MPT root

#[derive(Default)]
struct Alloc {
    /// addr to (nonce, balance, `code_hash`)
    accounts: BTreeMap<Address, (u64, U256, B256)>,
    /// (addr, slot) to value
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
                // Kardamom's empty-code sentinel is `B256::ZERO`
                // (normalized to `KECCAK_EMPTY` at the snapshot
                // boundary). The trie always encodes `KECCAK_EMPTY`.
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

/// The tx bytes are present, not a blob tx (type-3), and the sender is
/// known. `Err` carries the skip reason for a case this chain does not
/// model.
struct DecodedCase<'a> {
    txbytes: &'a AlloyBytes,
    sender: Address,
}

impl<'a> DecodedCase<'a> {
    fn decode(unit: &'a TestUnit, t: &'a Test) -> Result<Self, Outcome> {
        let Some(txbytes) = &t.txbytes else {
            return Err(Outcome::Skip("no txbytes"));
        };
        // Kardamom carries no blob transactions. Ingress rejects type-3,
        // and `max_blobs_per_tx = 0` in production cfg. Blob fixtures
        // test semantics the chain deliberately does not have.
        if txbytes.first() == Some(&0x03) {
            return Err(Outcome::Skip("blob tx (type-3 unsupported)"));
        }
        let Some(sender) = unit.transaction.sender else {
            return Err(Outcome::Skip("no sender"));
        };
        Ok(Self { txbytes, sender })
    }
}

/// The production env and initial state for one case: the fixture's
/// block/cfg env (not the production `ExecEnv` derivation, but built
/// from the same pinned-spec constructor production uses), and `pre`
/// materialized into both the mock state DB and the oracle alloc.
struct PreparedCase {
    scope: Executor<MockStateDatabase>,
    alloc: Alloc,
}

impl PreparedCase {
    fn build(unit: &TestUnit) -> Result<Self, Outcome> {
        let mut cfg = CfgEnv::new_with_spec(SPEC);
        cfg.chain_id = unit
            .env
            .current_chain_id
            .map_or(1, |id| id.try_into().unwrap_or(1));
        let block = unit.block_env(&mut cfg);
        let exec_env = ExecEnv {
            chain_id: cfg.chain_id,
            block_number: block.number.try_into().unwrap_or(0),
            l2_timestamp: block.timestamp.try_into().unwrap_or(0),
        };

        let mut alloc = Alloc::default();
        let mut db = MockStateDatabase::builder();
        for (addr, info) in &unit.pre {
            db = seed_account(db, &mut alloc, *addr, info);
        }

        let scope = Executor::new_with_envs(db.build(), None, exec_env, block, cfg)
            .map_err(|e| Outcome::Fail(format!("scope construction: {e}")))?;
        Ok(Self { scope, alloc })
    }

    /// Run the decoded tx against this prepared scope, and fold its
    /// `WriteSet` into the oracle alloc. Returns the receipt for the
    /// post-state check.
    fn execute(&mut self, decoded: &DecodedCase<'_>) -> Result<Receipt, Outcome> {
        let envelope = TxEnvelope {
            correlation_id: 0,
            raw_tx: bytes::Bytes::from(decoded.txbytes.to_vec()),
            sender: decoded.sender,
            tx_hash: keccak256(decoded.txbytes),
        };
        let (receipt, mut ws) = self
            .scope
            .execute_tx(
                kardamom_exec_core::TxSlot {
                    tx_idx: TxIndex(0),
                    tx_position: BPosition::from_index(0),
                    tx_index_in_block: 0,
                    cumulative_gas_used_before: 0,
                },
                &envelope,
                None,
                None,
            )
            .map_err(|e| Outcome::Fail(format!("executor error: {e}")))?;
        ws.finish();
        self.alloc.apply(&ws);
        Ok(receipt)
    }
}

/// Seed one EEST fixture account (its info and every storage slot) into
/// `db`/`alloc`. The single loop in [`PreparedCase::build`] calls this
/// once per account, so that function stays at one loop level.
fn seed_account(
    mut db: kardamom_exec_core::state::MockStateDatabaseBuilder,
    alloc: &mut Alloc,
    addr: Address,
    info: &revm::statetest_types::AccountInfo,
) -> kardamom_exec_core::state::MockStateDatabaseBuilder {
    let code_hash = if info.code.is_empty() {
        B256::ZERO
    } else {
        keccak256(&info.code)
    };
    db = db.account(addr, info.balance, info.nonce, code_hash);
    if !info.code.is_empty() {
        db = db.code(code_hash, bytes::Bytes::from(info.code.to_vec()));
    }
    alloc
        .accounts
        .insert(addr, (info.nonce, info.balance, code_hash));
    for (key, value) in &info.storage {
        let k = B256::from(*key);
        db = db.storage(addr, k, *value);
        if !value.is_zero() {
            alloc.storage.insert((addr, k), *value);
        }
    }
    db
}

/// The executed receipt and the folded oracle alloc, ready to check
/// against the fixture's expectation: the exception flag, the state
/// root, and the logs hash.
struct PostCheck<'a> {
    t: &'a Test,
    receipt: &'a Receipt,
    alloc: &'a Alloc,
}

impl PostCheck<'_> {
    fn check(&self) -> Outcome {
        // A fixture that expects a validation exception must land on
        // this code's deterministic invalid-skip (total derivation). An
        // executed receipt here means a validation gap, even if the
        // root happens to match.
        if self.t.expect_exception.is_some() && !self.receipt.is_invalid_skip() {
            return Outcome::Fail(format!(
                "expected exception {:?} but tx executed (status={}, gas={})",
                self.t.expect_exception, self.receipt.status, self.receipt.gas_used
            ));
        }

        let root = self.alloc.state_root();
        if root != self.t.hash {
            return Outcome::Fail(self.mismatch_detail(root));
        }
        let lh = logs_hash(self.receipt);
        if lh != self.t.logs {
            return Outcome::Fail(format!(
                "logs hash mismatch: got {lh}, want {}",
                self.t.logs
            ));
        }
        Outcome::Pass
    }

    /// When the fixture ships its full `post_state`, name the differing
    /// accounts. This is far more useful for triage than two root
    /// hashes.
    fn mismatch_detail(&self, root: B256) -> String {
        let mut diffs = Vec::new();
        for (addr, want) in &self.t.post_state {
            let got = self.alloc.accounts.get(addr);
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
        format!(
            "state root mismatch: got {root}, want {} (receipt status={} gas={}{}){}",
            self.t.hash,
            self.receipt.status,
            self.receipt.gas_used,
            if self.receipt.is_invalid_skip() {
                ", invalid-skip"
            } else {
                ""
            },
            if diffs.is_empty() {
                String::new()
            } else {
                format!("; account diffs: {}", diffs.join(" | "))
            },
        )
    }
}

/// One case: decode the fixture, build the production env and initial
/// state, execute, then check the post-state. Each step is its own
/// function; this just sequences them, short-circuiting on the first
/// `Skip`/`Fail`.
fn run_case(unit: &TestUnit, t: &Test) -> Outcome {
    fn try_run(unit: &TestUnit, t: &Test) -> Result<Outcome, Outcome> {
        let decoded = DecodedCase::decode(unit, t)?;
        let mut prepared = PreparedCase::build(unit)?;
        let receipt = prepared.execute(&decoded)?;
        Ok(PostCheck {
            t,
            receipt: &receipt,
            alloc: &prepared.alloc,
        }
        .check())
    }
    try_run(unit, t).unwrap_or_else(|outcome| outcome)
}

// ---------------------------------------------------------------------------
// The runner

/// Aggregated conformance counts across the whole fixture tree.
#[derive(Default)]
struct Stats {
    pass: usize,
    skip: BTreeMap<&'static str, usize>,
    /// Expected failures, keyed by the xfail entry's reason. Shows
    /// reviewers what each accepted deviation currently costs.
    expected_by_reason: BTreeMap<String, usize>,
    unexpected_failures: Vec<(String, String)>,
    unexpected_passes: Vec<String>,
    parse_failures: Vec<String>,
}

impl Stats {
    fn record_parse_failure(&mut self, path: &Path, err: impl core::fmt::Display) {
        self.parse_failures
            .push(format!("{}: {err}", path.display()));
    }

    fn record_case(&mut self, id: String, outcome: Outcome, xfails: &[Xfail]) {
        match outcome {
            Outcome::Pass => match xfail_match(xfails, &id) {
                // Strict only for exact entries. See `xfail_match`.
                Some(x) if !x.id.ends_with('*') => self.unexpected_passes.push(id),
                _ => self.pass += 1,
            },
            Outcome::Skip(reason) => *self.skip.entry(reason).or_default() += 1,
            Outcome::Fail(msg) => match xfail_match(xfails, &id) {
                Some(x) => *self.expected_by_reason.entry(x.reason.clone()).or_default() += 1,
                None => self.unexpected_failures.push((id, msg)),
            },
        }
    }

    fn expected_fail(&self) -> usize {
        self.expected_by_reason.values().sum()
    }

    fn total(&self) -> usize {
        self.pass + self.expected_fail() + self.skip.values().sum::<usize>()
    }

    /// Parse one fixture file and run every `FORK`-post case in it,
    /// folding results into `self`.
    fn run_file(&mut self, path: &Path, xfails: &[Xfail]) {
        let raw = match fs::read_to_string(path) {
            Ok(r) => r,
            Err(e) => return self.record_parse_failure(path, e),
        };
        let suite: TestSuite = match serde_json::from_str(&raw) {
            Ok(s) => s,
            Err(e) => return self.record_parse_failure(path, e),
        };
        for (name, unit) in suite.0 {
            let Some(posts) = unit.post.get(&FORK) else {
                continue;
            };
            for (i, t) in posts.iter().enumerate() {
                let id = format!("{name}[{i}]");
                let outcome = run_case(&unit, t);
                self.record_case(id, outcome, xfails);
            }
        }
    }

    /// Walk every fixture JSON file under `walk_root` and run its cases.
    fn walk(walk_root: &Path, xfails: &[Xfail]) -> Self {
        let mut stats = Self::default();
        for entry in walkdir::WalkDir::new(walk_root)
            .sort_by_file_name()
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
        {
            stats.run_file(entry.path(), xfails);
        }
        stats
    }

    /// Full failure list as JSON, for triage and CI artifacts, when
    /// `KARDAMOM_EEST_REPORT` names an output path.
    fn write_report_artifact(&self) {
        let Ok(report) = std::env::var("KARDAMOM_EEST_REPORT") else {
            return;
        };
        let body = serde_json::json!({
            "pass": self.pass,
            "expected_fail": self.expected_fail(),
            "expected_by_reason": self.expected_by_reason,
            "skip": self.skip,
            "unexpected_failures": self
                .unexpected_failures
                .iter()
                .map(|(id, msg)| serde_json::json!({"id": id, "msg": msg}))
                .collect::<Vec<_>>(),
            "unexpected_passes": self.unexpected_passes,
            "parse_failures": self.parse_failures,
        });
        fs::write(&report, serde_json::to_string_pretty(&body).unwrap()).unwrap();
        println!("report written to {report}");
    }

    /// Print the pass/expected-fail/skip summary and any failure detail.
    fn print_summary(&self, walk_root: &Path) {
        println!("== EEST state-test conformance ({FORK:?}) ==");
        println!("pass:            {}", self.pass);
        println!("expected-fail:   {}", self.expected_fail());
        let mut by_short = BTreeMap::<&str, usize>::new();
        for (reason, n) in &self.expected_by_reason {
            let short = reason.split([':', '(']).next().unwrap_or(reason).trim();
            *by_short.entry(short).or_default() += n;
        }
        for (short, n) in &by_short {
            println!("    {n:6}  {short}");
        }
        for (reason, n) in &self.skip {
            println!("skip ({reason}): {n}");
        }
        assert!(
            self.total() > 0,
            "no fixtures found under {}",
            walk_root.display()
        );

        if !self.parse_failures.is_empty() {
            println!("-- unparseable files ({}):", self.parse_failures.len());
            for p in self.parse_failures.iter().take(10) {
                println!("   {p}");
            }
        }
        if !self.unexpected_failures.is_empty() {
            println!(
                "-- UNEXPECTED FAILURES ({}):",
                self.unexpected_failures.len()
            );
            for (id, msg) in self.unexpected_failures.iter().take(50) {
                println!("   {id}: {msg}");
            }
        }
        if !self.unexpected_passes.is_empty() {
            println!(
                "-- UNEXPECTED PASSES ({}) — remove stale xfail entries:",
                self.unexpected_passes.len()
            );
            for id in self.unexpected_passes.iter().take(50) {
                println!("   {id}");
            }
        }
    }
}

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
    let stats = Stats::walk(&walk_root, &xfails);

    stats.write_report_artifact();
    stats.print_summary(&walk_root);

    assert!(
        stats.unexpected_failures.is_empty() && stats.unexpected_passes.is_empty(),
        "{} unexpected failures, {} unexpected passes",
        stats.unexpected_failures.len(),
        stats.unexpected_passes.len()
    );
}
