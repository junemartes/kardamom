//! `kardamom-statecheck`: an offline state-DB integrity sweep and cross-DB
//! diff tool.
//!
//! ```text
//! kardamom-statecheck <STATE_DIR> [--compare <OTHER_DIR>] [--expect-root <0xROOT>]
//! ```
//!
//! This runs `kardamom_state::integrity::sweep` over `STATE_DIR`. If
//! `OTHER_DIR` is given, it also sweeps that directory, then runs a
//! table-level `deep_compare`.
//!
//! `--expect-root` also requires the swept DB's persisted state root to
//! equal the given value. Only validator DBs have this root; executor DBs
//! store none.
//!
//! The tool exits non-zero on any problem. This lets it compose as a
//! chaos or end-to-end test assertion, the same way
//! `kardamom-reconstruct --expect-root` does.
//!
//! The DBs must not have a live writer. Run this tool against stopped
//! services, or against copies. The sweep opens plain mdbx transactions
//! on the given directories.

use kardamom_state::{StateEnvBuilder, integrity};

fn usage() -> ! {
    eprintln!(
        "usage: kardamom-statecheck <STATE_DIR> [--compare <OTHER_DIR>] [--expect-root <0xROOT>]"
    );
    std::process::exit(2);
}

fn open(dir: &str) -> kardamom_state::StateEnv {
    match StateEnvBuilder::new(dir).open() {
        Ok(env) => env,
        Err(e) => {
            eprintln!("statecheck: open {dir}: {e}");
            std::process::exit(2);
        }
    }
}

/// Runs [`integrity::sweep`] over `dir`'s already-open `env`, printing the
/// report, or exits the process on a sweep error. Both the primary
/// directory and `--compare`'s other directory share this.
fn sweep_or_exit(dir: &str, env: &kardamom_state::StateEnv) -> integrity::IntegrityReport {
    let r = match integrity::sweep(env) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("statecheck: sweep {dir}: {e}");
            std::process::exit(1);
        }
    };
    report(dir, &r);
    r
}

fn report(dir: &str, r: &integrity::IntegrityReport) {
    println!(
        "statecheck {dir}: block={} headers={} receipts={} accounts={} slots={} root={}",
        r.last_committed_block,
        r.headers,
        r.receipts,
        r.accounts,
        r.storage_slots,
        r.state_root
            .map_or_else(|| "none".into(), |h| h.to_string()),
    );
    for p in &r.problems {
        println!("statecheck {dir}: PROBLEM: {p}");
    }
}

/// Parsed command-line arguments.
struct Args {
    dir: String,
    compare: Option<String>,
    expect_root: Option<String>,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut dir: Option<String> = None;
    let mut compare: Option<String> = None;
    let mut expect_root: Option<String> = None;
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--compare" => compare = Some(it.next().unwrap_or_else(|| usage())),
            "--expect-root" => expect_root = Some(it.next().unwrap_or_else(|| usage())),
            "--help" | "-h" => usage(),
            _ if dir.is_none() => dir = Some(a),
            _ => usage(),
        }
    }
    let Some(dir) = dir else { usage() };
    Args {
        dir,
        compare,
        expect_root,
    }
}

impl Args {
    /// Checks the swept report's persisted state root against
    /// `--expect-root`. Returns true if the comparison found a problem.
    fn check_root(&self, r: &integrity::IntegrityReport, expect: &str) -> bool {
        let want: alloy_primitives::B256 = match expect.parse() {
            Ok(h) => h,
            Err(e) => {
                eprintln!("statecheck: --expect-root {expect}: {e}");
                std::process::exit(2);
            }
        };
        match r.state_root {
            Some(got) if got == want => {
                println!("statecheck {}: root matches {want}", self.dir);
                false
            }
            Some(got) => {
                println!(
                    "statecheck {}: PROBLEM: root {got} != expected {want}",
                    self.dir
                );
                true
            }
            None => {
                println!(
                    "statecheck {}: PROBLEM: no persisted root to compare",
                    self.dir
                );
                true
            }
        }
    }

    /// Sweeps `other`, then runs [`integrity::deep_compare`] against `env`
    /// (this run's already-open env). Returns true if a problem was found.
    fn compare_dirs(&self, env: &kardamom_state::StateEnv, other: &str) -> bool {
        let env_b = open(other);
        let rb = sweep_or_exit(other, &env_b);
        let mut failed = !rb.is_clean();
        match integrity::deep_compare(env, &env_b) {
            Ok(diffs) if diffs.is_empty() => {
                println!(
                    "statecheck: {} and {other} hold identical chain state",
                    self.dir
                );
            }
            Ok(diffs) => {
                for d in &diffs {
                    println!("statecheck: DIFF: {d}");
                }
                failed = true;
            }
            Err(e) => {
                eprintln!("statecheck: compare: {e}");
                std::process::exit(1);
            }
        }
        failed
    }
}

fn main() {
    let args = parse_args();

    let env = open(&args.dir);
    let mut failed = false;

    let r = sweep_or_exit(&args.dir, &env);
    failed |= !r.is_clean();

    if let Some(expect) = &args.expect_root {
        failed |= args.check_root(&r, expect);
    }

    if let Some(other) = &args.compare {
        failed |= args.compare_dirs(&env, other);
    }

    std::process::exit(i32::from(failed));
}
