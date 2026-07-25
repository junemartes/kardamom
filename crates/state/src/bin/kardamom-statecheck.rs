//! `kardamom-statecheck`: offline state-DB integrity sweep + cross-DB diff.
//!
//! ```text
//! kardamom-statecheck <STATE_DIR> [--compare <OTHER_DIR>] [--expect-root <0xROOT>]
//! ```
//!
//! Runs `kardamom_state::integrity::sweep` over `STATE_DIR` (and over
//! `OTHER_DIR` when given, followed by a table-level `deep_compare`).
//! `--expect-root` additionally requires the swept DB's persisted state root
//! to equal the given value (validator DBs only — executor DBs store none).
//! Exits non-zero on any problem, so it composes as a chaos/e2e assertion
//! the same way `kardamom-reconstruct --expect-root` does.
//!
//! The DBs must not have a live writer (run against stopped services or
//! copies) — the sweep opens plain mdbx transactions on the given dirs.

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

fn report(dir: &str, r: &integrity::IntegrityReport) {
    println!(
        "statecheck {dir}: block={} headers={} receipts={} accounts={} slots={} root={}",
        r.last_committed_block,
        r.headers,
        r.receipts,
        r.accounts,
        r.storage_slots,
        r.state_root
            .map(|h| h.to_string())
            .unwrap_or_else(|| "none".into()),
    );
    for p in &r.problems {
        println!("statecheck {dir}: PROBLEM: {p}");
    }
}

fn main() {
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

    let env = open(&dir);
    let mut failed = false;

    let r = match integrity::sweep(&env) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("statecheck: sweep {dir}: {e}");
            std::process::exit(1);
        }
    };
    report(&dir, &r);
    failed |= !r.is_clean();

    if let Some(expect) = expect_root {
        let want: alloy_primitives::B256 = match expect.parse() {
            Ok(h) => h,
            Err(e) => {
                eprintln!("statecheck: --expect-root {expect}: {e}");
                std::process::exit(2);
            }
        };
        match r.state_root {
            Some(got) if got == want => println!("statecheck {dir}: root matches {want}"),
            Some(got) => {
                println!("statecheck {dir}: PROBLEM: root {got} != expected {want}");
                failed = true;
            }
            None => {
                println!("statecheck {dir}: PROBLEM: no persisted root to compare");
                failed = true;
            }
        }
    }

    if let Some(other) = compare {
        let env_b = open(&other);
        let rb = match integrity::sweep(&env_b) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("statecheck: sweep {other}: {e}");
                std::process::exit(1);
            }
        };
        report(&other, &rb);
        failed |= !rb.is_clean();
        match integrity::deep_compare(&env, &env_b) {
            Ok(diffs) if diffs.is_empty() => {
                println!("statecheck: {dir} and {other} hold identical chain state");
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
    }

    std::process::exit(if failed { 1 } else { 0 });
}
