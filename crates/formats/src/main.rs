//! `kardamom-formats`: compare the format registry of a change with the
//! registry of its base revision. `just check-formats BASE` runs it. The
//! exit status is 1 when the change breaks a rule that no new waiver
//! covers, or when a code location in the registry is not found.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use kardamom_formats::{Comparison, Registry, RegistryError};

/// Compare the format registry with the registry of a base revision.
#[derive(Debug, Parser)]
struct Cli {
    /// The root of the change: the directory that holds its formats.toml.
    #[arg(long)]
    root: PathBuf,
    /// The registry of the base revision. Leave it out when the base has
    /// no registry: then only the head is checked.
    #[arg(long)]
    base: Option<PathBuf>,
}

impl Cli {
    /// Print each line of the check. Return true when nothing fails.
    fn run(&self) -> Result<bool, RegistryError> {
        let head = Registry::read(&self.root.join("formats.toml"))?;
        let missing = head.missing_code(&self.root);
        for line in &missing {
            println!("{line}");
        }
        let Some(base) = &self.base else {
            println!("The base has no format registry. There is nothing to compare.");
            return Ok(missing.is_empty());
        };
        let base = Registry::read(base)?;
        let report = Comparison::new(&base, &head).report();
        let lines = [&report.waived, &report.notes, &report.problems];
        for line in lines.into_iter().flatten() {
            println!("{line}");
        }
        Ok(missing.is_empty() && report.passed())
    }
}

fn main() -> ExitCode {
    match Cli::parse().run() {
        Ok(true) => {
            println!("format check passed");
            ExitCode::SUCCESS
        }
        Ok(false) => {
            eprintln!("format check failed");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("format check: {error}");
            ExitCode::FAILURE
        }
    }
}
