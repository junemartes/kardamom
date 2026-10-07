//! `kardamom-formats`: compare the format registry of a change with the
//! registry of its base revision. `just check-formats BASE` runs it. The
//! exit status is 1 when the change breaks a rule that no waiver covers,
//! or when a code location in the registry is not found.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use kardamom_formats::{Comparison, Registry, RegistryError};

/// Compare the format registry with the registry of a base revision.
#[derive(Debug, Parser)]
struct Cli {
    /// The registry of the change.
    #[arg(long)]
    head: PathBuf,
    /// The registry of the base revision. Leave it out when the base has
    /// no registry: then only the head is checked.
    #[arg(long)]
    base: Option<PathBuf>,
}

impl Cli {
    /// Print each problem. Return true when there is none.
    fn run(&self) -> Result<bool, RegistryError> {
        let head = Registry::read(&self.head)?;
        let missing = head.missing_code(self.head.parent().unwrap_or(Path::new(".")));
        for line in &missing {
            println!("{line}");
        }
        let Some(base) = &self.base else {
            println!("The base has no format registry. There is nothing to compare.");
            return Ok(missing.is_empty());
        };
        let base = Registry::read(base)?;
        let comparison = Comparison::new(&base, &head);
        let findings = comparison.findings();
        for finding in &findings {
            println!("{}", comparison.verdict(finding));
        }
        let waived = findings
            .iter()
            .all(|finding| comparison.waiver(finding).is_some());
        Ok(missing.is_empty() && waived)
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
