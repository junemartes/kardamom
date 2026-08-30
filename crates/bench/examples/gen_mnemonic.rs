//! Print a fresh BIP-39 mnemonic phrase.
//!
//! Use this to seed a custom `BenchWorkflow` that must not share signers with
//! the built-in Anvil-mnemonic workflows.
//!
//! Usage:
//!   `cargo run --example gen_mnemonic -p kardamom-bench`
//!   `cargo run --example gen_mnemonic -p kardamom-bench -- 24`

fn main() -> anyhow::Result<()> {
    let word_count: u32 = std::env::args()
        .nth(1)
        .map(|s| s.parse())
        .transpose()
        .map_err(|e| anyhow::anyhow!("bad word count: {e}"))?
        .unwrap_or(12);
    println!("{}", kardamom_bench::mnemonic::generate(word_count)?);
    Ok(())
}
