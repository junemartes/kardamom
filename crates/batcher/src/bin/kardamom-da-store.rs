//! kardamom-da-store: the file-backed stand-in for the EigenDA proxy, for
//! a deployment without EigenDA (nomad/da-store.nomad.hcl).

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use kardamom_batcher::da_store::DaStore;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-da-store",
    version,
    about = "serves the EigenDA proxy's API from a directory of payloads"
)]
struct Args {
    /// The directory of payloads.
    #[arg(long, env = "KARDAMOM_DA_STORE_DIR")]
    dir: PathBuf,
    /// The listen address.
    #[arg(long, default_value = "0.0.0.0:3100")]
    listen: SocketAddr,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let local = DaStore::open(&args.dir)
        .context("open the store")?
        .serve(args.listen)
        .context("serve")?;
    eprintln!(
        "kardamom-da-store: serving {} at {local}",
        args.dir.display()
    );
    loop {
        std::thread::park();
    }
}
