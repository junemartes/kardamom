//! The checkpoint peers of a rebuild: the executor checkpoint servers that
//! a rebuild fetches from when the local directory gets no new checkpoint.

use std::path::{Path, PathBuf};

use alloy_primitives::B256;
use anyhow::{Context, Result};
use kardamom_state::{BlockRange, fetch_best_checkpoint, genesis_digest, load_genesis};

/// The checkpoint servers (`host:port`) of a rebuild, the chain that
/// their images must belong to, and the directory of this mirror that
/// holds the fetched copies.
///
/// The directory belongs to the mirror. It is never the checkpoint
/// directory of the executor, which the mirror only reads.
#[derive(Clone)]
pub(crate) struct PeerFetch {
    peers: Vec<String>,
    genesis: B256,
    dir: PathBuf,
}

impl PeerFetch {
    /// The peers for the chain of the genesis TOML at `chain`. The
    /// fetched copies go to `dir`.
    pub(crate) fn new(peers: Vec<String>, chain: &Path, dir: PathBuf) -> Result<Self> {
        let genesis = load_genesis(chain).context("load the genesis of the checkpoint peers")?;
        Ok(Self {
            peers,
            genesis: genesis_digest(&genesis.to_alloc()),
            dir,
        })
    }

    /// The genesis digest that every restored image must state.
    pub(super) fn genesis(&self) -> B256 {
        self.genesis
    }

    /// The directory of the fetched copies.
    pub(super) fn dir(&self) -> &Path {
        &self.dir
    }

    /// Fetch the newest peer checkpoint, and the block log after it, into
    /// the directory, and return the directory. The fetch verifies the
    /// image keccak, the genesis digest, and the schema before the copy
    /// becomes visible. A failed or refused peer is logged and skipped,
    /// so the directory then holds only the copies of earlier fetches.
    /// A base that is already in the directory is not downloaded again.
    pub(super) fn fetch(&self) -> &Path {
        let _ = fetch_best_checkpoint(&self.peers, &self.dir, BlockRange::ANY, Some(self.genesis));
        &self.dir
    }
}
