//! A fake of the EigenDA proxy for tests: the file-backed [`DaStore`] on
//! a loopback port and a temporary directory.

use std::collections::HashMap;
use std::net::SocketAddr;

use alloy_primitives::Bytes;

use crate::da_store::DaStore;

/// The fake proxy. It serves until the process ends; the directory lives
/// with it.
pub struct FakeDaProxy {
    addr: SocketAddr,
    store: DaStore,
    dir: tempfile::TempDir,
}

impl FakeDaProxy {
    /// Start on a free loopback port.
    ///
    /// # Panics
    /// Panics when no directory or port can be had.
    #[must_use]
    pub fn start() -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let store = DaStore::open(dir.path()).expect("open the store");
        let addr = store
            .clone()
            .serve("127.0.0.1:0".parse().expect("a loopback address"))
            .expect("bind a loopback port");
        Self { addr, store, dir }
    }

    /// The proxy's base URL.
    #[must_use]
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The certificate this fake gives `payload`.
    #[must_use]
    pub fn cert_of(payload: &[u8]) -> Bytes {
        DaStore::cert_of(payload)
    }

    /// The payloads stored so far, by certificate.
    ///
    /// # Panics
    /// Panics when the directory cannot be read.
    #[must_use]
    pub fn stored(&self) -> HashMap<Bytes, Vec<u8>> {
        std::fs::read_dir(self.dir.path())
            .expect("read the store")
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name();
                let hex = name.to_str()?.strip_suffix(".bin")?;
                let cert = Bytes::from(alloy_primitives::hex::decode(hex).ok()?);
                let payload = self.store.get(&cert).ok()??;
                Some((cert, payload))
            })
            .collect()
    }
}
