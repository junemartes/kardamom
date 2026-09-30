//! The JSON-RPC API over the archive.
//!
//! Every method reads the store directly; the follower and the API share
//! the directory, not memory, so no lock sits between them. Methods:
//!
//! - `indexer_status() -> Cursor`
//! - `indexer_batch(index) -> BatchEntry | null`
//! - `indexer_payload(daCert) -> 0x-hex payload | null`
//! - `indexer_epoch(l1Block) -> 0x-hex rkyv EpochRecord | null`

use std::net::SocketAddr;

use alloy_primitives::Bytes;
use jsonrpsee::server::{RpcModule, Server, ServerHandle};
use jsonrpsee::types::{ErrorObject, ErrorObjectOwned};

use crate::store::Store;
use crate::{BatchEntry, Cursor, IndexerError};

/// The API over one store.
pub struct Api {
    store: Store,
}

fn rpc_error(e: &IndexerError) -> ErrorObjectOwned {
    ErrorObject::owned(-32000, e.to_string(), None::<()>)
}

fn api_error(e: impl std::fmt::Display) -> IndexerError {
    IndexerError::Api(e.to_string())
}

impl Api {
    #[must_use]
    pub const fn new(store: Store) -> Self {
        Self { store }
    }

    /// Bind `addr` and serve until the handle is stopped.
    ///
    /// # Errors
    /// Returns an error when the bind fails or a method name is taken.
    pub async fn serve(self, addr: SocketAddr) -> Result<(SocketAddr, ServerHandle), IndexerError> {
        let server = Server::builder().build(addr).await.map_err(api_error)?;
        let local = server.local_addr().map_err(api_error)?;
        let module = self.module()?;
        Ok((local, server.start(module)))
    }

    fn module(self) -> Result<RpcModule<Store>, IndexerError> {
        let mut module = RpcModule::new(self.store);
        module
            .register_method(
                "indexer_status",
                |_, store, _| -> Result<Cursor, ErrorObjectOwned> {
                    store.cursor().map_err(|e| rpc_error(&e))
                },
            )
            .map_err(api_error)?;
        module
            .register_method(
                "indexer_batch",
                |params, store, _| -> Result<Option<BatchEntry>, ErrorObjectOwned> {
                    let index: u64 = params.one()?;
                    store.batch(index).map_err(|e| rpc_error(&e))
                },
            )
            .map_err(api_error)?;
        module
            .register_method(
                "indexer_payload",
                |params, store, _| -> Result<Option<Bytes>, ErrorObjectOwned> {
                    let da_cert: Bytes = params.one()?;
                    store
                        .payload(&da_cert)
                        .map(|bytes| bytes.map(Bytes::from))
                        .map_err(|e| rpc_error(&e))
                },
            )
            .map_err(api_error)?;
        module
            .register_method(
                "indexer_epoch",
                |params, store, _| -> Result<Option<Bytes>, ErrorObjectOwned> {
                    let number: u64 = params.one()?;
                    store
                        .epoch_bytes(number)
                        .map(|bytes| bytes.map(Bytes::from))
                        .map_err(|e| rpc_error(&e))
                },
            )
            .map_err(api_error)?;
        Ok(module)
    }
}
