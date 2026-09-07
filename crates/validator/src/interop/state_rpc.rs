//! `eth_getStorageAt` on the validator's serving surface.
//!
//! The interop watcher reconciles its lane cursor with the destination's
//! `Inbox.nextSeq[origin]` at startup (audit H9). Nothing served that slot
//! before this: the ingress holds no state, and the validator served only
//! the two `kardamom_subscribe*` feeds. This method reads one storage slot
//! from the validator's own committed state, so the watcher's resume
//! position comes from the chain and not from a local file alone.
//!
//! Only the `latest` block tag is served. The validator keeps one committed
//! state, and a historical read would need an archive this node does not
//! have. A caller that asks for another tag gets an error, never a silent
//! `latest`.

use alloy_primitives::{Address, B256, U256};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::types::ErrorObjectOwned;
use kardamom_state::{StateEnv, StateSnapshot};
use kardamom_types::StateDatabase;

/// JSON-RPC error code for a block tag this node cannot serve.
const CODE_UNSUPPORTED_BLOCK: i32 = -32602;
/// JSON-RPC error code for a state-read failure.
const CODE_STATE: i32 = -32603;

/// The Ethereum `eth_getStorageAt` shape: `(address, slot, block)`. The slot
/// is a hex quantity, the result is a 32-byte hex word.
#[rpc(server, namespace = "eth")]
pub trait StateReadApi {
    #[method(name = "getStorageAt")]
    async fn get_storage_at(
        &self,
        address: Address,
        slot: U256,
        block: Option<String>,
    ) -> RpcResult<B256>;
}

/// Serves [`StateReadApi`] from a shared state environment.
pub struct StateReadHandler {
    env: StateEnv,
}

impl StateReadHandler {
    pub fn new(env: StateEnv) -> Self {
        Self { env }
    }
}

/// True for the block tags that name the committed state.
fn is_latest(block: Option<&str>) -> bool {
    matches!(
        block,
        None | Some("latest") | Some("finalized") | Some("safe") | Some("pending")
    )
}

#[jsonrpsee::core::async_trait]
impl StateReadApiServer for StateReadHandler {
    async fn get_storage_at(
        &self,
        address: Address,
        slot: U256,
        block: Option<String>,
    ) -> RpcResult<B256> {
        if !is_latest(block.as_deref()) {
            return Err(ErrorObjectOwned::owned(
                CODE_UNSUPPORTED_BLOCK,
                format!(
                    "eth_getStorageAt serves only the latest committed state, not {:?}",
                    block.unwrap_or_default()
                ),
                None::<()>,
            ));
        }
        let env = self.env.clone();
        let key = B256::from(slot.to_be_bytes::<32>());
        // A read transaction blocks for a moment. Keep it off the server's
        // async workers.
        let read = tokio::task::spawn_blocking(move || -> Result<U256, String> {
            let snap = StateSnapshot::open(&env).map_err(|e| e.to_string())?;
            snap.storage(address, key).map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| ErrorObjectOwned::owned(CODE_STATE, e.to_string(), None::<()>))?;
        let value = read.map_err(|e| ErrorObjectOwned::owned(CODE_STATE, e, None::<()>))?;
        Ok(B256::from(value.to_be_bytes::<32>()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpsee::core::client::ClientT;
    use jsonrpsee::rpc_params;
    use jsonrpsee::server::Server;
    use jsonrpsee::ws_client::WsClientBuilder;
    use kardamom_state::StateEnvBuilder;

    /// A fresh state env serves zero for every slot, over the real
    /// transport, and refuses a historical block tag.
    #[tokio::test]
    async fn serves_latest_storage_over_the_wire() {
        let dir = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(dir.path()).open().unwrap();
        let server = Server::builder().build("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let handle = server.start(StateReadApiServer::into_rpc(StateReadHandler::new(env)));

        let client = WsClientBuilder::default()
            .build(format!("ws://{addr}"))
            .await
            .unwrap();
        let slot = kardamom_types::xchain::inbox_next_seq_slot(412_346);
        let got: B256 = client
            .request(
                "eth_getStorageAt",
                rpc_params![
                    kardamom_types::xchain::INBOX,
                    U256::from_be_bytes(slot.0),
                    "latest"
                ],
            )
            .await
            .unwrap();
        assert_eq!(got, B256::ZERO);
        let got: B256 = client
            .request(
                "eth_getStorageAt",
                rpc_params![kardamom_types::xchain::INBOX, U256::from_be_bytes(slot.0)],
            )
            .await
            .unwrap();
        assert_eq!(got, B256::ZERO, "a missing tag reads as latest");

        let err = client
            .request::<B256, _>(
                "eth_getStorageAt",
                rpc_params![
                    kardamom_types::xchain::INBOX,
                    U256::from_be_bytes(slot.0),
                    "0x10"
                ],
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("latest"), "{err}");
        handle.stop().unwrap();
    }
}
