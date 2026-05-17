use std::net::SocketAddr;

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionReceipt, TransactionRequest};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{Server, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;

use crate::error::NodeError;
use crate::node::Node;

#[rpc(server, namespace = "eth")]
pub trait EthApi {
    #[method(name = "chainId")]
    async fn chain_id(&self) -> RpcResult<U256>;

    #[method(name = "blockNumber")]
    async fn block_number(&self) -> RpcResult<U256>;

    #[method(name = "getBalance")]
    async fn balance(&self, addr: Address, block: BlockNumberOrTag) -> RpcResult<U256>;

    #[method(name = "getTransactionCount")]
    async fn nonce(&self, addr: Address, block: BlockNumberOrTag) -> RpcResult<U256>;

    #[method(name = "call")]
    async fn call(&self, req: TransactionRequest, block: BlockNumberOrTag) -> RpcResult<Bytes>;

    #[method(name = "sendRawTransaction")]
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256>;

    #[method(name = "getTransactionReceipt")]
    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<TransactionReceipt>>;
}

pub struct EthHandlers {
    node: Node,
}

impl EthHandlers {
    pub fn new(node: Node) -> Self {
        Self { node }
    }
}

fn require_latest(block: BlockNumberOrTag) -> Result<(), NodeError> {
    matches!(block, BlockNumberOrTag::Latest)
        .then_some(())
        .ok_or(NodeError::UnsupportedBlockTag)
}

#[async_trait::async_trait]
impl EthApiServer for EthHandlers {
    async fn chain_id(&self) -> RpcResult<U256> {
        Ok(U256::from(self.node.chain_id()))
    }

    async fn block_number(&self) -> RpcResult<U256> {
        Ok(U256::from(self.node.block_number().await))
    }

    async fn balance(&self, addr: Address, block: BlockNumberOrTag) -> RpcResult<U256> {
        require_latest(block).map_err(ErrorObjectOwned::from)?;
        Ok(self.node.balance(addr).await)
    }

    async fn nonce(&self, addr: Address, block: BlockNumberOrTag) -> RpcResult<U256> {
        require_latest(block).map_err(ErrorObjectOwned::from)?;
        Ok(U256::from(self.node.nonce(addr).await))
    }

    async fn call(&self, req: TransactionRequest, block: BlockNumberOrTag) -> RpcResult<Bytes> {
        require_latest(block).map_err(ErrorObjectOwned::from)?;
        self.node.call(req).await.map_err(ErrorObjectOwned::from)
    }

    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        self.node
            .submit_raw_transaction(bytes)
            .await
            .map_err(ErrorObjectOwned::from)
    }

    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<TransactionReceipt>> {
        Ok(self.node.receipt(hash).await)
    }
}

pub async fn start_server(node: Node, addr: SocketAddr) -> Result<ServerHandle, NodeError> {
    let server = Server::builder()
        .build(addr)
        .await
        .map_err(|e| NodeError::Server(e.to_string()))?;
    Ok(server.start(EthHandlers::new(node).into_rpc()))
}
