use std::net::SocketAddr;

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionReceipt, TransactionRequest};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{Server, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;

use crate::error::NodeError;
use crate::instrument_method;
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
        instrument_method!("eth_chainId", {
            Ok::<U256, ErrorObjectOwned>(U256::from(self.node.chain_id()))
        })
    }

    async fn block_number(&self) -> RpcResult<U256> {
        instrument_method!("eth_blockNumber", {
            Ok::<U256, ErrorObjectOwned>(U256::from(self.node.block_number().await))
        })
    }

    async fn balance(&self, addr: Address, block: BlockNumberOrTag) -> RpcResult<U256> {
        instrument_method!("eth_getBalance", {
            require_latest(block).map_err(ErrorObjectOwned::from)?;
            Ok::<U256, ErrorObjectOwned>(self.node.balance(addr).await)
        })
    }

    async fn nonce(&self, addr: Address, block: BlockNumberOrTag) -> RpcResult<U256> {
        instrument_method!("eth_getTransactionCount", {
            require_latest(block).map_err(ErrorObjectOwned::from)?;
            Ok::<U256, ErrorObjectOwned>(U256::from(self.node.nonce(addr).await))
        })
    }

    async fn call(&self, req: TransactionRequest, block: BlockNumberOrTag) -> RpcResult<Bytes> {
        instrument_method!("eth_call", {
            require_latest(block).map_err(ErrorObjectOwned::from)?;
            self.node.call(req).await.map_err(ErrorObjectOwned::from)
        })
    }

    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        instrument_method!("eth_sendRawTransaction", {
            self.node
                .submit_raw_transaction(bytes)
                .await
                .map_err(ErrorObjectOwned::from)
        })
    }

    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<TransactionReceipt>> {
        instrument_method!("eth_getTransactionReceipt", {
            Ok::<Option<TransactionReceipt>, ErrorObjectOwned>(self.node.receipt(hash).await)
        })
    }
}

#[rpc(server, namespace = "kardamom")]
pub trait KardamomApi {
    #[method(name = "submitDepositTx")]
    async fn submit_deposit_tx(&self, bytes: Bytes) -> RpcResult<B256>;
}

pub struct KardamomHandlers {
    node: Node,
}

impl KardamomHandlers {
    pub fn new(node: Node) -> Self {
        Self { node }
    }
}

#[async_trait::async_trait]
impl KardamomApiServer for KardamomHandlers {
    async fn submit_deposit_tx(&self, bytes: Bytes) -> RpcResult<B256> {
        self.node
            .submit_deposit_transaction(bytes)
            .await
            .map_err(ErrorObjectOwned::from)
    }
}

pub async fn start_server(node: Node, addr: SocketAddr) -> Result<ServerHandle, NodeError> {
    let server = Server::builder()
        .build(addr)
        .await
        .map_err(|e| NodeError::Server(e.to_string()))?;

    let mut module = EthHandlers::new(node.clone()).into_rpc();
    module
        .merge(KardamomHandlers::new(node).into_rpc())
        .map_err(|e| NodeError::Server(e.to_string()))?;

    Ok(server.start(module))
}
