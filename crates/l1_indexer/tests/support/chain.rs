//! A controlled L1: a JSON-RPC server over a chain that the test grows
//! block by block. It answers the calls of the real `RpcL1Source`:
//! blocks by number or by the `finalized` tag, header batches and log
//! queries. The fault proxies stand in front of it, one per provider.
//!
//! The chain finalizes two blocks behind its head. Each block's hash is a
//! hash of its number, and each block names the one before it as its
//! parent. A posted batch is a real `BatchPosted` log of the settlement,
//! and a deposit is a real `DepositInitiated` log of the lockbox.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, B256, Bytes, LogData, U256, address, keccak256};
use alloy_rpc_types_eth::{Block, BlockNumberOrTag, Filter, Header, Log};
use alloy_sol_types::SolEvent;
use jsonrpsee::server::{RpcModule, Server, ServerHandle};
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_da_watcher::rpc_source::{DepositInitiated, decode_lockbox_log};
use kardamom_l1_indexer::{BatchEntry, L1Block};
use kardamom_types::epoch::derive_epoch;

pub(crate) const SETTLEMENT: Address = address!("00000000000000000000000000000000000000a1");
pub(crate) const LOCKBOX: Address = address!("00000000000000000000000000000000000000b2");
/// The timestamp of block 0. Each block is one 12-second slot later.
const GENESIS_TIME: u64 = 1_700_000_000;
const SLOT_SECONDS: u64 = 12;
/// The chain finalizes this many blocks behind its head.
const FINALITY_DEPTH: u64 = 2;

/// One block of the chain: its number, its logs, and the fields derived
/// from the number.
#[derive(Clone)]
struct ChainBlock {
    number: u64,
    logs: Vec<Log>,
}

impl ChainBlock {
    fn hash(number: u64) -> B256 {
        keccak256([b"kardamom-test-l1".as_slice(), &number.to_be_bytes()].concat())
    }

    fn rpc(&self) -> Block {
        Block {
            header: Header {
                hash: Self::hash(self.number),
                inner: alloy_consensus::Header {
                    number: self.number,
                    parent_hash: self.number.checked_sub(1).map_or(B256::ZERO, Self::hash),
                    timestamp: GENESIS_TIME + SLOT_SECONDS * self.number,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        }
    }
}

/// The blocks of the chain. The server's handlers read it on their tasks
/// while the test appends to it, so it sits behind a lock.
#[derive(Default)]
struct Blocks(Mutex<Vec<ChainBlock>>);

impl Blocks {
    fn head(&self) -> u64 {
        let blocks = self.0.lock().unwrap();
        u64::try_from(blocks.len()).unwrap().saturating_sub(1)
    }

    fn block(&self, tag: BlockNumberOrTag) -> Option<Block> {
        let head = self.head();
        let number = match tag {
            BlockNumberOrTag::Number(number) => number,
            BlockNumberOrTag::Finalized | BlockNumberOrTag::Safe => {
                head.checked_sub(FINALITY_DEPTH)?
            }
            _ => head,
        };
        let blocks = self.0.lock().unwrap();
        blocks
            .get(usize::try_from(number).ok()?)
            .map(ChainBlock::rpc)
    }

    fn logs(&self, filter: &Filter) -> Vec<Log> {
        let from = filter.get_from_block().unwrap_or(0);
        let to = filter.get_to_block().unwrap_or(u64::MAX);
        let blocks = self.0.lock().unwrap();
        let start = usize::try_from(from).unwrap().min(blocks.len());
        let end = usize::try_from(to.saturating_add(1))
            .unwrap_or(usize::MAX)
            .min(blocks.len());
        blocks[start..end.max(start)]
            .iter()
            .flat_map(|block| block.logs.iter())
            .filter(|log| filter.address.matches(&log.address()))
            .cloned()
            .collect()
    }

    fn logs_of(&self, number: u64) -> Vec<Log> {
        let blocks = self.0.lock().unwrap();
        blocks[usize::try_from(number).unwrap()].logs.clone()
    }

    fn push(&self, logs: impl FnOnce(u64, B256) -> Vec<Log>) -> u64 {
        let mut blocks = self.0.lock().unwrap();
        let number = u64::try_from(blocks.len()).unwrap();
        let logs = logs(number, ChainBlock::hash(number));
        blocks.push(ChainBlock { number, logs });
        number
    }
}

/// The running L1 and the test's handle on its chain.
pub(crate) struct FakeL1 {
    addr: SocketAddr,
    blocks: Arc<Blocks>,
    _server: ServerHandle,
}

impl FakeL1 {
    /// Serve a chain of `genesis` empty blocks on a loopback port.
    pub(crate) async fn start(genesis: u64) -> Self {
        let blocks = Arc::new(Blocks::default());
        (0..genesis).for_each(|_| {
            blocks.push(|_, _| Vec::new());
        });
        let server = Server::builder().build("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let mut module = RpcModule::new(Arc::clone(&blocks));
        module
            .register_method("eth_getBlockByNumber", |params, blocks, _| {
                let (tag, _full): (BlockNumberOrTag, bool) = params.parse().unwrap();
                serde_json::to_value(blocks.block(tag)).unwrap()
            })
            .unwrap();
        module
            .register_method("eth_getLogs", |params, blocks, _| {
                let (filter,): (Filter,) = params.parse().unwrap();
                serde_json::to_value(blocks.logs(&filter)).unwrap()
            })
            .unwrap();
        module
            .register_method("eth_chainId", |_, _, _| "0x1")
            .unwrap();
        Self {
            addr,
            blocks,
            _server: server.start(module),
        }
    }

    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub(crate) fn head(&self) -> u64 {
        self.blocks.head()
    }

    pub(crate) fn finalized(&self) -> u64 {
        self.head().saturating_sub(FINALITY_DEPTH)
    }

    /// Append `n` empty blocks.
    pub(crate) fn mine(&self, n: u64) {
        (0..n).for_each(|_| {
            self.blocks.push(|_, _| Vec::new());
        });
    }

    /// Append a block with the post of batch `index`, whose payload the DA
    /// store holds under `da_cert`, and a deposit of `mint` wei.
    pub(crate) fn post(&self, index: u64, da_cert: Bytes, mint: u64) -> u64 {
        self.blocks.push(|number, hash| {
            let posted = IKardamomL2Settlement::BatchPosted {
                batchIndex: index,
                daCert: da_cert,
                l2BlockStart: index,
                l2BlockEnd: index,
                recordsCommitment: B256::from(U256::from(index)),
            };
            let deposit = DepositInitiated {
                depositNonce: index,
                from: address!("00000000000000000000000000000000000000cc"),
                to: address!("00000000000000000000000000000000000000dd"),
                mint: U256::from(mint),
                gasLimit: 21_000,
                data: Bytes::new(),
            };
            vec![
                Self::log(number, hash, SETTLEMENT, posted.encode_log_data(), 0),
                Self::log(number, hash, LOCKBOX, deposit.encode_log_data(), 1),
            ]
        })
    }

    fn log(number: u64, hash: B256, address: Address, data: LogData, index: u64) -> Log {
        Log {
            inner: alloy_primitives::Log { address, data },
            block_hash: Some(hash),
            block_number: Some(number),
            block_timestamp: None,
            transaction_hash: Some(keccak256(hash)),
            transaction_index: Some(0),
            log_index: Some(index),
            removed: false,
        }
    }

    /// The record that a follower must publish for block `number`: its
    /// header fields, its epoch, and its batches, derived from the chain
    /// itself and not from any provider.
    pub(crate) fn record(&self, number: u64) -> L1Block {
        let logs = self.blocks.logs_of(number);
        let (posted, lockbox): (Vec<Log>, Vec<Log>) =
            logs.into_iter().partition(|l| l.address() == SETTLEMENT);
        let lockbox: Vec<_> = lockbox
            .iter()
            .map(|log| decode_lockbox_log(log).unwrap())
            .collect();
        let hash = ChainBlock::hash(number);
        L1Block {
            number,
            hash,
            parent_hash: number.checked_sub(1).map_or(B256::ZERO, ChainBlock::hash),
            timestamp: GENESIS_TIME + SLOT_SECONDS * number,
            epoch: derive_epoch(number, hash, &lockbox).unwrap(),
            batches: posted.iter().map(Self::entry).collect(),
        }
    }

    fn entry(log: &Log) -> BatchEntry {
        let ev = IKardamomL2Settlement::BatchPosted::decode_log(&log.inner).unwrap();
        BatchEntry {
            index: ev.data.batchIndex,
            da_cert: ev.data.daCert.clone(),
            l2_block_start: ev.data.l2BlockStart,
            l2_block_end: ev.data.l2BlockEnd,
            records_commitment: ev.data.recordsCommitment,
            l1_block: log.block_number.unwrap(),
            l1_tx: log.transaction_hash.unwrap(),
        }
    }
}
