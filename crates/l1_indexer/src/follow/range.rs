//! One range read: the headers, the chain check, the light client
//! anchor, the logs, and the record of each block.

use std::collections::BTreeMap;

use alloy_rpc_types_eth::{Filter, Log};
use alloy_sol_types::SolEvent;
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_da_watcher::rpc_source::{DepositInitiated, UpgradeInitiated, decode_lockbox_log};
use kardamom_da_watcher::{L1Header, L1Source};
use kardamom_types::epoch::derive_epoch;

use super::{Follower, Read};
use crate::sink::BlockSink;
use crate::{BatchEntry, IndexerError, L1Block};

impl<S: L1Source, K: BlockSink> Follower<S, K> {
    /// The checked records of the blocks `from..=to`. `at_tip` says the
    /// range ends at the finalized tip, where the light client anchors it.
    pub(super) async fn read_range(
        &self,
        from: u64,
        to: u64,
        at_tip: bool,
    ) -> Result<Vec<L1Block>, IndexerError> {
        Read::Headers.count();
        let headers = self.source.headers(from, to).await?;
        self.check_chain(&headers)?;
        if at_tip && let Some(last) = headers.last() {
            self.anchor(last).await?;
        }
        let mut logs = self.logs(from, to).await?;
        headers
            .iter()
            .map(|header| self.record(header, logs.remove(&header.number).unwrap_or_default()))
            .collect()
    }

    /// Each header names the one before it as its parent; the first names
    /// the cursor's block.
    fn check_chain(&self, headers: &[L1Header]) -> Result<(), IndexerError> {
        let cursor = self.cursor.l1_block.map(|b| b.hash);
        let parents = cursor.into_iter().chain(headers.iter().map(|h| h.hash));
        parents
            .zip(headers.iter().skip(usize::from(cursor.is_none())))
            .find(|(hash, header)| header.parent_hash != *hash)
            .map_or(Ok(()), |(expected, header)| {
                Err(IndexerError::ChainBreak {
                    number: header.number,
                    expected,
                    parent: header.parent_hash,
                })
            })
    }

    /// The step's last header against the light client's header for its
    /// number. With the parent chain, it verifies every header of the
    /// step. Without a light client, the two-source rule stands alone.
    async fn anchor(&self, last: &L1Header) -> Result<(), IndexerError> {
        let Some(light_client) = self.source.light_client_hash(last.number).await? else {
            return Ok(());
        };
        Read::LightClient.count();
        if light_client == last.hash {
            return Ok(());
        }
        Err(IndexerError::LightClientMismatch {
            number: last.number,
            follower: last.hash,
            light_client,
        })
    }

    /// The settlement's and the lockbox's logs of `from..=to`, by block.
    /// One query names both addresses; a query spans at most
    /// `max_log_range` blocks.
    async fn logs(&self, from: u64, to: u64) -> Result<BTreeMap<u64, Vec<Log>>, IndexerError> {
        let step = self.cfg.max_log_range.get();
        let starts = (from..=to).step_by(usize::try_from(step).unwrap_or(usize::MAX));
        let mut by_block: BTreeMap<u64, Vec<Log>> = BTreeMap::new();
        for start in starts {
            self.logs_chunk(start, to.min(start.saturating_add(step - 1)), &mut by_block)
                .await?;
        }
        Ok(by_block)
    }

    /// One log query, `from..=to`, into `by_block`.
    async fn logs_chunk(
        &self,
        from: u64,
        to: u64,
        by_block: &mut BTreeMap<u64, Vec<Log>>,
    ) -> Result<(), IndexerError> {
        let filter = Filter::new()
            .address(vec![self.cfg.settlement, self.cfg.lockbox])
            .event_signature(vec![
                IKardamomL2Settlement::BatchPosted::SIGNATURE_HASH,
                DepositInitiated::SIGNATURE_HASH,
                UpgradeInitiated::SIGNATURE_HASH,
            ])
            .from_block(from)
            .to_block(to);
        Read::Logs.count();
        Self::bucket(self.source.logs(&filter).await?, by_block)
    }

    /// Put each log under its block number.
    fn bucket(logs: Vec<Log>, into: &mut BTreeMap<u64, Vec<Log>>) -> Result<(), IndexerError> {
        logs.into_iter().try_for_each(|log| {
            let number = log
                .block_number
                .ok_or_else(|| IndexerError::Provider("a log without a block number".into()))?;
            into.entry(number).or_default().push(log);
            Ok(())
        })
    }

    /// The record of one block from its header and its logs. Every log
    /// must carry the header's hash: a log of another view of the block
    /// would derive an epoch L1 never held.
    fn record(&self, header: &L1Header, logs: Vec<Log>) -> Result<L1Block, IndexerError> {
        let derive = |error: String| IndexerError::Derive {
            number: header.number,
            error,
        };
        if let Some(log) = logs.iter().find(|l| l.block_hash != Some(header.hash)) {
            return Err(derive(format!(
                "a log carries block hash {:?}, the header {}",
                log.block_hash, header.hash
            )));
        }
        let (posted, lockbox): (Vec<Log>, Vec<Log>) = logs
            .into_iter()
            .partition(|l| l.address() == self.cfg.settlement);
        if let Some(log) = lockbox.iter().find(|l| l.address() != self.cfg.lockbox) {
            return Err(derive(format!(
                "a log of {}, which the query did not name",
                log.address()
            )));
        }
        let batches = posted
            .iter()
            .map(Self::batch_entry)
            .collect::<Result<Vec<_>, _>>()?;
        let lockbox = lockbox
            .iter()
            .map(decode_lockbox_log)
            .collect::<Result<Vec<_>, _>>()?;
        let epoch = derive_epoch(header.number, header.hash, &lockbox)
            .map_err(|e| derive(e.to_string()))?;
        Ok(L1Block {
            number: header.number,
            hash: header.hash,
            parent_hash: header.parent_hash,
            timestamp: header.timestamp,
            epoch,
            batches,
        })
    }

    /// The batch a `BatchPosted` log names.
    fn batch_entry(log: &Log) -> Result<BatchEntry, IndexerError> {
        let ev = IKardamomL2Settlement::BatchPosted::decode_log(&log.inner)
            .map_err(|e| IndexerError::Provider(format!("decode BatchPosted: {e}")))?;
        let l1_block = log.block_number.ok_or_else(|| {
            IndexerError::Provider("BatchPosted log without a block number".into())
        })?;
        let l1_tx = log
            .transaction_hash
            .ok_or_else(|| IndexerError::Provider("BatchPosted log without a tx hash".into()))?;
        Ok(BatchEntry {
            index: ev.data.batchIndex,
            da_cert: ev.data.daCert.clone(),
            l2_block_start: ev.data.l2BlockStart,
            l2_block_end: ev.data.l2BlockEnd,
            records_commitment: ev.data.recordsCommitment,
            l1_block,
            l1_tx,
        })
    }
}
