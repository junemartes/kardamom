//! [`ReceiptQuery`]: the read of a receipt that the memory cache of the
//! ingress does not hold.
//!
//! The read asks at most two layers, in this order:
//! 1. The receipt query endpoints (`--receipt-query-endpoints`): the
//!    egress. It answers a new receipt with low latency.
//! 2. The executor query (`--executor-query-endpoints`): the state DB of
//!    an executor. It keeps every committed receipt.
//!
//! With no receipt query endpoint, the executor query is the only layer.
//!
//! The egress keeps only its newest receipts, in memory, from its start. It
//! answers `null` for a hash that it does not hold, and it cannot tell an
//! old hash from one that did not execute yet. So every answer from the
//! egress that is not a receipt asks the executor query: a `null`, a
//! timeout, a failed connection, a `503` of a busy store. A receipt from
//! the egress ends the read, so a new receipt waits for no second query.
//! A hash that no layer holds answers `null`.

use std::sync::Arc;

use alloy_primitives::B256;
use kardamom_cache::{ExecutorQuery, ExecutorQueryConfig};
use kardamom_types::Receipt;

/// The labels of the three outcomes of one layer on
/// `kardamom_cache_lookups_total{layer="receipt"}`.
#[derive(Clone, Copy)]
struct Outcomes {
    hit: &'static str,
    miss: &'static str,
    error: &'static str,
}

/// The first layer: the egress, or the executor query when no receipt
/// query endpoint is set.
const FIRST: Outcomes = Outcomes {
    hit: "state_hit",
    miss: "miss",
    error: "state_error",
};

/// The executor query after the egress gave no receipt.
const FALLBACK: Outcomes = Outcomes {
    hit: "fallback_hit",
    miss: "fallback_miss",
    error: "fallback_error",
};

/// One layer: a query client and the labels of its outcomes.
#[derive(Clone)]
struct Layer {
    query: ExecutorQuery,
    outcomes: Outcomes,
}

impl Layer {
    /// The receipt of `tx_hash` from this layer. A failed query counts as
    /// an error and answers `None`.
    async fn receipt(&self, tx_hash: B256) -> Option<Receipt> {
        let found = self.query.receipt(tx_hash).await;
        let outcome = match &found {
            Ok(Some(_)) => self.outcomes.hit,
            Ok(None) => self.outcomes.miss,
            Err(_) => self.outcomes.error,
        };
        kardamom_cache::metrics::record_lookup("receipt", outcome);
        found.ok().flatten()
    }
}

/// The layers behind the memory cache, in the order of the read. Cheap to
/// clone: every clone shares the clients.
#[derive(Clone)]
pub(crate) struct ReceiptQuery {
    layers: Arc<[Layer]>,
}

impl ReceiptQuery {
    /// The layers of `receipt` (the egress) and `executor`. The executor
    /// query is the client of the account RPCs, so the receipt reads
    /// share its bounds: its in-flight bound, which sheds at once, and its
    /// timeout.
    pub(crate) fn new(receipt: &ExecutorQueryConfig, executor: Option<&ExecutorQuery>) -> Self {
        let layer = |query: &ExecutorQuery, outcomes| Layer {
            query: query.clone(),
            outcomes,
        };
        let layers = match ExecutorQuery::new(receipt) {
            Some(egress) => [
                Some(layer(&egress, FIRST)),
                executor.map(|q| layer(q, FALLBACK)),
            ],
            None => [executor.map(|q| layer(q, FIRST)), None],
        };
        Self {
            layers: layers.into_iter().flatten().collect(),
        }
    }

    /// True when the config names no layer: a cache miss stays a miss.
    pub(crate) fn is_off(&self) -> bool {
        self.layers.is_empty()
    }

    /// The receipt of `tx_hash` from the first layer that holds it.
    pub(crate) async fn receipt(&self, tx_hash: B256) -> Option<Receipt> {
        for layer in self.layers.iter() {
            if let Some(receipt) = layer.receipt(tx_hash).await {
                return Some(receipt);
            }
        }
        None
    }
}
