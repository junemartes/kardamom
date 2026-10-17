//! The executor answers for senders that no cache layer holds.
//!
//! A sender with no row in the local layer or in Redis is cold: it never
//! sent a transaction and never received value, or its rows aged out.
//! The admission path asks an executor once for such a sender and keeps
//! the answer here, so each cold sender costs one query pair per TTL, not
//! one per submit. A row in the local layer always wins: the admission
//! path reads this map only after the local layer and Redis missed.
//!
//! Only a fresh answer can refuse a submit. An answer is fresh when the
//! executor's snapshot is within `max_stale_txs` positions of the newest
//! position this ingress has seen, the same rule as for Redis. A stale
//! answer, a failed query, and a query shed over the in-flight bound
//! admit, and the map keeps that outcome for a short time, so a hung or
//! lagging executor costs one wait per sender, not one per submit.
//!
//! The admission path has its own query client, with its own in-flight
//! bound. A flood of cold submits can only make the admission fail open.
//! It cannot shed the account RPCs or the receipt lookups.
//!
//! The map is bounded, oldest first, like the receipt cache: the insert
//! that fills the ring retires the oldest address.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use kardamom_cache::{
    AccountView, ExecutorQuery, ExecutorQueryConfig, LiveAccounts, QueryAnswer,
    metrics as cache_metrics,
};

/// The bound on cold senders the map holds.
const CAPACITY: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();

/// The bound on admission queries in flight, for each ingress. Each cold
/// sender takes two: the nonce and the balance.
const IN_FLIGHT: NonZeroUsize = NonZeroUsize::new(16).unwrap();

/// How long a fresh answer stays. A cold sender that gets funds or lands
/// a transaction gets a row in the local layer, which wins. So the TTL
/// bounds only a missed receipt frame, and it matches the local layer's
/// default TTL.
const KNOWN_TTL: Duration = Duration::from_secs(30);

/// How long an unknown outcome stays: a stale answer, a failed query, or
/// a shed query. A submit in this time admits at once.
const UNKNOWN_TTL: Duration = Duration::from_secs(3);

/// What the admission path learned of one cold sender.
#[derive(Clone)]
enum Slot {
    /// A fresh answer: the committed state of the sender.
    Known(AccountView),
    /// No answer that can refuse a submit.
    Unknown,
}

impl Slot {
    fn ttl(&self) -> Duration {
        match self {
            Self::Known(_) => KNOWN_TTL,
            Self::Unknown => UNKNOWN_TTL,
        }
    }
}

/// The admission query client, its answers by address, and the staleness
/// rule that decides which answers can refuse.
pub(crate) struct ColdAccounts {
    query: Option<ExecutorQuery>,
    live: Arc<LiveAccounts>,
    max_stale: u64,
    map: DashMap<Address, (Slot, Instant)>,
    order: ArrayQueue<Address>,
}

impl ColdAccounts {
    /// The map over its own query client to the endpoints of `cfg`. No
    /// endpoint means no client: every cold sender admits.
    pub(crate) fn new(
        cfg: &ExecutorQueryConfig,
        max_stale: NonZeroU64,
        live: Arc<LiveAccounts>,
    ) -> Self {
        let query = ExecutorQuery::new(&ExecutorQueryConfig {
            max_in_flight: IN_FLIGHT,
            ..cfg.clone()
        });
        Self {
            query,
            live,
            max_stale: max_stale.get(),
            map: DashMap::new(),
            order: ArrayQueue::new(CAPACITY.get()),
        }
    }

    /// The fresh committed state of `sender`, from the map or from one
    /// query pair. `None` admits: no client, an unknown outcome, or a
    /// stale answer.
    ///
    /// The query future is boxed. Every submit of a sender with no cache
    /// row awaits this future inside its own task, so an inline query
    /// future would grow every submit task by the size of two HTTP
    /// requests. The box costs one allocation only for a query that goes
    /// out.
    pub(crate) async fn fresh_state(&self, sender: Address) -> Option<AccountView> {
        let slot = match self.get(sender) {
            Some(slot) => {
                cache_metrics::record_lookup("cold", "hit");
                slot
            }
            None => Box::pin(self.ask(self.query.as_ref()?, sender)).await,
        };
        match slot {
            Slot::Known(view) => Some(view),
            Slot::Unknown => None,
        }
    }

    /// Ask the executor for the nonce and the balance of `sender`, and
    /// keep the slot the answer gives.
    async fn ask(&self, query: &ExecutorQuery, sender: Address) -> Slot {
        let (nonce, balance) = tokio::join!(query.nonce(sender), query.balance(sender));
        let slot = if let (Ok(nonce), Ok(balance)) = (nonce, balance) {
            self.judge(&nonce, &balance)
        } else {
            cache_metrics::record_lookup("cold", "error");
            Slot::Unknown
        };
        self.insert(sender, slot.clone());
        slot
    }

    /// The slot for one answer pair. The older snapshot of the two
    /// decides the staleness. A snapshot with no position header is
    /// stale. Before the first receipt frame, this ingress knows no head
    /// to measure the lag against, for example after a restart on an idle
    /// chain, so every answer is stale.
    fn judge(&self, nonce: &QueryAnswer, balance: &QueryAnswer) -> Slot {
        let tx_idx = nonce.tx_idx.min(balance.tx_idx);
        let head = self.live.head();
        let fresh = tx_idx > 0 && head > 0 && head.saturating_sub(tx_idx) <= self.max_stale;
        let (Ok(nonce), true) = (u64::try_from(nonce.value), fresh) else {
            cache_metrics::record_lookup("cold", "stale");
            return Slot::Unknown;
        };
        cache_metrics::record_lookup("cold", "miss");
        Slot::Known(AccountView {
            nonce,
            balance: balance.value,
            tx_idx,
        })
    }

    /// The slot of `address`, when it is within its TTL.
    fn get(&self, address: Address) -> Option<Slot> {
        let entry = self.map.get(&address)?;
        let (slot, at) = entry.value();
        (at.elapsed() <= slot.ttl()).then(|| slot.clone())
    }

    /// Forget every answer. After a rollback, an answer can hold the state
    /// of a block that the chain lost. The next submit of each cold sender
    /// asks again.
    pub(crate) fn clear(&self) {
        self.map.clear();
        std::iter::from_fn(|| self.order.pop()).for_each(drop);
    }

    /// Keep the slot of `address`. A full ring retires the oldest
    /// address. A retired address that came again since then loses its
    /// newer slot too, which costs one more query and nothing else.
    fn insert(&self, address: Address, slot: Slot) {
        self.map.insert(address, (slot, Instant::now()));
        if let Some(oldest) = self.order.force_push(address) {
            self.map.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
    use kardamom_cache::LiveAccountsConfig;

    use super::*;

    fn view(nonce: u64) -> AccountView {
        AccountView {
            nonce,
            balance: U256::ZERO,
            tx_idx: 1,
        }
    }

    fn cold() -> ColdAccounts {
        with_writer().0
    }

    /// The map, and the writer of its local layer, with `max_stale` 8.
    fn with_writer() -> (ColdAccounts, kardamom_cache::LiveAccountsWriter) {
        let (live, writer) = LiveAccounts::new(&LiveAccountsConfig::default());
        let cold = ColdAccounts::new(
            &ExecutorQueryConfig::default(),
            NonZeroU64::new(8).unwrap(),
            live,
        );
        (cold, writer)
    }

    fn known_nonce(cold: &ColdAccounts, address: Address) -> Option<u64> {
        match cold.get(address)? {
            Slot::Known(view) => Some(view.nonce),
            Slot::Unknown => None,
        }
    }

    #[test]
    fn a_slot_is_kept_and_the_ring_retires_the_oldest() {
        let cold = cold();
        let first = Address::repeat_byte(0x01);
        cold.insert(first, Slot::Known(view(3)));
        assert_eq!(known_nonce(&cold, first), Some(3));
        (0..CAPACITY.get()).for_each(|i| {
            let mut bytes = [0xffu8; 20];
            bytes[..8].copy_from_slice(&u64::try_from(i).unwrap().to_be_bytes());
            cold.insert(Address::from(bytes), Slot::Unknown);
        });
        assert!(cold.get(first).is_none(), "the oldest address retires");
        assert_eq!(cold.map.len(), CAPACITY.get());
    }

    #[test]
    fn a_clear_forgets_every_answer() {
        let cold = cold();
        let address = Address::repeat_byte(0x02);
        cold.insert(address, Slot::Known(view(5)));
        cold.clear();
        assert!(cold.get(address).is_none());
        assert!(cold.order.is_empty());
    }

    /// An answer with no position header, an answer before the first
    /// head, and an answer far behind the head cannot refuse a submit.
    #[test]
    fn a_stale_answer_is_unknown() {
        let (cold, mut writer) = with_writer();
        let answer = |tx_idx| QueryAnswer {
            value: U256::ZERO,
            block: 0,
            tx_idx,
        };
        assert!(
            matches!(cold.judge(&answer(5), &answer(5)), Slot::Unknown),
            "no head seen yet"
        );
        writer.apply(kardamom_types::BPosition::from_index(10), &[]);
        assert!(matches!(cold.judge(&answer(0), &answer(0)), Slot::Unknown));
        assert!(matches!(cold.judge(&answer(1), &answer(5)), Slot::Unknown));
        assert!(matches!(cold.judge(&answer(5), &answer(5)), Slot::Known(_)));
    }
}
