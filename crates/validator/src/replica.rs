//! Per-replica result checks. Every executor replica publishes its own
//! BAL for each block and its own receipt for each transaction. The
//! validator compares every one of these results with its own
//! re-execution, and names the replica whose result differs.
//!
//! A replica result can arrive after the validator checked its key. The
//! validator keeps the checked result of each recent key for a bounded
//! window, so a late result is compared too. Each replica's result for a
//! key is compared once.

use std::collections::BTreeMap;
use std::fmt;

use crate::buffers::BufKey;
use crate::metrics;

/// One executor replica on one result stream: the Aeron session id of the
/// replica's publication on that stream. Each replica process opens its
/// own publication, so the session names one replica process. A restart
/// opens a new publication with a new session.
///
/// The `tx_bal` and `tx_receipts` publications of one replica have
/// different sessions. Two records map a session to its host: the
/// executor's log line `tx_bal publication open` or
/// `tx_receipts publication open`, and, with discovery, the
/// `session_id` meta of the publisher record in the catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicaId(i32);

impl ReplicaId {
    /// The replica that published on the Aeron session `session`.
    #[must_use]
    pub const fn from_session(session: i32) -> Self {
        Self(session)
    }

    /// The Aeron session id of the replica's publication.
    #[must_use]
    pub const fn session(self) -> i32 {
        self.0
    }
}

/// The check that compares a replica's result with the validator's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Check {
    /// The block write-set against the replica's BAL on `tx_bal`.
    Bal,
    /// A transaction receipt against the replica's receipt on `tx_receipts`.
    Receipt,
    /// The local account state against the replica's account rows on
    /// `tx_receipts`.
    Rows,
}

impl Check {
    /// The stable id: the `check` label of the replica metrics.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Bal => "bal",
            Self::Receipt => "receipt",
            Self::Rows => "rows",
        }
    }

    /// The stream that carries the replica results of this check.
    #[must_use]
    pub const fn stream(self) -> &'static str {
        match self {
            Self::Bal => "tx_bal",
            Self::Receipt | Self::Rows => "tx_receipts",
        }
    }
}

/// The replica that a proven divergence names, and the check that found
/// it. A repair of the executor fleet reads it to find the replica to
/// stop and rebuild.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Attribution {
    pub replica: ReplicaId,
    pub check: Check,
}

impl fmt::Display for Attribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "replica session {} on {}",
            self.replica.session(),
            self.check.stream()
        )
    }
}

/// One replica's result for one key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Arrival<V> {
    pub replica: ReplicaId,
    pub value: V,
}

/// What a take of key `K` gives the consumer: the replica results for
/// the key, and the results for lower keys that arrived after their own
/// take. An empty `current` means that no replica result arrived in time.
#[derive(Debug)]
pub struct Taken<K, V> {
    pub current: Vec<Arrival<V>>,
    pub late: Vec<(K, Arrival<V>)>,
}

/// A result that the validator compares with a replica's result of the
/// same key.
pub(crate) trait Compared: Clone {
    /// Whether the replica's result `other` agrees with `self`.
    fn agrees(&self, other: &Self) -> bool;
}

/// A replica result that differs from the validator's result. `local` is
/// the validator's result for `key`, or a replica result that agrees with
/// it.
pub(crate) struct Mismatch<K, V> {
    pub(crate) key: K,
    pub(crate) local: V,
    pub(crate) published: Arrival<V>,
}

/// The checked result of one key, and the replicas already compared
/// with it.
struct Reference<V> {
    value: V,
    replicas: Vec<ReplicaId>,
}

/// The checked results of the recent keys of one check. The window is in
/// key index units. A result for a key below the window has no reference
/// left, so it counts as unchecked.
pub(crate) struct Checked<K, V> {
    check: Check,
    window: u64,
    refs: BTreeMap<K, Reference<V>>,
}

impl<K: BufKey, V: Compared> Checked<K, V> {
    pub(crate) fn new(check: Check, window: u64) -> Self {
        Self {
            check,
            window,
            refs: BTreeMap::new(),
        }
    }

    /// Compare every replica result in `taken`: the late results with the
    /// reference of their key, and the current results with `local`. Then
    /// keep a reference for `key`. Returns the first result that differs.
    pub(crate) fn check(
        &mut self,
        key: K,
        local: &V,
        taken: Taken<K, V>,
    ) -> Result<(), Mismatch<K, V>> {
        let Taken { current, late } = taken;
        late.into_iter()
            .try_for_each(|(late_key, arrival)| self.check_late(late_key, arrival))?;
        let replicas = current
            .iter()
            .map(|arrival| self.compare(key, local, arrival))
            .collect::<Result<Vec<_>, _>>()?;
        // A current result agrees with `local`, so it serves as the
        // reference, and `local` needs no copy.
        let value = current
            .into_iter()
            .next()
            .map_or_else(|| local.clone(), |arrival| arrival.value);
        self.refs.insert(key, Reference { value, replicas });
        self.refs = self
            .refs
            .split_off(&K::from_index(key.index().saturating_sub(self.window)));
        Ok(())
    }

    /// Compare one current result with `local`. Returns its replica.
    fn compare(
        &self,
        key: K,
        local: &V,
        arrival: &Arrival<V>,
    ) -> Result<ReplicaId, Mismatch<K, V>> {
        if !local.agrees(&arrival.value) {
            return Err(Mismatch {
                key,
                local: local.clone(),
                published: arrival.clone(),
            });
        }
        metrics::counter_replica_checked(self.check);
        Ok(arrival.replica)
    }

    /// Compare one late result with the reference of its key. A key with
    /// no reference counts the result as unchecked. A replica already
    /// compared for the key is not compared again.
    fn check_late(&mut self, key: K, arrival: Arrival<V>) -> Result<(), Mismatch<K, V>> {
        let Some(reference) = self.refs.get_mut(&key) else {
            metrics::counter_replica_unchecked(self.check);
            return Ok(());
        };
        if reference.replicas.contains(&arrival.replica) {
            return Ok(());
        }
        if !reference.value.agrees(&arrival.value) {
            return Err(Mismatch {
                key,
                local: reference.value.clone(),
                published: arrival,
            });
        }
        reference.replicas.push(arrival.replica);
        metrics::counter_replica_checked(self.check);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.refs.len()
    }
}
