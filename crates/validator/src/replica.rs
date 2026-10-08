//! Per-replica result checks. Every executor replica publishes its own
//! BAL for each block and its own receipt for each transaction. The
//! validator compares every one of these results with its own
//! re-execution. A divergence names every replica whose result differs,
//! and says how many replicas agree.
//!
//! A replica result can arrive after the validator checked its key. The
//! validator keeps the checked result of each recent key for a bounded
//! window, so a late result is compared too.

use std::collections::BTreeMap;
use std::fmt;

use crate::buffers::BufKey;
use crate::metrics;

/// One publisher on one result stream: the Aeron session id of its
/// publication. Each replica opens its own publication per stream, and a
/// restart normally opens a new one with a new session.
///
/// A session is not always one process. Two replicas that share one media
/// driver (the `aeron:ipc` default), or a fast restart that attaches to a
/// live publication, publish under one session. The buffers keep every
/// distinct result of a session, so a shared session never hides a wrong
/// result, but it can name two processes at once.
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

/// Why a replica result does not count as checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Skip {
    /// It arrived below the check window, or its key has no checked
    /// result left.
    Late,
    /// The same session already published the same result for the key.
    Repeat,
    /// Its key already holds the bound of distinct results, or its result
    /// already names the bound of sessions.
    Bound,
    /// The buffer was full, and its key was the highest one.
    Evicted,
    /// Its key is more than the reach above the consumer's cursor.
    Ahead,
}

impl Skip {
    /// The stable id: the `reason` label of the unchecked metric.
    pub(crate) const fn id(self) -> &'static str {
        match self {
            Self::Late => "late",
            Self::Repeat => "repeat",
            Self::Bound => "bound",
            Self::Evicted => "evicted",
            Self::Ahead => "ahead",
        }
    }
}

/// One distinct result for one key, and the sessions that published it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Distinct<V> {
    pub value: V,
    pub replicas: Vec<ReplicaId>,
}

impl<V> Distinct<V> {
    /// The replica results that this value stands for.
    pub(crate) fn results(slots: &[Self]) -> usize {
        slots.iter().map(|d| d.replicas.len()).sum()
    }
}

/// What a take of key `K` gives the consumer: the distinct results for
/// the key, and the results for lower keys that arrived after their own
/// take. An empty `current` means that no replica result arrived in time.
#[derive(Debug)]
pub struct Taken<K, V> {
    pub current: Vec<Distinct<V>>,
    pub late: Vec<(K, Vec<Distinct<V>>)>,
}

/// The replicas that a proven divergence names, and the check that found
/// it. A repair of the executor fleet reads it to find the replicas to
/// stop and rebuild.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attribution {
    pub check: Check,
    /// The sessions whose result differs from the validator's.
    pub differ: Vec<ReplicaId>,
    /// The sessions whose result agrees with the validator's.
    pub agree: Vec<ReplicaId>,
    /// How many distinct results the differing sessions published.
    pub distinct: usize,
}

impl Attribution {
    /// Sort the sessions of `results` by whether their value `agrees`.
    /// `known_good` holds sessions that agreed in an earlier comparison
    /// of the same key. Returns the attribution and the first differing
    /// value, or `None` when every value agrees.
    pub(crate) fn judge<'a, V>(
        check: Check,
        results: &'a [Distinct<V>],
        known_good: &[ReplicaId],
        agrees: impl Fn(&V) -> bool,
    ) -> Option<(Self, &'a V)> {
        let (good, bad): (Vec<&Distinct<V>>, Vec<&Distinct<V>>) =
            results.iter().partition(|d| agrees(&d.value));
        let first = bad.first()?;
        let sessions = |set: &[&Distinct<V>]| -> Vec<ReplicaId> {
            let mut all: Vec<ReplicaId> = set
                .iter()
                .flat_map(|d| d.replicas.iter().copied())
                .collect();
            all.sort_unstable();
            all.dedup();
            all
        };
        let mut agree = sessions(&good);
        agree.extend_from_slice(known_good);
        agree.sort_unstable();
        agree.dedup();
        let attribution = Self {
            check,
            differ: sessions(&bad),
            agree,
            distinct: bad.len(),
        };
        Some((attribution, &first.value))
    }

    /// Every session differs, and all of them published one result. The
    /// replicas agree with each other, so the validator's side (its
    /// binary or its state) is the suspect.
    #[must_use]
    pub fn validator_suspect(&self) -> bool {
        self.agree.is_empty() && self.distinct == 1 && self.differ.len() > 1
    }

    /// The differing sessions, comma-separated, for a log field.
    #[must_use]
    pub fn sessions(&self) -> String {
        self.joined(",")
    }

    /// The differing sessions, joined by `sep`.
    fn joined(&self, sep: &str) -> String {
        self.differ
            .iter()
            .map(|r| r.session().to_string())
            .collect::<Vec<_>>()
            .join(sep)
    }

    /// How many distinct sessions published a result for the key.
    fn total(&self) -> usize {
        let agree_only = self.agree.iter().filter(|r| !self.differ.contains(r));
        self.differ.len() + agree_only.count()
    }
}

impl fmt::Display for Attribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} of {} replica sessions differ on {}: session {}",
            self.differ.len(),
            self.total(),
            self.check.stream(),
            self.joined(", session ")
        )?;
        if self.validator_suspect() {
            write!(
                f,
                "; the replicas agree with each other, so the validator is the suspect"
            )?;
        }
        Ok(())
    }
}

/// A result that the validator compares with a replica's result of the
/// same key.
pub(crate) trait Compared: Clone {
    /// Whether the replica's result `other` agrees with `self`.
    fn agrees(&self, other: &Self) -> bool;
}

/// Replica results that differ from the validator's result. `local` is
/// the validator's result for `key`, or a replica result that agreed
/// with it. `published` is the first differing value.
pub(crate) struct Mismatch<K, V> {
    pub(crate) key: K,
    pub(crate) local: V,
    pub(crate) published: V,
    pub(crate) attribution: Attribution,
}

/// The checked result of one key, and the sessions that agreed with it.
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
    /// keep a reference for `key`. Every result of a key is compared
    /// before a mismatch returns, so the mismatch names every differing
    /// session of the lowest key that differs.
    pub(crate) fn check(
        &mut self,
        key: K,
        local: &V,
        taken: Taken<K, V>,
    ) -> Result<(), Mismatch<K, V>> {
        let Taken { current, late } = taken;
        late.into_iter()
            .try_for_each(|(late_key, results)| self.check_late(late_key, &results))?;
        if let Some((attribution, published)) =
            Attribution::judge(self.check, &current, &[], |v| local.agrees(v))
        {
            return Err(Mismatch {
                key,
                local: local.clone(),
                published: published.clone(),
                attribution,
            });
        }
        let replicas: Vec<ReplicaId> = current
            .iter()
            .flat_map(|d| d.replicas.iter().copied())
            .collect();
        metrics::counter_replica_checked(self.check, replicas.len());
        // A current result agrees with `local`, so it serves as the
        // reference, and `local` needs no copy.
        let value = current
            .into_iter()
            .next()
            .map_or_else(|| local.clone(), |d| d.value);
        self.refs.insert(key, Reference { value, replicas });
        self.refs = self
            .refs
            .split_off(&K::from_index(key.index().saturating_sub(self.window)));
        Ok(())
    }

    /// Compare the late results of one key with its reference. A key with
    /// no reference counts its results as unchecked. A session that
    /// already agreed, and agrees again, counts as a repeat.
    fn check_late(&mut self, key: K, results: &[Distinct<V>]) -> Result<(), Mismatch<K, V>> {
        let Some(reference) = self.refs.get_mut(&key) else {
            metrics::counter_replica_unchecked(self.check, Skip::Late, Distinct::results(results));
            return Ok(());
        };
        if let Some((attribution, published)) =
            Attribution::judge(self.check, results, &reference.replicas, |v| {
                reference.value.agrees(v)
            })
        {
            return Err(Mismatch {
                key,
                local: reference.value.clone(),
                published: published.clone(),
                attribution,
            });
        }
        let (repeats, new): (Vec<ReplicaId>, Vec<ReplicaId>) = results
            .iter()
            .flat_map(|d| d.replicas.iter().copied())
            .partition(|r| reference.replicas.contains(r));
        metrics::counter_replica_unchecked(self.check, Skip::Repeat, repeats.len());
        metrics::counter_replica_checked(self.check, new.len());
        reference.replicas.extend(new);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.refs.len()
    }
}
