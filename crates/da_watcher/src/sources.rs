//! A set of L1 sources, cross-checked, with rotation.
//!
//! A follower that reads one public endpoint trusts it with the chain's
//! L1 view. A set answers every read from more than one endpoint:
//!
//! - A block's ids, or a log query, is accepted when two sources agree,
//!   or when the light client serves it. One public source alone is
//!   accepted only when the set holds one public source.
//! - The finalized tip is the lowest tip the agreeing sources report:
//!   every source has finalized through it.
//! - A source that errors or answers HTTP 429 rotates out for the
//!   backoff. The set keeps going on the rest. With every source out, or
//!   fewer live than the rule needs, the read fails, and the follower
//!   reports it every tick.
//! - A disagreement the light client does not settle is an error with
//!   both answers in the log and in [`metrics::L1_SOURCE_DISAGREEMENT_TOTAL`].
//!   It is never resolved by majority: two public endpoints can share a
//!   backend. With a light client, the source that disagrees with it is
//!   the liar, and rotates out.

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::{Filter, Log as RpcLog};
use async_trait::async_trait;
use futures::future::join_all;
use tracing::{error, warn};

use crate::metrics;
use crate::rpc_source::RpcL1Source;
use crate::source::{L1Source, L1SourceError, LockboxLog};

/// How long a failed source stays out of the set.
const DEFAULT_BACKOFF: Duration = Duration::from_secs(30);

/// Why a source set cannot serve a read: the follower's halt cause. The
/// error the follower reports, its log line and the disagreement counter
/// carry this one value, so a halt record carries it unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceHalt {
    /// Two sources gave different answers for `what`, and no light client
    /// settles it. A majority of public endpoints proves nothing, since
    /// two can share a backend. The halt clears when the sources agree
    /// again: the operator drops the lying endpoint.
    #[error("L1 sources disagree on {what}: {a_name} says {a}, {b_name} says {b}")]
    Disagreement {
        what: String,
        a_name: String,
        a: String,
        b_name: String,
        b: String,
    },
    /// Fewer sources answered than the agreement rule needs: the others
    /// are rotated out, or down. The halt clears when a source returns
    /// after its backoff.
    #[error("{answered} of {configured} L1 sources answered; {needed} agreeing answers needed")]
    NoQuorum {
        answered: usize,
        needed: usize,
        configured: usize,
    },
}

impl SourceHalt {
    /// The stable id of the cause.
    #[must_use]
    pub fn cause(&self) -> &'static str {
        match self {
            Self::Disagreement { .. } => "l1_source_disagreement",
            Self::NoQuorum { .. } => "l1_sources_out",
        }
    }

    /// Log the cause and count a disagreement. A disagreement the light
    /// client settles is reported here too: the liar rotates out instead
    /// of halting the follower, and the counter still says an endpoint
    /// lied.
    fn report(&self) {
        if let Self::Disagreement { .. } = self {
            ::metrics::counter!(metrics::L1_SOURCE_DISAGREEMENT_TOTAL).increment(1);
        }
        error!(target: "l1_sources", cause = self.cause(), detail = %self, "L1 source set cannot serve the read");
    }

    /// The disagreement of two answers to `what`.
    fn disagreement<R: Debug>(what: &str, a: (&str, &R), b: (&str, &R)) -> Self {
        Self::Disagreement {
            what: what.to_string(),
            a_name: a.0.to_string(),
            a: format!("{:?}", a.1),
            b_name: b.0.to_string(),
            b: format!("{:?}", b.1),
        }
    }
}

/// The endpoints a follower is given: the public RPC list, and the light
/// client when one runs. Parsed once at the CLI boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct L1Endpoints {
    pub rpcs: Vec<String>,
    pub light_client: Option<String>,
}

impl L1Endpoints {
    /// Connect every endpoint and build the set. The light client is its
    /// authoritative member.
    ///
    /// # Errors
    ///
    /// Returns a provider error when an endpoint does not connect.
    pub async fn connect(
        self,
    ) -> Result<L1Sources<RpcL1Source<impl Provider + 'static>>, L1SourceError> {
        let publics = join_all(self.rpcs.into_iter().map(Self::connect_one)).await;
        let mut set = L1Sources::new(publics.into_iter().collect::<Result<Vec<_>, _>>()?);
        if let Some(url) = self.light_client {
            let (name, source) = Self::connect_one(url).await?;
            set = set.with_light_client(name, source);
        }
        Ok(set)
    }

    async fn connect_one(
        url: String,
    ) -> Result<(String, RpcL1Source<impl Provider + 'static>), L1SourceError> {
        let provider = ProviderBuilder::new()
            .connect(&url)
            .await
            .map_err(|e| L1SourceError::Provider(format!("connect L1 RPC {url}: {e}")))?;
        Ok((url, RpcL1Source::new(provider)))
    }
}

/// One source of the set, with its rotation state.
struct Member<S> {
    name: String,
    source: S,
    /// The light client: its answer settles a read when it answers.
    authoritative: bool,
    /// The second, counted from the set's start, at which the member is
    /// back in the set. Zero means it never left. The trait reads through
    /// `&self` from many tasks, so the state is an atomic, not a lock.
    out_until: AtomicU64,
}

/// The set of sources a follower runs on. See the module docs for the
/// rules.
pub struct L1Sources<S> {
    members: Vec<Member<S>>,
    backoff: Duration,
    started: Instant,
}

/// One member's answer to a read.
struct Answer<'a, S, R> {
    member: &'a Member<S>,
    result: Result<R, L1SourceError>,
}

/// The answers of a read that succeeded, after the failed members
/// rotated out.
struct Settled<'a, S, R> {
    authoritative: Option<R>,
    publics: Vec<(&'a Member<S>, R)>,
}

impl<S: L1Source> L1Sources<S> {
    /// A set of public sources, each with the name its log lines carry.
    #[must_use]
    pub fn new(sources: Vec<(String, S)>) -> Self {
        let members = sources
            .into_iter()
            .map(|(name, source)| Member {
                name,
                source,
                authoritative: false,
                out_until: AtomicU64::new(0),
            })
            .collect();
        Self {
            members,
            backoff: DEFAULT_BACKOFF,
            started: Instant::now(),
        }
    }

    /// Add the light client. Its answer is accepted when it answers; a
    /// public source that disagrees with it is the liar.
    #[must_use]
    pub fn with_light_client(mut self, name: String, source: S) -> Self {
        self.members.push(Member {
            name,
            source,
            authoritative: true,
            out_until: AtomicU64::new(0),
        });
        self
    }

    /// How long a failed source stays out.
    #[must_use]
    pub fn backoff(mut self, backoff: Duration) -> Self {
        self.backoff = backoff;
        self
    }

    /// Seconds since the set started: the clock the rotation runs on.
    fn now(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// The members in the set now.
    fn live(&self) -> Vec<&Member<S>> {
        let now = self.now();
        self.members
            .iter()
            .filter(|m| m.out_until.load(Ordering::Relaxed) <= now)
            .collect()
    }

    /// How many agreeing public answers a read needs: two, or every
    /// public source when the set holds fewer.
    fn quorum(&self) -> usize {
        self.members
            .iter()
            .filter(|m| !m.authoritative)
            .count()
            .min(2)
    }

    /// Rotate `member` out for the backoff.
    fn rotate_out(&self, member: &Member<S>, reason: &'static str) {
        // The process does not run for 2^64 seconds; saturate instead of
        // wrapping back into the set.
        let until = self.now().saturating_add(self.backoff.as_secs());
        member.out_until.store(until, Ordering::Relaxed);
        ::metrics::counter!(
            metrics::L1_SOURCE_ROTATIONS_TOTAL,
            "source" => member.name.clone(),
            "reason" => reason
        )
        .increment(1);
        warn!(
            target: "l1_sources",
            source = %member.name,
            reason,
            backoff_s = self.backoff.as_secs(),
            "L1 source rotated out"
        );
    }

    /// The answers of `live`, in its order. Each trait method asks the
    /// live members concurrently, then settles the answers here.
    fn answered<R>(
        live: Vec<&Member<S>>,
        results: Vec<Result<R, L1SourceError>>,
    ) -> Vec<Answer<'_, S, R>> {
        live.into_iter()
            .zip(results)
            .map(|(member, result)| Answer { member, result })
            .collect()
    }

    /// Rotate out every member whose read failed, and keep the answers.
    /// `what` names the read in the log line.
    fn settle<'a, R>(&self, what: &str, answers: Vec<Answer<'a, S, R>>) -> Settled<'a, S, R> {
        let mut settled = Settled {
            authoritative: None,
            publics: Vec::new(),
        };
        for answer in answers {
            self.settle_one(what, answer, &mut settled);
        }
        settled
    }

    fn settle_one<'a, R>(
        &self,
        what: &str,
        answer: Answer<'a, S, R>,
        into: &mut Settled<'a, S, R>,
    ) {
        let member = answer.member;
        match answer.result {
            Ok(value) if member.authoritative => into.authoritative = Some(value),
            Ok(value) => into.publics.push((member, value)),
            Err(L1SourceError::RateLimited) => self.rotate_out(member, "rate_limited"),
            Err(e) => {
                warn!(target: "l1_sources", source = %member.name, what, error = %e, "L1 source failed");
                self.rotate_out(member, "error");
            }
        }
    }

    /// The halt of a read that `answered` public sources answered, fewer
    /// than the rule needs.
    fn no_quorum(&self, answered: usize) -> L1SourceError {
        let halt = SourceHalt::NoQuorum {
            answered,
            needed: self.quorum(),
            configured: self.members.len(),
        };
        halt.report();
        L1SourceError::Halt(halt)
    }

    /// Settle a read whose answers must be equal: the light client's
    /// answer, or the agreement of the quorum. See the module docs.
    fn agree<R: Clone + PartialEq + Debug>(
        &self,
        what: &str,
        answers: Vec<Answer<'_, S, R>>,
    ) -> Result<R, L1SourceError> {
        let settled = self.settle(what, answers);
        if let Some(truth) = settled.authoritative {
            settled
                .publics
                .iter()
                .filter(|(_, value)| *value != truth)
                .for_each(|(member, value)| {
                    SourceHalt::disagreement(what, ("light client", &truth), (&member.name, value))
                        .report();
                    self.rotate_out(member, "disagreement");
                });
            return Ok(truth);
        }
        // A set with no public source needs the light client's answer, so
        // an empty answer list is short of the quorum even at quorum zero.
        let [(first, truth), rest @ ..] = settled.publics.as_slice() else {
            return Err(self.no_quorum(0));
        };
        if settled.publics.len() < self.quorum() {
            return Err(self.no_quorum(settled.publics.len()));
        }
        match rest.iter().find(|(_, value)| value != truth) {
            None => Ok(truth.clone()),
            Some((other, value)) => {
                let halt =
                    SourceHalt::disagreement(what, (&first.name, truth), (&other.name, value));
                halt.report();
                Err(L1SourceError::Halt(halt))
            }
        }
    }

    /// Settle the finalized-tip read: the light client's tip, or the
    /// lowest tip of the quorum. `None` is a source with no finalized
    /// block yet.
    fn lowest_tip(&self, answers: Vec<Answer<'_, S, Option<u64>>>) -> Result<u64, L1SourceError> {
        let settled = self.settle("finalized tip", answers);
        if let Some(truth) = settled.authoritative {
            return truth.ok_or(L1SourceError::NotFinalized);
        }
        if settled.publics.is_empty() || settled.publics.len() < self.quorum() {
            return Err(self.no_quorum(settled.publics.len()));
        }
        settled
            .publics
            .into_iter()
            .map(|(_, tip)| tip.ok_or(L1SourceError::NotFinalized))
            .try_fold(u64::MAX, |lowest, tip| tip.map(|t| lowest.min(t)))
    }
}

#[async_trait]
impl<S: L1Source> L1Source for L1Sources<S> {
    async fn finalized_block_number(&self) -> Result<u64, L1SourceError> {
        let live = self.live();
        let results = join_all(live.iter().map(|m| m.source.finalized_block_number()))
            .await
            .into_iter()
            .map(|result| match result {
                Ok(tip) => Ok(Some(tip)),
                Err(L1SourceError::NotFinalized) => Ok(None),
                Err(e) => Err(e),
            })
            .collect();
        self.lowest_tip(Self::answered(live, results))
    }

    async fn block_ids(&self, number: u64) -> Result<(B256, B256), L1SourceError> {
        let live = self.live();
        let results = join_all(live.iter().map(|m| m.source.block_ids(number))).await;
        self.agree(&format!("block {number}"), Self::answered(live, results))
    }

    async fn logs(&self, filter: &Filter) -> Result<Vec<RpcLog>, L1SourceError> {
        let live = self.live();
        let results = join_all(live.iter().map(|m| m.source.logs(filter))).await;
        self.agree(&format!("logs {filter:?}"), Self::answered(live, results))
    }

    async fn lockbox_logs(
        &self,
        lockbox: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<LockboxLog>, L1SourceError> {
        let live = self.live();
        let results = join_all(
            live.iter()
                .map(|m| m.source.lockbox_logs(lockbox, from_block, to_block)),
        )
        .await;
        self.agree(
            &format!("lockbox logs {from_block}..={to_block}"),
            Self::answered(live, results),
        )
    }
}
