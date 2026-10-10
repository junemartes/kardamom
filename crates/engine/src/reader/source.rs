//! The transaction source of the `tx_ordering` reader: where the bytes of
//! each `TxRef` come from.
//!
//! The set of sources is closed:
//!
//! - [`TxDataSource`](super::TxDataSource): the `tx_data` lanes. The
//!   executor joins them by `(lane, session, position)`, and votes to void
//!   an entry that no archive holds.
//! - [`ExecStreamSource`](super::ExecStreamSource): the executor stream.
//!   The validator and the batcher key it by canonical index, check each
//!   record against the canonical hash, and never vote.
//!
//! A role that picks one at startup names [`Either`] of the two.
//!
//! A source starts in two steps. [`TxSource::start`] runs on the caller's
//! thread: it spawns the threads that feed the source and returns a
//! [`JoinSeed`]. The seed crosses into the `tx_ordering` reader thread,
//! which builds the [`TxJoin`] there, because a join can hold Aeron
//! resources that are bound to one thread.

use std::thread::JoinHandle;

use kardamom_types::{BPosition, TxEnvelope, TxRef};

use crate::actor::Either;
use crate::error::ExecutorError;

use super::join::ReaderConfig;
use super::ports::TxOrderingSubscription;
use super::void::ReadAhead;

/// One reader or feed thread's outcome: `Ok(())` on a clean close, or the
/// first error.
pub type FeedHandle = JoinHandle<Result<(), ExecutorError>>;

/// A transaction source, before its threads start.
pub trait TxSource: Send + 'static {
    /// What crosses into the `tx_ordering` reader thread.
    type Seed: JoinSeed;

    /// Spawn the threads that feed the source. Return their handles and
    /// the seed of the join.
    fn start(self) -> SourceStart<Self::Seed>;
}

/// A started source: its feed threads, and the seed of its join.
pub struct SourceStart<D> {
    /// The threads that feed the source. Each one blocks on its
    /// subscription until the subscription closes, so the caller joins
    /// them after the pipeline.
    pub feeds: Vec<FeedHandle>,
    pub seed: D,
}

/// The part of a join that crosses into the `tx_ordering` reader thread.
pub trait JoinSeed: Send + 'static {
    type Join: TxJoin;

    /// Build the join on the reader thread, with the reader's config.
    fn build(self, cfg: &ReaderConfig) -> Self::Join;
}

/// The join of one `TxRef`, on the `tx_ordering` reader thread.
pub trait TxJoin {
    /// Get the bytes of the `TxRef` at `at.position`.
    ///
    /// # Errors
    ///
    /// Returns the error that stops the reader: the bytes cannot arrive,
    /// or the subscription failed.
    fn join<O: TxOrderingSubscription>(
        &mut self,
        at: JoinAt<'_, O>,
    ) -> Result<Joined, ExecutorError>;
}

/// One join request: the entry, the reader config, and the canonical
/// order with its read-ahead. A join that waits for a void record reads
/// ahead in the order and leaves every message it read in `backlog`, in
/// order.
pub struct JoinAt<'a, O> {
    pub(crate) tx_ref: &'a TxRef,
    pub(crate) position: BPosition,
    pub(crate) cfg: &'a ReaderConfig,
    pub(crate) order: &'a mut O,
    pub(crate) backlog: &'a mut ReadAhead,
}

/// How a join ended.
#[derive(Debug)]
pub enum Joined {
    /// The bytes of the entry.
    Tx(TxEnvelope),
    /// The canonical order voided the entry. The reader drops it.
    Voided,
    /// The canonical order closed during the wait.
    Closed,
}

impl<A: TxSource, B: TxSource> TxSource for Either<A, B> {
    type Seed = Either<A::Seed, B::Seed>;

    fn start(self) -> SourceStart<Self::Seed> {
        match self {
            Self::Left(a) => a.start().map_seed(Either::Left),
            Self::Right(b) => b.start().map_seed(Either::Right),
        }
    }
}

impl<D> SourceStart<D> {
    fn map_seed<E>(self, f: impl FnOnce(D) -> E) -> SourceStart<E> {
        SourceStart {
            feeds: self.feeds,
            seed: f(self.seed),
        }
    }
}

impl<A: JoinSeed, B: JoinSeed> JoinSeed for Either<A, B> {
    type Join = Either<A::Join, B::Join>;

    fn build(self, cfg: &ReaderConfig) -> Self::Join {
        match self {
            Self::Left(a) => Either::Left(a.build(cfg)),
            Self::Right(b) => Either::Right(b.build(cfg)),
        }
    }
}

impl<A: TxJoin, B: TxJoin> TxJoin for Either<A, B> {
    fn join<O: TxOrderingSubscription>(
        &mut self,
        at: JoinAt<'_, O>,
    ) -> Result<Joined, ExecutorError> {
        match self {
            Self::Left(a) => a.join(at),
            Self::Right(b) => b.join(at),
        }
    }
}
