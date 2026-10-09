//! Where a canonical index is in the recordings of this executor's stream,
//! and the answer to `kardamom_getExecLocator`.
//!
//! A peer executor whose archives all failed for entry `i` asks this
//! executor where `i` is. One owner thread holds what the answer needs:
//!
//! - the progress of the current run of the `tx_ordering` reader: its
//!   first index, every index it passed, and the entries it parked at or
//!   dropped;
//! - a copy of the locator log: the stream publisher sends each locator
//!   that it appends.
//!
//! The reader and the publisher send over a bounded channel. The query
//! endpoint sends a question with a oneshot reply. No lock is shared.
//!
//! The answer, for entry `i`:
//!
//! | Answer | Condition |
//! |---|---|
//! | `located` | This executor joined `i`: its current run passed `i` with no park, or its state holds a receipt at `i`. The locator log names a position at or before `i`. |
//! | `not_held` | This executor reached `i` and did not join it: it parks at `i`, its current run dropped `i`, or its state shows `i` vacant. |
//! | `not_reached` | This executor has not reached `i`. |
//! | `lost` | The state of this executor holds a receipt at `i`, and the locator log names no position at or before `i`. |
//!
//! "Not reached" is not "not held": a reader that has not reached `i`
//! can still join it, so the asker asks again. `lost` is not "not held"
//! either: this executor executed `i`, or adopted a state that includes
//! `i`. A `located` answer is a lower bound. The asker replays from it and
//! checks the record, so a locator of an older session costs a replay
//! with no record, never a wrong record.

use std::collections::BTreeSet;

use crossbeam_channel::{Receiver, Sender};
use kardamom_types::{BPosition, StateDatabase};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::snapshot::StateSnapshot;

/// The depth of the channel to the owner thread. The owner does a set or
/// a vector operation per message, so the channel stays short. A full
/// channel makes a sender wait, and drops a question.
const CHANNEL_DEPTH: usize = 4096;

/// Where one record is: the session of its recording and a raw stream
/// position at or before the start of the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecLocator {
    /// The canonical index of the record.
    pub index: u64,
    /// The Aeron session id of the recorded publication.
    pub session_id: i32,
    /// A raw stream position at or before the start of the record.
    pub position: i64,
}

/// Locators in append order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Locators(Vec<ExecLocator>);

impl Locators {
    pub fn push(&mut self, locator: ExecLocator) {
        self.0.push(locator);
    }

    /// Every locator, in append order.
    #[must_use]
    pub fn entries(&self) -> &[ExecLocator] {
        &self.0
    }

    /// The newest locator whose index is at or below `index`. A replay of
    /// its session from its position reaches `index` when that session
    /// holds it. The locator is a lower bound: the replay can start before
    /// the record.
    #[must_use]
    pub fn lookup(&self, index: u64) -> Option<ExecLocator> {
        self.0.iter().rev().find(|l| l.index <= index).copied()
    }
}

/// The answer of one executor for one canonical entry, as JSON:
/// `{"status":"located","archive_id":…,"session_id":…,"position":…}`,
/// `{"status":"not_held"}`, `{"status":"not_reached"}` or
/// `{"status":"lost"}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ExecLocatorAnswer {
    /// The archive `archive_id` records the session `session_id`, and a
    /// replay from the raw position `position` reaches the entry.
    Located {
        archive_id: String,
        session_id: i32,
        position: i64,
    },
    /// The executor reached the entry and holds no record of it.
    NotHeld,
    /// The executor has not reached the entry. It can still join it.
    NotReached,
    /// The executor holds a state with the entry and no locator for it.
    Lost,
}

/// What the owner thread knows about one entry. The state read settles a
/// [`Self::Before`] answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunAnswer {
    NotHeld,
    NotReached,
    /// The current run joined the entry.
    Joined(Option<ExecLocator>),
    /// The entry is below the current run, or no run started yet.
    Before(Option<ExecLocator>),
}

/// What the committed state holds at one canonical index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommittedSlot {
    /// The state does not reach the index yet.
    Beyond,
    /// The state holds a receipt at the index.
    Executed,
    /// The state passed the index with no receipt there.
    Vacant,
}

impl CommittedSlot {
    /// The slot at `index` in a fresh snapshot. Blocking: mdbx reads.
    pub(crate) fn read(env: &StateEnv, index: u64) -> Result<Self, StateError> {
        let snapshot = StateSnapshot::open(env)?;
        // The end position is the record count of the committed blocks:
        // the first index that they do not hold.
        if index >= snapshot.end_tx_position()?.as_index() {
            return Ok(Self::Beyond);
        }
        Ok(match snapshot.get_receipt(BPosition::from_index(index))? {
            Some(_) => Self::Executed,
            None => Self::Vacant,
        })
    }
}

impl RunAnswer {
    /// Whether the answer needs the state read.
    pub(crate) fn needs_state(self) -> bool {
        matches!(self, Self::Before(_))
    }

    /// The final answer. `slot` is the state read, for a
    /// [`Self::Before`] answer.
    pub(crate) fn settle(self, archive_id: &str, slot: CommittedSlot) -> ExecLocatorAnswer {
        let located = |l: ExecLocator| ExecLocatorAnswer::Located {
            archive_id: archive_id.to_owned(),
            session_id: l.session_id,
            position: l.position,
        };
        match (self, slot) {
            (Self::NotHeld, _) | (Self::Before(_), CommittedSlot::Vacant) => {
                ExecLocatorAnswer::NotHeld
            }
            // The publisher sends the locator of a record after its
            // offer, so a joined record can still lack one for a moment.
            (Self::NotReached | Self::Joined(None), _)
            | (Self::Before(_), CommittedSlot::Beyond) => ExecLocatorAnswer::NotReached,
            (Self::Joined(Some(l)), _) => located(l),
            (Self::Before(l), CommittedSlot::Executed) => {
                l.map_or(ExecLocatorAnswer::Lost, located)
            }
        }
    }
}

/// One message to the owner thread.
enum AnswersMsg {
    /// The reader read its first message at this index.
    First(u64),
    /// The reader dispatched every slot at or below this index.
    Passed(u64),
    /// The reader parks at this entry: it joined no record for it.
    Parked(u64),
    /// The reader joined the entry it parked at.
    Fetched(u64),
    /// The publisher appended this locator.
    Located(ExecLocator),
    Ask(u64, oneshot::Sender<RunAnswer>),
    Locate(u64, oneshot::Sender<Option<ExecLocator>>),
}

/// The writers' side: the reader and the stream publisher.
#[derive(Clone, Debug)]
pub struct ExecAnswersFeed {
    tx: Sender<AnswersMsg>,
}

impl ExecAnswersFeed {
    /// The reader read its first message at `index`.
    pub fn first(&self, index: u64) {
        self.send(AnswersMsg::First(index));
    }

    /// The reader dispatched every slot at or below `through`.
    pub fn passed(&self, through: u64) {
        self.send(AnswersMsg::Passed(through));
    }

    /// The reader parks at entry `index`.
    pub fn parked(&self, index: u64) {
        self.send(AnswersMsg::Parked(index));
    }

    /// The reader joined entry `index`, at which it parked.
    pub fn fetched(&self, index: u64) {
        self.send(AnswersMsg::Fetched(index));
    }

    /// The publisher appended `locator`, or the log held it at open.
    pub fn located(&self, locator: ExecLocator) {
        self.send(AnswersMsg::Located(locator));
    }

    /// The owner thread lives while a feed or a lookup lives, so the send
    /// fails only after the owner panicked. The sender goes on: the
    /// answers serve peers, never this executor.
    fn send(&self, msg: AnswersMsg) {
        let _ = self.tx.send(msg);
    }
}

/// The query endpoint's side, with the archive id that a `located` answer
/// names.
#[derive(Clone, Debug)]
pub struct ExecAnswersLookup {
    tx: Sender<AnswersMsg>,
    archive_id: String,
}

impl ExecAnswersLookup {
    /// The archive id of this executor's archive.
    #[must_use]
    pub fn archive_id(&self) -> &str {
        &self.archive_id
    }

    /// What the owner thread knows about entry `index`. `None` when the
    /// owner thread is busy or gone: the asker then asks again.
    pub(crate) async fn ask(&self, index: u64) -> Option<RunAnswer> {
        let (reply, answer) = oneshot::channel();
        self.tx.try_send(AnswersMsg::Ask(index, reply)).ok()?;
        answer.await.ok()
    }

    /// The newest locator at or below `index`. `None` when there is none,
    /// or when the owner thread is busy or gone.
    pub(crate) async fn locate(&self, index: u64) -> Option<ExecLocator> {
        let (reply, answer) = oneshot::channel();
        self.tx.try_send(AnswersMsg::Locate(index, reply)).ok()?;
        answer.await.ok().flatten()
    }

    /// The whole answer for entry `index`, with the state read from `env`
    /// when the owner thread does not decide it alone.
    ///
    /// # Errors
    ///
    /// Returns the state error of the read.
    pub async fn answer(
        &self,
        env: &StateEnv,
        index: u64,
    ) -> Result<Option<ExecLocatorAnswer>, StateError> {
        let Some(run) = self.ask(index).await else {
            return Ok(None);
        };
        let slot = if run.needs_state() {
            let env = env.clone();
            tokio::task::spawn_blocking(move || CommittedSlot::read(&env, index))
                .await
                .map_err(|e| StateError::Recovery(format!("exec locator read task: {e}")))??
        } else {
            CommittedSlot::Beyond
        };
        Ok(Some(run.settle(&self.archive_id, slot)))
    }
}

/// The answers state. Only the owner thread touches it.
#[derive(Debug, Default)]
pub struct ExecAnswers {
    /// The first index of the current run of the reader.
    first: Option<u64>,
    /// Every index below this one is dispatched in the current run.
    reached: u64,
    /// The entries of the current run with no joined record.
    unjoined: BTreeSet<u64>,
    locators: Locators,
}

impl ExecAnswers {
    /// Start the owner thread. Its `located` answers name `archive_id`. It
    /// ends when every feed and every lookup is gone.
    ///
    /// # Errors
    ///
    /// Returns the error when the OS refuses the thread.
    pub fn spawn(archive_id: String) -> std::io::Result<(ExecAnswersFeed, ExecAnswersLookup)> {
        let (tx, rx) = crossbeam_channel::bounded(CHANNEL_DEPTH);
        std::thread::Builder::new()
            .name("exec-answers".into())
            .spawn(move || Self::default().run(&rx))?;
        Ok((
            ExecAnswersFeed { tx: tx.clone() },
            ExecAnswersLookup { tx, archive_id },
        ))
    }

    fn run(mut self, rx: &Receiver<AnswersMsg>) {
        rx.iter().for_each(|msg| self.apply(msg));
    }

    fn apply(&mut self, msg: AnswersMsg) {
        match msg {
            AnswersMsg::First(index) => {
                self.first.get_or_insert(index);
            }
            // `u64::MAX` is no canonical index; saturation keeps it reached.
            AnswersMsg::Passed(through) => {
                self.reached = self.reached.max(through.saturating_add(1));
            }
            AnswersMsg::Parked(index) => {
                self.unjoined.insert(index);
            }
            AnswersMsg::Fetched(index) => {
                self.unjoined.remove(&index);
            }
            AnswersMsg::Located(locator) => self.locators.push(locator),
            // The asker can be gone after its timeout. Nothing waits for
            // the answer then.
            AnswersMsg::Ask(index, reply) => {
                let _ = reply.send(self.answer(index));
            }
            AnswersMsg::Locate(index, reply) => {
                let _ = reply.send(self.locators.lookup(index));
            }
        }
    }

    /// What the current run knows about entry `index`.
    pub(crate) fn answer(&self, index: u64) -> RunAnswer {
        if self.unjoined.contains(&index) {
            return RunAnswer::NotHeld;
        }
        let locator = self.locators.lookup(index);
        match self.first {
            Some(first) if index >= first && index < self.reached => RunAnswer::Joined(locator),
            Some(first) if index >= first => RunAnswer::NotReached,
            _ => RunAnswer::Before(locator),
        }
    }
}

#[cfg(test)]
#[path = "exec_answers_tests.rs"]
mod tests;
