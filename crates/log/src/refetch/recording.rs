//! The recordings of one publisher session on a remote archive, the choice
//! of the recording that holds a wanted position, and the answer "this copy
//! holds no byte of the range".

use kardamom_types::BPosition;

use crate::error::LogError;
use crate::term_layout::TermLayout;

/// How far a recording reaches on the connected archive: the recorded
/// position of a recording that is still written, or the stop position
/// of one that ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RecordedLimit {
    pub(super) position: i64,
    pub(super) active: bool,
}

impl RecordedLimit {
    pub(super) fn read(
        archive: &rusteron_archive::AeronArchive,
        recording_id: i64,
    ) -> Result<Self, LogError> {
        if let Ok(position) = archive.get_recording_position(recording_id)
            && position >= 0
        {
            return Ok(Self {
                position,
                active: true,
            });
        }
        let position = archive
            .get_stop_position(recording_id)
            .map_err(|e| LogError::Aeron(format!("get_stop_position: {e}")))?;
        Ok(Self {
            position,
            active: false,
        })
    }

    /// The length of the replay `[from_raw, limit)`. Zero or less means
    /// that the recording holds nothing at or after `from_raw`.
    pub(super) fn replay_len(self, from_raw: i64) -> i64 {
        self.position - from_raw
    }

    /// The recording ended at or before `from_raw`. An ended recording
    /// never grows, so this copy holds no byte of the range, now or later.
    pub(super) fn ended_before(self, from_raw: i64) -> bool {
        !self.active && from_raw >= self.position
    }
}

/// A recording read from the catalog of the remote archive.
pub(super) struct FoundRecording {
    pub(super) recording_id: i64,
    pub(super) session_id: i32,
    pub(super) start_position: i64,
    /// The stop position in the catalog. `None` while the archive still
    /// writes the recording.
    pub(super) stop_position: Option<i64>,
    /// Power-of-two term length and initial term id, checked once when
    /// this recording was read from the catalog (see [`TermLayout::new`]).
    /// `Err` when the archive reported a malformed term length.
    /// `raw_position` gives this error on every call.
    pub(super) term_layout: Result<TermLayout, String>,
}

impl FoundRecording {
    /// Compute the raw stream position of a fragment-start [`BPosition`]
    /// within this recording's position space, using its [`TermLayout`].
    /// The archive replay API addresses recordings by these raw
    /// positions.
    pub(super) fn raw_position(&self, pos: BPosition) -> Result<i64, LogError> {
        let layout = self.term_layout.as_ref().map_err(|e| {
            LogError::Aeron(format!("refetch: recording {} has {e}", self.recording_id))
        })?;
        layout
            .position_of(pos, self.recording_id)
            .map_err(|e| LogError::Aeron(format!("refetch: {e}")))
    }

    fn locate(self, from: BPosition) -> Result<Located, LogError> {
        let from_raw = self.raw_position(from)?;
        Ok(Located {
            rec: self,
            from_raw,
        })
    }
}

/// A recording, and the wanted position as a raw position in it.
pub(super) struct Located {
    pub(super) rec: FoundRecording,
    pub(super) from_raw: i64,
}

impl Located {
    fn starts_at_or_before(&self) -> bool {
        self.rec.start_position <= self.from_raw
    }

    /// The catalog does not show that the recording ended at or before the
    /// position.
    fn covers(&self) -> bool {
        self.rec
            .stop_position
            .is_none_or(|stop| self.from_raw < stop)
    }

    /// The replay length from the position to `limit`. `None` when a live
    /// recording does not reach the position yet: a later attempt can find
    /// the range.
    ///
    /// # Errors
    ///
    /// Gives [`Unresolved::Absent`] when the recording ended at or before
    /// the position.
    pub(super) fn replay_len(&self, limit: RecordedLimit) -> Result<Option<i64>, Unresolved> {
        if limit.ended_before(self.from_raw) {
            return Err(Unresolved::Absent(format!(
                "recording {} of session {} ended at position {}, at or before the requested position {} — this copy holds no byte of the range",
                self.rec.recording_id, self.rec.session_id, limit.position, self.from_raw
            )));
        }
        let len = limit.replay_len(self.from_raw);
        Ok((len > 0).then_some(len))
    }
}

/// Why one archive cannot serve a range now.
pub(super) enum Unresolved {
    /// The archive answered, and it holds no byte of the range.
    Absent(String),
    /// The term layout of a recording cannot place the position.
    Layout(LogError),
}

impl Unresolved {
    /// The error of the archive `archive`. Only [`Self::Absent`] is a
    /// refusal ([`LogError::RangeAbsent`]).
    pub(super) fn at(self, archive: String) -> LogError {
        match self {
            Self::Absent(detail) => LogError::RangeAbsent { archive, detail },
            Self::Layout(e) => e,
        }
    }
}

/// The range that a refetch asks for: a publisher session on one stream,
/// from one position.
#[derive(Clone, Copy)]
pub(super) struct Wanted {
    pub(super) stream_id: i32,
    pub(super) session_id: i32,
    pub(super) from: BPosition,
}

impl Wanted {
    /// Pick the recording of the session that holds the position.
    ///
    /// An archive can keep several recordings of one session: it starts a
    /// new recording when the image of the session comes back. Only a
    /// recording that starts at or before the position can hold it. Of
    /// those, take the newest one that the catalog does not show ended
    /// before the position. When each of them ended before the position,
    /// take the newest one: the range then falls in a gap or after the
    /// end, and [`Located::replay_len`] refuses it.
    ///
    /// # Errors
    ///
    /// Gives [`Unresolved::Absent`] when the archive has no recording of
    /// the session, or when each recording starts after the position.
    /// Gives [`Unresolved::Layout`] when a term layout cannot place the
    /// position.
    pub(super) fn resolve(self, recs: Vec<FoundRecording>) -> Result<Located, Unresolved> {
        let (before, after): (Vec<Located>, Vec<Located>) = recs
            .into_iter()
            .filter(|r| r.session_id == self.session_id)
            .map(|rec| rec.locate(self.from))
            .collect::<Result<Vec<_>, _>>()
            .map_err(Unresolved::Layout)?
            .into_iter()
            .partition(Located::starts_at_or_before);
        before
            .into_iter()
            .max_by_key(|l| (l.covers(), l.rec.recording_id))
            .ok_or_else(|| {
                Unresolved::Absent(
                    self.absent_detail(after.iter().min_by_key(|l| l.rec.start_position)),
                )
            })
    }

    /// The detail of a refusal with no recording at or before the
    /// position. `oldest` is the oldest recording of the session, if any.
    fn absent_detail(self, oldest: Option<&Located>) -> String {
        match oldest {
            None => format!(
                "no recording for stream {} session {}",
                self.stream_id, self.session_id
            ),
            Some(l) => format!(
                "position {} precedes the oldest recording {} of session {}, which starts at {} — this copy holds no byte of the range",
                l.from_raw, l.rec.recording_id, self.session_id, l.rec.start_position
            ),
        }
    }
}

/// One archive's catalog of one publisher session, with no Aeron behind it.
/// It answers a `tx_data` refetch with the refetcher's own choice of
/// recording and its own refusal, up to the replay. Each recording has a
/// 64 KiB term and an initial term id of 0, so a raw position is
/// `term_id * 65536 + term_offset`.
#[cfg(any(test, feature = "testing"))]
pub struct FakeArchiveCatalog {
    /// The archive control endpoint that names this copy.
    pub archive: String,
    /// The recordings of the session as `(start, stop)` raw positions,
    /// oldest first. A `None` stop is a recording that the archive still
    /// writes. Its recorded position is its start.
    pub recordings: Vec<(i64, Option<i64>)>,
}

#[cfg(any(test, feature = "testing"))]
impl FakeArchiveCatalog {
    /// The answer of this archive to a refetch from `from`: the replay
    /// length, or `None` when a live recording does not reach `from` yet.
    ///
    /// # Errors
    ///
    /// Gives the error that the refetcher gives for this catalog.
    pub fn answer(&self, from: BPosition) -> Result<Option<i64>, LogError> {
        let recs = self
            .recordings
            .iter()
            .zip(0..)
            .map(|(&(start, stop), id)| FoundRecording::fake(id, start, stop))
            .collect();
        let wanted = Wanted {
            stream_id: 0,
            session_id: 0,
            from,
        };
        wanted
            .resolve(recs)
            .and_then(|found| {
                found.replay_len(RecordedLimit {
                    position: found.rec.stop_position.unwrap_or(found.rec.start_position),
                    active: found.rec.stop_position.is_none(),
                })
            })
            .map_err(|u| u.at(self.archive.clone()))
    }
}

#[cfg(any(test, feature = "testing"))]
impl FoundRecording {
    /// A recording of session 0 with the layout of [`FakeArchiveCatalog`].
    pub(super) fn fake(recording_id: i64, start_position: i64, stop_position: Option<i64>) -> Self {
        Self {
            recording_id,
            session_id: 0,
            start_position,
            stop_position,
            term_layout: TermLayout::new(1 << 16, 0),
        }
    }
}
