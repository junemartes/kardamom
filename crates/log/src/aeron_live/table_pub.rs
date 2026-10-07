//! One publication in the publication table of the Aeron thread: a
//! shared publication or an exclusive one.
//!
//! The driver shares a publication between every client that adds the same
//! channel and stream id, so they write into one session. An exclusive
//! publication has its own session, also on one shared driver.

use rusteron_client::{AeronExclusivePublication, AeronPublicationConstants, Handlers};

use super::Pub;
use crate::error::LogError;

pub(super) enum TablePub {
    Shared(Pub),
    Exclusive(AeronExclusivePublication),
}

impl TablePub {
    /// One offer of `bytes`. Returns the new stream position, or Aeron's
    /// negative status code.
    pub(super) fn offer(&self, bytes: &[u8]) -> i64 {
        let supplier = Handlers::no_reserved_value_supplier_handler();
        match self {
            Self::Shared(p) => p.offer(bytes, supplier),
            Self::Exclusive(p) => p.offer(bytes, supplier),
        }
    }

    /// The constants of the publication: its session id and term layout.
    ///
    /// # Errors
    ///
    /// Returns an error when the client cannot read them.
    pub(super) fn constants(&self) -> Result<AeronPublicationConstants, LogError> {
        match self {
            Self::Shared(p) => p.get_constants(),
            Self::Exclusive(p) => p.get_constants(),
        }
        .map_err(|e| LogError::Aeron(format!("publication constants: {e}")))
    }
}
