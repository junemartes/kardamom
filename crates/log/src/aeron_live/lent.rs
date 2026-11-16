//! A publication that the Aeron thread lends to one other thread, so that
//! thread offers on it with no hop to the Aeron thread.
//!
//! Why a lease and not a copy: the client wrapper of a publication holds
//! its C resource, and a link to the client, in `std::rc::Rc` counts. The
//! offer itself is safe from any thread, and it changes no count. A copy or
//! a drop changes the counts, which are not atomic. So two threads must
//! never copy or drop one publication at the same time. The lease moves the
//! one publication out of the table of the Aeron thread, and its drop moves
//! it back. At each time one thread holds the publication, and only the
//! Aeron thread drops it.
//!
//! Why a lease and not a second registration: the `tx_data` lanes are
//! dynamic MDC publications on control port 0. A second registration with
//! the same channel binds a new port, so it is a new session that no
//! subscriber discovered. The lease keeps the session, the port and its
//! discovery record.

use crossbeam_channel::Sender as CbSender;
use rusteron_client::Handlers;

use super::Pub;
use super::runtime::RuntimeCmd;
use crate::parked::Offered;
use crate::term_layout::TermLayout;

/// The parts of a lent publication that move between the two threads.
pub(super) struct Lent {
    pub(super) publication: Pub,
    pub(super) layout: TermLayout,
    pub(super) stream_id: i32,
}

/// A shared publication on loan from the Aeron thread. See the module text.
///
/// While the lease lasts, a publish on any [`super::PubHandle`] of the same
/// publication fails at once, as on an unknown publication. The drop gives
/// the publication back to its row, and those handles work again. The
/// lease has no `Clone`.
pub struct LentPublication {
    lent: Option<Lent>,
    pub_id: u32,
    session_id: i32,
    back: CbSender<RuntimeCmd>,
}

impl LentPublication {
    pub(super) fn new(
        lent: Lent,
        pub_id: u32,
        session_id: i32,
        back: CbSender<RuntimeCmd>,
    ) -> Self {
        Self {
            lent: Some(lent),
            pub_id,
            session_id,
            back,
        }
    }

    /// The Aeron session id of the publication: the same as before the
    /// lease.
    #[must_use]
    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    /// One offer of `bytes`, on the thread of the caller. It never waits.
    /// A refused offer is the caller's to retry, for example through a
    /// [`crate::parked::ParkedQueue`].
    #[must_use]
    pub fn offer(&self, bytes: &[u8]) -> Offered {
        let Some(lent) = &self.lent else {
            return Offered::UnknownPublication;
        };
        let code = lent
            .publication
            .offer(bytes, Handlers::no_reserved_value_supplier_handler());
        lent.layout.offered(code)
    }
}

impl Drop for LentPublication {
    /// Give the publication back to the Aeron thread, which puts it in its
    /// row again, or drops it there when its row closed during the lease.
    /// When that thread already ended, the publication leaks: a drop here
    /// could race with the teardown of the client on that thread.
    fn drop(&mut self) {
        let Some(lent) = self.lent.take() else {
            return;
        };
        let back = RuntimeCmd::ReturnPublication {
            pub_id: self.pub_id,
            lent,
        };
        if let Err(unsent) = self.back.send(back) {
            std::mem::forget(unsent.into_inner());
        }
    }
}
