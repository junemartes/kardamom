//! The publication side of [`AeronRuntime`]: the opens, and the
//! `Send + Sync` [`PubHandle`] they hand out.

use std::net::SocketAddr;

use crossbeam_channel::Sender as CbSender;
use rkyv::util::AlignedVec;

use super::{AeronRuntime, RuntimeCmd, request};
use crate::codec;
use crate::error::LogError;
use kardamom_types::BPosition;

impl AeronRuntime {
    /// Open a publication on the Aeron thread, returning a `Send + Sync`
    /// handle that forwards every offer through the command channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the publication
    /// (a malformed channel URI, or the driver's `add_publication`
    /// timeout elapsing), or if the command round trip itself times out.
    pub fn open_publication(&self, uri: &str, stream_id: i32) -> Result<PubHandle, LogError> {
        let uri = uri.to_string();
        let opened = request(
            &self.cmd_tx,
            |ack| RuntimeCmd::OpenPublication {
                uri,
                stream_id,
                ack,
            },
            "open_publication",
        )?;
        Ok(self.pub_handle(opened))
    }

    /// Open a dynamic MDC publication whose control endpoint names port
    /// 0, and return its handle and the control address the driver bound.
    /// The driver holds that socket for the life of the publication, so
    /// the address stays valid to advertise.
    ///
    /// # Errors
    ///
    /// Returns an error as [`Self::open_publication`] does, or if the
    /// driver reports no bound control address within the bind timeout.
    pub fn open_mdc_publication(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<(PubHandle, SocketAddr), LogError> {
        let uri = uri.to_string();
        let (opened, control) = request(
            &self.cmd_tx,
            |ack| RuntimeCmd::OpenMdcPublication {
                uri,
                stream_id,
                ack,
            },
            "open_mdc_publication",
        )?;
        Ok((self.pub_handle(opened), control))
    }

    fn pub_handle(&self, opened: OpenedPub) -> PubHandle {
        PubHandle {
            cmd_tx: self.cmd_tx.clone(),
            pub_id: opened.pub_id,
            session_id: opened.session_id,
        }
    }
}

/// The Aeron thread's reply to an open: the row index of the publication
/// in its table and the session id the driver assigned.
#[derive(Clone, Copy)]
pub(in crate::aeron_live) struct OpenedPub {
    pub(in crate::aeron_live) pub_id: u32,
    pub(in crate::aeron_live) session_id: i32,
}

/// `Send + Sync` publication handle. Forwards each publish through the
/// Aeron-thread command channel.
#[derive(Clone)]
pub struct PubHandle {
    cmd_tx: CbSender<RuntimeCmd>,
    pub_id: u32,
    session_id: i32,
}

impl PubHandle {
    /// The Aeron session id the driver assigned this publication. Every
    /// image and archive recording of it carries the same id.
    #[must_use]
    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    /// Blocking publish with `BPosition` ack. Waits
    /// [`ACK_TIMEOUT`](super::super::ACK_TIMEOUT) for the Aeron thread's
    /// reply. See that constant for why the ack always resolves first.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron offer fails or times out, or if the
    /// command round trip to the Aeron thread itself times out.
    pub fn publish_bytes(&self, bytes: AlignedVec) -> Result<BPosition, LogError> {
        let pub_id = self.pub_id;
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::Publish { pub_id, bytes, ack },
            "publish_bytes",
        )
    }

    /// Fire-and-forget publish. Errors are logged on the Aeron thread.
    pub fn publish_best_effort(&self, bytes: AlignedVec) {
        let _ = self.cmd_tx.send(RuntimeCmd::PublishBestEffort {
            pub_id: self.pub_id,
            bytes,
        });
    }

    /// Encode a typed message and publish blockingly.
    ///
    /// # Errors
    ///
    /// Returns an error if `msg` fails to encode, or if the publish
    /// itself fails (see [`publish_bytes`](Self::publish_bytes)).
    pub fn publish<T>(&self, msg: &T) -> Result<BPosition, LogError>
    where
        T: for<'a> rkyv::Serialize<
                rkyv::api::high::HighSerializer<
                    AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let bytes = codec::encode(msg)?;
        self.publish_bytes(bytes)
    }
}
