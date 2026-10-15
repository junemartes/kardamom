//! The size bound of one cluster session message.
//!
//! Aeron takes one message of at most a term length divided by 8. The
//! cluster log term is 8 MiB, so the log takes a message of at most 1 MiB.
//! The consensus module appends each ingress session message to the log
//! with the same 32-byte session header, so 1 MiB is also the bound of an
//! ingress session message. A larger message cannot enter the cluster.
//!
//! A message above one MTU travels as many Aeron fragments. The sender
//! writes the session header once, in front of the payload, and Aeron
//! splits the whole buffer. The receiver reassembles the fragments before
//! it decodes the header: the consensus module and the clustered service
//! in Java, and the fragment assembler of each Rust subscription.

use thiserror::Error;

use crate::protocol::SESSION_HEADER_LEN;

/// The longest session message, header included: the 8 MiB cluster log
/// term divided by 8.
pub const MAX_SESSION_MESSAGE_LEN: usize = 1 << 20;

/// The longest application payload in one session message.
pub const MAX_APP_PAYLOAD_LEN: usize = MAX_SESSION_MESSAGE_LEN - SESSION_HEADER_LEN;

/// An application payload is above [`MAX_APP_PAYLOAD_LEN`].
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error(
    "application payload of {len} bytes exceeds the {MAX_APP_PAYLOAD_LEN}-byte session message bound"
)]
pub struct PayloadTooLarge {
    pub len: usize,
}

/// An application payload that fits one session message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppPayload(Vec<u8>);

impl AppPayload {
    /// The payload bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<&[u8]> for AppPayload {
    type Error = PayloadTooLarge;

    fn try_from(bytes: &[u8]) -> Result<Self, PayloadTooLarge> {
        if bytes.len() > MAX_APP_PAYLOAD_LEN {
            return Err(PayloadTooLarge { len: bytes.len() });
        }
        Ok(Self(bytes.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bound_is_the_log_term_over_eight() {
        let log_term = 8 << 20;
        assert_eq!(MAX_SESSION_MESSAGE_LEN, log_term / 8);
        assert_eq!(MAX_APP_PAYLOAD_LEN + 32, MAX_SESSION_MESSAGE_LEN);
    }

    #[test]
    fn a_payload_at_the_bound_parses() {
        let bytes = vec![0xA5; MAX_APP_PAYLOAD_LEN];
        let payload = AppPayload::try_from(bytes.as_slice()).unwrap();
        assert_eq!(payload.as_bytes(), bytes.as_slice());
    }

    #[test]
    fn a_payload_above_the_bound_is_refused() {
        let bytes = vec![0; MAX_APP_PAYLOAD_LEN + 1];
        assert_eq!(
            AppPayload::try_from(bytes.as_slice()),
            Err(PayloadTooLarge {
                len: MAX_APP_PAYLOAD_LEN + 1
            })
        );
    }

    #[test]
    fn a_wrapped_payload_at_the_bound_fills_one_session_message() {
        let bytes = vec![7; MAX_APP_PAYLOAD_LEN];
        let framed = crate::protocol::wrap_session_message(1, 2, 3, &bytes);
        assert_eq!(framed.len(), MAX_SESSION_MESSAGE_LEN);
    }
}
