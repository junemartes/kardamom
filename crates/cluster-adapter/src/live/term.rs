//! The term length of one session's cluster channels, and the payload
//! bound that follows from it.
//!
//! Aeron takes one message of at most a term length divided by 8. The log
//! buffer of a channel is three terms. A busy stream writes all three, so
//! a busy channel takes three terms of tmpfs on each side. The cluster
//! members run an embedded media driver, so the tmpfs pages of their
//! ingress images and egress publications count against the memory limit
//! of the sealer. A session therefore asks for a larger term only on the
//! channel that carries large messages.

use kardamom_cluster_client::message::SESSION_HEADER_LEN;

use crate::wire::MAX_INGRESS_PAYLOAD_LEN;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The term length of one cluster channel of a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTerm {
    /// A 1 MiB term: a message of at most 128 KiB. A busy log takes 3 MiB.
    #[default]
    Control,
    /// A 2 MiB term: a message of at most 256 KiB, enough for a canonical
    /// entry with a raw transaction of up to 128 KiB. A busy log takes
    /// 6 MiB.
    Entries,
    /// An 8 MiB term: a message of at most 1 MiB, the bound of the cluster
    /// log. A busy log takes 24 MiB.
    Large,
}

/// A payload is above the bound of the session's ingress term.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("payload of {len} bytes exceeds the {max}-byte bound of the session's ingress term")]
pub struct PayloadTooLarge {
    pub len: usize,
    pub max: usize,
}

/// An application payload that fits one session message on the session's
/// ingress term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppPayload(Vec<u8>);

impl AppPayload {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl SessionTerm {
    /// The term length in bytes.
    const fn term_length(self) -> usize {
        match self {
            Self::Control => 1 << 20,
            Self::Entries => 2 << 20,
            Self::Large => 8 << 20,
        }
    }

    /// The longest application payload of one session message on a
    /// channel with this term: the term divided by 8, less the session
    /// header.
    #[must_use]
    pub const fn max_payload_len(self) -> usize {
        self.term_length() / 8 - SESSION_HEADER_LEN
    }

    /// Append this term length and sparse log buffers to a cluster channel
    /// URI, unless the URI already pins a term length. A sparse log takes
    /// tmpfs pages only as the stream writes them, so a quiet or
    /// short-lived session stays small.
    pub(crate) fn channel(self, uri: &str) -> String {
        if uri.contains("term-length") {
            uri.to_string()
        } else {
            format!("{uri}|term-length={}|sparse=true", self.term_length())
        }
    }

    /// The longest payload that a session with this ingress term offers:
    /// the bound of the term, and at most the ingress cap, so that the
    /// relayed frame of the payload fits one egress session message.
    const fn max_ingress_len(self) -> usize {
        let term = self.max_payload_len();
        if term < MAX_INGRESS_PAYLOAD_LEN {
            term
        } else {
            MAX_INGRESS_PAYLOAD_LEN
        }
    }

    /// Parse `bytes` into a payload that fits this term and the ingress cap.
    pub(crate) fn payload(self, bytes: &[u8]) -> Result<AppPayload, PayloadTooLarge> {
        let max = self.max_ingress_len();
        if bytes.len() > max {
            return Err(PayloadTooLarge {
                len: bytes.len(),
                max,
            });
        }
        Ok(AppPayload(bytes.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use kardamom_cluster_client::message::MAX_SESSION_MESSAGE_LEN;

    use super::*;

    #[test]
    fn each_term_bounds_a_message_at_an_eighth() {
        assert_eq!(SessionTerm::Control.max_payload_len(), (128 << 10) - 32);
        assert_eq!(SessionTerm::Entries.max_payload_len(), (256 << 10) - 32);
        assert_eq!(
            SessionTerm::Large.max_payload_len(),
            MAX_SESSION_MESSAGE_LEN - 32
        );
    }

    #[test]
    fn the_large_term_offers_up_to_the_ingress_cap() {
        assert_eq!(
            SessionTerm::Large.max_ingress_len(),
            MAX_INGRESS_PAYLOAD_LEN
        );
        assert_eq!(
            SessionTerm::Control.max_ingress_len(),
            SessionTerm::Control.max_payload_len()
        );
    }

    #[test]
    fn the_default_is_the_control_term() {
        assert_eq!(SessionTerm::default(), SessionTerm::Control);
        assert_eq!(
            SessionTerm::Control.channel("aeron:udp?endpoint=127.0.0.1:9050"),
            "aeron:udp?endpoint=127.0.0.1:9050|term-length=1048576|sparse=true"
        );
    }

    #[test]
    fn a_channel_keeps_a_pinned_term_length() {
        let pinned = "aeron:udp?endpoint=127.0.0.1:9050|term-length=64k";
        assert_eq!(SessionTerm::Large.channel(pinned), pinned);
    }

    #[test]
    fn a_payload_at_the_bound_parses_and_one_more_byte_is_refused() {
        [
            SessionTerm::Control,
            SessionTerm::Entries,
            SessionTerm::Large,
        ]
        .into_iter()
        .map(|term| (term, term.max_payload_len()))
        .for_each(|(term, max)| {
            let at = vec![0xA5; max];
            assert_eq!(term.payload(&at).unwrap().as_bytes(), at.as_slice());
            assert_eq!(
                term.payload(&vec![0; max + 1]),
                Err(PayloadTooLarge { len: max + 1, max })
            );
        });
    }

    #[test]
    fn the_terms_parse_from_toml_names() {
        #[derive(Deserialize)]
        struct Holder {
            term: SessionTerm,
        }
        let parse = |name: &str| {
            toml::from_str::<Holder>(&format!("term = \"{name}\""))
                .unwrap()
                .term
        };
        assert_eq!(parse("control"), SessionTerm::Control);
        assert_eq!(parse("entries"), SessionTerm::Entries);
        assert_eq!(parse("large"), SessionTerm::Large);
    }
}
