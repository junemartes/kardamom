//! The size bound of one cluster session message.
//!
//! Aeron takes one message of at most a term length divided by 8. The
//! cluster log term is 8 MiB, so the log takes a message of at most 1 MiB.
//! The consensus module appends each ingress session message to the log
//! with the same 32-byte session header, so 1 MiB is also the bound of an
//! ingress session message. A smaller term on a session's own ingress or
//! egress channel gives that channel a smaller bound.
//!
//! A message above one MTU travels as many Aeron fragments. The sender
//! writes the session header once, in front of the payload, and Aeron
//! splits the whole buffer. The receiver reassembles the fragments before
//! it decodes the header: the consensus module and the clustered service
//! in Java, and the fragment assembler of each Rust subscription.

/// The length of the `SessionMessageHeader` in front of each payload.
pub const SESSION_HEADER_LEN: usize = crate::protocol::SESSION_HEADER_LEN;

/// The longest session message that the cluster log takes, header
/// included: the 8 MiB cluster log term divided by 8.
pub const MAX_SESSION_MESSAGE_LEN: usize = 1 << 20;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bound_is_the_log_term_over_eight() {
        let log_term = 8 << 20;
        assert_eq!(MAX_SESSION_MESSAGE_LEN, log_term / 8);
        assert_eq!(SESSION_HEADER_LEN, 32);
    }

    #[test]
    fn a_wrapped_payload_at_the_bound_fills_one_session_message() {
        let bytes = vec![7; MAX_SESSION_MESSAGE_LEN - SESSION_HEADER_LEN];
        let framed = crate::protocol::wrap_session_message(1, 2, 3, &bytes);
        assert_eq!(framed.len(), MAX_SESSION_MESSAGE_LEN);
    }
}
