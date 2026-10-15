#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("codec: {0}")]
    Codec(String),

    #[error("aeron: {0}")]
    Aeron(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("supervisor: {0}")]
    Supervisor(String),

    #[error("config: {0}")]
    Config(String),

    #[error("discovery: {0}")]
    Discovery(String),

    /// The catalog agent gave no answer: no connection, no answer within
    /// the request budget, or a server error status. A loaded host or an
    /// agent restart causes it, and it ends. See [`Self::is_transient`].
    #[error("discovery: {0}")]
    CatalogUnavailable(String),

    /// An archive answered, and it does not hold the requested range: it
    /// has no recording of the session, each recording of the session
    /// starts after the range, or the recording before the range ended at
    /// or before it. This is a definite answer about one copy. An archive that did not answer gives [`Self::Aeron`] instead.
    /// The join layer counts these answers: when every archive gives one, no
    /// retry can recover the range.
    #[error("refetch: archive {archive} does not hold the range: {detail}")]
    RangeAbsent { archive: String, detail: String },
}

impl LogError {
    /// The error of a catalog request `what` that got no answer. A
    /// request that cannot be built (a bad address) is
    /// [`Self::Discovery`]. A failed connection, a timeout, or a broken
    /// transfer is [`Self::CatalogUnavailable`].
    pub(crate) fn catalog_send(what: &str, e: &reqwest::Error) -> Self {
        let msg = format!("{what}: {e}");
        if e.is_connect() || e.is_timeout() || e.is_request() {
            Self::CatalogUnavailable(msg)
        } else {
            Self::Discovery(msg)
        }
    }

    /// The error of a catalog request `what` that the agent refused with
    /// `status`. A server error status is [`Self::CatalogUnavailable`].
    /// Every other status (an ACL denial, a malformed request) is
    /// [`Self::Discovery`].
    pub(crate) fn catalog_status(what: &str, status: reqwest::StatusCode, body: &str) -> Self {
        let msg = format!("{what}: {status}: {body}");
        if status.is_server_error() {
            Self::CatalogUnavailable(msg)
        } else {
            Self::Discovery(msg)
        }
    }

    /// Whether a later try of the same start-up registration can
    /// succeed: the catalog agent was slow or down, not wrong. A refused
    /// permission or a malformed request fails every try.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::CatalogUnavailable(_))
    }
}
