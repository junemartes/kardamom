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

    /// An archive answered, and it does not hold the requested range: it
    /// has no recording of the session, or its newest recording of the
    /// session ended before the range. This is a definite answer about one
    /// copy. An archive that did not answer gives [`Self::Aeron`] instead.
    /// The join layer counts these answers: when every archive gives one, no
    /// retry can recover the range.
    #[error("refetch: archive {archive} does not hold the range: {detail}")]
    RangeAbsent { archive: String, detail: String },
}
