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

    #[error("quorum stalled: only {present} of {required} recorders reporting")]
    QuorumStalled { present: usize, required: usize },
}
