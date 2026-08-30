//! Cooperative shutdown signal for the sequencer loop.
//!
//! Thin wrapper over [`CancellationToken`] so the sync core (publish loop,
//! deposit pump) polls [`Shutdown::is_signaled`] while the tokio shell awaits
//! [`Shutdown::cancelled`] in `select!`.

use tokio_util::sync::CancellationToken;

/// Cooperative shutdown signal for the loop driver.
/// The struct is cloneable. The signal handler task keeps one copy, and the
/// loop thread keeps another.
#[derive(Clone, Debug)]
pub struct Shutdown {
    token: CancellationToken,
}

impl Shutdown {
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    /// Wrap an existing token (share one cancellation tree with other tasks).
    pub fn from_token(token: CancellationToken) -> Self {
        Self { token }
    }

    pub fn signal(&self) {
        self.token.cancel();
    }

    pub fn is_signaled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// The underlying token, for tasks that want to `select!` on it directly.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Resolves once [`Shutdown::signal`] has been called.
    pub async fn cancelled(&self) {
        self.token.cancelled().await
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}
