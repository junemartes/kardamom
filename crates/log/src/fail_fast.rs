//! The fail-fast error handler of every Aeron context in this crate.
//!
//! A client that loses its driver, or that stalls past its service
//! interval, closes all of its publications and subscriptions. It cannot
//! recover. A process that keeps running with such a client keeps its
//! `/metrics` port up, so supervisors and probes see a live service that
//! is stuck. So the handler ends the process, and the supervisor restarts
//! it. The restart is the one recovery path.
//!
//! `rusteron-client` and `rusteron-archive` each generate their own
//! context type and their own handler trait. One macro gives the handler
//! and the install method to both.

use tracing::error;

use crate::error::LogError;

/// Routes Aeron C-client errors through `tracing`, then ends the process
/// with status 1. The line is often the last line of the service. It
/// carries the timestamp of this service and lands in its own log, so an
/// operator can order the exit against the other services.
struct TracingErrorHandler;

impl TracingErrorHandler {
    fn exit(error_code: std::os::raw::c_int, msg: &str) -> ! {
        error!(
            code = error_code,
            msg, "aeron client error; exiting (fail-fast, as aeron's default handler does)"
        );
        std::process::exit(1);
    }
}

/// An Aeron context that can take the fail-fast handler.
pub(crate) trait FailFast {
    /// Install the fail-fast handler as the error handler of this
    /// context. Call it before the client starts.
    fn fail_fast(&self) -> Result<(), LogError>;
}

macro_rules! fail_fast_for {
    ($krate:ident) => {
        impl $krate::AeronErrorHandlerCallback for TracingErrorHandler {
            fn handle_aeron_error_handler(&mut self, error_code: std::os::raw::c_int, msg: &str) {
                Self::exit(error_code, msg);
            }
        }

        impl FailFast for $krate::AeronContext {
            fn fail_fast(&self) -> Result<(), LogError> {
                let handler = $krate::Handler::leak(TracingErrorHandler);
                self.set_error_handler(Some(&handler)).map_err(|e| {
                    LogError::Aeron(format!("{} set_error_handler: {e}", stringify!($krate)))
                })?;
                // The C side holds the leaked pointer for the life of the
                // process. Forgetting the wrapper suppresses its drop-time
                // complaint that release() was never called.
                std::mem::forget(handler);
                Ok(())
            }
        }
    };
}

fail_fast_for!(rusteron_client);
fail_fast_for!(rusteron_archive);
