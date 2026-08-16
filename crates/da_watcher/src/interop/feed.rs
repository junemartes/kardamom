//! Re-export shim: the interop feed wire contract moved to the shared
//! `kardamom-interop-feed` crate (egress spec E1 — the validator now
//! implements the server side, so the DTOs can no longer live inside the
//! watcher without a validator→watcher dependency). This module keeps every
//! existing `crate::interop::feed::*` path working; new code may import
//! `kardamom_interop_feed` directly.

pub use kardamom_interop_feed::*;
