//! Small shared helpers for `std::sync` primitives.

use std::sync::{Mutex, MutexGuard, PoisonError};

/// Locks a `std::sync::Mutex`, recovering the guard from a poisoned lock
/// instead of panicking.
///
/// This crate never panics while holding one of these locks, so a
/// poisoned lock cannot mean the protected data is inconsistent; it can
/// only mean an unrelated panic unwound through an unlucky moment. Every
/// caller in this crate already tolerates a stale-but-valid read, so
/// recovering the guard is strictly better than propagating the poison
/// and taking the whole process down over an unrelated panic.
pub(crate) trait LockIgnorePoison<T> {
    fn lock_ignore_poison(&self) -> MutexGuard<'_, T>;
}

impl<T> LockIgnorePoison<T> for Mutex<T> {
    fn lock_ignore_poison(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
