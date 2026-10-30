//! The fixture of the follower tests: a controlled L1 behind independent
//! fault proxies, the `l1_blocks` stream with its recording, the real
//! follower and da-watcher, and the checks of what they published.

#![allow(dead_code, reason = "each test binary uses a part of the fixture")]

pub(crate) mod chain;
pub(crate) mod check;
pub(crate) mod child;
pub(crate) mod rig;
pub(crate) mod stream;
