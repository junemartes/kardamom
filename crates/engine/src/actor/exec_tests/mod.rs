//! Exec-thread tests. These cover streaming execution, boundary emission,
//! BAL capture, block-close protocol actions, interop remote-epoch
//! delivery, and the offline replay's parity with the exec thread.

mod bal_shadow;
mod block_close;
mod interop;
mod replay_parity;
mod streaming;
