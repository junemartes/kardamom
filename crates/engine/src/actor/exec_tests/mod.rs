//! Exec-thread tests. These cover streaming execution, boundary emission,
//! BAL capture, block-close protocol actions, interop remote-epoch
//! delivery, and the tx hook.

mod bal_shadow;
mod block_close;
mod interop;
mod streaming;
mod tx_hook;
