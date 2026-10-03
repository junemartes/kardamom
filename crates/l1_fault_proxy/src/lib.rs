//! An L1 JSON-RPC proxy that lies on command.
//!
//! The proxy sits between a follower (the batcher, the da-watcher, the
//! inbox indexer, the validator) and a real L1 endpoint. It forwards
//! every call and mutates the reply as the active faults say. Every
//! fault is a lie a public endpoint told during a real outage: a wrong
//! block hash, a broken parent chain, a swallowed log, a null receipt,
//! a rate limit, an endpoint that is down.
//!
//! The faults change at run time through a control endpoint on the same
//! listener: `POST /fault` with one fault or a list, `GET /fault` for the
//! active list, `GET /health` for the job check. Every other `POST` is a
//! JSON-RPC call for the upstream.
//!
//! The proxy is a pipe, not a parser: a call travels upstream as its raw
//! body, and only the fields a fault names change in the reply.

pub mod fault;
mod http;
mod server;

pub use fault::{Fault, Faults};
pub use server::FaultProxy;
