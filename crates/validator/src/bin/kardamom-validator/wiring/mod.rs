//! Startup and shutdown wiring for the validator binary.
//!
//! `main` is exactly the chain
//! `Startup::init(args).await?.open_state()?.open_streams()?.spawn_pumps()?
//! .spawn_writer()?.spawn_attester()?.build_sink().run().await`. Each step
//! is a method that reads only its own fields and returns the next phase;
//! there is no argument list to keep in sync with the step before it. Each
//! phase type nests the one before it as a single field, plus a small
//! struct of only what that step adds (`Opened { base: Startup, state:
//! OpenedState }`, and so on): no field is declared more than once, and no
//! step's struct lists a field it does not itself add. A few fields that
//! only matter for one step (`file_cfg`, `aeron_cfg`, `channels`, `genesis`,
//! `recovery`) simply ride along unused afterward, in exchange for never
//! re-declaring the fields that do carry all the way to [`run::Ready::run`].
//! `StateEnv` clones cheaply (it is `Arc`-backed), so `Streamed::spawn_writer`
//! never has to break a wrapper open just to move it.
//!
//! The chain splits across three modules, in the order it runs:
//! [`startup`] (tracing/config through the open subscriptions), [`pipeline`]
//! (the trie-aware writer and the optional attester), and [`run`] (the
//! receipts sink, the engine loop, and shutdown).

mod pipeline;
mod run;
mod startup;

pub(crate) use startup::Startup;
