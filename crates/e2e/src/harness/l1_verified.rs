//! A stand-in for the L1 light client: same JSON-RPC surface, in-process.
//!
//! The validator reads L1 through a verified endpoint (helios), not a
//! blindly trusted RPC (see
//! `deploy/cluster/nomad/l1-light-client.nomad.hcl`). This test cannot
//! exercise that real client: every kardamom test environment runs anvil
//! as L1, and anvil is execution-only, so there is no beacon chain for a
//! consensus light client to sync from.
//!
//! What this test can check is the half that is ours: the contract
//! between the validator and whatever serves it L1 data. The fault proxy
//! speaks the same `eth_*` methods helios does, proxying to anvil so the
//! data is real, and it can be told to lie in specific ways.
//!
//! That second part is the point. A passthrough proves the validator
//! works through an interposed endpoint at all. The fault modes prove it
//! actually rejects a bad L1 view, instead of trusting whatever arrives.
//! Without them, "the validator verifies against L1" is an untested claim.
//!
//! This does not test helios's cryptography: the sync-committee signature
//! checks and the Merkle proofs against the beacon-authenticated roots. A
//! proxy cannot stand in for that part, which needs a real network. A
//! green run here does not validate the light client.
//!
//! The proxy is the same binary the chaos suite deploys in front of the
//! in-cluster L1 (`kardamom-l1-fault-proxy`), driven in-process here.

pub use kardamom_l1_fault_proxy::{Fault, FaultProxy as VerifiedL1};
