//! A `ServiceEvent` with a new `HaltCause` through the decoder of the
//! release before the cause.
//!
//! The `events` stream carries the archived `HaltCause` discriminant. A
//! reader of release N-1 decodes with its own variant set. Each module
//! below mirrors the event types of one older release: the same fields in
//! the same order, and the enums cut before a later variant. The tests pin
//! the exact behavior of that reader:
//!
//! - an event that names a cause it knows decodes, the halt included;
//! - an event that names a later cause fails the validation of the
//!   archive, so the reader drops it as a malformed frame. The drop is
//!   per record: the next record decodes.

use kardamom_types::service::{
    Halt, HaltCause, HaltRef, Pause, PauseReason, ServiceEvent, ServiceState,
};

/// The event types of one older release. `$cause` and `$recovery` list the
/// variants of `HaltCause` and `RecoveryId` that the release knows, in
/// order.
macro_rules! older_release {
    ($name:ident, [$($cause:ident),*], [$($recovery:ident),*]) => {
        mod $name {
            use rkyv::{Archive, Deserialize, Serialize};

            #[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
            pub(crate) enum HaltCause { $($cause),* }

            #[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
            pub(crate) enum RecoveryId { $($recovery),* }

            #[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
            pub(crate) enum Clears { Auto, Operator }

            #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
            pub(crate) struct Halt {
                pub(crate) cause: HaltCause,
                pub(crate) detail: String,
                pub(crate) recovery: RecoveryId,
                pub(crate) since_unix_ms: u64,
                pub(crate) clears: Clears,
            }

            #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
            pub(crate) struct HaltRef {
                pub(crate) service: String,
                pub(crate) instance: String,
                pub(crate) cause: HaltCause,
            }

            #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
            pub(crate) enum PauseReason {
                Upstream(HaltRef),
                Operator { note: String },
            }

            #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
            pub(crate) struct Pause {
                pub(crate) reason: PauseReason,
                pub(crate) since_unix_ms: u64,
            }

            #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
            pub(crate) enum ServiceState {
                Running,
                Halted(Halt),
                Paused(Pause),
                Resumed,
            }

            #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
            pub(crate) struct ServiceEvent {
                pub(crate) service: String,
                pub(crate) instance: String,
                pub(crate) seq: u64,
                pub(crate) state: ServiceState,
            }

            /// Decode `bytes` as this release does: with the validation
            /// of the archive, as `kardamom_log::codec::materialize` does.
            pub(crate) fn decode(bytes: &[u8]) -> Result<ServiceEvent, rkyv::rancor::Error> {
                rkyv::from_bytes::<ServiceEvent, rkyv::rancor::Error>(bytes)
            }
        }
    };
}

// The release before the executor stream check: every cause up to
// `L1FollowerDisagreement`.
older_release!(
    before_exec_record_mismatch,
    [
        L1SourceDisagreement,
        L1ChainBreak,
        L1Unreachable,
        ReplayUnavailable,
        DaLag,
        ValidatorDivergence,
        SealerNoQuorum,
        L1CursorUnreadable,
        OriginGap,
        RecordLag,
        L1LightClientMismatch,
        L1FollowerDisagreement
    ],
    [
        L1SourceDisagreement,
        L1ChainBreak,
        L1Unreachable,
        ReplayUnavailable,
        DaLag,
        ValidatorDivergence,
        SealerNoQuorum,
        RevertToPostedHead,
        L1CursorUnreadable,
        OriginGap,
        RecordLag,
        L1LightClientMismatch,
        L1FollowerDisagreement
    ]
);

// The release before the record-lag guard: every cause up to `OriginGap`.
older_release!(
    before_record_lag,
    [
        L1SourceDisagreement,
        L1ChainBreak,
        L1Unreachable,
        ReplayUnavailable,
        DaLag,
        ValidatorDivergence,
        SealerNoQuorum,
        L1CursorUnreadable,
        OriginGap
    ],
    [
        L1SourceDisagreement,
        L1ChainBreak,
        L1Unreachable,
        ReplayUnavailable,
        DaLag,
        ValidatorDivergence,
        SealerNoQuorum,
        RevertToPostedHead,
        L1CursorUnreadable,
        OriginGap
    ]
);

fn encode(event: &ServiceEvent) -> rkyv::util::AlignedVec {
    rkyv::to_bytes::<rkyv::rancor::Error>(event).unwrap()
}

fn halted(service: &str, cause: HaltCause) -> ServiceEvent {
    ServiceEvent {
        service: service.into(),
        instance: "host-1".into(),
        seq: 3,
        state: ServiceState::Halted(Halt::new(cause, "detail")),
    }
}

fn paused_on(cause: HaltCause) -> ServiceEvent {
    ServiceEvent {
        service: "attester".into(),
        instance: "host-1".into(),
        seq: 4,
        state: ServiceState::Paused(Pause {
            reason: PauseReason::Upstream(HaltRef {
                service: "validator".into(),
                instance: "host-1".into(),
                cause,
            }),
            since_unix_ms: 1,
        }),
    }
}

#[test]
fn an_older_reader_drops_an_event_with_exec_record_mismatch() {
    let new_halt = encode(&halted("validator", HaltCause::ExecRecordMismatch));
    assert!(
        before_exec_record_mismatch::decode(&new_halt).is_err(),
        "the release before the cause fails the validation of the archive"
    );
    let new_pause = encode(&paused_on(HaltCause::ExecRecordMismatch));
    assert!(before_exec_record_mismatch::decode(&new_pause).is_err());

    let known = encode(&halted("validator", HaltCause::ValidatorDivergence));
    let decoded = before_exec_record_mismatch::decode(&known).expect("a known cause decodes");
    let before_exec_record_mismatch::ServiceState::Halted(halt) = decoded.state else {
        panic!("the state stays halted");
    };
    assert_eq!(
        halt.cause,
        before_exec_record_mismatch::HaltCause::ValidatorDivergence
    );
    let running = encode(&ServiceEvent {
        state: ServiceState::Running,
        ..halted("validator", HaltCause::ExecRecordMismatch)
    });
    assert!(before_exec_record_mismatch::decode(&running).is_ok());
}

#[test]
fn an_older_reader_drops_an_event_with_record_lag() {
    let new_halt = encode(&halted("sealer", HaltCause::RecordLag));
    assert!(before_record_lag::decode(&new_halt).is_err());
    let known = encode(&halted("sealer", HaltCause::DaLag));
    assert!(before_record_lag::decode(&known).is_ok());
}
