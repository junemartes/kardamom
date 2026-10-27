use super::*;

struct StartupProbe {
    config: SequencerConfig,
}

impl StartupProbe {
    fn new(admission: bool) -> Self {
        let mut config = SequencerConfig::default();
        config.admission.enabled = admission;
        Self { config }
    }

    fn capture(&self) -> (ResyncChannel, String) {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let channel = metrics::with_local_recorder(&recorder, || {
            ResyncChannel::for_config(&self.config).unwrap()
        });
        (channel, recorder.handle().render())
    }
}

#[test]
fn admission_startup_does_not_claim_the_legacy_resync_loop_is_active() {
    let (channel, body) = StartupProbe::new(true).capture();
    assert!(!channel.controller.active());
    assert!(
        body.lines().any(|line| {
            line.starts_with("kardamom_sequencer_resync_mode{") && line.ends_with(" 0")
        }),
        "{body}"
    );
    assert!(
        !body
            .lines()
            .any(|line| line.starts_with("kardamom_sequencer_resync_entered_total{")),
        "{body}"
    );
}

#[test]
fn receipt_floor_startup_keeps_its_legacy_resync_guard_and_metric() {
    let (channel, body) = StartupProbe::new(false).capture();
    assert!(channel.controller.active());
    assert!(
        body.lines().any(|line| {
            line.starts_with("kardamom_sequencer_resync_mode{") && line.ends_with(" 1")
        }),
        "{body}"
    );
}

#[test]
fn admission_startup_keeps_the_terminal_refusal_channel_owned_and_live() {
    let (mut channel, _) = StartupProbe::new(true).capture();
    let refusal = SealerRefusal {
        sender: alloy_primitives::Address::repeat_byte(7),
        nonce: 3,
        reason: kardamom_types::TxErrorReason::DaLag {
            sealed_head: 100,
            posted_head: 1,
            budget_blocks: 10,
        },
    };
    channel.deadline_tx.send(refusal.clone()).unwrap();
    assert_eq!(channel.controller.drain_deadline_rejects(), [refusal]);
}
