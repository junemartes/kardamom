use super::*;

#[test]
fn the_default_vars_set_only_an_empty_genesis_carry() {
    assert_eq!(
        DeployVars::default().env(),
        vec![("KARDAMOM_GENESIS_CARRY", String::new())]
    );
}

#[test]
fn a_local_canary_uses_the_cluster_test_alert_receiver() {
    let vars = DeployVars {
        canary_local: Some(true),
        ..DeployVars::default()
    };
    assert!(vars.env().contains(&("CANARY_LOCAL", "1".to_string())));
    let lifecycle = Lifecycle::new(Path::new("/repo")).with_vars(vars);
    let command = lifecycle.command("ansible-playbook", &[]);
    let setting = command
        .as_std()
        .get_envs()
        .find(|(key, _)| *key == "ALERTMANAGER_CONFIG_FILE");
    assert_eq!(
        setting.unwrap().1,
        Some(std::ffi::OsStr::new(
            "/repo/deploy/cluster/config/alertmanager-chaos.yml"
        ))
    );
    let defaults = Lifecycle::new(Path::new("/repo")).command("ansible-playbook", &[]);
    assert!(
        !defaults
            .as_std()
            .get_envs()
            .any(|(key, _)| key == "ALERTMANAGER_CONFIG_FILE")
    );
}

#[test]
fn the_carry_switches_reach_the_deploy_variables() {
    let vars = DeployVars {
        exec_cursor: true,
        genesis_carry: true,
        carry: CarrySwitches {
            exec_on_sealers: true,
            egress: EgressStage::Readers,
            admission: true,
            version_floor: Some(5),
            carry_executors: vec![0, 1],
            lockbox: Some("0xab".to_string()),
        },
        ..DeployVars::default()
    };
    let env = vars.env();
    let want = [
        ("KARDAMOM_EXEC_CURSOR", "on"),
        ("KARDAMOM_GENESIS_CARRY", "on"),
        ("KARDAMOM_MEMBER_FEED", "on"),
        ("KARDAMOM_EXEC_ON_SEALERS", "on"),
        ("KARDAMOM_EXEC_FRAMES", "on"),
        ("KARDAMOM_EGRESS", "1"),
        ("KARDAMOM_SEQUENCER_ADMISSION", "on"),
        ("KARDAMOM_BATCHER_EGRESS", "on"),
        ("KARDAMOM_VALIDATOR_EGRESS", "on"),
        ("KARDAMOM_SEALER_VERSION_FLOOR", "5"),
        ("KARDAMOM_CARRY_EXECUTORS", "0,1"),
        ("LOCKBOX_ADDRESS", "0xab"),
    ];
    assert_eq!(
        env,
        want.map(|(k, v)| (k, v.to_string())).to_vec(),
        "{env:?}"
    );
}

#[test]
fn a_saved_cluster_resumes_in_place_of_the_convergence() {
    let new = Lifecycle {
        prepared: None,
        ..Lifecycle::new(Path::new("/repo"))
    };
    let saved = Lifecycle {
        prepared: Some(Prepared {
            dir: PathBuf::from("/saved"),
        }),
        ..new.clone()
    };
    assert_eq!(new.bring_up_playbook(), "ansible/cluster.yml");
    assert_eq!(saved.bring_up_playbook(), "ansible/resume.yml");
}

#[test]
fn the_chaos_knobs_reach_the_deploy_variables() {
    let vars = DeployVars {
        knobs: ChaosKnobs {
            cluster_jvm_opts: Some("-Daeron.cluster.startup.canvass.timeout=15s".to_string()),
            heartbeat_grace_s: Some(30),
            canary_stalled_window: Some("1m".to_string()),
            canary_force_pull: Some(false),
            checkpoint_interval_s: Some(5),
        },
        ..DeployVars::default()
    };
    let want = [
        ("KARDAMOM_GENESIS_CARRY", ""),
        (
            "KARDAMOM_CLUSTER_JVM_OPTS",
            "-Daeron.cluster.startup.canvass.timeout=15s",
        ),
        ("NOMAD_HEARTBEAT_GRACE", "30s"),
        ("KARDAMOM_CANARY_STALLED_WINDOW", "1m"),
        ("KARDAMOM_CANARY_FORCE_PULL", "off"),
        ("KARDAMOM_CHECKPOINT_INTERVAL_S", "5"),
    ];
    assert_eq!(vars.env(), want.map(|(k, v)| (k, v.to_string())).to_vec());
}

#[test]
fn a_lifecycle_keeps_its_vars() {
    let vars = DeployVars {
        checkpoint_log: true,
        ..DeployVars::default()
    };
    let lifecycle = Lifecycle::new(Path::new("/repo")).with_vars(vars);
    assert!(lifecycle.vars().checkpoint_log);
    assert_eq!(lifecycle.cluster_dir(), Path::new("/repo/deploy/cluster"));
}
