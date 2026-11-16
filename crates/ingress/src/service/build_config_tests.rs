//! The flags that `IngressService::build_config` checks together.

use clap::Parser;

use super::IngressService;
use crate::config::IngressFileConfig;
use crate::service::settings::IngressSettings;

#[derive(Parser)]
struct Flags {
    #[command(flatten)]
    settings: IngressSettings,
}

/// The service of the flags `args`, with the default files.
fn service(args: &[&str]) -> IngressService {
    let settings =
        Flags::try_parse_from(std::iter::once("kardamom-ingress").chain(args.iter().copied()))
            .expect("the flags parse")
            .settings;
    IngressService {
        settings,
        aeron_dir: None,
        log_cfg: kardamom_log::config::LogConfig::default(),
        file_cfg: IngressFileConfig::default(),
    }
}

#[test]
fn the_verify_agent_is_off_by_default() {
    let cfg = service(&["--ack", "validated"])
        .build_config()
        .expect("valid");
    assert!(!cfg.verify_agent);
}

#[test]
fn the_verify_agent_runs_with_the_validated_ack() {
    let cfg = service(&["--ack", "validated", "--verify-agent", "on"])
        .build_config()
        .expect("valid");
    assert!(cfg.verify_agent);
}

#[test]
fn the_verify_agent_needs_the_validated_ack() {
    let err = service(&["--verify-agent", "on"])
        .build_config()
        .unwrap_err();
    assert_eq!(err.to_string(), "--verify-agent on needs --ack validated");
}
