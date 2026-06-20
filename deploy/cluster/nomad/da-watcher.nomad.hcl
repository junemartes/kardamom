# kardamom-da-watcher — polls L1 for deposits and publishes Deposit envelopes
# onto tx_deposits. Placed on dawatcher1 (192.168.56.34).
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-da-watcher --l1-rpc http://192.168.56.10:8546 --lockbox <addr> \
#       --aeron-dir <dir> --poll-interval-secs 1
#
# --l1-rpc points at the in-cluster anvil on r1 (control_ip:anvil_l1 =
# 192.168.56.10:8546). --lockbox is the chain-specific Lockbox contract
# address; it is NOT known until the deployer deploys it, so it is exposed as
# the HCL variable `lockbox_address` below with a clearly-marked PLACEHOLDER
# default. Override at submit time:
#   nomad run -var 'lockbox_address=0x<real-addr>' da-watcher.nomad.hcl
#
# Shares the node Aeron media driver via the bind-mounted tmpfs aeron.dir.

variable "lockbox_address" {
  type        = string
  description = "L1 Lockbox contract address (chain-specific; supplied by the deployer after the Lockbox is deployed). The default below is a PLACEHOLDER and will not work against a real chain."
  # PLACEHOLDER — replace via `-var lockbox_address=0x...` at submit time.
  default = "0x0000000000000000000000000000000000000000"
}

job "da-watcher" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "aux"
  }

  group "da-watcher" {
    count = 1

    # Resilience (chaos tests): restart a crashed task on the same node;
    # reschedule onto a healthy node on node loss. Singleton on a single
    # aux-role node, so node-failure recovers when the node returns.
    restart {
      attempts = 3
      interval = "1m"
      delay    = "5s"
      mode     = "delay"
    }

    reschedule {
      delay          = "10s"
      delay_function = "exponential"
      max_delay      = "1m"
      unlimited      = true
    }

    update {
      max_parallel     = 1
      health_check     = "task_states"
      min_healthy_time = "10s"
      healthy_deadline = "2m"
      auto_revert      = false
    }

    network {
      mode = "host"
    }

    task "da-watcher" {
      driver = "docker"

      config {
        image        = "192.168.56.10:5000/kardamom-da-watcher:dev"
        # Always pull the freshly-built image: the mutable :dev tag would otherwise
        # let Nomad reuse a stale node-cached layer across rebuilds (caused a
        # crash-retry storm that stalled the deploy).
        force_pull    = true
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--l1-rpc", "http://192.168.56.10:8546",
          "--lockbox", "${var.lockbox_address}",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--poll-interval-secs", "1",
        ]
      }

      # Cluster LogConfig (UDP multicast channels), consumed via --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      resources {
        cpu    = 300
        memory = 256
      }
    }
  }
}
