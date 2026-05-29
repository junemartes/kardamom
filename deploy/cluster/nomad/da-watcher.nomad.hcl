# kardamom-da-watcher — polls L1 for deposits and publishes Deposit envelopes
# onto tx_deposits. Placed on w2 (192.168.56.22).
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-da-watcher --l1-rpc http://192.168.56.11:8546 --lockbox <addr> \
#       --aeron-dir <dir> --poll-interval-secs 1
#
# --l1-rpc points at the in-cluster anvil on r1 (control_ip:anvil_l1 =
# 192.168.56.11:8546). --lockbox is the chain-specific Lockbox contract
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
    attribute = "${meta.kardamom_node}"
    value     = "w2"
  }

  group "da-watcher" {
    count = 1

    network {
      mode = "host"
    }

    task "da-watcher" {
      driver = "docker"

      config {
        image        = "192.168.56.11:5000/kardamom-da-watcher:dev"
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--l1-rpc", "http://192.168.56.11:8546",
          "--lockbox", "${var.lockbox_address}",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--poll-interval-secs", "1",
        ]
      }

      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
