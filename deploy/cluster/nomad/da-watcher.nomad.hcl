# kardamom-da-watcher polls L1 for deposits, and publishes Deposit
# envelopes onto tx_deposits. It runs on dawatcher1 (192.168.56.34).
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-da-watcher --l1-rpc http://192.168.56.10:8546 --lockbox <addr> \
#       --aeron-dir <dir> --poll-interval-secs 1
#
# --l1-rpc defaults to the in-cluster anvil on r1 (control_ip:anvil_l1 =
# 192.168.56.10:8546), through the `l1_rpc_url` variable below. --lockbox is the chain-specific Lockbox
# contract address. It is not known until the deployer deploys it, so
# it is exposed as the HCL variable `lockbox_address` below, with a
# clearly marked placeholder default. Override it at submit time:
#   nomad run -var 'lockbox_address=0x<real-addr>' da-watcher.nomad.hcl
#
# This shares the node's Aeron media driver, through the bind-mounted
# tmpfs aeron.dir.

# The L1 endpoint the watcher derives epochs from. The default is the
# in-cluster anvil on the control node. When the L1 light client is
# deployed (l1-light-client.nomad.hcl), scripts/deploy.sh points this at
# the light client, the same as the validator. The watcher is the epoch
# SOURCE, so a lying endpoint here produces bad epochs at the source
# rather than false halts (issue #163). Routing it through a verifying
# client closes that.
variable "l1_rpc_url" {
  type        = string
  description = "L1 JSON-RPC the watcher derives epochs from. Default: the in-cluster anvil. Point it at the light client on a real network."
  default     = "http://192.168.56.10:8546"
}

variable "lockbox_address" {
  type        = string
  description = "L1 Lockbox contract address (chain-specific; supplied by the deployer after the Lockbox is deployed). The default below is a PLACEHOLDER and will not work against a real chain."
  # This is a placeholder. Replace it with `-var
  # lockbox_address=0x...` at submit time.
  default = "0x0000000000000000000000000000000000000000"
}

# Digest-pinned image. scripts/deploy.sh
# passes the repo:tag@sha256:... reference captured at push time
# (deploy/cluster/images.digests). The empty default falls back to the
# mutable :dev tag in the task config. That fallback is a dev
# affordance for manual `nomad job run` during debugging, not a
# production path.
variable "image_ref" {
  type        = string
  description = "Digest-pinned image reference (repo:tag@sha256:...) from the deploy's push manifest. Empty = mutable :dev tag fallback (dev-only)."
  default     = ""
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

    # Resilience (chaos tests): restart a crashed task on the same
    # node, and reschedule onto a healthy node on node loss. This is a
    # singleton on a single aux-role node, so node-failure recovers
    # when the node returns.
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
        image = var.image_ref != "" ? var.image_ref : "192.168.56.10:5000/kardamom-da-watcher:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it. On the pinned path, the
        # 1.9.5 driver pulls the tag but resolves the image by digest,
        # so the pin holds.
        force_pull = true
        # Read-only rootfs. The da-watcher
        # writes only to the bind-mounted aeron directory, plus
        # Nomad's alloc, local, and secrets mounts. cluster-e2e
        # validates this.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--l1-rpc", "${var.l1_rpc_url}",
          "--lockbox", "${var.lockbox_address}",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--poll-interval-secs", "1",
          # Record tx_deposits to the archive, so a restarted
          # executor can replay deposit envelopes (Phase 2 crash
          # recovery).
          "--archive-durability",
        ]
      }

      # Cluster LogConfig (UDP multicast channels), read through
      # --log-config.
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
