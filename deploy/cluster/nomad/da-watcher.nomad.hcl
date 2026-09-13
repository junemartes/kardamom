# kardamom-da-watcher polls L1 for deposits, and publishes Deposit
# envelopes onto tx_deposits. It runs on the aux node.
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-da-watcher --l1-rpc http://anvil.service.consul:8546 --lockbox <addr> \
#       --aeron-dir <dir> --poll-interval-secs 1
#
# --l1-rpc points at the in-cluster anvil by its Consul service record
# (var.l1_rpc). --lockbox is the chain-specific Lockbox
# contract address. It is not known until the deployer deploys it, so
# it is exposed as the HCL variable `lockbox_address` below, with a
# clearly marked placeholder default. Override it at submit time:
#   nomad run -var 'lockbox_address=0x<real-addr>' da-watcher.nomad.hcl
#
# This shares the node's Aeron media driver, through the bind-mounted
# tmpfs aeron.dir.

variable "lockbox_address" {
  type        = string
  description = "L1 Lockbox contract address (chain-specific; supplied by the deployer after the Lockbox is deployed). The default below is a PLACEHOLDER and will not work against a real chain."
  # This is a placeholder. Replace it with `-var
  # lockbox_address=0x...` at submit time.
  default = "0x0000000000000000000000000000000000000000"
}

# Digest-pinned image. ansible/deploy.yml
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

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

variable "l1_rpc" {
  type        = string
  description = "The L1 JSON-RPC endpoint. The default is the in-cluster anvil by its Consul service record."
  default     = "http://anvil.service.consul:8546"
}

job "da-watcher" {
  datacenters = [var.datacenter]
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
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-da-watcher:dev"
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
          "--l1-rpc", var.l1_rpc,
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

      env {
        # The UDP ports the discovered tx_deposits and tx_remote_epochs
        # publications bind on this node.
        KARDAMOM_MDC_PORTS = "40330-40339"
      }

      # Cluster LogConfig (Aeron streams and discovery), read through
      # --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
        # The template reads the archive records from Consul. A change
        # there re-renders the file; the process reads it once at start
        # and follows the catalog through discovery, so never restart.
        change_mode = "noop"
      }

      resources {
        cpu    = 300
        memory = 256
      }
    }
  }
}
