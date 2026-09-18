# Redis for the account-state projection and the receipt index: one
# primary, one replica, and three sentinels. See
# docs/specs/2026-09-13-redis-account-cache-design.md, section 6.
#
# Placement: the primary on aux-0, the replica on ingress-1, and one
# sentinel each on aux-0, ingress-0, and ingress-1. aux has count 1, so a
# replica there adds nothing. The executor nodes are memory-pressured
# (executor, mirror, Aeron driver). The ingress nodes are light, and a
# sentinel is tiny. No new node class.
#
# The readers (ingress, sequencer, mirror) discover the primary through
# the sentinels, not through Consul. Consul carries the three services for
# the health view and the chaos suite.
#
# No persistence: the state mirror rebuilds the projection from an
# executor checkpoint, so a restarted Redis comes back empty and warms.

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

job "redis" {
  datacenters = [var.datacenter]
  type        = "service"

  group "primary" {
    count = 1

    constraint {
      attribute = "${meta.role}"
      value     = "aux"
    }

    restart {
      attempts = 3
      interval = "1m"
      delay    = "5s"
      mode     = "delay"
    }

    network {
      mode = "host"
      port "redis" {
        static = 6379
      }
    }

    task "redis" {
      driver = "docker"

      config {
        image           = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-redis:dev"
        force_pull      = true
        readonly_rootfs = true
        network_mode    = "host"
        args = [
          "redis-server", "/usr/local/etc/redis/redis.conf",
          "--dir", "/local",
          # The node record this instance reports to its primary once a
          # failover makes it a replica. The sentinels resolve and
          # announce hostnames, so every instance must announce one too.
          # Without it the sentinels learned the demoted primary a second
          # time, under its IP from the new primary's INFO, and then held
          # two identities for one instance: a failover promoted one and
          # reconfigured the other as its replica, so the node replicated
          # from itself and never served (issue #373).
          "--replica-announce-ip", "aux-0.node.${var.datacenter}.consul",
        ]
      }

      service {
        name     = "redis-primary"
        port     = "redis"
        provider = "consul"

        check {
          type     = "tcp"
          interval = "10s"
          timeout  = "2s"
        }
      }

      resources {
        cpu    = 500
        memory = 1280
      }
    }
  }

  group "replica" {
    count = 1

    constraint {
      attribute = "${meta.role}"
      value     = "ingress"
    }
    constraint {
      attribute = "${meta.node_index}"
      value     = "1"
    }

    restart {
      attempts = 3
      interval = "1m"
      delay    = "5s"
      mode     = "delay"
    }

    network {
      mode = "host"
      port "redis" {
        static = 6379
      }
    }

    task "redis" {
      driver = "docker"

      config {
        image           = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-redis:dev"
        force_pull      = true
        readonly_rootfs = true
        network_mode    = "host"
        args = [
          "redis-server", "/usr/local/etc/redis/redis.conf",
          "--dir", "/local",
          # The primary, by its node record. A sentinel failover
          # reconfigures this replica in place.
          "--replicaof", "aux-0.node.${var.datacenter}.consul", "6379",
          # The node record this replica reports to its primary. See the
          # primary task: the sentinels must know each instance by one
          # name.
          "--replica-announce-ip", "ingress-1.node.${var.datacenter}.consul",
        ]
      }

      service {
        name     = "redis-replica"
        port     = "redis"
        provider = "consul"

        check {
          type     = "tcp"
          interval = "10s"
          timeout  = "2s"
        }
      }

      resources {
        cpu    = 500
        memory = 1280
      }
    }
  }

  group "sentinel" {
    count = 3

    constraint {
      attribute = "${meta.role}"
      operator  = "regexp"
      value     = "^(aux|ingress)$"
    }
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

    restart {
      attempts = 3
      interval = "1m"
      delay    = "5s"
      mode     = "delay"
    }

    network {
      mode = "host"
      port "sentinel" {
        static = 26379
      }
    }

    task "sentinel" {
      driver = "docker"

      config {
        image           = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-redis:dev"
        force_pull      = true
        readonly_rootfs = true
        network_mode    = "host"
        # Sentinel rewrites its own config file, so it runs on the
        # writable alloc dir.
        args = ["redis-sentinel", "/local/sentinel.conf"]
      }

      # A quorum of 2 of 3 sentinels declares the primary down after 5 s
      # and elects the replica. Readers degrade during the election; the
      # spec's fallback rule covers the gap.
      template {
        destination = "local/sentinel.conf"
        change_mode = "noop"
        data        = <<EOF
port 26379
dir /local
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel monitor kardamom aux-0.node.${var.datacenter}.consul 6379 2
sentinel down-after-milliseconds kardamom 5000
sentinel failover-timeout kardamom 30000
sentinel parallel-syncs kardamom 1
EOF
      }

      service {
        name     = "redis-sentinel"
        port     = "sentinel"
        provider = "consul"

        check {
          type     = "tcp"
          interval = "10s"
          timeout  = "2s"
        }
      }

      resources {
        cpu    = 100
        memory = 64
      }
    }
  }
}
