# Redis for the account-state projection and the receipt index: one
# primary, one replica, and three sentinels. See
# docs/specs/2026-09-13-redis-account-cache-design.md, section 6.
#
# Placement, by the role set of a node (group_vars/all.yml,
# node_classes): the primary on the node whose roles hold redis-primary,
# the replica on the node with redis-replica, one sentinel on each of
# three nodes with redis. Production gives redis three nodes of its own;
# the local profile packs them onto aux-0 and the ingress nodes. No group
# names a node or a node index: an instance announces the node record of
# the node it runs on, and the replica and the sentinels read the
# primary's node from its Consul service.
#
# The initial primary is the node with redis-primary: the placement rule
# of the primary group. The replica and the sentinels name it by the
# expression in local.primary_node, so every instance names the same node
# on a cold start, before the primary has registered its service.
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

locals {
  # A consul-template expression that prints the node of the primary:
  # - the node of the registered redis-primary service, when one passes
  #   its check;
  # - else the first catalog node, by name, whose Consul node meta
  #   `roles` holds redis-primary (roles/consul writes it at agent start).
  # The second rule is the placement rule of the primary group, so on a
  # cold start every sentinel and the replica name the node where the
  # primary starts. The two rules agree only when exactly one node holds
  # redis-primary; roles/profile refuses an inventory with another count.
  # The expression prints no whitespace, and prints an empty name when
  # neither rule finds a node: no node meta yet, or a Consul token that
  # cannot read the node catalog (Consul then filters the catalog to
  # empty with no error).
  primary_node = trimspace(<<EOF
{{ $primary := "" }}{{ with service "redis-primary" }}{{ $primary = (index . 0).Node }}{{ end }}{{ range nodes }}{{ if and (eq $primary "") (.Meta.roles | split "," | contains "redis-primary") }}{{ $primary = .Node }}{{ end }}{{ end }}{{ $primary }}
EOF
  )
}

job "redis" {
  datacenters = [var.datacenter]
  type        = "service"

  group "primary" {
    count = 1

    constraint {
      attribute = "${meta.roles}"
      operator  = "set_contains"
      value     = "redis-primary"
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
          "--replica-announce-ip", "${node.unique.name}.node.${var.datacenter}.consul",
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
      attribute = "${meta.roles}"
      operator  = "set_contains"
      value     = "redis-replica"
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
          # The primary, by its node record (REDIS_PRIMARY_NODE, from
          # its Consul service). A sentinel failover reconfigures this
          # replica in place.
          "--replicaof", "${REDIS_PRIMARY_NODE}.node.${var.datacenter}.consul", "6379",
          # The node record this replica reports to its primary. See the
          # primary task: the sentinels must know each instance by one
          # name.
          "--replica-announce-ip", "${node.unique.name}.node.${var.datacenter}.consul",
        ]
      }
      # The node of the primary, read once at start (local.primary_node).
      # A later failover is the sentinels' work.
      template {
        destination = "local/primary.env"
        env         = true
        change_mode = "noop"
        data        = <<EOF
REDIS_PRIMARY_NODE=${local.primary_node}
EOF
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
      attribute = "${meta.roles}"
      operator  = "set_contains"
      value     = "redis"
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
        # Sentinel rewrites its own config file (its id, the config
        # epoch, the current primary, the known replicas and sentinels),
        # so the live file is on the writable alloc dir and only Sentinel
        # writes it. A live file with `sentinel myid` is a file that
        # Sentinel loaded and rewrote: the task keeps it, so a restart in
        # place keeps the state of Sentinel. Else the task copies the
        # rendered seed over it at each start, so a later re-render of
        # the seed reaches the next start. A seed that names no primary
        # (an empty node name) stops the task with a log line, and the
        # task never copies it.
        args = ["sh", "-c", <<EOF
if ! grep -q '^sentinel myid' /local/sentinel.conf 2>/dev/null; then
  if grep -q '^sentinel monitor kardamom \.node\.' /local/sentinel.seed.conf; then
    echo 'redis-sentinel: the seed names no primary: no redis-primary service passes, and no Consul node meta roles holds redis-primary' >&2
    exit 1
  fi
  cp /local/sentinel.seed.conf /local/sentinel.conf
fi
exec redis-sentinel /local/sentinel.conf
EOF
        ]
      }

      # The seed always has a `sentinel monitor` line (local.primary_node):
      # Sentinel stops at start on a config without one.
      # From the seed, Sentinel follows each failover by its own state and
      # by the hello messages of the other sentinels, which carry the
      # newest config epoch.
      #
      # A quorum of 2 of 3 sentinels declares the primary down after 5 s
      # and elects the replica. Readers degrade during the election; the
      # spec's fallback rule covers the gap.
      template {
        destination = "local/sentinel.seed.conf"
        change_mode = "noop"
        data        = <<EOF
port 26379
dir /local
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel monitor kardamom ${local.primary_node}.node.${var.datacenter}.consul 6379 2
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
