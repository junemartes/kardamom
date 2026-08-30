# This is an RPC dispatch proxy: one haproxy in front of both ingress
# replicas. It spreads submits that would otherwise all land on
# whichever replica the client was pointed at (measured: ingress-0 at
# 126% of a core versus ingress-1 at 71%, during a 3,200 tx/s soak).
# It is safe to balance randomly: both replicas consume the same
# tx_receipts MDS fan-in, so any replica can answer any receipt query
# and serve any receipt subscription. WebSocket upgrades pass through
# haproxy's http mode unchanged; the timeout tunnel keeps long-lived
# subscription connections open.
#
# Image: pull haproxy upstream once on the orchestrator, and push it to
# the in-cluster registry as 192.168.56.10:5000/haproxy:2.9-alpine.
# This is the same flow as the service images; DinD nodes trust only
# the local registry.

# Digest-pinned image. This job is not in
# deploy.sh's default path; the haproxy image is pushed manually, as
# described above. So there is no automatic manifest line for it. An
# operator who wants the pin passes the digest of their own push, in
# the combined repo:tag@digest form. Nomad 1.9.5's docker driver cannot
# parse a bare repo@digest on a registry host with a port; see
# ci-images.sh.
# nomad job run -var image_ref=192.168.56.10:5000/haproxy:2.9-alpine@sha256:... rpc-proxy.nomad.hcl
# The empty default falls back to the mutable :2.9-alpine tag. That is
# a dev affordance, not a production path.
variable "image_ref" {
  type        = string
  description = "Digest-pinned image reference (repo:tag@sha256:...) of the in-cluster haproxy push. Empty = mutable tag fallback (dev-only)."
  default     = ""
}

job "rpc-proxy" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "aux"
  }

  group "proxy" {
    count = 1

    restart {
      attempts = 3
      interval = "2m"
      delay    = "5s"
      mode     = "delay"
    }

    network {
      port "rpc" {
        static = 8545
      }
    }

    task "haproxy" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "192.168.56.10:5000/haproxy:2.9-alpine"
        # This skips readonly_rootfs on
        # purpose. haproxy writes runtime state into the rootfs
        # (master and worker sockets, plus pid, under /run, and
        # /var/lib/haproxy). This job is also outside the default
        # deploy path, so the cluster-e2e suite would not validate the
        # change. It needs explicit tmpfs mounts for those paths
        # first.
        network_mode = "host"
        volumes      = ["local/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro"]
      }

      template {
        destination = "local/haproxy.cfg"
        data        = <<EOF
global
  maxconn 16384
  nbthread 4

defaults
  mode http
  # Reuse backend connections across requests. At thousands of
  # requests per second, the per-request connect cost became the
  # proxy's own limit on throughput. The accept ratio degraded at
  # 3,000 tx/s, while the pipeline behind it sustains 4,750 tx/s
  # direct.
  http-reuse always
  maxconn 16384
  timeout connect 5s
  timeout client  120s
  timeout server  120s
  # Long-lived WebSocket subscriptions (kardamom_subscribeReceipts).
  timeout tunnel  1h

frontend rpc
  bind *:8545
  default_backend ingress

backend ingress
  # Use leastconn, not roundrobin. With http-reuse and long-lived
  # client pools, roundrobin assigns a backend per frontend
  # connection, and reuse then pins it there. Under a single pooled
  # client, this produced a 78/22 request imbalance across the two
  # replicas, with CPU spikes of 132% versus 9%. leastconn tracks live
  # load continuously, and rebalances as connections churn.
  balance leastconn
  # v0 ingress has no HTTP health endpoint. `check` alone does
  # TCP-connect liveness, which is correct here: a replica that
  # accepts connections serves traffic, and the conn-cap wedge class
  # is gone with subscribe mode.
  server ingress0 192.168.56.31:8545 check
  server ingress1 192.168.56.32:8545 check
EOF
      }

      resources {
        cpu    = 2000
        memory = 256
      }
    }
  }
}
