# RPC dispatch proxy: one haproxy in front of both ingress replicas, spreading
# submits that would otherwise all land on whichever replica the client was
# pointed at (measured: ingress-0 at 126% of a core vs ingress-1 at 71% during
# a 3,200 tx/s soak). Safe to balance randomly: both replicas consume the same
# tx_receipts MDS fan-in, so any replica answers any receipt query and serves
# any receipt subscription. WebSocket upgrades pass through haproxy's http mode
# untouched (timeout tunnel keeps long-lived subscription connections open).
#
# Image: pull haproxy upstream once on the orchestrator and push to the
# in-cluster registry as 192.168.56.10:5000/haproxy:2.9-alpine (same flow as
# the service images; DinD nodes only trust the local registry).

# Digest-pinned image (attested-identity P0.1). This job is NOT in
# deploy.sh's default path (the haproxy image is pushed manually — see the
# header), so there is no automatic manifest line for it; an operator who
# wants the pin passes the digest of their own push, in the COMBINED
# repo:tag@digest form (Nomad 1.9.5's docker driver mis-parses bare
# repo@digest on a port-carrying registry host — see ci-images.sh):
#   nomad job run -var image_ref=192.168.56.10:5000/haproxy:2.9-alpine@sha256:... rpc-proxy.nomad.hcl
# Empty default = the mutable :2.9-alpine tag fallback (dev affordance, NOT a
# production path).
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
        # NO readonly_rootfs (attested-identity P0.3, deliberately skipped):
        # haproxy writes runtime state into the rootfs (master/worker sockets
        # + pid under /run, /var/lib/haproxy) and this job is outside the
        # default deploy path, so the cluster-e2e suite would not validate
        # the flip. Needs explicit tmpfs mounts for those paths first.
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
  # Reuse backend connections across requests: at thousands of rps the
  # per-request connect cost was the proxy's own admission ceiling
  # (measured: accept ratio degraded at 3,000 tx/s while the pipeline
  # behind it sustains 4,750 direct).
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
  # leastconn, not roundrobin: with http-reuse + long-lived client pools,
  # roundrobin assigns a backend per frontend CONNECTION and reuse then pins
  # it — measured 78/22 request imbalance (and 132% vs 9% CPU instants)
  # across the two replicas under a single pooled client. leastconn tracks
  # live load continuously and rebalances as connections churn.
  balance leastconn
  # No HTTP health endpoint in v0 ingress: `check` alone does TCP-connect
  # liveness, which is exactly right (a replica that accepts connections
  # serves traffic; the conn-cap wedge class is gone with subscribe mode).
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
