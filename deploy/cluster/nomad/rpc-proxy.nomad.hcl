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
        image        = "192.168.56.10:5000/haproxy:2.9-alpine"
        network_mode = "host"
        volumes      = ["local/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro"]
      }

      template {
        destination = "local/haproxy.cfg"
        data        = <<EOF
defaults
  mode http
  timeout connect 5s
  timeout client  120s
  timeout server  120s
  # Long-lived WebSocket subscriptions (kardamom_subscribeReceipts).
  timeout tunnel  1h

frontend rpc
  bind *:8545
  default_backend ingress

backend ingress
  balance roundrobin
  # No HTTP health endpoint in v0 ingress: `check` alone does TCP-connect
  # liveness, which is exactly right (a replica that accepts connections
  # serves traffic; the conn-cap wedge class is gone with subscribe mode).
  server ingress0 192.168.56.31:8545 check
  server ingress1 192.168.56.32:8545 check
EOF
      }

      resources {
        cpu    = 500
        memory = 128
      }
    }
  }
}
