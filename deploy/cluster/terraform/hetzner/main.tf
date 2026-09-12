# The Hetzner root of the hybrid deployment. It owns the Cloud Network and
# its two subnets, the vSwitch attachment, one spread placement group and
# one public firewall per elastic pool, the SSH keys, the Consul bootstrap
# DNS records and the public RPC load balancer. It owns no server: the
# Nomad Autoscaler creates and deletes the elastic VMs, and the dedicated
# hosts are inventory inputs. See docs/agents/hetzner-hybrid-infra-spec.md.

locals {
  pool_ids = { for k, p in var.pools : k => "${var.cluster_id}-${k}" }

  consul_join_names = var.dns_zone == "" ? [] : [for name in sort(keys(var.consul_servers)) : "${name}.${var.dns_zone}"]

  # The nonsecret first-boot inputs of each pool. cloud-init writes them
  # to /etc/kardamom/node.yml and starts the bootstrap unit. No token and
  # no key is in here: enrollment fetches credentials at first boot.
  user_data = {
    for k, p in var.pools : k => templatefile("${path.module}/user-data.yaml.tftpl", {
      cluster_id         = var.cluster_id
      datacenter         = var.datacenter
      pool_id            = local.pool_ids[k]
      role               = p.role
      tier               = p.tier
      private_interface  = p.private_interface
      consul_retry_join  = local.consul_join_names
      bootstrap_revision = var.bootstrap_revision
    })
  }
}

resource "hcloud_network" "this" {
  name     = "${var.cluster_id}-net"
  ip_range = var.network_cidr
  labels   = { "kardamom.cluster" = var.cluster_id }
}

resource "hcloud_network_subnet" "cloud" {
  network_id   = tonumber(hcloud_network.this.id)
  type         = "cloud"
  network_zone = var.network_zone
  ip_range     = var.cloud_subnet_cidr

  lifecycle {
    precondition {
      condition     = cidrcontains(var.network_cidr, cidrhost(var.cloud_subnet_cidr, 0)) && cidrcontains(var.network_cidr, cidrhost(var.cloud_subnet_cidr, -1))
      error_message = "cloud_subnet_cidr must lie inside network_cidr."
    }
    precondition {
      condition     = !cidrcontains(var.vswitch_subnet_cidr, cidrhost(var.cloud_subnet_cidr, 0)) && !cidrcontains(var.cloud_subnet_cidr, cidrhost(var.vswitch_subnet_cidr, 0))
      error_message = "cloud_subnet_cidr and vswitch_subnet_cidr must not overlap."
    }
  }
}

resource "hcloud_network_subnet" "vswitch" {
  network_id   = tonumber(hcloud_network.this.id)
  type         = "vswitch"
  network_zone = var.network_zone
  ip_range     = var.vswitch_subnet_cidr
  vswitch_id   = var.vswitch_id

  lifecycle {
    precondition {
      condition     = cidrcontains(var.network_cidr, cidrhost(var.vswitch_subnet_cidr, 0)) && cidrcontains(var.network_cidr, cidrhost(var.vswitch_subnet_cidr, -1))
      error_message = "vswitch_subnet_cidr must lie inside network_cidr."
    }
    precondition {
      condition     = alltrue([for ip in values(var.consul_servers) : cidrcontains(var.vswitch_subnet_cidr, ip)])
      error_message = "Every Consul server address must lie inside vswitch_subnet_cidr."
    }
  }
}

# One spread group per pool: Hetzner places its members on distinct
# physical hosts, at most 10 per group.
resource "hcloud_placement_group" "pool" {
  for_each = var.pools

  name   = "${local.pool_ids[each.key]}-spread"
  type   = "spread"
  labels = { "kardamom.cluster" = var.cluster_id, "kardamom.pool" = local.pool_ids[each.key] }
}

# The public firewall of a pool. It filters the public interface only;
# private Network traffic reaches the host, where roles/firewall filters
# it. The Autoscaler labels every VM it creates with group-id=<pool id>,
# and the firewall follows that label.
resource "hcloud_firewall" "pool" {
  for_each = var.pools

  name   = "${local.pool_ids[each.key]}-public"
  labels = { "kardamom.cluster" = var.cluster_id, "kardamom.pool" = local.pool_ids[each.key] }

  dynamic "rule" {
    for_each = length(var.ssh_allowed_cidrs) == 0 ? [] : [1]
    content {
      description = "ssh from the operator ranges"
      direction   = "in"
      protocol    = "tcp"
      port        = "22"
      source_ips  = var.ssh_allowed_cidrs
    }
  }

  rule {
    description = "icmp"
    direction   = "in"
    protocol    = "icmp"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }

  apply_to {
    label_selector = "group-id=${local.pool_ids[each.key]}"
  }
}

resource "hcloud_ssh_key" "this" {
  for_each = var.ssh_keys

  name       = "${var.cluster_id}-${each.key}"
  public_key = each.value
  labels     = { "kardamom.cluster" = var.cluster_id }
}

# The Consul bootstrap records. Every agent joins these names before any
# Consul service record exists, so they resolve through the authoritative
# zone, never through Consul DNS.
data "hcloud_zone" "this" {
  count = var.dns_zone == "" ? 0 : 1
  name  = var.dns_zone
}

resource "hcloud_zone_rrset" "consul" {
  for_each = var.dns_zone == "" ? {} : var.consul_servers

  zone    = data.hcloud_zone.this[0].name
  name    = each.key
  type    = "A"
  ttl     = 60
  records = [{ value = each.value }]
  labels  = { "kardamom.cluster" = var.cluster_id }
}

# The public RPC entry point. The proxies behind it resolve the healthy
# ingress replicas through Consul; the load balancer only knows the core
# hosts that run a proxy.
resource "hcloud_load_balancer" "rpc" {
  name               = "${var.cluster_id}-rpc"
  load_balancer_type = var.load_balancer_type
  location           = var.location
  labels             = { "kardamom.cluster" = var.cluster_id }

  algorithm {
    type = "least_connections"
  }
}

resource "hcloud_load_balancer_network" "rpc" {
  load_balancer_id = tonumber(hcloud_load_balancer.rpc.id)
  subnet_id        = hcloud_network_subnet.cloud.id
}

resource "hcloud_load_balancer_service" "rpc" {
  load_balancer_id = hcloud_load_balancer.rpc.id
  protocol         = "tcp"
  listen_port      = var.rpc_port
  destination_port = var.rpc_port

  # A TCP check proves that a proxy accepts connections. Application
  # readiness is the proxy's own check against the ingress services.
  health_check {
    protocol = "tcp"
    port     = var.rpc_port
    interval = 10
    timeout  = 5
    retries  = 3
  }
}

resource "hcloud_load_balancer_target" "rpc" {
  for_each = toset(var.rpc_proxy_target_ips)

  load_balancer_id = tonumber(hcloud_load_balancer.rpc.id)
  type             = "ip"
  ip               = each.value
}
