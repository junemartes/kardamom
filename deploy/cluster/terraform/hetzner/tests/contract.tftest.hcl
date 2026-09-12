# Plan-time checks of the Hetzner root with a mocked provider. No API
# call, no token, no resource.

mock_provider "hcloud" {
  mock_resource "hcloud_network" {
    defaults = { id = "1001" }
  }
  mock_resource "hcloud_load_balancer" {
    defaults = { id = "1002", ipv4 = "192.0.2.1" }
  }
}

variables {
  cluster_id          = "kardamom-test"
  location            = "fsn1"
  network_cidr        = "10.10.0.0/16"
  cloud_subnet_cidr   = "10.10.0.0/24"
  vswitch_subnet_cidr = "10.10.1.0/24"
  vswitch_id          = 1
  dns_zone            = "kardamom.internal"
  consul_servers = {
    "consul-0.core" = "10.10.1.10"
    "consul-1.core" = "10.10.1.11"
    "consul-2.core" = "10.10.1.12"
  }
  ssh_keys             = { operator = "ssh-ed25519 AAAA operator" }
  ssh_allowed_cidrs    = ["203.0.113.0/24"]
  elastic_image        = "kardamom-elastic-test"
  bootstrap_revision   = "abc123"
  rpc_proxy_target_ips = ["203.0.113.10", "203.0.113.11"]
  pools = {
    ingress   = { role = "ingress", tier = "worker", server_type = "ccx23", min = 2, max = 6 }
    sequencer = { role = "sequencer", tier = "sequencer", server_type = "ccx33", min = 2, max = 8 }
  }
}

run "contract" {
  command = plan

  assert {
    condition     = output.pool_contract.version == 1
    error_message = "the pool contract is version 1"
  }

  assert {
    condition     = alltrue([for id, p in output.pool_contract.pools : p.node_class == id && p.node_pool == id && p.group_label.value == id && p.labels["group-id"] == id])
    error_message = "a pool id maps one-to-one to node_class, node_pool, the group label and the group-id server label"
  }

  assert {
    condition     = keys(output.pool_contract.pools) == ["kardamom-test-ingress", "kardamom-test-sequencer"]
    error_message = "a pool id carries the cluster id and the pool name"
  }

  assert {
    condition     = alltrue([for p in values(output.pool_contract.pools) : p.max <= 10 && p.min >= 2])
    error_message = "pool bounds fit a spread placement group and keep two nodes"
  }

  assert {
    condition     = tolist(output.pool_contract.consul_retry_join) == tolist(["consul-0.core.kardamom.internal", "consul-1.core.kardamom.internal", "consul-2.core.kardamom.internal"])
    error_message = "the join names are the sorted DNS records of the Consul servers"
  }

  assert {
    condition     = alltrue([for p in values(output.pool_contract.pools) : strcontains(p.user_data, "kardamom-bootstrap.service") && strcontains(p.user_data, "consul-0.core.kardamom.internal")])
    error_message = "user data starts the bootstrap unit and carries the join names"
  }

  assert {
    condition     = alltrue([for p in values(output.pool_contract.pools) : !strcontains(lower(p.user_data), "token") && !strcontains(lower(p.user_data), "secret")])
    error_message = "user data carries no credential"
  }

  assert {
    condition     = length(hcloud_zone_rrset.consul) == 3
    error_message = "three Consul bootstrap records"
  }

  assert {
    condition     = alltrue([for k, f in hcloud_firewall.pool : anytrue([for a in f.apply_to : a.label_selector == "group-id=kardamom-test-${k}"])])
    error_message = "a pool firewall selects the pool's own group label only"
  }

  assert {
    condition     = length(hcloud_load_balancer_target.rpc) == 2
    error_message = "the RPC edge has two proxy targets"
  }
}

run "no_dns_zone" {
  command = plan

  variables {
    dns_zone = ""
  }

  assert {
    condition     = length(hcloud_zone_rrset.consul) == 0 && length(output.pool_contract.consul_retry_join) == 0
    error_message = "without a zone the root publishes no record and the join list is empty"
  }
}

run "pool_max_over_placement_limit" {
  command = plan

  variables {
    pools = {
      ingress = { role = "ingress", tier = "worker", server_type = "ccx23", min = 2, max = 11 }
    }
  }

  expect_failures = [var.pools]
}

run "pool_min_below_two" {
  command = plan

  variables {
    pools = {
      ingress = { role = "ingress", tier = "worker", server_type = "ccx23", min = 1, max = 4 }
    }
  }

  expect_failures = [var.pools]
}

run "subnet_outside_network" {
  command = plan

  variables {
    vswitch_subnet_cidr = "10.20.1.0/24"
    consul_servers = {
      "consul-0.core" = "10.20.1.10"
      "consul-1.core" = "10.20.1.11"
      "consul-2.core" = "10.20.1.12"
    }
  }

  expect_failures = [hcloud_network_subnet.vswitch]
}

run "subnets_overlap" {
  command = plan

  variables {
    cloud_subnet_cidr   = "10.10.0.0/23"
    vswitch_subnet_cidr = "10.10.1.0/24"
  }

  expect_failures = [hcloud_network_subnet.cloud]
}

run "consul_server_outside_vswitch_subnet" {
  command = plan

  variables {
    consul_servers = {
      "consul-0.core" = "10.10.0.10"
      "consul-1.core" = "10.10.1.11"
      "consul-2.core" = "10.10.1.12"
    }
  }

  expect_failures = [hcloud_network_subnet.vswitch]
}

run "one_proxy_target" {
  command = plan

  variables {
    rpc_proxy_target_ips = ["203.0.113.10"]
  }

  expect_failures = [var.rpc_proxy_target_ips]
}
