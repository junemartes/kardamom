# Plan-time checks of the container root with a mocked provider. No
# daemon, no image build, no container.

mock_provider "docker" {
  mock_resource "docker_image" {
    defaults = { image_id = "sha256:0123" }
  }
}

variables {
  contract_file = "tests/contract.yml"
  docker_dir    = "tests"
}

run "contract" {
  command = plan

  assert {
    condition     = output.node_contract.version == 1
    error_message = "the node contract is version 1"
  }

  assert {
    condition     = keys(output.node_contract.nodes) == ["aux-0", "control-0", "worker-0", "worker-1", "worker-2"]
    error_message = "a node is <class>-<i>"
  }

  assert {
    condition     = output.node_contract.nodes["worker-2"].ip == "10.99.0.43" && output.node_contract.nodes["worker-2"].index == 2
    error_message = "a node takes ip_prefix.<ip_start + i>"
  }

  assert {
    condition     = output.node_contract.nodes["control-0"].control_plane && !output.node_contract.nodes["aux-0"].control_plane
    error_message = "only the control class is the control plane"
  }

  assert {
    condition     = alltrue([for n in values(output.node_contract.nodes) : n.container == "kardamom-${n.name}"])
    error_message = "a container is named kardamom-<node>, the name the scripts and the docker connection plugin use"
  }

  assert {
    condition     = output.node_contract.network.subnet == "10.99.0.0/24" && output.node_contract.network.bridge == "kardamom-br0"
    error_message = "the network is the /24 of ip_prefix on the named bridge"
  }

  assert {
    condition     = alltrue([for c in docker_container.node : c.privileged && c.cgroupns_mode == "host" && c.name == "kardamom-${c.hostname}"])
    error_message = "every node is privileged on the host cgroup namespace and carries its node name as hostname"
  }

  assert {
    condition     = alltrue([for c in docker_container.node : one(c.healthcheck).test[0] == "CMD-SHELL" && c.wait])
    error_message = "apply waits for the systemd readiness check of every node"
  }

  assert {
    condition     = length(docker_volume.node) == 2 * length(docker_container.node)
    error_message = "two durable volumes per node"
  }

  assert {
    condition     = alltrue([for c in docker_container.node : anytrue([for n in c.networks_advanced : n.name == "kardamom-net" && n.ipv4_address == output.node_contract.nodes[c.hostname].ip])])
    error_message = "a container takes the static address of its node"
  }
}

run "lane_overlap" {
  command = plan

  variables {
    contract_file = "tests/contract-overlap.yml"
  }

  expect_failures = [docker_network.this]
}
