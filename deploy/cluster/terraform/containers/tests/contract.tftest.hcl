# Plan-time checks of the container root with a mocked provider. No
# daemon, no image build, no container.

mock_provider "docker" {
  mock_resource "docker_image" {
    defaults = { image_id = "sha256:0123" }
  }
  mock_resource "docker_container" {
    defaults = { network_data = [{ network_name = "kardamom-net", ip_address = "10.99.0.7", ip_prefix_length = 24, gateway = "10.99.0.1", global_ipv6_address = "", global_ipv6_prefix_length = 0, ipv6_gateway = "", mac_address = "" }] }
  }
}

variables {
  contract_file = "tests/contract.yml"
  docker_dir    = "tests"
  subnet        = "10.99.0.0/24"
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
    condition     = output.node_contract.nodes["worker-2"].index == 2 && output.node_contract.nodes["worker-2"].role == "worker"
    error_message = "a node carries its class and index"
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
    error_message = "the network is the declared range on the named bridge"
  }

  assert {
    condition     = alltrue([for n in values(output.node_contract.nodes) : n.ip == "10.99.0.7"])
    error_message = "a node's address is the one Docker assigned, read back from the container"
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
    condition     = alltrue([for c in docker_container.node : anytrue([for n in c.networks_advanced : n.name == "kardamom-net" && n.ipv4_address == null])])
    error_message = "a container joins the network without a declared address"
  }
}
