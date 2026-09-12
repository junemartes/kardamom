variable "contract_file" {
  description = "The Ansible group_vars file that declares ip_prefix and node_classes. A relative path is resolved from this root."
  type        = string
  default     = "../../ansible/group_vars/all.yml"
}

variable "network_name" {
  description = "The Docker network of the node containers."
  type        = string
  default     = "kardamom-net"
}

variable "subnet" {
  description = "The range of the container network. Docker assigns every node address from it; the Aeron multicast groups are separate."
  type        = string
  default     = "192.168.56.0/24"
}

variable "bridge_name" {
  description = "The Linux bridge that backs the network. The host prep role tunes its multicast snooping."
  type        = string
  default     = "kardamom-br0"
}

variable "image_name" {
  description = "The tag of the systemd node image built from docker/node.Dockerfile."
  type        = string
  default     = "kardamom-node:ci"
}

variable "docker_dir" {
  description = "The build context with node.Dockerfile. A relative path is resolved from this root."
  type        = string
  default     = "../../docker"
}

variable "label" {
  description = "The label key that marks every resource of this root."
  type        = string
  default     = "io.kardamom.cluster"
}

variable "ready_timeout" {
  description = "Seconds to wait for systemd inside a node to report running or degraded."
  type        = number
  default     = 180
}
