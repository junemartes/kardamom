# =============================================================================
# node.Dockerfile — a cluster "node" as a systemd container, for the
# container-based cluster e2e (.github/workflows/cluster-e2e.yml).
# =============================================================================
#
# EXPERIMENTAL. This replaces a Vagrant VM with a privileged systemd container
# so the SAME Ansible playbook (site.yml) can provision Docker + Consul + Nomad
# and Nomad's docker driver can run workloads via Docker-in-Docker. It is the
# CI analogue of a VM; it is NOT a production pattern.
#
# Requirements at run time (set by scripts/ci-cluster.sh):
#   * --privileged (systemd + an inner dockerd need it)
#   * cgroup v2 mount, tmpfs /run + /run/lock
#   * a user-defined bridge network with the node's static 192.168.56.x IP
#
# Ansible reaches these over the community.docker connection plugin (see
# ansible/inventory.containers.ini), so no SSH is installed.

# geerlingguy's image is the de-facto base for testing Ansible against systemd
# containers: multi-arch (amd64 + arm64, so it also builds locally on Apple
# Silicon), runs `/lib/systemd/systemd` as PID 1, and ships python3 + systemctl
# + sudo preinstalled.
FROM geerlingguy/docker-ubuntu2204-ansible:latest

# Extras the roles + HashiCorp downloads need on top of the base (curl, unzip,
# iproute2, gnupg, ca-certificates). docker-ce is installed by the docker role.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl unzip iproute2 gnupg && \
    rm -rf /var/lib/apt/lists/*

# systemd (/lib/systemd/systemd) is the CMD, inherited from the base image.
