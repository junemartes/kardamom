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

FROM jrei/systemd-ubuntu:22.04

# python3 for Ansible modules; ca-certificates/curl/iproute2 for the roles +
# the HashiCorp downloads. The docker role installs docker-ce itself.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-apt ca-certificates curl iproute2 sudo gnupg && \
    rm -rf /var/lib/apt/lists/*

# systemd is the entrypoint (inherited from the base image).
