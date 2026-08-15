# shellcheck shell=bash
# =============================================================================
# integrity-lib.sh — shared enumeration helpers for the integrity sweeps.
# =============================================================================
# SOURCED (never executed) by image-drift.sh / fs-drift.sh /
# egress-inventory.sh. Reuses the chaos suite's access pattern for the DinD
# cluster: each cluster node is a privileged container `kardamom-<class>-<i>`
# running its own dockerd, and the pipeline services are INNER docker
# containers (Nomad docker-driver tasks) — so everything here goes through
# `docker exec kardamom-<node> docker ...`, exactly like chaos-asserts.sh's
# injectors. On the Vagrant/VM topology substitute `vagrant ssh <node> -c`
# for the exec (not wired here; the sweeps target the container cluster the
# e2e + chaos suites run against).
#
# Requires (sourced first by each entry script):
#   ../lib.sh           log/fail
#   ../lib-topology.sh  topology_load -> NODES/NODE_IP/NODE_ROLE/NODE_TIER

# Image namespace of the kardamom services in the in-cluster registry; inner
# containers are recognised by it (mirrors group_vars image_prefix).
INTEGRITY_IMAGE_PREFIX="${INTEGRITY_IMAGE_PREFIX:-192.168.56.10:5000/kardamom-}"

# Nodes that can host kardamom task containers. The control node runs only
# registry + anvil + the consul/nomad servers (no kardamom-* images), but it
# is still enumerated — filtering happens by image, so a misplaced kardamom
# container on the control node IS a finding, not a blind spot.
integrity_nodes() {
  local n
  for n in "${NODES[@]}"; do
    echo "kardamom-${n}"
  done
}

# True if the node container is up and answering docker commands.
node_reachable() {
  docker exec "$1" docker version >/dev/null 2>&1
}

# Inner kardamom task containers on a node, one per line:
#   <cid>|<name>|<image-ref-as-started>
# The name is Nomad's <task>-<alloc-id>; the image ref is exactly what the
# docker driver was asked to run (repo@sha256:... when the deploy pinned it,
# repo:dev on the fallback path).
kardamom_containers() {
  local node="$1"
  docker exec "${node}" docker ps --no-trunc --format '{{.ID}}|{{.Names}}|{{.Image}}' 2>/dev/null \
    | grep -F "${INTEGRITY_IMAGE_PREFIX}" || true
}

# Short service key for a container's image ref (manifest key namespace):
# 192.168.56.10:5000/kardamom-ingress@sha256:... -> ingress
# (Basename FIRST, then strip the tag: the registry component carries a colon
# of its own, so a naive %:* on the full ref would eat everything after the
# registry host.)
svc_from_image() {
  local ref="${1%%@*}"
  local name="${ref##*/}"
  name="${name%%:*}"
  echo "${name#kardamom-}"
}
