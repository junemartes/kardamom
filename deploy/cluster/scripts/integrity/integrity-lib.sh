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

# Nodes that can host kardamom task containers. The control node runs only
# registry + anvil + the consul/nomad servers, but it is still enumerated —
# classification happens per container, so a misplaced kardamom task on the
# control node IS a finding, not a blind spot.
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
#   <cid>|<name>|<svc>
#
# Enumerated by the Nomad docker driver's alloc_id LABEL, not by image ref:
# Nomad 1.9.5 creates task containers with `Image: <imageID>` (the sha256
# config id, drivers/docker/driver.go), so `docker ps` shows an ID — an
# image-string filter would miss every task. The service key comes from the
# container NAME, which Nomad forms as <task>-<alloc-uuid>.
kardamom_containers() {
  local node="$1" cid name svc
  while IFS='|' read -r cid name; do
    [[ -n "${cid}" ]] || continue
    svc="$(svc_from_task_name "${name}")"
    [[ -n "${svc}" ]] && echo "${cid}|${name}|${svc}"
  done < <(docker exec "${node}" docker ps --no-trunc \
      --filter label=com.hashicorp.nomad.alloc_id \
      --format '{{.ID}}|{{.Names}}' 2>/dev/null || true)
}

# Container name (<task>-<alloc-uuid>) -> digest-manifest service key. Empty
# for Nomad tasks OUTSIDE the manifest's scope, which the sweeps skip:
# anvil (digest-pinned directly in its job spec), haproxy/rpc-proxy (manual
# push, operator-pinned), spire server/agent (pinned upstream digests).
svc_from_task_name() {
  case "$1" in
    archiving-media-driver-*)    echo aeron ;;
    cluster-*)                   echo cluster ;;
    ingress-*)                   echo ingress ;;
    sequencer-a-*|sequencer-b-*) echo sequencer ;;
    executor-*)                  echo executor ;;
    validator-*)                 echo validator ;;
    da-watcher-*)                echo da-watcher ;;
    batcher-*)                   echo batcher ;;
    *)                           echo "" ;;
  esac
}

# Strip the advisory :tag from a repo:tag@sha256:... reference, yielding the
# bare repo@sha256:... form dockerd records in an image's RepoDigests. A ref
# without a digest (the :dev fallback) or without a tag passes through
# unchanged. Careful with the registry-port colon: only a colon in the
# BASENAME is a tag separator.
bare_digest_ref() {
  local ref="$1"
  if [[ "${ref}" != *@* ]]; then
    echo "${ref}"
    return
  fi
  local repo="${ref%%@*}" digest="${ref#*@}"
  local base="${repo##*/}"
  [[ "${base}" == *:* ]] && repo="${repo%:*}"
  echo "${repo}@${digest}"
}
