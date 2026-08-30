# shellcheck shell=bash
# =============================================================================
# lib-topology.sh — the node-class model, in one place.
# =============================================================================
# This file is sourced, never run directly, by chaos.sh, ci-cluster.sh, and
# smoke-load.sh. It has two layers:
#   1. Static mirrors of ansible/group_vars/all.yml: node names, bridge IPs,
#      and metrics ports. Before this file, chaos.sh, ci-cluster.sh (§7c),
#      and smoke-load.sh each hardcoded these constants separately.
#      group_vars is the canonical source; `make check-contract` checks the
#      other mirrors against it.
#   2. topology_load(): builds the full generated node list (NODES plus
#      NODE_IP, NODE_ROLE, NODE_TIER). It parses group_vars node_classes
#      with the same no-PyYAML regex that ci-cluster.sh and smoke-load.sh
#      each used to carry privately.
#
# This file does not depend on lib.sh, and defines no log() or fail().
# topology_load returns non-zero when group_vars is missing or fails to
# parse. Callers decide what that means.

# Resolve group_vars using this file's own location. A sourcing script may
# run from any working directory. ci-cluster.sh changes to deploy/cluster,
# but chaos.sh does not.
_LIB_TOPOLOGY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPOLOGY_GROUP_VARS="${TOPOLOGY_GROUP_VARS:-${_LIB_TOPOLOGY_DIR}/../ansible/group_vars/all.yml}"

# --- executor tier (node_classes: executor count=3, ip_start=41) -------------
# EXECUTOR_NODES may arrive from the environment as a space-separated
# string. This is a documented setting for smoke-load.sh, since bash
# cannot export arrays. Both forms exist on purpose: the array form
# supports index-paired iteration (chaos.sh probes pair EXECUTOR_NODES[i]
# with EXECUTOR_IPS[i]), and the _STR form supports display and
# word-split consumers.
if [ -n "${EXECUTOR_NODES:-}" ]; then
  # shellcheck disable=SC2128  # deliberate: env string -> array
  read -r -a EXECUTOR_NODES <<<"${EXECUTOR_NODES}"
else
  EXECUTOR_NODES=(kardamom-executor-0 kardamom-executor-1 kardamom-executor-2)
fi
EXECUTOR_NODES_STR="${EXECUTOR_NODES[*]}"
# Executor node bridge IPs (executor ip_start=41), index-paired with
# EXECUTOR_NODES. The executor exporter binds to 0.0.0.0 so the chaos suite
# can probe it over the cluster bridge. See executor.nomad.hcl.
if [ -n "${EXECUTOR_IPS:-}" ]; then
  # shellcheck disable=SC2128  # deliberate: env string -> array
  read -r -a EXECUTOR_IPS <<<"${EXECUTOR_IPS}"
else
  EXECUTOR_IPS=(192.168.56.41 192.168.56.42 192.168.56.43)
fi
EXECUTOR_PORT="${EXECUTOR_PORT:-9004}"
EXECUTOR_METRICS_PORT="${EXECUTOR_METRICS_PORT:-${EXECUTOR_PORT}}"
# The executor's block gauge only goes up
# (crates/executor/src/metrics.rs: kardamom_executor_block_number, set for
# each committed block in actor.rs). This is the pipeline-progress signal
# for cluster mode: the cluster commits blocks out its egress, the executor
# applies them, and this gauge ticks up.
EXECUTOR_BLOCK_METRIC="${EXECUTOR_BLOCK_METRIC:-kardamom_executor_block_number}"

# --- sealer metrics (re-exported) --------------------------------------------
# The Java Aeron Cluster node has no Prometheus endpoint. The executors
# re-export the sealer's boundary and block stream from cluster egress on
# :9004. So the script reads sealer metrics from an executor node.
SEALER_NODE="${SEALER_NODE:-kardamom-executor-0}"

# --- validator (aux tier: aux count=1, ip_start=61) --------------------------
# The validator is the only service scraped on the aux node. Its metrics
# are on port 9006 (the executor holds port 9004 elsewhere).
VALIDATOR_NODE="${VALIDATOR_NODE:-kardamom-aux-0}"
VALIDATOR_PORT="${VALIDATOR_PORT:-9006}"
# Candidate list form. ci-cluster.sh's §7c liveness probe iterates this list.
VALIDATOR_NODES=("${VALIDATOR_NODE}")

# --- ingress (active/active: ingress count=2, ip_start=31) -------------------
# Ingress binds its exporter on loopback, so scrapes go through docker exec.
INGRESS_NODES=(kardamom-ingress-0 kardamom-ingress-1)
INGRESS_PORT="${INGRESS_PORT:-9006}"

# --- clustered sealer --------------------------------------------------------
# The Nomad task name inside the `cluster` job (cluster.nomad.hcl names the
# task "cluster"). This lets `inject_hard kardamom-sealer-<id> cluster`
# match its inner container.
CLUSTER_TASK="cluster"

# Declared empty here, so `${NODE_IP[x]:-}` lookups are safe under
# `set -u`, before or without a call to topology_load.
declare -a NODES=()
declare -A NODE_IP=() NODE_ROLE=() NODE_TIER=()

# Populate NODES and NODE_IP/NODE_ROLE/NODE_TIER from group_vars
# node_classes. This function uses `declare -g` throughout. The function
# body runs in its own scope, but the sourcing script's cleanup and
# diagnostics code reads these arrays afterward (ci-cluster.sh's teardown
# trap iterates NODES and indexes NODE_ROLE).
#   NODES     : bare instance names (<class>-<i>), in deploy order
#   NODE_IP   : keyed both ways — bare name and kardamom-<name> container
#               name (ci-cluster.sh indexes by bare name, smoke-load.sh by
#               prefixed name)
#   NODE_ROLE / NODE_TIER : same dual keying
# Returns 1 if group_vars is missing, or yields no nodes.
topology_load() {
  local group_vars="${1:-${TOPOLOGY_GROUP_VARS}}"
  declare -ga NODES=()
  declare -gA NODE_IP=() NODE_ROLE=() NODE_TIER=()
  [ -f "${group_vars}" ] || return 1
  local _name _ip _role _tier
  while read -r _name _ip _role _tier; do
    [ -z "${_name}" ] && continue
    NODES+=("${_name}")
    NODE_IP[${_name}]="${_ip}";     NODE_IP[kardamom-${_name}]="${_ip}"
    NODE_ROLE[${_name}]="${_role}"; NODE_ROLE[kardamom-${_name}]="${_role}"
    NODE_TIER[${_name}]="${_tier}"; NODE_TIER[kardamom-${_name}]="${_tier}"
  done < <(python3 - "${group_vars}" <<'PY'
# Parse node_classes with a plain regex. This avoids a PyYAML dependency,
# the same approach scripts/check-contract.py uses, so it runs anywhere
# python3 does. Class line:
#   <name>: { count: N, ip_start: M, tier: T }
import re, sys
text = open(sys.argv[1]).read()
pref = re.search(r'^ip_prefix:\s*"([\d.]+)"', text, re.M).group(1)
for m in re.finditer(
        r'^\s{2}(\w+):\s*\{\s*count:\s*(\d+),\s*ip_start:\s*(\d+),\s*tier:\s*(\w+)',
        text, re.M):
    cls, count, ip_start, tier = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
    for i in range(count):
        print(f"{cls}-{i} {pref}.{ip_start + i} {cls} {tier}")
PY
  )
  [ "${#NODES[@]}" -gt 0 ]
}
