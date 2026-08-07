# shellcheck shell=bash
# =============================================================================
# lib-topology.sh — the node-class model, in one place.
# =============================================================================
# SOURCED (never executed) by chaos.sh, ci-cluster.sh and smoke-load.sh.
# Two layers:
#   1. Static mirrors of ansible/group_vars/all.yml (node names, bridge IPs,
#      metrics ports) — the constants previously hardcoded separately in
#      chaos.sh, ci-cluster.sh (§7c) and smoke-load.sh. group_vars is the
#      canonical contract; `make check-contract` guards the other mirrors.
#   2. topology_load(): the full generated node list (NODES + NODE_IP/
#      NODE_ROLE/NODE_TIER), parsed from group_vars node_classes with the same
#      no-PyYAML regex ci-cluster.sh and smoke-load.sh each carried privately.
#
# No dependency on lib.sh (no log/fail): topology_load returns non-zero on a
# missing/unparsable group_vars and callers own the verdict.

# Resolve group_vars via THIS file's location (sourcing scripts may run from
# any cwd; ci-cluster.sh cd's to deploy/cluster but chaos.sh does not).
_LIB_TOPOLOGY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPOLOGY_GROUP_VARS="${TOPOLOGY_GROUP_VARS:-${_LIB_TOPOLOGY_DIR}/../ansible/group_vars/all.yml}"

# --- executor tier (node_classes: executor count=3, ip_start=41) -------------
# EXECUTOR_NODES may arrive from the environment as a space-separated STRING
# (smoke-load.sh's documented knob — bash cannot export arrays). Both forms
# are provided deliberately: the ARRAY for index-paired iteration (chaos.sh
# probes pair EXECUTOR_NODES[i] with EXECUTOR_IPS[i]) and the _STR form for
# display/word-split consumers.
if [ -n "${EXECUTOR_NODES:-}" ]; then
  # shellcheck disable=SC2128  # deliberate: env string -> array
  read -r -a EXECUTOR_NODES <<<"${EXECUTOR_NODES}"
else
  EXECUTOR_NODES=(kardamom-executor-0 kardamom-executor-1 kardamom-executor-2)
fi
EXECUTOR_NODES_STR="${EXECUTOR_NODES[*]}"
# Executor node bridge IPs (executor ip_start=41), index-paired with
# EXECUTOR_NODES. The executor exporter binds 0.0.0.0 precisely so the chaos
# suite can probe it over the cluster bridge (see executor.nomad.hcl).
if [ -n "${EXECUTOR_IPS:-}" ]; then
  # shellcheck disable=SC2128  # deliberate: env string -> array
  read -r -a EXECUTOR_IPS <<<"${EXECUTOR_IPS}"
else
  EXECUTOR_IPS=(192.168.56.41 192.168.56.42 192.168.56.43)
fi
EXECUTOR_PORT="${EXECUTOR_PORT:-9004}"
EXECUTOR_METRICS_PORT="${EXECUTOR_METRICS_PORT:-${EXECUTOR_PORT}}"
# The executor's monotonically-advancing block gauge (crates/executor/src/
# metrics.rs: kardamom_executor_block_number — set per committed block in
# actor.rs). The cluster-mode pipeline-progress signal (the cluster commits
# blocks out its egress, the executor applies them, this gauge ticks up).
EXECUTOR_BLOCK_METRIC="${EXECUTOR_BLOCK_METRIC:-kardamom_executor_block_number}"

# --- sealer metrics (re-exported) --------------------------------------------
# The Java Aeron Cluster node has NO Prometheus endpoint; the executors
# re-export the sealer's boundary/block stream from cluster egress on :9004,
# so sealer metrics are read from an EXECUTOR node.
SEALER_NODE="${SEALER_NODE:-kardamom-executor-0}"

# --- validator (aux tier: aux count=1, ip_start=61) --------------------------
# The validator is the sole service scraped on the aux node; metrics on 9006
# (executor holds 9004 elsewhere).
VALIDATOR_NODE="${VALIDATOR_NODE:-kardamom-aux-0}"
VALIDATOR_PORT="${VALIDATOR_PORT:-9006}"
# Candidate list form (ci-cluster.sh's §7c liveness probe iterates candidates).
VALIDATOR_NODES=("${VALIDATOR_NODE}")

# --- ingress (active/active: ingress count=2, ip_start=31) -------------------
# Ingress binds its exporter on loopback, so scrapes go through docker exec.
INGRESS_NODES=(kardamom-ingress-0 kardamom-ingress-1)
INGRESS_PORT="${INGRESS_PORT:-9006}"

# --- clustered sealer --------------------------------------------------------
# The Nomad task name inside the `cluster` job (cluster.nomad.hcl: task
# "cluster"), so inject_hard kardamom-sealer-<id> cluster matches its inner
# container.
CLUSTER_TASK="cluster"

# Empty-but-declared so `${NODE_IP[x]:-}` lookups are safe under `set -u`
# before (or without) topology_load.
declare -a NODES=()
declare -A NODE_IP=() NODE_ROLE=() NODE_TIER=()

# Populate NODES + NODE_IP/NODE_ROLE/NODE_TIER from group_vars node_classes.
# `declare -g` throughout: this runs inside a function but the arrays are read
# by the SOURCING script's cleanup/diagnostics (ci-cluster.sh's teardown trap
# iterates NODES and indexes NODE_ROLE).
#   NODES     : bare instance names (<class>-<i>), deploy order
#   NODE_IP   : keyed BOTH ways — bare name AND kardamom-<name> container name
#               (ci-cluster.sh indexes bare, smoke-load.sh indexes prefixed)
#   NODE_ROLE / NODE_TIER : same dual keying
# Returns 1 if group_vars is missing or yields no nodes.
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
# Parse node_classes with a plain regex (no PyYAML dependency — same approach
# as scripts/check-contract.py, so this runs anywhere python3 does). Class line:
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
