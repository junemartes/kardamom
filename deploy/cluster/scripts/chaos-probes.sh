# shellcheck shell=bash
# =============================================================================
# chaos-probes.sh — read-only probes for the chaos suite.
# =============================================================================
# This file is sourced into chaos.sh's shell, never run as a child
# process. The probes read the topology constants from lib-topology.sh,
# and scrape metrics through lib-metrics.sh. This file must not install
# traps; chaos.sh owns the single EXIT trap.
#
# These matches on captured command output must be SIGPIPE-safe. Never
# use `echo "${big}" | grep -q` or `producer | grep -q` for asserts in
# this suite. `grep -q` exits at the first match. Once the producer's
# output passes the pipe buffer (64KB), the producer gets a SIGPIPE
# (141). Under `set -o pipefail`, this discards the successful match.
# This has happened for real: the retention-overrun asserts reported
# "consumer never hit REPLAY_UNAVAILABLE", while the refusal was sitting
# in the very alloc logs they grepped. The repeated `echo: write error:
# Broken pipe` lines in the CI log were each match being thrown away.
# Negated checks are worse: a SIGPIPE'd match reads as absence. Pure-bash
# matching has no pipe to break.
has_line()  { [[ "$1" == *"$2"* ]]; }   # fixed substring (no regex chars)
has_match() { [[ "$1" =~ $2 ]]; }       # POSIX ERE

# --- inner-container helpers -------------------------------------------------

# Get the first running inner (Nomad docker-driver) container on a node,
# whose name starts with a prefix. This replaces the
# `docker exec <node> docker ps | grep -m1` pattern, which used to
# repeat about 11 times. The match is case-insensitive and anchored at
# the start, since task containers are named <task>-<alloc-id>. Prints
# the name, or empty if none match.
inner_container() { # <node> <name-prefix>
  timeout 15 docker exec "$1" sh -c \
    'docker ps --format "{{.Names}}" | grep -im1 "^'"$2"'"' 2>/dev/null || true
}

# Get a container's StartedAt timestamp on a node. This is the signal
# that tells a newborn container from a survivor. A container name
# survives an in-place task restart (task-<alloc-id>), so matching names
# proves nothing. A Nomad restart always creates a new generation with a
# new StartedAt. This is the lesson from the sequencer-lapse and
# validator-lapse cases. Empty on failure.
container_started_at() { # <node> <inner-container>
  timeout 15 docker exec "$1" docker inspect -f '{{.State.StartedAt}}' "$2" \
    2>/dev/null || true
}

# --- executor-tier probes ----------------------------------------------------

# Fetch one executor node's /metrics body. Tries the bridge IP directly
# first, then falls back to docker exec. See fetch_metrics in
# lib-metrics.sh for the reasoning.
# $1 = index into EXECUTOR_NODES.
exec_metrics() {
  fetch_metrics "${EXECUTOR_IPS[$1]}" "${EXECUTOR_NODES[$1]}" "${EXECUTOR_PORT}"
}

# Sealer boundary-counter probe. This reads the sealer's boundary
# stream, as re-exported by the executors from cluster egress (the Java
# cluster node itself has no Prometheus endpoint). It ticks about 4
# times a second while the sealer is alive, a finer liveness signal than
# the block gauge. It tries each executor node, like executor_progress.
sealer_boundaries() {
  # Take the max across all responding executors, not the first
  # responder. This matches the reasoning in executor_progress below. A
  # replica that restarted, or is replaying and catching up, can fairly
  # report a low or frozen counter while its peers and the pipeline are
  # fine. Pinning the probe to that replica would read as a pipeline
  # stall when nothing is wrong.
  local i v best=""
  for i in "${!EXECUTOR_NODES[@]}"; do
    v="$(prom_value "$(exec_metrics "${i}" || true)" \
      kardamom_sealer_boundaries_emitted_total first)"
    [ -n "${v}" ] && { [ -z "${best}" ] || [ "${v}" -gt "${best}" ]; } && best="${v}"
  done
  [ -n "${best}" ] && { printf '%s' "${best}"; return 0; }
  return 1
}

# Cluster-mode progress probe. Reads the most recently committed block
# number, as seen by the executor (EXECUTOR_BLOCK_METRIC gauge on
# :9004). The Java cluster node has no Prometheus endpoint, so this
# probe measures pipeline liveness at the executor. The executor applies
# the blocks the cluster commits out its egress, so its block gauge
# advancing means the cluster is making progress. Prints the integer
# value; empty (or a nonzero return) if every scrape failed.
executor_progress() {
  # Take the max across all responding executors, not the first
  # responder. A replica that a chaos case restarted can fairly report
  # gauge 0, or a low block, while it replays and recovers, even though
  # its peers and the pipeline are fine. Pinning the probe to that
  # replica would read as "pipeline not progressing" when nothing is
  # wrong, as seen when node-failure-executor runs right after
  # hard-executor restarts executor-0.
  local i v best=""
  for i in "${!EXECUTOR_NODES[@]}"; do
    v="$(prom_value "$(exec_metrics "${i}" || true)" "${EXECUTOR_BLOCK_METRIC}" first)"
    [ -n "${v}" ] && { [ -z "${best}" ] || [ "${v}" -gt "${best}" ]; } && best="${v}"
  done
  [ -n "${best}" ] && { printf '%s' "${best}"; return 0; }
  return 1
}

# --- ingress probe -----------------------------------------------------------

# Ingress submit counter (kardamom_ingress_tx_received_total), summed
# across the active/active ingress nodes. This is the "is load actually
# flowing?" signal for the injection gate in run_case. Ingress binds its
# exporter on loopback, so this goes through docker exec. Prints the
# sum, or fails if no ingress answered.
ingress_received() {
  local n v total=0 got=0
  for n in "${INGRESS_NODES[@]}"; do
    v="$(prom_value "$(fetch_metrics '' "${n}" "${INGRESS_PORT}" || true)" \
      kardamom_ingress_tx_received_total sum)"
    [ -n "${v}" ] && { total=$(( total + v )); got=1; }
  done
  [ "${got}" -eq 1 ] && { printf '%s' "${total}"; return 0; }
  return 1
}

# --- validator probe ---------------------------------------------------------

val_metric() { # <metric-name> -> integer (empty on scrape failure)
  # Keep the `|| true` on the capture. It is load-bearing. Under
  # `set -euo pipefail`, a failed curl would stop the whole script
  # mid-case with no fail() message. The validator's exporter can stall
  # for more than 5 seconds right after `docker unpause`, while it works
  # through the lapse backlog. An empty result is the documented
  # contract, and callers supply a default.
  local body
  body="$(fetch_metrics "" "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" || true)"
  prom_value "${body}" "$1" first
}

# val_metric_req is the required-scrape companion to val_metric.
# val_metric's empty-on-failure contract fits progress polling, but
# "divergence == 0" style asserts were reading a failed scrape as 0. The
# assert passed exactly when the validator was too wedged to answer.
# This function retries, then fails the case loudly.
val_metric_req() { # <metric-name> <why>
  local v i
  for i in 1 2 3 4 5; do
    v="$(val_metric "$1")"
    [ -n "${v}" ] && { printf '%s' "${v}"; return 0; }
    # An absent metric can mean a dead exporter, or a metric never
    # incremented. metrics-rs counters do not export until first
    # incremented, so a healthy zero-divergence validator has no
    # divergence_total line at all. If the always-present canary
    # scrapes, the exporter is alive, and the absent counter really is
    # 0.
    if [ -n "$(val_metric validator_committed_block)" ]; then
      printf '0'
      return 0
    fi
    sleep 3
  done
  fail "validator exporter unscrapeable after 5 tries — refusing to treat a dead exporter as 0 ($2)"
}

# --- sequencer shard-0 replica probes ----------------------------------------

seqa_metric() { # <metric-name> -> integer sum across label lines (empty on scrape failure)
  # seq-a runs on node-0. The sequencer IP lane starts at .21, and seq-a
  # exposes metrics on :9001. This tries the bridge IP first, then falls
  # back to docker exec, mirroring assert_replica_healthy's probe.
  local body
  body="$(fetch_metrics 192.168.56.21 kardamom-sequencer-0 9001 || true)"
  prom_value "${body}" "$1" sum
}
seqb_twin_metric() { # <metric-name> — shard 0's replica B: seq-b on node-1 (.22:9011)
  local body
  body="$(fetch_metrics 192.168.56.22 kardamom-sequencer-1 9011 || true)"
  prom_value "${body}" "$1" sum
}
