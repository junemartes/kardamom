#!/usr/bin/env bash
# =============================================================================
# chaos-iter.sh — runs one local iteration of the CI chaos suite (untracked helper).
#
# CI runs 4 chaos shards. Each shard runs against a fresh cluster (14 cases
# total). The chaos accounts are #7 through #15, and a fresh chain holds at
# most 9 accounts. So a full local pass needs two fresh chains. Half `a` runs
# the chaos-executor and chaos-ingress shard cases. Half `b` runs
# chaos-sequencer and chaos-cluster. Each half purges every Nomad job, wipes
# all durable cluster state for a fresh chain with nonce-0 accounts, then
# runs the unmodified ansible/run.yml with the CI settings: bring-up, smoke
# gate, chaos cases, ingress-churn re-smoke, and validator verdict.
#
# Run inside the orchestrator: bash /work/deploy/cluster/scripts/chaos-iter.sh a
# =============================================================================
set -euo pipefail

HALF="${1:?usage: chaos-iter.sh <a|b>}"
export NOMAD_ADDR="http://192.168.56.10:4646"

case "${HALF}" in
  # The halves track the CI shards' case lists. cluster-e2e.yml is the
  # source of truth. The retention-overrun* cases need
  # KARDAMOM_CLUSTER_RETENTION deployed small. They are opt-in here, not
  # part of a half.
  a) CASES="graceful-executor hard-executor node-failure-executor state-checkpoint-restore replay-window-resync graceful-ingress hard-ingress archive-driver-loss archive-tx-data-wipe archive-corruption" ;;
  b) CASES="graceful-sequencer hard-sequencer sequencer-replica-kill sequencer-lapse validator-lapse validator-join cluster-leader-kill cluster-follower-kill cluster-member-rejoin cluster-quorum-loss-recover" ;;
  *) echo "unknown half ${HALF}" >&2; exit 1 ;;
esac

echo "==> [iter] resetting cluster and testing half ${HALF}"
RUN_LOAD=0 RUN_CHAOS=1 \
  CHAOS_CASES="${CASES}" \
  CHAOS_TPS=200 CHAOS_CASE_S=120 CHAOS_RESCHEDULE_SLO_S=200 CHAOS_LEADER_SLO_S=45 \
  KEEP=1 REGISTRY_PUSH_NODE=control-0 \
  ansible-playbook -i localhost, /work/deploy/cluster/ansible/run.yml -e cluster_run_operation=reset
