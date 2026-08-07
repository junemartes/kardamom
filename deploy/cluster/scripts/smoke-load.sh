#!/usr/bin/env bash
# =============================================================================
# smoke-load.sh — EXHAUSTIVE load smoke for the kardamom cluster.
# =============================================================================
#
# The basic smoke (scripts/smoke.sh) sends a SINGLE tx and checks one receipt.
# That is too weak to catch the sustained-load freezes we are chasing. This
# script instead drives a SUSTAINED stream of transactions through ingress for
# a configurable duration/rate and asserts two properties the cluster must hold
# under load:
#
#   1. MUST-DELIVER  — every submitted tx gets a receipt back within a timeout.
#                      (This is the durability/quorum property; a frozen or
#                      lossy pipeline shows up as missing receipts.)
#
#   2. KEEP-PACE     — the executor(s) keep up with the sealer: the
#                      executor-vs-sealer block-number gap stays bounded AND
#                      every executor's block number STRICTLY ADVANCES over the
#                      run (no frozen executor).
#
# It prints a clear summary (submitted / receipted / missing, receipt-latency
# p50/p99, per-executor final block gap) and exits non-zero on ANY failure.
#
# ---------------------------------------------------------------------------
# HOW IT TALKS TO THE CLUSTER
# ---------------------------------------------------------------------------
#  * Transactions: pre-signed OFFLINE with foundry `cast mktx` (explicit
#    --nonce/--chain/--gas-price/--gas-limit, no RPC round-trip), then
#    submitted to ingress via `eth_sendRawTransaction` over curl, and receipts
#    polled via `eth_getTransactionReceipt`. We pre-sign rather than use
#    `cast send` because ingress does NOT implement `eth_getTransactionCount`
#    (it returns an error — see crates/ingress/src/json_rpc.rs), so cast's
#    auto-nonce fill would fail. Nonces are therefore managed ENTIRELY locally:
#    sequential per sender, starting at SMOKE_NONCE_START (0 on a fresh chain).
#
#  * Throughput: spread across multiple sender accounts (the 16 well-known
#    Anvil/Hardhat test-mnemonic accounts, all prefunded in
#    config/genesis/dev.toml). More senders => more in-flight nonces => higher
#    achievable TPS without nonce-gap stalls.
#
#  * Metrics: the executor/sealer Prometheus exporters bind 127.0.0.1 INSIDE
#    their service container, which runs with host networking inside the node
#    container (Nomad docker driver / DinD). So they are reachable from the
#    node container's loopback. We scrape them exactly the way smoke.sh /
#    ci-cluster.sh reach nodes: `docker exec kardamom-<node> curl 127.0.0.1:<port>`.
#    Defaults (from the binaries' --metrics-addr defaults):
#       executor  kardamom-executor-0  127.0.0.1:9004  kardamom_executor_block_number
#       sealer    kardamom-executor-0  127.0.0.1:9004  kardamom_sealer_block_number
#    The clustered sealer (Java Aeron Cluster) has no Prometheus endpoint;
#    executors re-export its boundary stream from cluster egress, so the
#    sealer block metric is read from an executor node.
#
# ---------------------------------------------------------------------------
# ENV KNOBS (all optional; sane defaults)
# ---------------------------------------------------------------------------
#   RPC_URL            ingress JSON-RPC          (default http://192.168.56.31:8545)
#   CHAIN_ID           L2 chain id               (default 412346)
#   SMOKE_DURATION_S   send for this many seconds(default 60)
#   SMOKE_TPS          target tx/sec             (default 5)
#   SMOKE_TX_COUNT     fixed tx count; if set it OVERRIDES duration*tps
#   SMOKE_SENDERS      number of sender accounts (default 4, max 16)
#   SMOKE_SENDER_OFFSET  index of the first sender account in the 16-key table
#                        (default 0). Set to 1 to reserve account #0 for the
#                        single-tx smoke (scripts/smoke.sh) so their nonces do
#                        not collide when both run against the same chain.
#   SMOKE_NONCE_START  first nonce per sender    (default 0 — fresh chain)
#   SMOKE_RECEIPT_TIMEOUT_S  per-run receipt drain timeout (default 90)
#   SMOKE_MAX_GAP      max allowed executor-behind-sealer block gap (default 5)
#   GAS_PRICE          legacy gas price (wei)    (default 1000000000)
#   VALUE              wei per transfer          (default 1)
#   TO                 sink address              (default 0x...dEaD)
#
#   Node/metrics overrides (verify against a live cluster — see RETURN notes):
#   EXECUTOR_NODES     space-separated node-container names that run an executor
#                                  (default "kardamom-executor-0 ...-1 ...-2")
#   SEALER_NODE        node-container to read the re-exported sealer metric
#                                  from (default kardamom-executor-0)
#   EXECUTOR_METRICS_PORT  (default 9004)
#   SEALER_METRICS_PORT    (default 9004 — the executor exporter)
#   EXECUTOR_BLOCK_METRIC  (default kardamom_executor_block_number)
#   SEALER_BLOCK_METRIC    (default kardamom_sealer_block_number)
#   METRICS_VIA_DOCKER 1 => scrape via `docker exec <node> curl 127.0.0.1:port`
#                      0 => scrape <node-ip>:port directly over the network
#                           (only works if the exporter is bound to a routable
#                           addr; by default it is NOT — it binds 127.0.0.1).
#                      (default 1)
#
# Usage:
#   deploy/cluster/scripts/smoke-load.sh
#   SMOKE_DURATION_S=120 SMOKE_TPS=20 SMOKE_SENDERS=8 deploy/cluster/scripts/smoke-load.sh
#   SMOKE_TX_COUNT=500 deploy/cluster/scripts/smoke-load.sh
# =============================================================================
set -euo pipefail

# This script uses associative arrays (declare -A), which need bash >= 4.
# The CI orchestrator (the Linux runner that runs ci-cluster.sh / has cast +
# docker + nomad) ships bash 5.x, so this is satisfied there. macOS' stock
# /bin/bash is 3.2 — if you run this locally on a Mac, use Homebrew bash
# (`brew install bash`). Fail fast with a clear message rather than a cryptic
# `declare: -A: invalid option` later.
if (( BASH_VERSINFO[0] < 4 )); then
  echo "ERROR: smoke-load.sh needs bash >= 4 (found ${BASH_VERSION})." >&2
  echo "       On macOS: 'brew install bash' then run with that interpreter." >&2
  exit 1
fi

log()  { echo "==> $*"; }
warn() { echo "WARN: $*" >&2; }
fail() { echo "RESULT: FAIL — $*" >&2; exit 1; }

# Shared node-class model + Prometheus scrape/parse helpers. Neither lib
# defines log/fail — the "RESULT: FAIL" contract above stays this script's.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/cluster/scripts/lib-topology.sh
source "${SCRIPT_DIR}/lib-topology.sh"
# shellcheck source=deploy/cluster/scripts/lib-metrics.sh
source "${SCRIPT_DIR}/lib-metrics.sh"

# --- config -----------------------------------------------------------------
RPC_URL="${RPC_URL:-http://192.168.56.31:8545}"
CHAIN_ID="${CHAIN_ID:-412346}"
SMOKE_DURATION_S="${SMOKE_DURATION_S:-60}"
SMOKE_TPS="${SMOKE_TPS:-5}"
SMOKE_TX_COUNT="${SMOKE_TX_COUNT:-}"
SMOKE_SENDERS="${SMOKE_SENDERS:-4}"
SMOKE_SENDER_OFFSET="${SMOKE_SENDER_OFFSET:-0}"
SMOKE_NONCE_START="${SMOKE_NONCE_START:-0}"
SMOKE_RECEIPT_TIMEOUT_S="${SMOKE_RECEIPT_TIMEOUT_S:-90}"
SMOKE_MAX_GAP="${SMOKE_MAX_GAP:-5}"
GAS_PRICE="${GAS_PRICE:-1000000000}"
VALUE="${VALUE:-1}"
TO="${TO:-0x000000000000000000000000000000000000dEaD}"

# EXECUTOR_NODES (env-string override honored), SEALER_NODE,
# EXECUTOR_METRICS_PORT and EXECUTOR_BLOCK_METRIC come from lib-topology.sh.
SEALER_METRICS_PORT="${SEALER_METRICS_PORT:-9004}"
SEALER_BLOCK_METRIC="${SEALER_BLOCK_METRIC:-kardamom_sealer_block_number}"
METRICS_VIA_DOCKER="${METRICS_VIA_DOCKER:-1}"

# 16 well-known test-mnemonic accounts ("test test ... junk"), all prefunded
# 1000 ETH in config/genesis/dev.toml. Index = account number. Public dev keys.
SENDER_KEYS=(
  0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
  0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d
  0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a
  0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6
  0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a
  0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba
  0x92db14e403b83dfe3df233f83dfa3a0d7096f21ca9b0d6d6b8d88b2b4ec1564e
  0x4bbbf85ce3377467afe5d46f804f221813b2bb87f24d81f60f1fcdbf7cbf4356
  0xdbda1821b80551c9d65939329250298aa3472ba22feea921c0cf5d620ea67b97
  0x2a871d0798f97d79848a013d4936a73bf4cc922c825d33c1cf7073dff6d409c6
  0xf214f2b2cd398c806f84e317254e0f0b801d0643303237d97a22a48e01628897
  0x701b615bbdfb9de65240bc28bd21bbc0d996645a3dd57e7b12bc2bdf6f192c82
  0xa267530f49f8280200edf313ee7af6b827f2a8bce2897751d06a843f644967b1
  0x47c99abed3324a2707c28affff1267e45918ec8c3f20b8aa892e8b065d2942dd
  0xc526ee95bf44d8fc405a158bb884d9d1238d99f0612e9f33d006bb0789009aaa
  0x8166f546bab6da521a8369cab06c5d2b9e46670292d85c875ee9ec20e84ffb61
)

# Validate the sender offset and clamp the sender count to [1, 16], keeping the
# offset+count window inside the 16-key table so senders never wrap back onto a
# reserved low account (e.g. account #0, reserved for scripts/smoke.sh).
# A NEGATIVE offset is a hard error rather than a clamp: silently clamping to 0
# would land the load back on exactly the reserved account the knob exists to
# avoid, reintroducing the nonce collision with no diagnostic.
if (( SMOKE_SENDER_OFFSET < 0 )); then
  fail "SMOKE_SENDER_OFFSET=${SMOKE_SENDER_OFFSET} is negative (offset 0 is the reserved smoke account; pass 0..$(( ${#SENDER_KEYS[@]} - 1 )))"
fi
if (( SMOKE_SENDER_OFFSET > ${#SENDER_KEYS[@]} - 1 )); then
  warn "SMOKE_SENDER_OFFSET=${SMOKE_SENDER_OFFSET} exceeds the key table; clamping to $(( ${#SENDER_KEYS[@]} - 1 ))"
  SMOKE_SENDER_OFFSET=$(( ${#SENDER_KEYS[@]} - 1 ))
fi
if (( SMOKE_SENDERS < 1 )); then SMOKE_SENDERS=1; fi
if (( SMOKE_SENDERS > ${#SENDER_KEYS[@]} - SMOKE_SENDER_OFFSET )); then
  warn "SMOKE_SENDERS=${SMOKE_SENDERS} does not fit above offset ${SMOKE_SENDER_OFFSET}; clamping to $(( ${#SENDER_KEYS[@]} - SMOKE_SENDER_OFFSET ))"
  SMOKE_SENDERS=$(( ${#SENDER_KEYS[@]} - SMOKE_SENDER_OFFSET ))
fi

# Resolve target tx count.
if [[ -n "${SMOKE_TX_COUNT}" ]]; then
  TX_TOTAL="${SMOKE_TX_COUNT}"
else
  TX_TOTAL=$(( SMOKE_DURATION_S * SMOKE_TPS ))
fi
if (( TX_TOTAL < 1 )); then TX_TOTAL=1; fi

WORK="$(mktemp -d)"
# shellcheck disable=SC2329  # invoked indirectly by the EXIT trap below.
cleanup() { rm -rf "${WORK}"; }
trap cleanup EXIT

# --- preflight --------------------------------------------------------------
command -v cast >/dev/null 2>&1 || fail "foundry 'cast' not on PATH (needed to sign txs)"
command -v curl >/dev/null 2>&1 || fail "'curl' not on PATH (needed to submit txs)"
HAVE_JQ=0; command -v jq >/dev/null 2>&1 && HAVE_JQ=1

cat <<EOF
==> smoke-load against ingress ${RPC_URL} (chain-id ${CHAIN_ID})
    senders=${SMOKE_SENDERS}  nonce-start=${SMOKE_NONCE_START}
    target tx=${TX_TOTAL}  (duration=${SMOKE_DURATION_S}s tps=${SMOKE_TPS}${SMOKE_TX_COUNT:+  tx-count override=${SMOKE_TX_COUNT}})
    receipt-timeout=${SMOKE_RECEIPT_TIMEOUT_S}s  max-gap=${SMOKE_MAX_GAP}
    executors=[${EXECUTOR_NODES_STR}] sealer=[${SEALER_NODE}] via-docker=${METRICS_VIA_DOCKER}
EOF

# ---------------------------------------------------------------------------
# JSON-RPC helpers
# ---------------------------------------------------------------------------

# Extract the first 0x-hex "result" value from a JSON-RPC response, or empty.
rpc_result_hex() {
  if (( HAVE_JQ )); then
    printf '%s' "$1" | jq -r 'if type=="array" then .[].result else .result end' 2>/dev/null \
      | grep -E '^0x[0-9a-fA-F]+$' | head -1 || true
  else
    printf '%s' "$1" | grep -oE '"result"[ ]*:[ ]*"0x[0-9a-fA-F]+"' \
      | grep -oE '0x[0-9a-fA-F]+' | head -1 || true
  fi
}

# eth_blockNumber -> decimal, or empty on failure. Liveness signal for ingress.
ingress_block_number() {
  local resp hex
  resp="$(curl -fsS --max-time 5 -X POST "${RPC_URL}" \
    -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' 2>/dev/null || true)"
  hex="$(rpc_result_hex "${resp}")"
  [[ -n "${hex}" ]] && printf '%d' "$(( hex ))" || true
}

# ---------------------------------------------------------------------------
# Metrics scraping
# ---------------------------------------------------------------------------

# Fetch a node's /metrics body. $1=node-container $2=ip $3=port
# METRICS_VIA_DOCKER keeps its documented either/or semantics (1 = docker-exec
# ONLY, 0 = direct bridge ONLY — no silent fallback to the other transport,
# which could scrape the wrong endpoint and surface as a bogus verdict);
# fetch_metrics (lib-metrics.sh) skips whichever leg gets an empty argument.
scrape_metrics() {
  local node="$1" ip="$2" port="$3"
  if [[ "${METRICS_VIA_DOCKER}" == "1" ]]; then
    fetch_metrics "" "${node}" "${port}" || true
  else
    fetch_metrics "${ip}" "" "${port}" || true
  fi
}

# Read a node's block-number metric as an integer (empty if unavailable).
# prom_value (lib-metrics.sh) int-truncates float/scientific gauge renderings.
# $1=node $2=ip $3=port $4=metric-name
read_block_metric() {
  local body
  body="$(scrape_metrics "$1" "$2" "$3")"
  [[ -z "${body}" ]] && { printf ''; return; }
  prom_value "${body}" "$4" first
}

# Node -> IP map, generated from the node-class model in group_vars/all.yml
# via lib-topology.sh's topology_load (the same single source of truth
# ci-cluster.sh materialises the cluster from: <class>-<i> gets
# ip_prefix.<ip_start+i>). Only needed when METRICS_VIA_DOCKER=0 (direct
# bridge scrapes); an unmappable node is a hard error there — falling back to
# 127.0.0.1 would scrape the WRONG host and surface as a bogus METRIC-MISSING.
if [[ "${METRICS_VIA_DOCKER}" != "1" ]]; then
  topology_load \
    || fail "METRICS_VIA_DOCKER=0 needs ${TOPOLOGY_GROUP_VARS} to derive node IPs"
fi
node_ip() {
  if [[ "${METRICS_VIA_DOCKER}" == "1" ]]; then
    echo "127.0.0.1"   # unused on the docker-exec path
  else
    [[ -n "${NODE_IP[$1]:-}" ]] || fail "no bridge IP known for node $1 (not in group_vars node_classes?)"
    echo "${NODE_IP[$1]}"
  fi
}

# ---------------------------------------------------------------------------
# 1. PRE-SIGN all txs offline (cast mktx, explicit nonce — no RPC round-trip).
# ---------------------------------------------------------------------------
# Round-robin senders; nonces are sequential PER sender starting at
# SMOKE_NONCE_START. raw_txs[i] holds the i-th signed raw tx, sent in order.
log "pre-signing ${TX_TOTAL} txs across ${SMOKE_SENDERS} sender(s)..."
declare -a RAW_TXS=()
declare -a SENDER_NONCE=()
for ((s=0; s<SMOKE_SENDERS; s++)); do SENDER_NONCE[s]=$SMOKE_NONCE_START; done

for ((i=0; i<TX_TOTAL; i++)); do
  s=$(( i % SMOKE_SENDERS ))
  nonce=${SENDER_NONCE[s]}
  # Logical sender `s` maps to key index `SMOKE_SENDER_OFFSET + s` so the run can
  # start above reserved accounts (per-sender nonces are tracked by `s`).
  raw="$(cast mktx "${TO}" \
        --value "${VALUE}" \
        --private-key "${SENDER_KEYS[$(( SMOKE_SENDER_OFFSET + s ))]}" \
        --nonce "${nonce}" \
        --chain "${CHAIN_ID}" \
        --legacy \
        --gas-price "${GAS_PRICE}" \
        --gas-limit 21000 2>/dev/null)" \
    || fail "cast mktx failed for sender #${s} nonce ${nonce}"
  [[ "${raw}" == 0x* ]] || fail "cast mktx produced non-hex output for sender #${s} nonce ${nonce}: ${raw}"
  RAW_TXS+=("${raw}")
  SENDER_NONCE[s]=$(( nonce + 1 ))
done
log "signed ${#RAW_TXS[@]} txs."

# ---------------------------------------------------------------------------
# 2. Capture baseline block numbers (for the keep-pace / not-frozen check).
# ---------------------------------------------------------------------------
log "capturing baseline block metrics..."
declare -A EXEC_BASE
for node in "${EXECUTOR_NODES[@]}"; do
  v="$(read_block_metric "${node}" "$(node_ip "${node}")" "${EXECUTOR_METRICS_PORT}" "${EXECUTOR_BLOCK_METRIC}")"
  EXEC_BASE["${node}"]="${v:-}"
  echo "    baseline ${EXECUTOR_BLOCK_METRIC}@${node} = ${v:-<unavailable>}"
done
SEALER_BASE="$(read_block_metric "${SEALER_NODE}" "$(node_ip "${SEALER_NODE}")" "${SEALER_METRICS_PORT}" "${SEALER_BLOCK_METRIC}")"
echo "    baseline ${SEALER_BLOCK_METRIC}@${SEALER_NODE} = ${SEALER_BASE:-<unavailable>}"
INGRESS_BASE_BLOCK="$(ingress_block_number)"
echo "    baseline ingress eth_blockNumber = ${INGRESS_BASE_BLOCK:-<unavailable>}"

# ---------------------------------------------------------------------------
# 3. SUBMIT the stream, pacing to ~SMOKE_TPS, recording submit time per tx.
# ---------------------------------------------------------------------------
# We submit one tx per "slot". With a fixed SMOKE_TX_COUNT (and no explicit
# duration) we still pace at SMOKE_TPS. submit_ts[i]/hash[i] track each tx;
# a per-tx interval keeps the offered load at the target rate.
log "submitting stream at ~${SMOKE_TPS} tx/s..."
declare -a TX_HASH=()
declare -a SUBMIT_TS=()
SUBMIT_OK=0
SUBMIT_ERR=0

# Per-tx pacing interval in seconds (float for sub-second rates).
INTERVAL="$(awk -v t="${SMOKE_TPS}" 'BEGIN{ if (t<=0) print 0; else printf "%.6f", 1.0/t }')"
RUN_START="$(date +%s.%N)"

submit_one() {
  local raw="$1" resp hash err
  resp="$(curl -fsS --max-time 8 -X POST "${RPC_URL}" \
    -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_sendRawTransaction\",\"params\":[\"${raw}\"]}" 2>/dev/null || true)"
  hash="$(rpc_result_hex "${resp}")"
  if [[ -n "${hash}" ]]; then
    printf '%s' "${hash}"
    return 0
  fi
  # Surface the first few errors, then go quiet to avoid log spam.
  if (( SUBMIT_ERR < 5 )); then
    err="$(printf '%s' "${resp}" | tr -d '\n' | cut -c1-200)"
    warn "submit error: ${err:-<no response>}"
  fi
  return 1
}

for ((i=0; i<${#RAW_TXS[@]}; i++)); do
  now="$(date +%s.%N)"
  SUBMIT_TS+=("${now}")
  if h="$(submit_one "${RAW_TXS[i]}")"; then
    TX_HASH+=("${h}")
    SUBMIT_OK=$(( SUBMIT_OK + 1 ))
  else
    TX_HASH+=("")          # keep index alignment with SUBMIT_TS
    SUBMIT_ERR=$(( SUBMIT_ERR + 1 ))
  fi

  # Progress heartbeat every ~50 txs.
  if (( (i+1) % 50 == 0 )); then
    log "  submitted $((i+1))/${#RAW_TXS[@]} (ok=${SUBMIT_OK} err=${SUBMIT_ERR})"
  fi

  # Pace: sleep until the next slot, accounting for elapsed time so we don't
  # drift slower than the target rate. Only when an interval is configured.
  if [[ "${INTERVAL}" != "0" ]] && (( i + 1 < ${#RAW_TXS[@]} )); then
    target="$(awk -v s="${RUN_START}" -v n="$((i+1))" -v iv="${INTERVAL}" 'BEGIN{printf "%.6f", s + n*iv}')"
    sleep_for="$(awk -v t="${target}" -v now="$(date +%s.%N)" 'BEGIN{d=t-now; if (d<0) d=0; printf "%.6f", d}')"
    awk -v d="${sleep_for}" 'BEGIN{exit !(d>0)}' && sleep "${sleep_for}"
  fi
done

SUBMIT_END="$(date +%s.%N)"
SUBMIT_ELAPSED="$(awk -v a="${RUN_START}" -v b="${SUBMIT_END}" 'BEGIN{printf "%.2f", b-a}')"
ACHIEVED_TPS="$(awk -v n="${SUBMIT_OK}" -v t="${SUBMIT_ELAPSED}" 'BEGIN{ if (t>0) printf "%.2f", n/t; else print "0" }')"
log "submission done: ok=${SUBMIT_OK} err=${SUBMIT_ERR} in ${SUBMIT_ELAPSED}s (~${ACHIEVED_TPS} tx/s)"

# ---------------------------------------------------------------------------
# 4. MUST-DELIVER: drain receipts for every accepted tx within the timeout.
# ---------------------------------------------------------------------------
# We poll eth_getTransactionReceipt for each accepted hash until present or the
# global drain timeout elapses. Receipt latency = first-seen-time - submit-time.
log "draining receipts (timeout ${SMOKE_RECEIPT_TIMEOUT_S}s)..."
declare -a LATENCY=()       # seconds, float, for txs that got a receipt
RECEIPTED=0
RECEIPT_BAD_STATUS=0
declare -A RECEIVED            # hash -> 1 once receipted

DRAIN_DEADLINE="$(awk -v s="$(date +%s.%N)" -v t="${SMOKE_RECEIPT_TIMEOUT_S}" 'BEGIN{printf "%.6f", s+t}')"

receipt_for() {
  local hash="$1" resp
  resp="$(curl -fsS --max-time 5 -X POST "${RPC_URL}" \
    -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getTransactionReceipt\",\"params\":[\"${hash}\"]}" 2>/dev/null || true)"
  printf '%s' "${resp}"
}

receipt_status() {
  # null result -> empty (not yet mined). Otherwise the status hex (0x1/0x0).
  if (( HAVE_JQ )); then
    printf '%s' "$1" | jq -r 'if .result==null then "" else (.result.status // "0x?") end' 2>/dev/null || true
  else
    printf '%s' "$1" | grep -oE '"status"[ ]*:[ ]*"0x[0-9a-fA-F]+"' \
      | grep -oE '0x[0-9a-fA-F]+' | head -1 || true
  fi
}

while :; do
  pending=0
  for ((i=0; i<${#TX_HASH[@]}; i++)); do
    h="${TX_HASH[i]}"
    [[ -z "${h}" ]] && continue                 # never accepted; nothing to drain
    [[ -n "${RECEIVED["$h"]:-}" ]] && continue       # already receipted
    resp="$(receipt_for "${h}")"
    status="$(receipt_status "${resp}")"
    if [[ -n "${status}" ]]; then
      RECEIVED["$h"]=1
      RECEIPTED=$(( RECEIPTED + 1 ))
      lat="$(awk -v a="${SUBMIT_TS[i]}" -v b="$(date +%s.%N)" 'BEGIN{printf "%.3f", b-a}')"
      LATENCY+=("${lat}")
      if [[ "${status}" != "0x1" && "${status}" != "0x01" ]]; then
        RECEIPT_BAD_STATUS=$(( RECEIPT_BAD_STATUS + 1 ))
        warn "tx ${h} receipt status ${status} (expected 0x1)"
      fi
    else
      pending=$(( pending + 1 ))
    fi
  done

  (( pending == 0 )) && break
  now="$(date +%s.%N)"
  if awk -v n="${now}" -v d="${DRAIN_DEADLINE}" 'BEGIN{exit !(n>=d)}'; then
    warn "receipt drain timed out with ${pending} tx still missing"
    break
  fi
  sleep 1
done

MISSING=$(( SUBMIT_OK - RECEIPTED ))

# Receipt-latency p50/p99 over the receipted txs.
LAT_P50="n/a"; LAT_P99="n/a"; LAT_MAX="n/a"
if (( ${#LATENCY[@]} > 0 )); then
  read -r LAT_P50 LAT_P99 LAT_MAX < <(
    printf '%s\n' "${LATENCY[@]}" | sort -n | awk '
      { a[NR]=$1 }
      END{
        n=NR
        p50i=int((n-1)*0.50)+1
        p99i=int((n-1)*0.99)+1
        printf "%s %s %s\n", a[p50i], a[p99i], a[n]
      }'
  )
fi

# ---------------------------------------------------------------------------
# 5. KEEP-PACE: re-read block metrics; assert advance + bounded gap.
# ---------------------------------------------------------------------------
# Give the pipeline a brief settle window so the last in-flight blocks commit
# before we read the final gauges.
log "settling 3s before final metric read..."
sleep 3

SEALER_FINAL="$(read_block_metric "${SEALER_NODE}" "$(node_ip "${SEALER_NODE}")" "${SEALER_METRICS_PORT}" "${SEALER_BLOCK_METRIC}")"
INGRESS_FINAL_BLOCK="$(ingress_block_number)"

# Failures are accumulated as node-name lists and acted on in the verdict.
FROZEN_NODES=""
GAP_FAIL_NODES=""
METRIC_MISSING=0

echo
echo "---- keep-pace per executor ----"
printf '%-16s %12s %12s %10s %8s %s\n' "node" "base_blk" "final_blk" "advanced" "gap" "verdict"
for node in "${EXECUTOR_NODES[@]}"; do
  fin="$(read_block_metric "${node}" "$(node_ip "${node}")" "${EXECUTOR_METRICS_PORT}" "${EXECUTOR_BLOCK_METRIC}")"
  base="${EXEC_BASE[$node]:-}"

  if [[ -z "${fin}" ]]; then
    METRIC_MISSING=1
    printf '%-16s %12s %12s %10s %8s %s\n' "${node}" "${base:-?}" "<none>" "?" "?" "METRIC-MISSING"
    continue
  fi

  advanced="?"
  if [[ -n "${base}" ]]; then
    advanced=$(( fin - base ))
  fi

  gap="?"
  verdict="OK"
  if [[ -n "${SEALER_FINAL}" ]]; then
    gap=$(( SEALER_FINAL - fin ))
    # gap can be slightly negative if the executor read raced ahead; treat <0 as 0.
    (( gap < 0 )) && gap=0
    if (( gap > SMOKE_MAX_GAP )); then
      verdict="GAP>${SMOKE_MAX_GAP}"
      GAP_FAIL_NODES="${GAP_FAIL_NODES} ${node}(gap=${gap})"
    fi
  fi

  # Frozen check: if the sealer produced blocks during the run (so there was
  # work to keep up with) but this executor did not advance at all, it is
  # frozen. Only assert when we have both baselines.
  if [[ "${advanced}" != "?" && -n "${SEALER_BASE}" && -n "${SEALER_FINAL}" ]]; then
    sealer_adv=$(( SEALER_FINAL - SEALER_BASE ))
    if (( sealer_adv > 0 && advanced <= 0 )); then
      verdict="FROZEN"
      FROZEN_NODES="${FROZEN_NODES} ${node}"
    fi
  fi

  printf '%-16s %12s %12s %10s %8s %s\n' \
    "${node}" "${base:-?}" "${fin}" "${advanced}" "${gap}" "${verdict}"
done

# ---------------------------------------------------------------------------
# 6. SUMMARY + verdict
# ---------------------------------------------------------------------------
echo
echo "================= SMOKE-LOAD SUMMARY ================="
echo "ingress            : ${RPC_URL}  (chain ${CHAIN_ID})"
echo "offered tx         : ${TX_TOTAL}  (senders=${SMOKE_SENDERS}, ~${SMOKE_TPS} tx/s target)"
echo "submit accepted    : ${SUBMIT_OK}"
echo "submit rejected    : ${SUBMIT_ERR}"
echo "achieved tps       : ${ACHIEVED_TPS} (over ${SUBMIT_ELAPSED}s)"
echo "receipts received  : ${RECEIPTED}"
echo "receipts MISSING   : ${MISSING}"
echo "bad-status receipts: ${RECEIPT_BAD_STATUS}"
echo "receipt latency p50: ${LAT_P50}s  p99: ${LAT_P99}s  max: ${LAT_MAX}s"
echo "sealer block       : base=${SEALER_BASE:-?} final=${SEALER_FINAL:-?}"
echo "ingress block       : base=${INGRESS_BASE_BLOCK:-?} final=${INGRESS_FINAL_BLOCK:-?}"
echo "max allowed gap    : ${SMOKE_MAX_GAP}"
echo "====================================================="
echo

# --- verdict: any of these is a FAIL ---------------------------------------
FAILED=0
if (( SUBMIT_OK == 0 )); then
  echo "FAIL: ingress accepted ZERO txs (pipeline not reachable / not accepting)." >&2
  FAILED=1
fi
if (( MISSING > 0 )); then
  echo "FAIL: ${MISSING} accepted tx(s) never got a receipt within ${SMOKE_RECEIPT_TIMEOUT_S}s (must-deliver violated)." >&2
  FAILED=1
fi
if (( RECEIPT_BAD_STATUS > 0 )); then
  echo "FAIL: ${RECEIPT_BAD_STATUS} receipt(s) had non-0x1 status." >&2
  FAILED=1
fi
if (( METRIC_MISSING == 1 )); then
  echo "FAIL: could not read an executor block-number metric (exporter unreachable?)." >&2
  echo "      Check EXECUTOR_NODES / EXECUTOR_METRICS_PORT / METRICS_VIA_DOCKER." >&2
  FAILED=1
fi
if [[ -n "${FROZEN_NODES// }" ]]; then
  echo "FAIL: executor(s) FROZEN (no block advance while sealer advanced):${FROZEN_NODES}" >&2
  FAILED=1
fi
if [[ -n "${GAP_FAIL_NODES// }" ]]; then
  echo "FAIL: executor-vs-sealer gap exceeded ${SMOKE_MAX_GAP}:${GAP_FAIL_NODES}" >&2
  FAILED=1
fi

if (( FAILED == 1 )); then
  echo "RESULT: FAIL" >&2
  exit 1
fi

echo "RESULT: PASS"
exit 0
