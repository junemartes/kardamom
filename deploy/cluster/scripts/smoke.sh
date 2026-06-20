#!/usr/bin/env bash
# Pipeline smoke test: submit a transfer through the kardamom ingress JSON-RPC
# proxy and assert a receipt with status 0x1 comes back.
#
# Ingress endpoint: http://192.168.56.31:8545 (ingress_ip:ingress_rpc from
# ansible/group_vars/all.yml). Signer: Anvil account #0 (prefunded with 1000
# ETH in config/genesis/dev.toml).
#
# Prefers foundry `cast` (cast send + cast receipt). Falls back to a documented
# curl flow with a pre-signed raw tx if cast is absent.
#
# Prints PASS/FAIL; exits nonzero on failure.
set -euo pipefail

RPC_URL="${RPC_URL:-http://192.168.56.31:8545}"
CHAIN_ID="${CHAIN_ID:-412346}"
# Anvil account #0 private key (public, dev only).
PK="${PK:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
# Burn / sink address for the transfer.
TO="${TO:-0x000000000000000000000000000000000000dEaD}"
VALUE="${VALUE:-1}"   # wei
# Nonce for the signer. The ingress JSON-RPC surface deliberately does NOT
# implement eth_getTransactionCount (it errors "deferred to S6 state writer";
# see crates/ingress/src/json_rpc.rs), exactly like the e2e tests which sign
# raw txs with explicit nonces. So we MUST pass the nonce ourselves — otherwise
# `cast send` calls eth_getTransactionCount to fill it and fails. Account #0
# starts at nonce 0; the caller bumps NONCE per send (the redundancy re-smoke
# in ci-cluster.sh passes NONCE=1).
NONCE="${NONCE:-0}"

echo "==> Smoke test against ingress: ${RPC_URL} (chain-id ${CHAIN_ID}, nonce ${NONCE})"

fail() { echo "RESULT: FAIL — $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Path A: foundry cast (preferred).
# ---------------------------------------------------------------------------
if command -v cast >/dev/null 2>&1; then
  echo "==> Using foundry 'cast'."

  CAST_ERR="$(mktemp)"
  trap 'rm -f "${CAST_ERR}"' EXIT

  # cast send signs + submits and (without --async) waits for the receipt,
  # printing it as JSON with --json. status is "0x1" on success.
  #
  # nonce/gas-price/gas-limit/chain are ALL passed EXPLICITLY so cast does not
  # issue its usual fill calls (eth_getTransactionCount / eth_gasPrice /
  # eth_estimateGas) — the ingress implements only eth_chainId / eth_blockNumber
  # / eth_sendRawTransaction / eth_getTransactionReceipt and returns an error
  # for eth_getTransactionCount + eth_getBalance ("deferred to S6 state writer",
  # crates/ingress/src/json_rpc.rs). With every field provided, cast's only RPC
  # calls are eth_sendRawTransaction + eth_getTransactionReceipt.
  set +e
  RECEIPT_JSON="$(cast send "${TO}" \
      --value "${VALUE}" \
      --private-key "${PK}" \
      --rpc-url "${RPC_URL}" \
      --chain "${CHAIN_ID}" \
      --legacy \
      --nonce "${NONCE}" \
      --gas-price "${GAS_PRICE:-1000000000}" \
      --gas-limit 21000 \
      --json 2>"${CAST_ERR}")"
  rc=$?
  set -e
  if [[ ${rc} -ne 0 ]]; then
    echo "---- cast stderr ----" >&2
    cat "${CAST_ERR}" >&2 || true
    fail "cast send returned ${rc}"
  fi

  # Extract status with jq if available, else grep.
  if command -v jq >/dev/null 2>&1; then
    STATUS="$(printf '%s' "${RECEIPT_JSON}" | jq -r '.status')"
    TXH="$(printf '%s' "${RECEIPT_JSON}" | jq -r '.transactionHash')"
  else
    STATUS="$(printf '%s' "${RECEIPT_JSON}" | grep -oE '"status"[ ]*:[ ]*"0x[0-9a-fA-F]+"' | grep -oE '0x[0-9a-fA-F]+' | head -1)"
    TXH="$(printf '%s' "${RECEIPT_JSON}" | grep -oE '"transactionHash"[ ]*:[ ]*"0x[0-9a-fA-F]+"' | grep -oE '0x[0-9a-fA-F]+' | head -1)"
  fi

  echo "==> tx hash: ${TXH:-<unknown>}  receipt status: ${STATUS:-<none>}"
  # Accept "0x1" or "1" (cast may render decoded status).
  if [[ "${STATUS}" == "0x1" || "${STATUS}" == "1" || "${STATUS}" == "true" ]]; then
    echo "RESULT: PASS"
    exit 0
  fi
  fail "receipt status was '${STATUS}' (expected 0x1)"
fi

# ---------------------------------------------------------------------------
# Path B: curl fallback (cast not installed).
# ---------------------------------------------------------------------------
echo "==> 'cast' not found; falling back to curl + pre-signed raw tx."
echo "    NOTE: a raw tx is nonce-, gasprice-, and chainid-specific. The value"
echo "    below is a PLACEHOLDER for nonce=0 on chain ${CHAIN_ID}; if the signer"
echo "    has already sent txs (nonce != 0) this will be rejected. Install"
echo "    foundry 'cast' for a robust signed-and-submitted flow (Path A)."

if ! command -v curl >/dev/null 2>&1; then
  fail "neither 'cast' nor 'curl' is available"
fi

# PLACEHOLDER pre-signed legacy tx: account #0 -> 0x...dEaD, value 1 wei,
# nonce 0, gasPrice 1, gas 21000, chain-id 412346. Replace RAW_TX with a tx
# signed for the current signer nonce if this is rejected.
RAW_TX="${RAW_TX:-0xPLACEHOLDER_REPLACE_WITH_PRESIGNED_RAW_TX}"

if [[ "${RAW_TX}" == 0xPLACEHOLDER* ]]; then
  fail "no signed RAW_TX available (set RAW_TX=0x... or install foundry 'cast')"
fi

send_resp="$(curl -fsS -X POST "${RPC_URL}" \
  -H 'content-type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_sendRawTransaction\",\"params\":[\"${RAW_TX}\"]}")"
echo "==> eth_sendRawTransaction -> ${send_resp}"

TXH="$(printf '%s' "${send_resp}" | grep -oE '"result"[ ]*:[ ]*"0x[0-9a-fA-F]+"' | grep -oE '0x[0-9a-fA-F]+' | head -1)"
[[ -n "${TXH}" ]] || fail "no tx hash in send response: ${send_resp}"

# Poll eth_getTransactionReceipt for status.
for i in $(seq 1 30); do
  rcpt="$(curl -fsS -X POST "${RPC_URL}" \
    -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getTransactionReceipt\",\"params\":[\"${TXH}\"]}")"
  STATUS="$(printf '%s' "${rcpt}" | grep -oE '"status"[ ]*:[ ]*"0x[0-9a-fA-F]+"' | grep -oE '0x[0-9a-fA-F]+' | head -1)"
  if [[ -n "${STATUS}" ]]; then
    echo "==> receipt status: ${STATUS}"
    if [[ "${STATUS}" == "0x1" ]]; then
      echo "RESULT: PASS"
      exit 0
    fi
    fail "receipt status was '${STATUS}' (expected 0x1)"
  fi
  sleep 1
done
fail "no receipt for ${TXH} within timeout"
