#!/usr/bin/env bash
# This is a pipeline smoke test. It submits a transfer through the
# kardamom ingress JSON-RPC proxy. It checks that the receipt status
# comes back as 0x1.
#
# Ingress endpoint: http://192.168.56.31:8545 (ingress_ip:ingress_rpc in
# ansible/group_vars/all.yml). Signer: Anvil account #0, prefunded with
# 1000 ETH in config/genesis/dev.toml.
#
# The script prefers foundry `cast` (cast send plus cast receipt). It
# falls back to a documented curl flow with a pre-signed raw tx when cast
# is missing.
#
# The script prints PASS or FAIL, and exits nonzero on failure.
set -euo pipefail

RPC_URL="${RPC_URL:-http://192.168.56.31:8545}"
CHAIN_ID="${CHAIN_ID:-412346}"
# Anvil account #0's private key. This key is public; use it for dev only.
PK="${PK:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
# Burn (sink) address for the transfer.
TO="${TO:-0x000000000000000000000000000000000000dEaD}"
VALUE="${VALUE:-1}"   # wei
# The nonce is always 0. Every caller submits from its own dedicated
# funded account: the gate, each load/chaos case, and the churn
# re-smokes. See the account-budget note in ci-cluster.sh. So each
# account's first and only tx has nonce 0. The ingress JSON-RPC does not
# implement eth_getTransactionCount on purpose (deferred to the state
# writer; see crates/ingress/src/json_rpc.rs). So cast cannot auto-fill
# the nonce. Since each check uses a fresh account, no check ever needs a
# non-zero nonce. This is why there is no NONCE setting and no
# cross-call nonce coordination. Pick the account through PK, not the
# nonce.

echo "==> Smoke test against ingress: ${RPC_URL} (chain-id ${CHAIN_ID}, nonce 0)"

fail() { echo "RESULT: FAIL — $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Path A: foundry cast (preferred).
# ---------------------------------------------------------------------------
if command -v cast >/dev/null 2>&1; then
  echo "==> Using foundry 'cast'."

  CAST_ERR="$(mktemp)"
  trap 'rm -f "${CAST_ERR}"' EXIT

  # cast send signs and submits the tx. Without --async, it waits for the
  # receipt and prints it as JSON, using --json. status is "0x1" on
  # success.
  #
  # This command passes nonce, gas price, gas limit, and chain id
  # explicitly. This stops cast from making its usual fill calls
  # (eth_getTransactionCount, eth_gasPrice, eth_estimateGas). The ingress
  # implements only eth_chainId, eth_blockNumber, eth_sendRawTransaction,
  # and eth_getTransactionReceipt. It returns an error for
  # eth_getTransactionCount and eth_getBalance (deferred to the state
  # writer; see crates/ingress/src/json_rpc.rs). With every field
  # provided, cast's only RPC calls are eth_sendRawTransaction and
  # eth_getTransactionReceipt.
  set +e
  RECEIPT_JSON="$(cast send "${TO}" \
      --value "${VALUE}" \
      --private-key "${PK}" \
      --rpc-url "${RPC_URL}" \
      --chain "${CHAIN_ID}" \
      --legacy \
      --nonce 0 \
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
  # Accept "0x1" or "1". cast may render a decoded status.
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

# This is a placeholder pre-signed legacy tx: account #0 to 0x...dEaD,
# value 1 wei, nonce 0, gas price 1, gas 21000, chain id 412346. Replace
# RAW_TX with a tx signed for the current signer nonce if this is
# rejected.
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
