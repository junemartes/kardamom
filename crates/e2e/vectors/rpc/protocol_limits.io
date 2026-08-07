# Protocol-limit rejections added by the W1b audit
# (docs/agents/l1-client-suite-port-spec.md): a tx that can never execute is
# refused at submission with a clear error, not burned into a skip receipt.

# Gas limit above the EIP-7825 per-tx cap (2^24).
>> {"method": "eth_sendRawTransaction", "params": ["${RAW_TX_OVERCAP}"]}
<< {"error": {"code": -32602, "message": "transaction gas limit 30000000 exceeds the EIP-7825 per-tx cap of 16777216 — the tx can never execute"}}

# Type-3 (EIP-4844) envelope: kardamom carries no blob transactions.
>> {"method": "eth_sendRawTransaction", "params": ["${RAW_TX_TYPE3}"]}
<< {"error": {"code": -32602, "message": "unsupported transaction type 0x03: blob (EIP-4844) transactions are not supported"}}
