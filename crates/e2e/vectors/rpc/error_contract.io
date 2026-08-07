# The v0 error contract (crates/ingress/src/error.rs): every rejection is a
# clean, coded JSON-RPC error — never a hang, never a transport drop.

# Malformed RLP → invalid params.
>> {"method": "eth_sendRawTransaction", "params": ["0xdeadbeef"]}
<< {"error": {"code": -32602, "message": "${ANY}"}}

# Well-formed tx whose signature cannot recover → invalid params, exact text.
>> {"method": "eth_sendRawTransaction", "params": ["${RAW_TX_BADSIG}"]}
<< {"error": {"code": -32602, "message": "signature verification failed"}}

# Deferred state endpoints (S6 pending): clean internal error with the exact
# documented message — NOT "method not found", NOT a hang.
>> {"method": "eth_getBalance", "params": ["${SENDER}", "latest"]}
<< {"error": {"code": -32603, "message": "internal server error: eth_getBalance deferred to S6 state writer"}}
>> {"method": "eth_getTransactionCount", "params": ["${SENDER}", "latest"]}
<< {"error": {"code": -32603, "message": "internal server error: eth_getTransactionCount deferred to S6 state writer"}}

# Unknown tx hash → null, not an error.
>> {"method": "eth_getTransactionReceipt", "params": ["0x00000000000000000000000000000000000000000000000000000000000000aa"]}
<< {"result": null}
