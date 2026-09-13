# The v0 error contract (crates/ingress/src/error.rs): every rejection is a
# clean, coded JSON-RPC error — never a hang, never a transport drop.

# Malformed RLP → invalid params.
>> {"method": "eth_sendRawTransaction", "params": ["0xdeadbeef"]}
<< {"error": {"code": -32602, "message": "${ANY}"}}

# Well-formed tx whose signature cannot recover → invalid params, exact text.
>> {"method": "eth_sendRawTransaction", "params": ["${RAW_TX_BADSIG}"]}
<< {"error": {"code": -32602, "message": "signature verification failed"}}

# Account state endpoints: the local account layer, then one executor
# query. The sender is a funded dev account at nonce 0 (submit_receipt runs
# after this file). Only the head block is served: history is invalid params.
>> {"method": "eth_getBalance", "params": ["${SENDER}", "latest"]}
<< {"result": "${HEX}"}
>> {"method": "eth_getTransactionCount", "params": ["${SENDER}", "latest"]}
<< {"result": "0x0"}
>> {"method": "eth_getBalance", "params": ["${SENDER}", "earliest"]}
<< {"error": {"code": -32602, "message": "${ANY}"}}

# Unknown tx hash → null, not an error.
>> {"method": "eth_getTransactionReceipt", "params": ["0x00000000000000000000000000000000000000000000000000000000000000aa"]}
<< {"result": null}
