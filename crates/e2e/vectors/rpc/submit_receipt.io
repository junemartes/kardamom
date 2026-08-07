# Happy path: a funded transfer lands and its receipt pins the v0 shape.
# Object matching is subset (listed keys must match; extras allowed).

# Submit parks until the receipt exists, then returns the canonical hash.
>> {"method": "eth_sendRawTransaction", "params": ["${RAW_TX_VALID}"]}
<< {"result": "${TX_HASH_VALID}"}

# The receipt is immediately retrievable and carries the v0 contract:
# blockHash is null BY DESIGN (no state commitment in the slim boundary),
# type reports the real EIP-2718 byte, gasUsed is the transfer intrinsic.
>> {"method": "eth_getTransactionReceipt", "params": ["${TX_HASH_VALID}"]}
<< {"result": {"blockHash": null, "transactionHash": "${TX_HASH_VALID}", "transactionIndex": "${HEX}", "from": "${SENDER}", "to": "${RECIPIENT}", "status": "0x1", "type": "0x0", "gasUsed": "0x5208", "effectiveGasPrice": "0x3b9aca00", "logs": []}}
