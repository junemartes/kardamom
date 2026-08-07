# Chain metadata: always answered, correct values, even on an idle chain.
# eth_chainId returned a hardcoded 1 until #90 — this vector pins the fix.
>> {"method": "eth_chainId", "params": []}
<< {"result": "${CHAIN_ID_HEX}"}
>> {"method": "eth_blockNumber", "params": []}
<< {"result": "${HEX}"}
