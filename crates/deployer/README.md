# kardamom-deploy

`kardamom-deploy` is a stateless Rust CLI. It deploys and upgrades the kardamom L1 contracts.

- All upgrade state lives on-chain, in the registry of the kardamom factory.
- There is no local manifest and no per-environment state file.

## Bootstrap path

Bootstrap deploys the factory through the **ERC-7955 permissionless CREATE2 factory**. Its canonical address is `0xC0DEb853af168215879d284cc8B4d0A645fA9b0E`. It is present on every chain that supports EIP-7702.

1. `ensure-factory` checks that ERC-7955 is present. If it is absent, the command fails with `Erc7955FactoryAbsent`. See https://github.com/safe-research/erc-7955 for the EIP-7702 bootstrap procedure of a chain.
2. It computes the factory proxy address. The inputs are `--owner`, the compiled `KardamomFactoryV1` bytecode and the canonical salts. If that address has code, it returns `AlreadyDeployed`.
3. Otherwise it sends transactions to the ERC-7955 factory:
   - It deploys the factory impl with salt `keccak256("kardamom.factory.impl.v1")`. It sends this transaction only when the impl address has no code.
   - It deploys the proxy with salt `keccak256("kardamom.factory.proxy.v1")`. The init data is `initialize(address owner)`.
4. All owners on a chain share one factory impl, because the impl salt holds no owner. The first owner sends two transactions. A second owner finds the impl and sends one transaction.
5. After the proxy transaction, the command checks that the proxy has code. If not, it fails with `FactoryNotDeployed`.
6. The transaction signer (`--private-key`) only pays gas. It has no privileged role. The `--owner` value in the proxy init data sets the owner of the factory.

## CLI examples

Bootstrap on mainnet (production):

```sh
kardamom-deploy ensure-factory \
  --rpc-url https://mainnet.infura.io/v3/$KEY \
  --owner 0xPRODUCTION_SAFE_ADDRESS \
  --private-key env:DEPLOYER_KEY
```

Deploy ETHLockbox on L2 chainIDs 42 and 43 in one transaction:

```sh
kardamom-deploy deploy ETHLockbox \
  --rpc-url https://... \
  --owner 0xSAFE \
  --l2-chain-id 42 --l2-chain-id 43 \
  --l2-minter 0xAAA... --l2-minter 0xBBB... \
  --private-key env:DEPLOYER_KEY
```

Atomically upgrade ETHLockbox across L2s 42, 43, 44 in one transaction:

```sh
kardamom-deploy upgrade ETHLockbox \
  --rpc-url https://... \
  --owner 0xSAFE \
  --l2-chain-id 42 --l2-chain-id 43 --l2-chain-id 44 \
  --private-key env:DEPLOYER_KEY
```

The CLI groups the specs by `(id, version)`. It uses `targetImpl` to deploy the new impl once and to re-point N proxies in one transaction.

List all registered contracts:

```sh
kardamom-deploy addresses --owner 0xSAFE --rpc-url https://...
```

Filter to one L2:

```sh
kardamom-deploy addresses --owner 0xSAFE --l2-chain-id 42 --rpc-url https://...
```

## Address derivation

| Address | Formula |
|---|---|
| ERC-7955 factory | `0xC0DEb853af168215879d284cc8B4d0A645fA9b0E` (canonical on every EIP-7702 chain) |
| Kardamom factory impl | `CREATE2(ERC7955, keccak256("kardamom.factory.impl.v1"), keccak256(impl_initcode))` |
| **Kardamom factory proxy** | `CREATE2(ERC7955, keccak256("kardamom.factory.proxy.v1"), keccak256(ERC1967Proxy_init ‖ abi.encode(impl_addr, initialize_calldata(owner))))` |
| App impl (shared across L2s) | `CREATE2(kardamomFactory, keccak256(abi.encode(id, "impl", version)), keccak256(impl_initcode))` |
| App proxy (per L2) | `CREATE2(kardamomFactory, keccak256(abi.encode(l2ChainId, id, "proxy")), keccak256(ERC1967Proxy_init ‖ abi.encode(impl_addr, init_data)))` |

Different `--owner` values give different factory proxy addresses. This is intended.

- Each environment (mainnet, testnet, dev) has its own canonical owner. It therefore has its own canonical factory address.
- An owner with the same address on several L1s (for example a deterministic Safe) gives the same factory address on those L1s.

## Bytecode pinning

The canonical address of the factory depends on the exact bytecode of `KardamomFactoryV1` and its embedded OpenZeppelin code. These inputs fix that bytecode:

| Input | Locked at |
|---|---|
| Solidity compiler | `solc = "0.8.26"` in `contracts/foundry.toml` |
| Optimizer | `optimizer = true`, `optimizer_runs = 200` and `via_ir = true` in `contracts/foundry.toml` |
| Metadata hash | `bytecode_hash = "none"` in `contracts/foundry.toml`. The bytecode is then the same on every build host. |
| OpenZeppelin contracts | `openzeppelin-contracts@v5.0.2` (CI installs this version explicitly) |
| OpenZeppelin upgradeable | `openzeppelin-contracts-upgradeable@v5.0.2` (CI installs this version explicitly) |

- `contracts/expected_bytecode_hash.txt` holds the SHA-256 of the runtime bytecode of `KardamomFactoryV1`.
- The test `factory_address_sync` (`crates/deployer/tests/factory_address_sync.rs`) checks that the `FACTORY` constant in `KardamomUUPSBase.sol` equals the computed factory address for the dev owner.
- A deliberate change that shifts the bytecode needs these steps:
  1. Bump the salt suffix in `crates/deployer/src/addresses.rs` (`v1` to `v2`).
  2. Regenerate `contracts/expected_bytecode_hash.txt`.
  3. Run `cargo test -p kardamom-deployer --test factory_address_sync`. Update `KardamomUUPSBase.FACTORY` with the new address.
- The v1 factory stays at its old address. The v2 factory has a new canonical address.

## What if ERC-7955 is not on my chain?

A public deployer EOA signs an EIP-7702 transaction that deploys the ERC-7955 factory. Anyone can submit it.

- The procedure is at https://github.com/safe-research/erc-7955. It costs about 100k gas.
- After it runs once on a chain, every kardamom user can run `ensure-factory` with no permission.
- On a dev chain (anvil), `kardamom-deploy bootstrap-7955-anvil` installs the ERC-7955 runtime with `anvil_setCode`. It works only on dev chains. Run it before the first `deploy` on a fresh anvil. It is idempotent.

## Other commands

- `verify` cross-checks each registry entry with the ERC-1967 implementation slot of its proxy. It reports each mismatch.
- `addresses --l2-chain-id <id> --contract KardamomL2Settlement --json` prints a JSON array. The array has the entries for that chain and that contract. It is for automation.
  - An absent registration gives `[]`.
  - An RPC error or a factory lookup error still fails the command.
  - Without `--json`, the output is human-readable.
