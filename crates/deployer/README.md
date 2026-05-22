# kardamom-deploy

Stateless Rust CLI for deploying and upgrading kardamom L1 contracts. All upgrade state lives on-chain in the kardamom factory's registry — there is no local manifest or per-environment state file.

## Bootstrap path

Bootstrap deploys the factory through **ERC-7955's permissionless CREATE2 factory** at the canonical address `0xC0DEb853af168215879d284cc8B4d0A645fA9b0E` (present on every EIP-7702-supporting chain).

1. `ensure-factory` checks ERC-7955 is present. If not, it bails with `Erc7955FactoryAbsent`. See https://github.com/safe-research/erc-7955 for the EIP-7702 bootstrap procedure for that chain.
2. It computes the factory proxy address from the supplied `--owner`, the compiled `KardamomFactoryV1` bytecode, and the canonical salts. If the address already has code, it returns `AlreadyDeployed`.
3. Otherwise it sends two transactions to the ERC-7955 factory:
   - Deploy the kardamom factory impl with salt `keccak256("kardamom.factory.impl.v1")`.
   - Deploy the proxy with salt `keccak256("kardamom.factory.proxy.v1")`, init data = `initialize(address owner)`.
4. The transaction signer (`--private-key`) just pays gas; it holds no privileged role. The factory's owner is set by `--owner` baked into the proxy's init data.

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

The CLI groups specs by `(id, version)` and uses `targetImpl` to deploy the new impl once and re-point N proxies in a single transaction.

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

Different `--owner` values produce different factory proxy addresses. This is intentional: each environment (mainnet, testnet, dev) has its own canonical owner and therefore its own canonical factory address. Owners with the same address across L1s (e.g. a deterministic Safe) yield the same factory address across L1s.

## Bytecode pinning

The factory's canonical address depends on the byte-identical bytecode of `KardamomFactoryV1` plus its embedded OpenZeppelin code. We pin the inputs that determine that bytecode:

| Input | Locked at |
|---|---|
| Solidity version | `pragma solidity 0.8.26;` in every source file |
| Solidity optimizer | `optimizer = true`, `optimizer_runs = 200`, no `via_ir` in `contracts/foundry.toml` |
| OpenZeppelin contracts | `openzeppelin-contracts@v5.0.2` (CI installs this version explicitly) |
| OpenZeppelin upgradeable | `openzeppelin-contracts-upgradeable@v5.0.2` (CI installs this version explicitly) |

The `bytecode-hash` CI job (in `.github/workflows/ci.yml`) compiles the factory and asserts the SHA-256 of its runtime bytecode equals `contracts/expected_bytecode_hash.txt`. If you intentionally change something that shifts the bytecode (a deliberate factory upgrade), bump the salt suffix in `crates/deployer/src/addresses.rs` (`v1 → v2`) and regenerate `expected_bytecode_hash.txt`. The v1 factory at the old address continues to exist; v2 lands at a new canonical address.

## What if ERC-7955 isn't on my chain?

ERC-7955's factory is deployed by submitting an EIP-7702 transaction signed by a publicly-known deployer EOA. Anyone can submit it. The procedure is documented at https://github.com/safe-research/erc-7955 — running it costs ~100k gas. After it runs once on a chain, every kardamom user can `ensure-factory` permissionlessly.

A `kardamom-deploy bootstrap-erc-7955` subcommand to automate this on chains that lack the factory is planned but not in this release.
