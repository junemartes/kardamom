// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

/// Forces forge to compile `ERC1967Proxy` into `out/ProxyArtifact.sol/ProxyArtifact.json`
/// so the Rust deployer can read its creation code. `ProxyArtifact` inherits
/// `ERC1967Proxy` with no overrides — its initcode is byte-identical to a direct
/// `new ERC1967Proxy(impl, data)` call.
contract ProxyArtifact is ERC1967Proxy {
    constructor(address impl, bytes memory data) ERC1967Proxy(impl, data) {}
}
