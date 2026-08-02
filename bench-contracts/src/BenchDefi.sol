// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

/// Bench workloads for kardamom-load --workload defi. Realistic STORAGE and
/// GAS profiles matter here, not economic correctness:
///  - SwapPool hammers two hot reserve slots every swap (chunk-collapsible
///    BAL writes) plus per-user unique balance slots.
///  - Vault mixes two hot aggregate slots with a unique per-user slot.
///  - Clob is unique-slot-heavy (fresh order structs) with a hot id counter
///    and best-price slots — the workload chunked BALs CANNOT compress.
/// All are self-contained (no external token calls) so the harness can fund
/// senders by simply calling them.

contract SwapPool {
    uint256 public reserve0; // hot: every swap writes it
    uint256 public reserve1; // hot: every swap writes it
    mapping(address => uint256) public bal0; // unique per sender
    mapping(address => uint256) public bal1; // unique per sender

    event Swap(address indexed who, bool zeroForOne, uint256 amountIn, uint256 amountOut);

    constructor() {
        reserve0 = 1_000_000 ether;
        reserve1 = 1_000_000 ether;
    }

    /// Faucet: seed the caller's internal balances (storage-realistic, no
    /// external transfers).
    function seed() external {
        bal0[msg.sender] = 1_000 ether;
        bal1[msg.sender] = 1_000 ether;
    }

    function swap(bool zeroForOne, uint256 amountIn) external {
        uint256 rIn = zeroForOne ? reserve0 : reserve1;
        uint256 rOut = zeroForOne ? reserve1 : reserve0;
        uint256 inWithFee = amountIn * 997;
        uint256 out = (inWithFee * rOut) / (rIn * 1000 + inWithFee);
        if (zeroForOne) {
            bal0[msg.sender] -= amountIn;
            bal1[msg.sender] += out;
            reserve0 = rIn + amountIn;
            reserve1 = rOut - out;
        } else {
            bal1[msg.sender] -= amountIn;
            bal0[msg.sender] += out;
            reserve1 = rIn + amountIn;
            reserve0 = rOut - out;
        }
        emit Swap(msg.sender, zeroForOne, amountIn, out);
    }
}

contract Vault {
    uint256 public totalAssets; // hot
    uint256 public totalShares; // hot
    mapping(address => uint256) public shares; // unique per sender

    event Deposit(address indexed who, uint256 assets, uint256 sharesOut);
    event Withdraw(address indexed who, uint256 sharesIn, uint256 assetsOut);

    function deposit(uint256 assets) external {
        uint256 s = totalShares == 0 ? assets : (assets * totalShares) / totalAssets;
        totalAssets += assets;
        totalShares += s;
        shares[msg.sender] += s;
        emit Deposit(msg.sender, assets, s);
    }

    function withdraw(uint256 sharesIn) external {
        uint256 have = shares[msg.sender];
        if (sharesIn > have) sharesIn = have;
        if (sharesIn == 0) return;
        uint256 assets = (sharesIn * totalAssets) / totalShares;
        shares[msg.sender] = have - sharesIn;
        totalShares -= sharesIn;
        totalAssets -= assets;
        emit Withdraw(msg.sender, sharesIn, assets);
    }
}

contract Clob {
    struct Order {
        address owner;
        uint96 size; // packs with owner into one slot
        uint256 price;
    }

    uint256 public nextId = 1; // hot
    uint256 public bestBid; // hot
    uint256 public bestAsk; // hot
    mapping(uint256 => Order) public orders; // unique: 2 slots per order
    // price level -> [head, tail] order ids (two slots per touched level)
    mapping(uint256 => uint256) public levelDepth;

    event Place(address indexed who, bool bid, uint256 price, uint96 size, uint256 id);
    event Cancel(uint256 id);
    event Fill(uint256 makerId, uint96 size);

    /// Place an order; if it crosses the current best opposite price, fill
    /// against (up to) the two oldest resting ids at that level — bounded
    /// work per call, storage-heavy either way.
    function place(bool bid, uint256 price, uint96 size) external returns (uint256 id) {
        id = nextId++;
        orders[id] = Order({owner: msg.sender, size: size, price: price});
        levelDepth[price] += size;
        if (bid) {
            uint256 ask = bestAsk;
            if (ask != 0 && price >= ask) {
                // cross: consume one maker id deterministically (id - 1 keeps
                // it storage-realistic without a full queue walk)
                uint256 makerId = id > 1 ? id - 1 : id;
                Order storage m = orders[makerId];
                uint96 fill = m.size < size ? m.size : size;
                m.size -= fill;
                emit Fill(makerId, fill);
            }
            if (price > bestBid) bestBid = price;
        } else {
            uint256 bidP = bestBid;
            if (bidP != 0 && price <= bidP) {
                uint256 makerId = id > 1 ? id - 1 : id;
                Order storage m = orders[makerId];
                uint96 fill = m.size < size ? m.size : size;
                m.size -= fill;
                emit Fill(makerId, fill);
            }
            if (bestAsk == 0 || price < bestAsk) bestAsk = price;
        }
        emit Place(msg.sender, bid, price, size, id);
    }

    function cancel(uint256 id) external {
        Order storage o = orders[id];
        if (o.owner != msg.sender || o.size == 0) return;
        levelDepth[o.price] -= o.size;
        o.size = 0;
        emit Cancel(id);
    }
}
