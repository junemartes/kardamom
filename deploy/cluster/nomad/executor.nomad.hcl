# kardamom-executor — replays the canonical order and applies state (embeds the
# libmdbx StateWriter). Placed on w1 (192.168.56.21), co-located with sequencer
# #0 and ingress.
#
# Invocation (from crates/e2e/tests/multiprocess_e2e.rs):
#   kardamom-executor --config <executor.toml> --aeron-dir <dir> --shards 2 \
#       --chain-id 412346 --chain <genesis.toml>
#
# executor.toml is presence-checked only (empty). The genesis is rendered from
# config/genesis/dev.toml and passed via --chain. chain-id 412346 from
# group_vars/all.yml. shards 2 = partition_count.
#
# Mounts both the shared Aeron tmpfs aeron.dir AND the persistent state_dir
# (/opt/kardamom/state) for the libmdbx StateWriter so state survives restarts.

job "executor" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.kardamom_node}"
    value     = "w1"
  }

  group "executor" {
    count = 1

    network {
      mode = "host"
    }

    task "executor" {
      driver = "docker"

      config {
        image        = "192.168.56.11:5000/kardamom-executor:dev"
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/state:/opt/kardamom/state",
        ]
        args = [
          "--config", "/local/executor.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--shards", "2",
          "--chain-id", "412346",
          "--chain", "/local/genesis.toml",
        ]
      }

      # Presence-checked empty config.
      template {
        destination = "local/executor.toml"
        data        = <<EOF
EOF
      }

      # Chain genesis (mirrors config/genesis/dev.toml). Prefunds Anvil
      # accounts #0..#15 with 1000 ETH each + the ERC-7955 factory predeploy.
      template {
        destination = "local/genesis.toml"
        data        = <<EOF
chain_id = 412346

[[alloc]]
address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
balance = "1000000000000000000000"
[[alloc]]
address = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
balance = "1000000000000000000000"
[[alloc]]
address = "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"
balance = "1000000000000000000000"
[[alloc]]
address = "0x90F79bf6EB2c4f870365E785982E1f101E93b906"
balance = "1000000000000000000000"
[[alloc]]
address = "0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65"
balance = "1000000000000000000000"
[[alloc]]
address = "0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc"
balance = "1000000000000000000000"
[[alloc]]
address = "0x976EA74026E726554dB657fA54763abd0C3a0aa9"
balance = "1000000000000000000000"
[[alloc]]
address = "0x14dC79964da2C08b23698B3D3cc7Ca32193d9955"
balance = "1000000000000000000000"
[[alloc]]
address = "0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f"
balance = "1000000000000000000000"
[[alloc]]
address = "0xa0Ee7A142d267C1f36714E4a8F75612F20a79720"
balance = "1000000000000000000000"
[[alloc]]
address = "0xBcd4042DE499D14e55001CcbB24a551F3b954096"
balance = "1000000000000000000000"
[[alloc]]
address = "0x71bE63f3384f5fb98995898A86B02Fb2426c5788"
balance = "1000000000000000000000"
[[alloc]]
address = "0xFABB0ac9d68B0B445fB7357272Ff202C5651694a"
balance = "1000000000000000000000"
[[alloc]]
address = "0x1CBd3b2770909D4e10f157cABC84C7264073C9Ec"
balance = "1000000000000000000000"
[[alloc]]
address = "0xdF3e18d64BC6A983f673Ab319CCaE4f1a57C7097"
balance = "1000000000000000000000"
[[alloc]]
address = "0xcd3B766CCDd6AE721141F452C550Ca635964ce71"
balance = "1000000000000000000000"

# ERC-7955 permissionless CREATE2 factory predeploy.
[[alloc]]
address = "0xC0DEb853af168215879d284cc8B4d0A645fA9b0E"
code    = "0x60203d3d3582360380843d373d34f5806019573d813d933efd5b3d52f33d52"
nonce   = 1
EOF
      }

      resources {
        cpu    = 2000
        memory = 2048
      }
    }
  }
}
