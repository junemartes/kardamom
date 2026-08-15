# =============================================================================
# expected-peers.tpl — seed expected-peer set for egress-inventory.sh.
# =============================================================================
# DERIVED from config/channels.toml.tpl + the nomad job specs +
# ansible/group_vars/all.yml (the address/port contract). This is a TEMPLATE:
# copy it to expected-peers.txt, run `egress-inventory.sh --expected` against
# a healthy cluster, and tighten/extend from what the diff shows — then keep
# the txt under change control. Or bootstrap the other way with --generate and
# reconcile against this file.
#
# Fields (globs): <node> <kind> <addr> <proc>   — see egress-inventory.sh -h.
# Process names are comm-truncated by ss (15 chars): "kardamom-ingres".
#
# ---- cluster infrastructure (every node) ------------------------------------
# nomad client -> server RPC + HTTP (control-0 .10), consul gossip/API.
kardamom-* tcp-out 192.168.56.10:4646 *
kardamom-* tcp-out 192.168.56.10:4647 *
kardamom-* tcp-out 192.168.56.10:8300 consul
kardamom-* tcp-out 192.168.56.10:8301 consul
kardamom-* tcp-in  :4646 *
kardamom-* tcp-in  :4647 *
kardamom-* tcp-in  :4648 *
kardamom-* tcp-in  :8300 consul
kardamom-* tcp-in  :8301 consul
kardamom-* tcp-in  :8500 consul
kardamom-* udp-bind *:8301 consul
kardamom-* udp-bind *:8302 consul
kardamom-* udp-bind *:8600 consul
kardamom-* udp-bind *:4648 nomad
# in-cluster registry (image pulls by each node's inner dockerd)
kardamom-* tcp-out 192.168.56.10:5000 *
kardamom-control-0 tcp-in :5000 *
# all-hosts group every interface joins + mDNS/LLMNR noise from base images
kardamom-* mcast 224.0.0.1 -
#
# ---- Aeron data plane (channels.toml.tpl multicast groups) ------------------
# tx_data .11:40000, tx_receipts .15:40020, tx_errors .17:40030,
# tx_deposits .19:40040, tx_bal .21:40050, fsync .23:40060, watermark .25:40070
kardamom-* mcast 239.192.56.11 -
kardamom-* mcast 239.192.56.15 -
kardamom-* mcast 239.192.56.17 -
kardamom-* mcast 239.192.56.19 -
kardamom-* mcast 239.192.56.21 -
kardamom-* mcast 239.192.56.23 -
kardamom-* mcast 239.192.56.25 -
# multicast data/control sockets + media-driver/service UDP endpoints: the
# drivers bind the channel ports on 0.0.0.0 or the node IP (base 40000 lane)
kardamom-* udp-bind *:400?? *
kardamom-* udp-bind *:401?? *
#
# ---- Aeron archive (aeron.system job, ports 8010/8011/8020/8021) -----------
kardamom-* udp-bind *:8010 java
kardamom-* udp-bind *:8011 java
kardamom-* udp-bind *:8020 java
kardamom-* udp-bind *:8021 java
#
# ---- Aeron Cluster (Raft sealer, .51-.53, cluster_ports 40200-40204) --------
kardamom-sealer-* udp-bind *:4020? java
# cluster-client egress response endpoints on the CLIENT nodes
# (sequencer/executor :40210, seq-b :40211, validator :40230, batcher :40231)
kardamom-sequencer-* udp-bind *:4021? *
kardamom-executor-*  udp-bind *:40210 kardamom-*
kardamom-aux-0       udp-bind *:4023? *
#
# ---- ingress JSON-RPC front door (.31/.32:8545) -----------------------------
kardamom-ingress-* tcp-in :8545 kardamom-*
# clients/proxy/load-harness reaching ingress from the bridge
kardamom-* tcp-out 192.168.56.31:8545 *
kardamom-* tcp-out 192.168.56.32:8545 *
#
# ---- L1 RPC (anvil on control-0:8546; da-watcher, batcher, deploy tooling) --
kardamom-control-0 tcp-in :8546 anvil
kardamom-aux-0 tcp-out 192.168.56.10:8546 kardamom-*
#
# ---- metrics + checkpoint exchange ------------------------------------------
# Prometheus-format exporters: seq-a :9001, batcher :9002, executor :9004,
# validator/ingress :9006, seq-b :9011. Scrapers (chaos suite / operator /
# a Prometheus if one is pointed at the cluster) connect IN.
kardamom-sequencer-* tcp-in :9001 kardamom-*
kardamom-sequencer-* tcp-in :9011 kardamom-*
kardamom-aux-0       tcp-in :9002 kardamom-*
kardamom-executor-*  tcp-in :9004 kardamom-*
kardamom-aux-0       tcp-in :9006 kardamom-*
kardamom-ingress-*   tcp-in :9006 kardamom-*
# executor/validator peer-checkpoint fetch (serve :9014 on executors)
kardamom-executor-* tcp-in :9014 kardamom-*
kardamom-* tcp-out 192.168.56.41:9014 kardamom-*
kardamom-* tcp-out 192.168.56.42:9014 kardamom-*
kardamom-* tcp-out 192.168.56.43:9014 kardamom-*
