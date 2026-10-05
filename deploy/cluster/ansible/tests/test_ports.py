"""Every fixed port in the kernel's ephemeral range lies in the reserved block.

Run with: python3 -m unittest discover -s deploy/cluster/ansible/tests -v
"""
import re
import unittest
from pathlib import Path

import yaml

ANSIBLE = Path(__file__).resolve().parents[1]
CLUSTER = ANSIBLE.parent
# The Linux default for net.ipv4.ip_local_port_range.
EPHEMERAL = range(32768, 61000)


class FixedPorts(unittest.TestCase):
    def setUp(self):
        self.vars = yaml.safe_load((ANSIBLE / 'group_vars/all.yml').read_text())
        low, high = self.vars['fixed_port_block'].split('-')
        self.block = range(int(low), int(high) + 1)

    def fixed_ports(self):
        channels = (CLUSTER / 'config/channels.toml.tpl').read_text()
        ports = {int(p) for p in re.findall(r':(\d{4,5})\b', channels)}
        ports |= set(self.vars['cluster_ports'].values())
        return ports

    def test_every_ephemeral_fixed_port_is_reserved(self):
        unreserved = sorted(p for p in self.fixed_ports() if p in EPHEMERAL and p not in self.block)
        self.assertEqual(unreserved, [], 'fixed ports in the ephemeral range outside fixed_port_block')

    def test_the_block_holds_the_cluster_ports(self):
        self.assertTrue(set(self.vars['cluster_ports'].values()) <= set(self.block))


if __name__ == '__main__':
    unittest.main()
