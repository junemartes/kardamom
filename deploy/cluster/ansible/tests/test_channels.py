"""The low-rate side streams use small Aeron terms.

Run with: python3 -m unittest discover -s deploy/cluster/ansible/tests -v
"""
import re
import unittest
from pathlib import Path

import jinja2
import yaml

CLUSTER = Path(__file__).resolve().parents[2]


class Channels(unittest.TestCase):
    def setUp(self):
        self.channels = (CLUSTER / 'config/channels.toml.tpl').read_text()

    def channel(self, name):
        found = re.search(rf'^{name} = "(.*)"$', self.channels, re.MULTILINE)
        self.assertIsNotNone(found, f'{name} is missing from channels.toml.tpl')
        return found.group(1)

    def test_the_events_channel_carries_the_minimum_term_length(self):
        self.assertIn('|term-length=65536', self.channel('events_channel'))

    def archive_topics(self, role, node_roles=None):
        template = (CLUSTER / 'ansible/roles/nomad/templates/nomad.hcl.j2').read_text()
        expression = re.search(r'archive_topics = "(\{\{.*?\}\})"', template).group(1)
        roles = node_roles if node_roles is not None else [role]
        return jinja2.Template(expression).render(roles_set=roles)

    def follower_topic(self, roles):
        template = (CLUSTER / 'ansible/roles/nomad/templates/nomad.hcl.j2').read_text()
        expression = re.search(r'archive_topics_follower = "(\{\{.*?\}\})"', template).group(1)
        return jinja2.Template(expression).render(roles_set=roles)

    def test_two_follower_nodes_record_the_l1_blocks_stream(self):
        self.assertIn('service "l1_blocks.kardamom-aeron-archive"', self.channels)
        job = (CLUSTER / 'nomad/aeron.system.nomad.hcl').read_text()
        self.assertIn('tags = ["${meta.archive_topics}", "${meta.archive_topics_follower}"]', job)
        self.assertIn('topics            = "${meta.archive_topics},${meta.archive_topics_follower}"', job)
        local = yaml.safe_load((CLUSTER / 'ansible/group_vars/all.yml').read_text())['node_classes']
        nodes = [
            [name] + spec.get('roles', []) + spec.get('instance_roles', {}).get(str(i), [])
            for name, spec in local.items()
            for i in range(spec['count'])
        ]
        followers = [roles for roles in nodes if self.follower_topic(roles) == 'l1_blocks']
        self.assertEqual(len(followers), 2, 'the local profile runs the follower on two nodes')
        self.assertEqual(self.follower_topic(['executor']), '')
        production = (CLUSTER / 'ansible/inventories/production/hosts.example.ini').read_text()
        self.assertEqual(len(re.findall(r'^indexer-\d .*\brole=indexer', production, re.MULTILINE)), 2)

    def test_the_deposits_fallback_selects_the_da_watcher_node_in_both_profiles(self):
        self.assertIn('service "tx_deposits.kardamom-aeron-archive"', self.channels)
        self.assertIn('service "tx_data.kardamom-aeron-archive"', self.channels)
        job = (CLUSTER / 'nomad/aeron.system.nomad.hcl').read_text()
        self.assertIn('tags = ["${meta.archive_topics}", ', job)
        local = yaml.safe_load((CLUSTER / 'ansible/group_vars/all.yml').read_text())['node_classes']
        self.assertEqual(self.archive_topics('aux', ['aux'] + local['aux']['roles']), 'tx_deposits')
        production = (CLUSTER / 'ansible/inventories/production/hosts.example.ini').read_text()
        found = re.search(r'^da-watcher-0 .*\brole=(\S+)', production, re.MULTILINE)
        self.assertEqual(self.archive_topics(found.group(1)), 'tx_deposits')
        self.assertEqual(self.archive_topics('ingress', ['ingress'] + local['ingress']['roles']), 'tx_data')


if __name__ == '__main__':
    unittest.main()
