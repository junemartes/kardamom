"""The low-rate side streams use small Aeron terms.

Run with: python3 -m unittest discover -s deploy/cluster/ansible/tests -v
"""
import re
import unittest
from pathlib import Path

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


if __name__ == '__main__':
    unittest.main()
