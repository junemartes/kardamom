"""Every service job gives each instance its own host id.

The host id is the host_id label on the metrics and the instance of a
service on the events stream. Two instances with the same id merge into
one state there, so one instance can hide the halt of the other.

Run with: python3 -m unittest discover -s deploy/cluster/ansible/tests -v
"""
import re
import unittest
from pathlib import Path

NOMAD = Path(__file__).resolve().parents[2] / 'nomad'
# The jobs that run a service built on kardamom_obs.
SERVICES = ['ingress', 'executor', 'sequencer', 'batcher', 'da-watcher', 'l1-indexer',
            'validator', 'notifier', 'state-mirror']
# A value is per instance when it names the allocation index, the node, or
# the task group key.
PER_INSTANCE = re.compile(r'NOMAD_ALLOC_INDEX|node_index|node\.unique|group\.key')


class HostId(unittest.TestCase):
    def test_every_service_sets_a_per_instance_host_id(self):
        for job in SERVICES:
            with self.subTest(job=job):
                text = (NOMAD / f'{job}.nomad.hcl').read_text()
                found = re.findall(r'KARDAMOM_HOST_ID\s*=\s*"([^"]+)"|"--host-id",\s*"([^"]+)"', text)
                values = [a or b for a, b in found]
                self.assertTrue(values, f'{job} sets no host id')
                for value in values:
                    self.assertRegex(value, PER_INSTANCE, f'{job} host id {value} is the same on every instance')


if __name__ == '__main__':
    unittest.main()
