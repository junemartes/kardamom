"""Every Grafana dashboard file is in the monitoring job, and Grafana can load it.

The monitoring job renders each dashboard of its `dashboards` list with file(),
so a file outside the list never reaches Grafana, and a listed name without a
file fails the job. The job renders the files with the consul-template
delimiters [[[ and ]]], so a dashboard must not contain them.

Run with: python3 -m unittest discover -s deploy/cluster/ansible/tests -v
"""
import json
import re
import unittest
from pathlib import Path

DEPLOY = Path(__file__).resolve().parents[3]
DASHBOARDS = DEPLOY / 'grafana/provisioning/dashboards-json'
JOB = DEPLOY / 'cluster/nomad/monitoring.nomad.hcl'


class Dashboards(unittest.TestCase):
    def setUp(self):
        listed = re.search(r'^\s*dashboards\s*=\s*\[(.*?)\]', JOB.read_text(), re.M | re.S)
        self.assertIsNotNone(listed, 'the monitoring job has no dashboards list')
        self.listed = re.findall(r'"([^"]+)"', listed.group(1))
        self.files = sorted(DASHBOARDS.glob('*.json'))

    def test_the_job_lists_every_file(self):
        self.assertEqual(sorted(self.listed), sorted(f.stem for f in self.files))

    def test_the_job_lists_each_file_once(self):
        self.assertEqual(len(self.listed), len(set(self.listed)))

    def test_every_file_parses_with_its_own_uid(self):
        for path in self.files:
            with self.subTest(path.name):
                dashboard = json.loads(path.read_text())
                self.assertEqual(dashboard['uid'], path.stem)
                ids = [panel['id'] for panel in dashboard['panels']]
                self.assertEqual(len(ids), len(set(ids)), 'panel ids repeat')

    def test_no_file_holds_the_template_delimiters(self):
        for path in self.files:
            with self.subTest(path.name):
                text = path.read_text()
                self.assertNotIn('[[[', text)
                self.assertNotIn(']]]', text)

    def test_every_query_reads_the_provisioned_datasource(self):
        for path in self.files:
            panels = json.loads(path.read_text())['panels']
            for target in (t for p in panels for t in p.get('targets', [])):
                with self.subTest(path.name, expr=target['expr']):
                    self.assertEqual(target['datasource']['uid'], 'prometheus')


if __name__ == '__main__':
    unittest.main()
