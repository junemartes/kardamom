"""Run the Ansible contract against disposable inputs; no cluster is required."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib
import unittest

ANSIBLE = Path(__file__).resolve().parents[1]
REPO = ANSIBLE.parents[2]


@unittest.skipUnless(shutil.which('ansible-playbook'), 'ansible-playbook required')
class ContractTest(unittest.TestCase):
    def play(self, name, variables=None, success=True, check=False):
        cmd = ['ansible-playbook', '-i', 'localhost,', str(ANSIBLE / name),
               '-e', json.dumps(variables or {})] + (['--check'] if check else [])
        result = subprocess.run(cmd, cwd=ANSIBLE.parent, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
        self.assertEqual(result.returncode == 0, success, result.stdout)
        return result.stdout

    def test_contract_is_read_only_and_rejects_invalid_bounds(self):
        self.play('contract.yml', check=True)
        self.play('contract.yml', {'tx_ttl_ms': 0}, success=False)

    def test_identity_rendering_is_idempotent_and_protects_versioned_maps(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'map.toml'
            for lanes in [1, 2, 4, 8]:
                settings = {'shard_map_output': str(path), 'partition_count': lanes}
                self.play('shard-map.yml', settings)
                result = tomllib.loads(path.read_text())
                self.assertEqual(result, {'version': 0, 'table': [v % lanes for v in range(256)]})
                self.assertIn('changed=0', self.play('shard-map.yml', settings))
            original = path.read_text().replace('version = 0', 'version = 1')
            path.write_text(original)
            self.play('shard-map.yml', settings, success=False)
            self.assertEqual(path.read_text(), original)

    def test_contract_detects_drift_and_node_addresses(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            cluster = repo / 'deploy/cluster'
            shutil.copytree(ANSIBLE.parent, cluster,
                            ignore=shutil.ignore_patterns('.terraform', '__pycache__'))
            for name in ['crates', 'chains', '.github', 'justfile']:
                (repo / name).symlink_to(REPO / name)
            settings = {'contract_cluster_dir': str(cluster)}
            path = cluster / 'nomad/ingress.nomad.hcl'
            original = path.read_text()
            path.write_text(original.replace('--log-config', '--missing-config'))
            self.assertIn('missing', self.play('contract.yml', settings, success=False))
            path.write_text(original + '\n# forbidden node: 192.0.2.7\n')
            self.assertIn('names an address', self.play('contract.yml', settings, success=False))
