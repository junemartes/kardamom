"""Exercise inventory precedence with the real playbook's shared defaults."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml

ANSIBLE = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which('ansible-inventory'), 'Ansible required')
class ProfileTest(unittest.TestCase):
    def inventory(self, path, host, production=True):
        env = {k: v for k, v in os.environ.items() if not k.startswith('ANSIBLE_')}
        result = subprocess.run(
            ['ansible-inventory', '-i', str(path), '--playbook-dir', str(ANSIBLE), '--host', host],
            env=env, text=True, capture_output=True, check=True, timeout=30)
        values = json.loads(result.stdout)
        self.assertEqual(values['deployment_profile'], 'production' if production else 'local')
        self.assertEqual(values['consul_acl_enabled'], production)
        self.assertEqual(values['nomad_acl_enabled'], production)
        self.assertEqual(values['consul_server_expect'], 3 if production else 1)
        return values

    def test_dedicated_inventory_and_vault_override_shared_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            inventory = Path(tmp) / 'hetzner'
            shutil.copytree(ANSIBLE / 'inventories/hetzner', inventory)
            (inventory / 'group_vars/production/vault.yml').write_text('consul_agent_token: test-token\n')
            values = self.inventory(inventory / 'hosts.example.ini', 'sealer-0')
            self.assertEqual(values['consul_agent_token'], 'test-token')
            self.assertEqual(values['cluster_id'], 'kardamom-prod')
            self.assertEqual(values['consul_client_addr'], '127.0.0.1')

    def test_image_inventory_preserves_profile_and_enrollment(self):
        tasks = yaml.safe_load((ANSIBLE / 'roles/elastic_image/tasks/main.yml').read_text())
        inventory = next(t['ansible.builtin.copy']['content'] for t in tasks
                         if t['name'] == 'Write the first-boot inventory')
        profile_dest = next(t['ansible.builtin.copy']['dest'] for t in tasks
                            if t['name'] == 'Copy the profile values')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'inventory.ini').write_text(inventory)
            profile = root / Path(profile_dest).relative_to('/etc/kardamom')
            profile.parent.mkdir(parents=True)
            shutil.copyfile(ANSIBLE / 'inventories/hetzner/group_vars/production/profile.yml', profile)
            (profile.parent / 'enrollment.yml').write_text('enrollment_argv: [enroll, --node]\n')
            values = self.inventory(root / 'inventory.ini', 'localhost')
            self.assertEqual(values['enrollment_argv'], ['enroll', '--node'])

    def test_local_defaults_are_unchanged(self):
        self.inventory('localhost,', 'localhost', production=False)
