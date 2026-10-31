"""Prepared software skips installation only when the deploy contract matches."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml

ANSIBLE = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which('ansible-playbook'), 'Ansible required')
class NodeImageTest(unittest.TestCase):
    def check_marker(self, marker, changed=False):
        with tempfile.TemporaryDirectory(prefix='kardamom-node-marker-') as directory:
            root = Path(directory)
            (root / 'roles').symlink_to(ANSIBLE / 'roles', target_is_directory=True)
            shutil.copyfile(ANSIBLE / 'node-software.yml', root / 'node-software.yml')
            tasks = []
            if marker != 'missing':
                content = "{{ {'version': 1, 'fingerprint': node_software_fingerprint} | to_json }}"
                if marker == 'stale':
                    content = json.dumps({'version': 1, 'fingerprint': 'stale'})
                tasks.append({'name': 'Write the fixture marker', 'ansible.builtin.copy': {
                    'dest': str(root / 'marker.json'), 'content': content, 'mode': '0600'}})
            if changed:
                tasks.append({'name': 'Change the requested Nomad release',
                              'ansible.builtin.set_fact': {'nomad_version': '0.0.0'}})
            tasks.extend([
                {'name': 'Check with the production task', 'ansible.builtin.import_role': {
                    'name': 'common', 'tasks_from': 'image'}},
                {'name': 'Check the install decision', 'ansible.builtin.assert': {
                    'that': [f"common_node_software_preinstalled == {'false' if marker == 'missing' else 'true'}"]}},
            ])
            play = [{'name': 'Check prepared node software', 'hosts': 'localhost',
                     'connection': 'local', 'become': False, 'gather_facts': True,
                     'vars_files': [str(ANSIBLE / 'group_vars/all.yml'), str(root / 'node-software.yml')],
                     'tasks': tasks}]
            (root / 'check.yml').write_text(yaml.safe_dump(play))
            env = {key: value for key, value in os.environ.items() if not key.startswith('ANSIBLE_')}
            result = subprocess.run([
                'ansible-playbook', '-i', 'localhost,', str(root / 'check.yml'),
                '-e', json.dumps({'node_software_marker_path': str(root / 'marker.json'),
                                  'ansible_python_interpreter': shutil.which('python3')}),
            ], cwd=root, env=env, text=True, capture_output=True, timeout=45)
            return result

    def test_an_unprepared_node_keeps_the_install_path(self):
        result = self.check_marker('missing')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_a_matching_image_skips_installation(self):
        result = self.check_marker('current')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_a_stale_image_cannot_skip_installation(self):
        result = self.check_marker('stale')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('rebuild the node image', result.stdout)

    def test_a_version_override_invalidates_the_prepared_image(self):
        result = self.check_marker('current', changed=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('rebuild the node image', result.stdout)


if __name__ == '__main__':
    unittest.main()
