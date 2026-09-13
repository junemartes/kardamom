"""Clean a remote image filesystem that has identities absent on the controller.

Run with KARDAMOM_IMAGE_IDENTITY_TEST_IMAGE=kardamom-node:ci after building
that image. The temporary container needs Python, but no privileged mode.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import uuid

ANSIBLE = Path(__file__).resolve().parents[1]
IMAGE = os.environ.get('KARDAMOM_IMAGE_IDENTITY_TEST_IMAGE')


@unittest.skipUnless(IMAGE, 'set KARDAMOM_IMAGE_IDENTITY_TEST_IMAGE to a Python-capable image')
class ImageIdentityTest(unittest.TestCase):
    def test_remote_only_keys_and_fixed_identities_are_removed_idempotently(self):
        name = 'kardamom-image-identity-' + uuid.uuid4().hex
        subprocess.run(['docker', 'run', '-d', '--name', name, '--entrypoint', 'sleep',
                        IMAGE, '300'], check=True, capture_output=True)
        self.addCleanup(subprocess.run, ['docker', 'rm', '--force', name],
                        check=True, capture_output=True)
        key = '/etc/ssh/ssh_host_' + uuid.uuid4().hex
        self.assertFalse(Path(key).exists())
        script = '''from pathlib import Path
import sys
for name in [sys.argv[1], '/root/.ssh/authorized_keys', '/var/lib/dbus/machine-id',
             '/etc/machine-id', '/etc/ssh/sshd_config.keep']:
    p=Path(name)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text('image-build-identity')
'''
        self.remote(name, script, key)
        with tempfile.TemporaryDirectory() as tmp:
            play = Path(tmp) / 'clean.yml'
            play.write_text(json.dumps([{
                'hosts': 'image', 'gather_facts': False,
                'tasks': [{'ansible.builtin.include_tasks': str(
                    ANSIBLE / 'roles/elastic_image/tasks/identity.yml')}],
            }]))
            inventory = Path(tmp) / 'inventory.ini'
            inventory.write_text('[image]\n' + name +
                                 ' ansible_connection=community.docker.docker '
                                 'ansible_python_interpreter=/usr/bin/python3\n')
            env = {k: v for k, v in os.environ.items() if not k.startswith('ANSIBLE_')}
            for _ in range(2):
                result = subprocess.run(['ansible-playbook', '-i', str(inventory), str(play)],
                                        env=env, text=True, capture_output=True, timeout=60)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.remote(name, '''from pathlib import Path
import sys
for name in [sys.argv[1], '/root/.ssh/authorized_keys', '/var/lib/dbus/machine-id']:
    assert not Path(name).exists(), name
assert Path('/etc/machine-id').read_text() == ''
assert Path('/etc/ssh/sshd_config.keep').read_text() == 'image-build-identity'
''', key)

    def remote(self, name, script, key):
        result = subprocess.run(['docker', 'exec', name, 'python3', '-c', script, key],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
