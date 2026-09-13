"""Run node task control flow against Docker module-boundary doubles."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ANSIBLE = Path(__file__).resolve().parents[1]
MODULE = '''
import json, os
from ansible.module_utils.basic import AnsibleModule, _load_params
params = _load_params()
m = AnsibleModule(argument_spec={k: {'type': 'raw'} for k in params if not k.startswith('_ansible_')}, supports_check_mode=True)
name = __file__.split('/')[-1].split('.')[0]
# __file__ inside ansiballz retains the original module name.
with open(os.environ['NODE_TEST_EVENTS'], 'a') as f:
    f.write(json.dumps({'module': name, 'args': m.params}) + '\\n')
if name == 'docker_container_info':
    m.exit_json(changed=False, exists=os.environ.get('NODE_TEST_EXISTS') == '1', container={
      'NetworkSettings': {'Networks': {'kardamom-net': {
        'IPAMConfig': {'IPv4Address': os.environ.get('NODE_TEST_IP', '192.168.56.10')},
        'IPAddress': '192.168.56.10'}}}})
if name == 'docker_container_exec':
    m.exit_json(changed=False, rc=1, stdout=os.environ.get('NODE_TEST_SYSTEMD', 'degraded'))
m.exit_json(changed=True)
'''


@unittest.skipUnless(shutil.which('ansible-playbook'), 'ansible-playbook required')
class ContainerNodesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='kardamom-node-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        modules = self.root / 'collections/ansible_collections/community/docker/plugins/modules'
        modules.mkdir(parents=True)
        for name in ('docker_container_info', 'docker_container', 'docker_volume', 'docker_container_exec'):
            (modules / (name + '.py')).write_text(MODULE)
        shutil.copytree(ANSIBLE / 'roles/container_nodes', self.root / 'roles/container_nodes')
        # Limit readiness retries in this isolated test, preserving its condition.
        node = self.root / 'roles/container_nodes/tasks/node.yml'
        node.write_text(node.read_text().replace('retries: 30', 'retries: 1').replace('delay: 2', 'delay: 0'))
        (self.root / 'test.yml').write_text('''---
- hosts: localhost
  connection: local
  gather_facts: false
  vars:
    ip_prefix: 192.168.56
    container_nodes_node: {name: control-0, ip: 192.168.56.10}
  tasks:
    - ansible.builtin.include_role:
        name: container_nodes
        tasks_from: node
''')

    def run_node(self, exists=False, ip='192.168.56.10', systemd='degraded', success=True):
        env = {k: v for k, v in os.environ.items() if not k.startswith('ANSIBLE_')}
        env.update(ANSIBLE_COLLECTIONS_PATH=str(self.root / 'collections'),
                   ANSIBLE_STDOUT_CALLBACK='default', OBJC_DISABLE_INITIALIZE_FORK_SAFETY='YES',
                   NODE_TEST_EVENTS=str(self.root / 'events'), NODE_TEST_EXISTS=str(int(exists)),
                   NODE_TEST_IP=ip, NODE_TEST_SYSTEMD=systemd)
        result = subprocess.run(['ansible-playbook', '-i', 'localhost,', str(self.root / 'test.yml')],
                                env=env, cwd=self.root, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=45)
        self.assertEqual(result.returncode == 0, success, result.stdout)
        return [json.loads(x) for x in (self.root / 'events').read_text().splitlines()]

    def test_new_node_has_privilege_network_and_dedicated_storage(self):
        calls = self.run_node()
        node = next(x['args'] for x in calls if x['module'] == 'docker_container')
        self.assertTrue(node['privileged'])
        self.assertEqual(node['cgroupns_mode'], 'host')
        self.assertEqual(node['networks'][0]['ipv4_address'], '192.168.56.10')
        self.assertIn('kardamom-control-0-containerd:/var/lib/containerd', node['volumes'])

    def test_existing_node_is_started_without_image_replacement(self):
        calls = self.run_node(exists=True)
        nodes = [x['args'] for x in calls if x['module'] == 'docker_container']
        self.assertEqual(len(nodes), 1)
        self.assertNotIn('image', nodes[0])
        self.assertEqual(nodes[0]['comparisons'], {'*': 'ignore'})

    def test_drift_fails_before_mutating_resources(self):
        calls = self.run_node(exists=True, ip='192.168.56.99', success=False)
        self.assertEqual([x['module'] for x in calls], ['docker_container_info'])

    def test_starting_is_not_ready(self):
        self.run_node(systemd='starting', success=False)


if __name__ == '__main__':
    unittest.main()
