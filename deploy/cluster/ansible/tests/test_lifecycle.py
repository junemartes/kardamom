"""Exercise real Ansible lifecycle control flow with isolated deployment boundaries."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ANSIBLE = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which('ansible-playbook'), 'ansible-playbook required')
class LifecycleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='kardamom-lifecycle-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for role in ('cluster_run', 'topology'):
            shutil.copytree(ANSIBLE / 'roles' / role, self.root / 'roles' / role)
        shutil.copytree(ANSIBLE / 'group_vars', self.root / 'group_vars')
        shutil.copyfile(ANSIBLE / 'run.yml', self.root / 'run.yml')
        tasks = self.root / 'roles/container_nodes/tasks'
        tasks.mkdir(parents=True)
        (tasks / 'down.yml').write_text('''---
- name: Record teardown
  ansible.builtin.lineinfile:
    path: "{{ playbook_dir }}/events"
    line: teardown
    create: true
''')
        (self.root / 'cluster.yml').write_text('''---
- hosts: localhost
  connection: local
  gather_facts: false
  tasks:
    - ansible.builtin.lineinfile:
        path: "{{ playbook_dir }}/events"
        line: deploy
        create: true
    - ansible.builtin.fail:
        msg: intentional deployment failure
      when: test_deploy_failure | default(false) | bool
''')
        (self.root / 'scripts').mkdir()
        for script, event in [('run-tests.sh', 'tests'), ('ci-diagnostics.sh', 'diagnostics')]:
            (self.root / 'scripts' / script).write_text(f'echo {event} >> "{self.root}/events"\n')
        self.lock = self.root / 'lock'

    def run_play(self, extra=None, success=True, check=False):
        settings = {'cluster_run_dir': str(self.root), 'cluster_run_lock': str(self.lock),
                    'cluster_run_keep': False} | (extra or {})
        env = {k: v for k, v in os.environ.items() if not k.startswith('ANSIBLE_')}
        env.update(ANSIBLE_STDOUT_CALLBACK='default', ANSIBLE_NOCOLOR='1',
                   OBJC_DISABLE_INITIALIZE_FORK_SAFETY='YES')
        result = subprocess.run(['ansible-playbook', '-i', 'localhost,', str(self.root / 'run.yml'),
                                 '-e', json.dumps(settings)] + (['--check'] if check else []),
                                cwd=self.root, env=env, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=90)
        self.assertEqual(result.returncode == 0, success, result.stdout)
        return result.stdout

    def events(self):
        p = self.root / 'events'
        return p.read_text().splitlines() if p.exists() else []

    def test_success_tests_then_cleanup(self):
        self.run_play()
        self.assertEqual(self.events(), ['deploy', 'tests', 'teardown'])
        self.assertFalse(self.lock.exists())

    def test_failed_deploy_diagnoses_before_cleanup(self):
        self.run_play({'cluster_run_vars': {'test_deploy_failure': True}}, success=False)
        self.assertEqual(self.events(), ['deploy', 'diagnostics', 'teardown'])
        self.assertFalse(self.lock.exists())

    def test_failed_tests_keep_cluster(self):
        (self.root / 'scripts/run-tests.sh').write_text('exit 42\n')
        self.run_play({'cluster_run_keep': True}, success=False)
        self.assertEqual(self.events(), ['deploy', 'diagnostics'])
        self.assertFalse(self.lock.exists())

    def test_lock_contender_cannot_teardown(self):
        self.lock.mkdir()
        self.run_play(success=False)
        self.assertEqual(self.events(), [])
        self.assertTrue(self.lock.exists())

    def test_reset_removes_old_cluster_before_deploy(self):
        self.run_play({'cluster_run_operation': 'reset', 'cluster_run_keep': True})
        self.assertEqual(self.events(), ['teardown', 'deploy', 'tests'])

    def test_down_never_deploys_or_tests(self):
        self.run_play({'cluster_run_operation': 'down'})
        self.assertEqual(self.events(), ['teardown'])
        self.assertFalse(self.lock.exists())

    def test_check_mode_has_no_effects(self):
        self.run_play(check=True)
        self.assertEqual(self.events(), [])
        self.assertFalse(self.lock.exists())

    def test_invalid_topology_fails_before_lock_or_cleanup(self):
        self.run_play({'node_classes': {
            'control': {'count': 1, 'ip_start': 10, 'tier': 'control'},
            'other': {'count': 1, 'ip_start': 10, 'tier': 'worker'},
        }}, success=False)
        self.assertEqual(self.events(), [])
        self.assertFalse(self.lock.exists())

    def test_inventory_expands_canonical_replicas(self):
        (self.root / 'run.yml').write_text('''---
- hosts: localhost
  connection: local
  gather_facts: false
  vars_files: [group_vars/all.yml]
  roles: [topology]
  tasks:
    - ansible.builtin.assert:
        that:
          - groups.container_nodes | length == 12
          - groups.sealer | length == 3
          - hostvars['ingress-1'].node_ip == '192.168.56.32'
          - hostvars['sequencer-1'].node_index | int == 1
          - hostvars['control-0'].ansible_user == 'root'
          - "'localhost' not in groups.container_nodes"
''')
        self.run_play()

    def test_cleanup_failure_still_releases_lock(self):
        (self.root / 'roles/container_nodes/tasks/down.yml').write_text('''---
- ansible.builtin.fail:
    msg: intentional teardown failure
''')
        self.run_play(success=False)
        self.assertFalse(self.lock.exists())


if __name__ == '__main__':
    unittest.main()
