"""Render the actual agent template after the production input checks."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ANSIBLE = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which('ansible-playbook'), 'Ansible required')
class DnsTokenTest(unittest.TestCase):
    def render(self, production=True, token='dns-read-only'):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root / 'consul.hcl'
            play = root / 'render.yml'
            play.write_text(json.dumps([{
                'hosts': 'localhost', 'connection': 'local', 'gather_facts': False,
                'vars_files': [str(ANSIBLE / 'group_vars/all.yml')],
                'tasks': [
                    {'ansible.builtin.include_role': {'name': 'profile'}},
                    {'ansible.builtin.template': {
                        'src': str(ANSIBLE / 'roles/consul/templates/consul.hcl.j2'),
                        'dest': str(output), 'mode': '0600'}, 'no_log': True},
                ],
            }]))
            settings = dict(deployment_profile='production' if production else 'local',
                            kardamom_node='test', role='sealer', tier='worker', node_ip='127.0.0.1',
                            consul_gossip_key='test', consul_agent_token='agent-token',
                            consul_dns_token=token, nomad_consul_token='nomad-token',
                            consul_acl_enabled=production, nomad_acl_enabled=production,
                            consul_tls_dir='/test/tls', nomad_tls_dir='/test/tls',
                            consul_server_expect=3, nomad_server_expect=3,
                            consul_retry_join=['a', 'b', 'c'], private_cidrs=['10.0.0.0/8'])
            env = {k: v for k, v in os.environ.items() if not k.startswith('ANSIBLE_')}
            env['ANSIBLE_ROLES_PATH'] = str(ANSIBLE / 'roles')
            result = subprocess.run(['ansible-playbook', '-i', 'localhost,', str(play),
                                     '-e', json.dumps(settings)], env=env, text=True,
                                    capture_output=True, timeout=30)
            return result, output.read_text() if output.exists() else None

    def test_production_renders_separate_agent_and_dns_tokens_without_logging_them(self):
        result, config = self.render()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('agent   = "agent-token"', config)
        self.assertIn('default = "dns-read-only"', config)
        self.assertNotIn('dns-read-only', result.stdout + result.stderr)

    def test_missing_dns_token_fails_before_render(self):
        result, config = self.render(token='')
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(config)

    def test_local_profile_does_not_need_a_dns_token(self):
        result, config = self.render(production=False, token='')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('tokens {', config)
