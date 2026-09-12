"""Exercise the actual playbook and Nomad HCL compiler against an isolated API.

Run with: python3 -m unittest discover -s deploy/cluster/ansible/tests -v
Requires ansible-playbook and nomad on PATH; never connects to a real cluster.
"""
import json
import os
from pathlib import Path
import shutil
import socket
import time
import urllib.request
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ANSIBLE = Path(__file__).resolve().parents[1]
SERVICES = ['aeron', 'cluster', 'sequencer', 'ingress', 'executor', 'validator', 'da-watcher', 'batcher']


class NomadAPI(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def respond(self, body):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        state = self.server.state
        if '/plan?' in self.path:
            job = body['Job']
            state.setdefault('plans', {})[job['ID']] = job
            old = state['jobs'].get(job['ID'])
            self.respond({'Diff': {'Type': 'None' if old == job else 'Edited' if old else 'Added'},
                          'JobModifyIndex': 1 if old else 0})
        elif self.path.startswith('/v1/jobs?'):
            assert body['EnforceIndex'] is True
            state['jobs'][body['Job']['ID']] = body['Job']
            state['writes'].append(body['Job']['ID'])
            self.respond({'JobModifyIndex': 1})
        else:
            raise AssertionError(self.path)

    def do_GET(self):
        name = self.path.split('/')[3].split('?')[0]
        state = self.server.state
        if '/allocations?' in self.path:
            allocs = []
            job = state['jobs'][name]
            for group in job['TaskGroups']:
                count = 11 if job['Type'] == 'system' else group['Count']
                for i in range(count):
                    # Historical/stopping allocations must not satisfy readiness.
                    stale = state.get('missing_replica') == name and i > 0
                    allocs.append({'TaskGroup': group['Name'], 'JobVersion': 0 if stale else 1,
                                   'DesiredStatus': 'run', 'ClientStatus': 'running'})
            self.respond(allocs)
        else:
            self.respond({'Version': 1})


@unittest.skipUnless(shutil.which('nomad') and shutil.which('ansible-playbook'),
                     'nomad and ansible-playbook required')
class DeployTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='kardamom-deploy-test-')
        self.addCleanup(self.tmp.cleanup)
        self.manifest = Path(self.tmp.name) / 'images.digests'
        self.manifest.write_text(''.join(
            f'{s} registry.example:5000/kardamom-{s}:test@sha256:{"a" * 64}\n' for s in SERVICES))
        self.api = ThreadingHTTPServer(('127.0.0.1', 0), NomadAPI)
        self.api.state = {'jobs': {}, 'writes': []}
        self.thread = threading.Thread(target=self.api.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.api.server_close)
        self.addCleanup(self.api.shutdown)

    def run_deploy(self, extra=None, check=False, success=True):
        variables = {
            'workloads_nomad_addr': f'http://127.0.0.1:{self.api.server_port}',
            'workloads_manifest': str(self.manifest),
            'workloads_settlement_address': '0x' + '1' * 40,
            'workloads_require_signed': False,
            'workloads_poll_retries': 1,
            'workloads_poll_delay': 0,
            'workloads_light_execution_rpc': '',
            'workloads_light_consensus_rpc': '',
        } | (extra or {})
        env = {k: v for k, v in os.environ.items() if not k.startswith(('ANSIBLE_', 'NOMAD_'))}
        env.update(ANSIBLE_NOCOLOR='1', ANSIBLE_STDOUT_CALLBACK='default',
                   ANSIBLE_LOCAL_TEMP=self.tmp.name + '/ansible',
                   OBJC_DISABLE_INITIALIZE_FORK_SAFETY='YES')
        cmd = ['ansible-playbook', '-i', 'localhost,', str(ANSIBLE / 'deploy.yml'),
               '-e', json.dumps(variables)] + (['--check'] if check else [])
        result = subprocess.run(cmd, cwd=ANSIBLE.parent, env=env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
        self.assertEqual(result.returncode == 0, success, result.stdout)
        return result.stdout

    def test_deploy_order_pinning_and_repeat(self):
        self.run_deploy()
        expected = ['aeron', 'anvil', 'cluster', 'sequencer', 'ingress', 'executor',
                    'validator', 'da-watcher', 'batcher']
        self.assertEqual(self.api.state['writes'], expected)
        for name in SERVICES:
            tasks = [t for g in self.api.state['jobs'][name]['TaskGroups'] for t in g['Tasks']]
            self.assertTrue(all(t['Config']['image'].endswith('@sha256:' + 'a' * 64) for t in tasks))
        self.run_deploy()
        self.assertEqual(self.api.state['writes'], expected, 'unchanged redeploy must not register jobs')

    @unittest.skipUnless(shutil.which('anvil') and (ANSIBLE.parents[2] / 'target/debug/kardamom-deploy').exists(),
                         'anvil and a debug kardamom-deploy binary required')
    def test_settlement_bootstrap_and_repeat(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        anvil = subprocess.Popen(['anvil', '--port', str(port), '--silent'],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        def stop():
            anvil.terminate()
            anvil.wait(timeout=10)
        self.addCleanup(stop)
        rpc = f'http://127.0.0.1:{port}'
        def block_number():
            request = urllib.request.Request(rpc, data=json.dumps({
                'jsonrpc': '2.0', 'id': 1, 'method': 'eth_blockNumber', 'params': []}).encode(),
                headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(request, timeout=2) as response:
                return json.load(response)['result']
        for _ in range(50):
            try:
                block_number()
                break
            except OSError:
                time.sleep(0.1)
        settings = {'workloads_l1_rpc': rpc, 'workloads_settlement_address': '',
                    'workloads_deploy_binary': str(ANSIBLE.parents[2] / 'target/debug/kardamom-deploy')}
        self.run_deploy(settings)
        height = block_number()
        self.assertGreater(int(height, 16), 0)
        self.run_deploy(settings)
        self.assertEqual(block_number(), height, 'redeploy must not send another settlement transaction')

    def test_check_mode_never_registers(self):
        self.run_deploy(check=True)
        self.assertEqual(self.api.state['writes'], [])

    def test_bad_manifest_fails_before_any_job(self):
        self.manifest.write_text('ingress registry.example:5000/ingress:dev@sha256:bad\n')
        self.run_deploy(success=False)
        self.assertEqual(self.api.state['writes'], [])

    def test_signed_manifest_requires_complete_coverage(self):
        self.manifest.write_text('')
        self.run_deploy({'workloads_require_signed': True}, success=False)
        self.assertEqual(self.api.state['writes'], [])

    def test_missing_signature_stops_deploy(self):
        self.run_deploy({'workloads_require_signed': True, 'workloads_cosign_binary': '/usr/bin/false'}, success=False)
        self.assertEqual(self.api.state['writes'], [])

    def test_rejected_signature_stops_deploy(self):
        Path(str(self.manifest) + '.sigbundle').write_text('invalid bundle')
        self.run_deploy({'workloads_require_signed': True, 'workloads_cosign_binary': '/usr/bin/false'}, success=False)
        self.assertEqual(self.api.state['writes'], [])

    def test_optional_light_client_and_cluster_overrides(self):
        self.run_deploy({
            'workloads_light_execution_rpc': 'http://execution.example',
            'workloads_light_consensus_rpc': 'http://consensus.example',
            'workloads_light_checkpoint': '0x' + 'a' * 64,
            'workloads_lockbox_address': '0x' + '2' * 40,
            'workloads_namespace': 'staging',
            'workloads_cluster_retention': '8192',
            'workloads_cluster_snapshot_s': '60',
            'workloads_remote_origins': '412399',
        }, check=True)
        plans = self.api.state['plans']
        self.assertIn('l1-light-client', plans)
        self.assertTrue(all(job['Namespace'] == 'staging' for job in plans.values()))
        validator = json.dumps(plans['validator'])
        self.assertIn('http://192.168.56.61:8548', validator)
        self.assertNotIn('http://execution.example', validator)
        self.assertIn('8192', json.dumps(plans['cluster']))
        self.assertEqual(self.api.state['writes'], [])

    def test_outdated_deployer_fails_before_any_job(self):
        output = self.run_deploy({'workloads_settlement_address': '',
                                  'workloads_deploy_binary': '/usr/bin/true'}, success=False)
        self.assertIn('Rebuild kardamom-deploy', output)
        self.assertEqual(self.api.state['writes'], [])

    def test_old_replicas_cannot_satisfy_readiness(self):
        self.api.state['missing_replica'] = 'ingress'
        self.run_deploy(success=False)
        self.assertNotIn('executor', self.api.state['writes'])


if __name__ == '__main__':
    unittest.main()
