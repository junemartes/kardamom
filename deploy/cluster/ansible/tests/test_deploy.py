"""Exercise the actual playbook and Nomad HCL compiler against an isolated API.

Run with: python3 -m unittest discover -s deploy/cluster/ansible/tests -v
Requires ansible-playbook and nomad on PATH; never connects to a real cluster.
"""
import json
import os
import re
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
SERVICES = ['aeron', 'cluster', 'redis', 'sequencer', 'ingress', 'executor', 'validator', 'da-watcher',
            'batcher', 'state-mirror', 'notifier', 'da-store', 'canary']
# The images the manifest pins beyond the default deployment: the jobs a
# real L1 or the chaos-l1 shard adds.
MANIFEST = SERVICES + ['l1-indexer', 'l1-fault-proxy']
# The Nomad Variable a job's template renders into the task environment.
SECRET_PATH = re.compile(r'nomadVar "(nomad/jobs/[\w-]+)"')
# A secret no rendered job, job variable or play output may hold.
SENTINEL = 'SECRET-SENTINEL'
# The chain status of a chain that stands on nothing.
HEALTHY_CHAIN = {'roots': [], 'sealer': {'pause': None}, 'ingress': {'pause': None}, 'services': []}
# The format registry of the repository: the formats of every target.
FORMATS = (ANSIBLE.parents[2] / 'formats.toml').read_text()


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
        elif self.path == '/':
            # The ingress JSON-RPC: the chain status the release gate reads.
            assert body['method'] == 'kardamom_chainStatus', body
            self.respond({'jsonrpc': '2.0', 'id': body['id'], 'result': state.get('chain_status', HEALTHY_CHAIN)})
        elif self.path.startswith('/v1/jobs?'):
            assert body['EnforceIndex'] is True
            # A job that renders its Nomad Variable into the task
            # environment registers only after the role wrote it: a new
            # task blocks on a missing one.
            env_templates = [t['EmbeddedTmpl'] for g in body['Job']['TaskGroups'] for task in g['Tasks']
                             for t in task['Templates'] or [] if t['Envvars']]
            for path in SECRET_PATH.findall(''.join(env_templates)):
                assert path in state['variables'], f'{body["Job"]["ID"]} registers before {path}'
            self.register(body['Job'])
            state['writes'].append(body['Job']['ID'])
            # The sealer groups that carry the roll's marker (the new
            # retention, or the digest a rollback restores), per
            # registration: the staged roll adds one group per step.
            if body['Job']['ID'] == 'cluster':
                marker = state.get('roll_marker', 'retention=4096')
                state.setdefault('rolled', []).append(sorted(
                    g['Name'] for g in body['Job']['TaskGroups'] if marker in json.dumps(g)))
            self.respond({'JobModifyIndex': 1})
        elif self.path.startswith('/v1/deployment/promote/'):
            name = self.path.split('/')[4].split('?')[0]
            assert body['All'] is True
            state['deployments'][name].promote()
            self.respond({'EvalID': 'promoted'})
        elif self.path.split('?')[0].endswith('/revert'):
            # The revert registers the old version as a new one, and only
            # when the job is still at the version the caller names.
            name = self.path.split('/')[3]
            versions = state['versions'][name]
            assert body['JobID'] == name and isinstance(body['JobVersion'], int), body
            if body['EnforcePriorVersion'] != len(versions) - 1:
                self.send_response(400)
                self.end_headers()
                return
            self.register(versions[body['JobVersion']])
            state['writes'].append(f'revert:{name}:{body["JobVersion"]}')
            self.respond({'EvalID': 'reverted', 'JobModifyIndex': len(versions)})
        else:
            raise AssertionError(self.path)

    def register(self, job):
        """Store `job` as the current definition and as a new version."""
        state = self.server.state
        state['jobs'][job['ID']] = job
        state.setdefault('versions', {}).setdefault(job['ID'], []).append(job)
        if job['ID'] in state['deployments']:
            state['deployments'][job['ID']].registered(job)

    def version(self, name):
        return len(self.server.state['versions'][name]) - 1

    def auto_revert(self, name):
        """Whether a group of the job has auto_revert in its update stanza."""
        return any((g.get('Update') or {}).get('AutoRevert') for g in self.server.state['jobs'][name]['TaskGroups'])

    def do_HEAD(self):
        # The registry: the manifest of an image, by digest.
        assert self.path.startswith('/v2/') and '/manifests/sha256:' in self.path, self.path
        name = self.path.removeprefix('/v2/').split('/manifests/')[0]
        self.send_response(404 if name in self.server.state.get('missing_images', ()) else 200)
        self.end_headers()

    def do_PUT(self):
        # The bootstrap variable of a new sealer cluster, or the secrets
        # of a job.
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        path = self.path.split('?')[0].removeprefix('/v1/var/')
        assert self.path.startswith(('/v1/var/nomad/jobs/', '/v1/var/kardamom/deploys/')) and body['Path'] == path, (self.path, body)
        if path.startswith('kardamom/deploys/'):
            # The deploy record: two JSON documents in one variable, written
            # with a check-and-set on the modify index. A test clobbers the
            # record once to stand for another controller.
            assert set(body['Items']) <= {'attempt', 'accepted'}, body
            state = self.server.state
            index = state['record_index'].get(path, 0) + int(state.pop('record_clobber', False))
            if int(self.path.split('cas=')[1]) != index:
                self.send_response(409)
                self.end_headers()
                return
            state['records'][path] = {k: json.loads(v) for k, v in body['Items'].items()}
            state['record_index'][path] = index + 1
            state['record_writes'].append(state['records'][path]['attempt']['status'])
            self.respond(body | {'ModifyIndex': index + 1})
            return
        elif path == 'nomad/jobs/cluster':
            assert body['Items'] == {'bootstrap': 'true'}, body
            self.server.state['writes'].append('bootstrap-open')
        else:
            self.server.state['variables'][path] = body['Items']
            self.server.state['variable_writes'].append(path)
        self.respond(body)

    def do_DELETE(self):
        if self.path.startswith('/v1/job/'):
            # The stop of a job that a rolled-back release added.
            name = self.path.split('/')[3].split('?')[0]
            del self.server.state['jobs'][name]
            self.server.state['writes'].append(f'stop:{name}')
            self.respond({'EvalID': 'stopped'})
            return
        assert self.path.startswith('/v1/var/nomad/jobs/cluster?'), self.path
        self.server.state['writes'].append('bootstrap-close')
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        parts = self.path.split('?')[0].split('/')
        parts += [''] * (6 - len(parts))
        state = self.server.state
        if parts[2] == 'var':
            path = '/'.join(self.path.split('?')[0].split('/')[3:])
            if path.startswith('kardamom/deploys/') and path in state['records']:
                self.respond({'Path': path, 'ModifyIndex': state['record_index'][path],
                              'Items': {k: json.dumps(v) for k, v in state['records'][path].items()}})
            elif path in state['variables']:
                self.respond({'Path': path, 'Items': state['variables'][path]})
            else:
                self.send_response(404)
                self.end_headers()
        elif parts[2] == 'jobs':
            # The job list stubs of Nomad carry the modify indexes and no
            # version; only a job's own record has one.
            self.respond([{'ID': name, 'Name': name, 'Type': job['Type'], 'Status': 'running',
                           'JobModifyIndex': 1, 'ModifyIndex': 1} for name, job in state['jobs'].items()])
        elif parts[2] == 'deployment' and parts[3] == 'allocations':
            allocs = self.allocations(parts[4])
            allocs[0]['DeploymentStatus']['Canary'] = True
            self.respond(allocs)
        elif parts[2] == 'deployment':
            # The deployment's verdict, by the job's scripted outcome. A
            # failed deployment of a job with auto_revert makes Nomad
            # register the previous version again, as a new version; a
            # job with no previous version stays where it is.
            deployment = state['deployments'][parts[3]]
            verdict = deployment.state()
            if (verdict['Status'] == 'failed' and not deployment.reverted and self.auto_revert(parts[3])
                    and self.version(parts[3]) > 0):
                deployment.reverted = True
                self.register(state['versions'][parts[3]][-2])
                state['writes'].append(f'auto-revert:{parts[3]}')
            self.respond(verdict)
        elif parts[2] == 'node':
            # Every node advertises an address of this fake API, so the
            # member status and the smoke target resolve back here. A
            # sealer node gets its own loopback address, so the status
            # request names the member through the Host header.
            self.respond({'ID': parts[3], 'HTTPAddr': f'{self.node_ip(parts[3])}:{self.server.server_port}'})
        elif parts[1] == 'status':
            # The sealer member status of the node the Host header names;
            # member 0 leads unless the test says otherwise.
            octet = int(self.headers['Host'].split(':')[0].rsplit('.', 1)[1])
            node = f'node-cluster-{octet - 1}'
            default = 'LEADER' if node.endswith('-0') else 'FOLLOWER'
            self.respond({'memberId': octet - 1, 'role': state['roles'].get(node, default), 'election': 'CLOSED'})
        elif parts[2] == 'job' and parts[4] == 'deployment':
            # A job with a scripted deployment gets one; every other job
            # has none, like a system job.
            deployment = state['deployments'].get(parts[3])
            self.respond(deployment.state(poll=False) if deployment else None)
        elif parts[2] == 'job' and parts[4] == 'allocations':
            self.respond(self.allocations(parts[3]) if parts[3] in state['jobs'] else [])
        elif parts[2] == 'job' and parts[4] == 'versions':
            # Newest first, without the versions the test says Nomad dropped.
            dropped = state.get('dropped', {}).get(parts[3], [])
            versions = [job | {'Version': i} for i, job in enumerate(state['versions'][parts[3]]) if i not in dropped]
            self.respond({'Versions': versions[::-1]})
        elif parts[2] == 'job' and parts[3] in state['jobs']:
            self.respond(state['jobs'][parts[3]] | {'Version': self.version(parts[3])})
        elif parts[2] == 'job':
            self.send_response(404)
            self.end_headers()
        else:
            raise AssertionError(self.path)

    @staticmethod
    def node_ip(node):
        if node.startswith('node-cluster-'):
            return f'127.0.0.{int(node.rsplit("-", 1)[1]) + 1}'
        return '127.0.0.1'

    def allocations(self, name):
        state = self.server.state
        allocs = []
        job = state['jobs'][name]
        for group in job['TaskGroups']:
            # The scheduler places a system group on the nodes its
            # constraints admit; the playbook must not assume a node count.
            count = 8 if job['Type'] == 'system' else group['Count']
            for i in range(count):
                # Historical/stopping allocations must not satisfy readiness.
                stale = state.get('missing_replica') == name and i > 0
                # A job the test names has no running allocation: a crash loop.
                running = name not in state.get('not_running', ())
                allocs.append({'TaskGroup': group['Name'], 'JobVersion': self.version(name) - int(stale),
                               'DesiredStatus': 'run', 'ClientStatus': 'running' if running else 'pending',
                               'NodeID': f'node-{group["Name"]}',
                               'DeploymentStatus': {'Canary': False}})
        return allocs


class Deployment:
    """A scripted Nomad deployment of one job: its ID is the job name.

    `outcomes` is the status sequence the polls read, one per poll, and
    the last one repeats. A `canary` deployment reports one healthy,
    unpromoted canary until the role promotes it; every poll after the
    promotion reads `successful`.
    """

    def __init__(self, name, outcomes, canary=False):
        self.name = name
        self.outcomes = list(outcomes)
        self.canary = canary
        self.promoted = False
        self.reverted = False
        self.polls = 0
        self.groups = ['group']

    def registered(self, job):
        self.groups = [g['Name'] for g in job['TaskGroups']]

    def promote(self):
        assert self.canary and not self.promoted, 'promotion of a deployment without a waiting canary'
        self.promoted = True

    def state(self, poll=True):
        if self.canary and not self.promoted:
            status = 'running'
        elif self.canary:
            status = 'successful'
        else:
            status = self.outcomes[min(self.polls, len(self.outcomes) - 1)]
            self.polls += int(poll)
        groups = {g: {'DesiredCanaries': 1 if self.canary else 0, 'Promoted': self.promoted,
                      'HealthyAllocs': 1, 'DesiredTotal': 2} for g in self.groups}
        return {'ID': self.name, 'JobVersion': 1, 'Status': status,
                'StatusDescription': f'scripted {status}', 'TaskGroups': groups}


@unittest.skipUnless(shutil.which('nomad') and shutil.which('ansible-playbook'),
                     'nomad and ansible-playbook required')
class Deploys(unittest.TestCase):
    """The fixtures of every deploy test: the fake API, the manifest and the
    operator binary. The test classes below split the suite so CI runs them
    in parallel processes."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='kardamom-deploy-test-')
        self.addCleanup(self.tmp.cleanup)
        self.manifest = Path(self.tmp.name) / 'images.digests'
        self.manifest.write_text(''.join(
            f'{s} registry.example:5000/kardamom-{s}:test@sha256:{"a" * 64}\n' for s in MANIFEST))
        # Every loopback address, so the sealer nodes' 127.0.0.<n> resolve here.
        self.api = ThreadingHTTPServer(('0.0.0.0', 0), NomadAPI)
        self.api.state = {'jobs': {}, 'writes': [], 'deployments': {}, 'roles': {},
                          'variables': {}, 'variable_writes': [], 'records': {}, 'record_index': {},
                          'record_writes': []}
        self.record_dir = Path(self.tmp.name) / 'deployed'
        # The operator binary: the smoke, and the format findings of the
        # release gate, which a test scripts in `smoke.sh.findings`.
        self.smoke = Path(self.tmp.name) / 'smoke.sh'
        self.smoke.write_text('#!/bin/sh\necho "$@" >> "$0.calls"\n'
                              'if [ "$1" = formats ]; then cat "$0.findings" 2>/dev/null || echo "[]"; fi\n')
        self.smoke.chmod(0o755)
        self.thread = threading.Thread(target=self.api.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.api.server_close)
        self.addCleanup(self.api.shutdown)

    def run_deploy(self, extra=None, check=False, success=True, playbook="deploy.yml", environ=None):
        variables = {
            'workloads_nomad_addr': f'http://127.0.0.1:{self.api.server_port}',
            'workloads_manifest': str(self.manifest),
            'workloads_settlement_address': '0x' + '1' * 40,
            'workloads_require_signed': False,
            'workloads_poll_retries': 1,
            'workloads_poll_delay': 0,
            'workloads_light_execution_rpc': '',
            'workloads_light_consensus_rpc': '',
            'workloads_da_proxy_probe': False,
            'workloads_deployment_retries': 3,
            'workloads_record_dir': str(self.record_dir),
            'workloads_cluster_binary': str(self.smoke),
            'workloads_sealer_admin_port': self.api.server_port,
            'workloads_cluster_bootstrap': False,
            'workloads_ingress_rpc_port': self.api.server_port,
            'workloads_registry_url': f'http://127.0.0.1:{self.api.server_port}',
            'workloads_revision': 'rev-test',
            'workloads_operator': 'tester',
        } | (extra or {})
        env = {k: v for k, v in os.environ.items() if not k.startswith(('ANSIBLE_', 'NOMAD_'))}
        env.update(ANSIBLE_NOCOLOR='1', ANSIBLE_STDOUT_CALLBACK='default',
                   ANSIBLE_LOCAL_TEMP=self.tmp.name + '/ansible',
                   OBJC_DISABLE_INITIALIZE_FORK_SAFETY='YES')
        env.pop('AERON_STALL_TOLERANCE_MS', None)
        env.update(environ or {})
        cmd = ['ansible-playbook', '-i', 'localhost,', str(ANSIBLE / playbook),
               '-e', json.dumps(variables)] + (['--check'] if check else [])
        result = subprocess.run(cmd, cwd=ANSIBLE.parent, env=env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=900)
        self.assertEqual(result.returncode == 0, success, result.stdout)
        return result.stdout

    # The jobs a manifest with new digests changes, in deploy order: every
    # pinned image, and not anvil or the monitoring.
    REPINNED = ['aeron', 'cluster', 'sequencer', 'redis', 'ingress', 'executor', 'state-mirror', 'notifier',
                'validator', 'da-watcher', 'da-store', 'batcher']

    def record(self):
        return self.api.state['records']['kardamom/deploys/local']

    def repin(self, digest):
        """Point every manifest line at `digest`: a new release."""
        self.manifest.write_text(self.manifest.read_text().replace(
            re.search(r'@sha256:([0-9a-f]{64})', self.manifest.read_text()).group(1), digest * 64))

    def run_rollback(self, success=True, environ=None):
        return self.run_deploy(playbook='rollback.yml', success=success, environ=environ)

    def gate_formats(self, findings):
        """Deploy once, then script the format findings of the next deploy."""
        self.run_deploy()
        self.api.state['writes'] = []
        self.api.state['record_writes'] = []
        Path(str(self.smoke) + '.findings').write_text(json.dumps(findings))
        self.repin('b')


class DeployTest(Deploys):
    """The deploy: order, pins, secrets, waits, canary, sealer roll."""

    def test_deploy_order_pinning_and_repeat(self):
        # The local canary runs only on request: the CI shards count
        # transactions.
        local_canary = {'CANARY_LOCAL': '1'}
        self.run_deploy(environ=local_canary)
        self.assertIn('-Daeron.archive.file.sync.level=1', json.dumps(self.api.state['jobs']['aeron']))
        expected = ['aeron', 'anvil', 'cluster', 'sequencer', 'redis', 'ingress', 'executor',
                    'state-mirror', 'notifier', 'validator', 'da-watcher', 'node-exporter', 'monitoring',
                    'da-store', 'l1-indexer', 'batcher', 'canary']
        self.assertEqual(self.api.state['writes'], expected)
        exporter = self.api.state['jobs']['node-exporter']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertIn('--collector.disable-defaults', exporter, 'the local profile skips the host hardware collectors')
        for name in SERVICES:
            tasks = [t for g in self.api.state['jobs'][name]['TaskGroups'] for t in g['Tasks']]
            self.assertTrue(all(t['Config']['image'].endswith('@sha256:' + 'a' * 64) for t in tasks))
        # Without a real L1, the batcher and the L1 follower get the
        # in-cluster anvil, and the batcher the anvil dev key. The
        # da-watcher reads no L1, so it has no secret.
        variables = self.api.state['variables']
        self.assertEqual(variables['nomad/jobs/l1-indexer'], {'KARDAMOM_L1_RPC': 'http://anvil.service.consul:8546'})
        self.assertNotIn('nomad/jobs/da-watcher', variables)
        self.assertEqual(variables['nomad/jobs/batcher']['KARDAMOM_L1_RPC'], 'http://anvil.service.consul:8546')
        self.assertEqual(variables['nomad/jobs/batcher']['KARDAMOM_L1_KEY'][:10], '0x5de4111a')
        self.assertEqual(variables['nomad/jobs/canary'], {
            'KARDAMOM_CANARY_MNEMONIC': 'test test test test test test test test test test test junk'})
        canary = self.api.state['jobs']['canary']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertEqual(str(canary[canary.index('--ring-offset') + 1]), '34')
        self.assertEqual(sorted(self.api.state['variable_writes']),
                         ['nomad/jobs/batcher', 'nomad/jobs/canary', 'nomad/jobs/l1-indexer'])
        self.run_deploy(environ=local_canary)
        self.assertEqual(self.api.state['writes'], expected, 'unchanged redeploy must not register jobs')
        self.assertEqual(len(self.api.state['variable_writes']), 3, 'unchanged redeploy must not write secrets')

    def test_the_local_profile_runs_no_canary_by_default(self):
        self.run_deploy(check=True)
        self.assertNotIn('canary', self.api.state['plans'])

    def test_secrets_reach_tasks_only_through_nomad_variables(self):
        # Every keyed URL and key the deploy gets, by the environment the
        # deploy workflow sets.
        l1 = f'https://l1.example/v3/{SENTINEL}-L1'
        followers = f'https://a.example/v2/{SENTINEL}-A,https://b.example/v3/{SENTINEL}-B'
        key = f'0x{SENTINEL}-KEY'
        # An Alertmanager configuration with a receiver token, and its own
        # Go templates, which must arrive as data.
        alertmanager = Path(self.tmp.name) / 'alertmanager.yml'
        alertmanager.write_text(
            'route:\n  receiver: telegram\nreceivers:\n  - name: telegram\n    telegram_configs:\n'
            f'      - bot_token: "{SENTINEL}-BOT"\n        chat_id: 1\n'
            '        message: \'{{ .CommonLabels.alertname }} {{ range .Alerts }}{{ .Annotations.runbook }}{{ end }}\'\n')
        output = self.run_deploy(environ={
            'L1_RPC': l1, 'L1_FOLLOWERS_RPC': followers, 'BATCHER_KEY': key,
            'L1_OWNER_KEY': f'0x{SENTINEL}-OWNER', 'EIGENDA_NETWORK': 'sepolia_testnet',
            'CANARY_MNEMONIC': f'{SENTINEL}-MNEMONIC',
            'ALERTMANAGER_CONFIG_FILE': str(alertmanager)})
        self.assertNotIn(SENTINEL, output)
        state = self.api.state
        for name, job in [*state['plans'].items(), *state['jobs'].items()]:
            self.assertNotIn(SENTINEL, json.dumps(job), name)
        monitoring = state['variables'].pop('nomad/jobs/monitoring')
        self.assertEqual(monitoring['rules'], 'groups: []')
        self.assertTrue(monitoring['alertmanager'].startswith(alertmanager.read_text()), monitoring['alertmanager'].replace(SENTINEL, '<S>'))
        self.assertIn((ANSIBLE.parents[1] / 'alertmanager-inhibit.yml').read_text(), monitoring['alertmanager'])
        self.assertEqual(state['variables'], {
            'nomad/jobs/batcher': {'KARDAMOM_L1_RPC': l1, 'KARDAMOM_L1_KEY': key},
            'nomad/jobs/canary': {'KARDAMOM_CANARY_MNEMONIC': f'{SENTINEL}-MNEMONIC'},
            'nomad/jobs/da-proxy': {'EIGENDA_PROXY_EIGENDA_V2_ETH_RPC': l1,
                                    'EIGENDA_PROXY_EIGENDA_V2_SIGNER_PRIVATE_KEY_HEX': key},
            'nomad/jobs/l1-indexer': {'KARDAMOM_L1_RPC': followers},
        })
        # Each job renders its own variable into the task environment.
        for path in list(state['variables']):
            job = state['jobs'][path.rsplit('/', 1)[1]]
            templates = [t for g in job['TaskGroups'] for task in g['Tasks'] for t in task['Templates'] or []]
            self.assertIn(f'nomadVar "{path}"', ''.join(t['EmbeddedTmpl'] for t in templates if t['Envvars']), path)

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
            'workloads_light_execution_rpc': f'https://execution.example/{SENTINEL}',
            'workloads_light_consensus_rpc': f'https://consensus.example/{SENTINEL}',
            'workloads_light_checkpoint': '0x' + 'a' * 64,
            'workloads_lockbox_address': '0x' + '2' * 40,
            'workloads_namespace': 'staging',
            'workloads_cluster_retention': '8192',
            'workloads_cluster_snapshot_s': '60',
            'workloads_cluster_log_purge_keep': '5',
            'workloads_cluster_file_sync_level': '2',
            'workloads_archive_file_sync_level': '0',
            'workloads_remote_origins': '412399',
            'workloads_priority_fees': 'on',
        })
        plans = self.api.state['plans']
        self.assertIn('l1-light-client', plans)
        self.assertNotIn(SENTINEL, json.dumps(plans))
        self.assertNotIn(SENTINEL, json.dumps(self.api.state['jobs']))
        self.assertEqual(self.api.state['variables']['nomad/jobs/l1-light-client'], {
            'HELIOS_EXECUTION_RPC': f'https://execution.example/{SENTINEL}',
            'HELIOS_CONSENSUS_RPC': f'https://consensus.example/{SENTINEL}',
        })
        self.assertTrue(all(job['Namespace'] == 'staging' for job in plans.values()))
        validator = json.dumps(plans['validator'])
        self.assertIn('http://kardamom-l1-light-client.service.dc1.consul:8548', validator)
        self.assertNotIn('http://execution.example', validator)
        # The indexer follows the light client and reads the payloads from
        # the DA proxy; the batcher resumes from the indexer.
        self.assertEqual(self.api.state['variables']['nomad/jobs/l1-indexer'],
                         {'KARDAMOM_L1_RPC': 'http://kardamom-l1-light-client.service.dc1.consul:8548',
                          'KARDAMOM_BEACON_API': f'https://consensus.example/{SENTINEL}'})
        self.assertIn('http://kardamom-da-proxy.service.consul:3100', json.dumps(plans['l1-indexer']))
        follower = plans['l1-indexer']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertNotIn('--beacon-api', follower)
        self.assertIn('http://kardamom-l1-indexer.service.dc1.consul:8549', json.dumps(plans['batcher']))
        self.assertIn('8192', json.dumps(plans['cluster']))
        self.assertIn('-Dkardamom.cluster.fileSyncLevel=2', json.dumps(plans['cluster']))
        self.assertIn('-Dkardamom.cluster.logPurgeKeepSnapshots=5', json.dumps(plans['cluster']))
        self.assertIn('-Daeron.archive.file.sync.level=0', json.dumps(plans['aeron']))
        self.assertIn('-Daeron.archive.catalog.file.sync.level=0', json.dumps(plans['aeron']))
        # One value turns priority fees on for every role that has a say.
        self.assertIn('-Dkardamom.cluster.orderingWindow=20', json.dumps(plans['cluster']))
        self.assertEqual(self.sequencer_env(plans)['KARDAMOM_PRIORITY_FEES'], 'true')
        for name in ('executor', 'validator'):
            self.assertIn('base_fee_initial', self.genesis_template(plans[name]), name)

    def test_fault_proxy_routes_the_followers_through_it(self):
        self.run_deploy({'workloads_l1_fault_proxy': True, 'workloads_indexer_poll_s': '2'})
        plans = self.api.state['plans']
        proxy = 'http://kardamom-l1-fault-proxy.service.dc1.consul:8547'
        self.assertIn('http://anvil.service.consul:8546', json.dumps(plans['l1-fault-proxy']))
        anvil = plans['anvil']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertEqual(anvil[anvil.index('--slots-in-an-epoch') + 1], '1')
        variables = self.api.state['variables']
        self.assertEqual(variables['nomad/jobs/batcher']['KARDAMOM_L1_RPC'], proxy)
        # The follower reads the proxy and its second source, so a lie of
        # the first is a disagreement.
        self.assertEqual(variables['nomad/jobs/l1-indexer']['KARDAMOM_L1_RPC'], f'{proxy},{proxy}/second')
        indexer = plans['l1-indexer']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertEqual(indexer[indexer.index('--poll-interval-secs') + 1], '2')
        self.assertEqual(indexer[indexer.index('--start-block') + 1], '1')
        self.assertEqual(indexer[indexer.index('--lockbox') + 1], '0x' + '0' * 40)
        self.assertIn('http://kardamom-l1-indexer.service.dc1.consul:8549', json.dumps(plans['batcher']))

    def test_the_follower_runs_twice_and_records_its_stream(self):
        # Two instances on two nodes, each on the node's Aeron driver and
        # recording l1_blocks; anvil has no beacon chain, so no schedule.
        self.run_deploy({'workloads_l1_fault_proxy': True}, check=True)
        group = self.api.state['plans']['l1-indexer']['TaskGroups'][0]
        self.assertEqual(group['Count'], 2)
        job = self.api.state['plans']['l1-indexer']
        constraints = job.get('Constraints') or []
        self.assertIn('distinct_hosts', json.dumps(constraints))
        task = group['Tasks'][0]
        args = task['Config']['args']
        self.assertIn('--archive-durability', args)
        self.assertEqual(args[args.index('--log-config') + 1], '/local/channels.toml')
        self.assertEqual(args[args.index('--aeron-dir') + 1], '/opt/kardamom/aeron-mount/dir')
        self.assertNotIn('--beacon-api', args)
        self.assertIn('/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount', task['Config']['volumes'])

    def test_the_follower_takes_its_count_and_its_log_range(self):
        self.run_deploy({'workloads_l1_fault_proxy': True, 'workloads_indexer_count': '1',
                         'workloads_indexer_max_log_range': '2000'}, check=True)
        group = self.api.state['plans']['l1-indexer']['TaskGroups'][0]
        self.assertEqual(group['Count'], 1)
        args = group['Tasks'][0]['Config']['args']
        self.assertEqual(args[args.index('--max-log-range') + 1], '2000')

    def test_the_da_watcher_reads_the_follower_stream_with_no_l1_access(self):
        # The watcher has no L1 endpoint; its history reads replay the
        # follower archives onto its own ports and fall back to the
        # follower's API.
        self.run_deploy({'workloads_l1_fault_proxy': True}, check=True)
        plans = self.api.state['plans']
        task = plans['da-watcher']['TaskGroups'][0]['Tasks'][0]
        args = task['Config']['args']
        self.assertIn('--l1-blocks', args)
        self.assertNotIn('--l1-rpc', args)
        self.assertNotIn('kardamom-l1-fault-proxy', json.dumps(plans['da-watcher']))
        self.assertEqual(args[args.index('--indexer-url') + 1], 'http://kardamom-l1-indexer.service.consul:8549')
        self.assertIn('--replay-destination-endpoint', args)
        self.assertIn('--archive-control-response-endpoint', args)
        self.assertIn('l1-indexer', plans)

    def test_every_deployment_runs_the_follower_and_anvil_reads_every_second(self):
        self.run_deploy(check=True)
        follower = self.api.state['plans']['l1-indexer']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertEqual(follower[follower.index('--poll-interval-secs') + 1], '1')

    def test_the_da_watcher_keeps_its_l1_cursor_on_the_node(self):
        # A restart resumes after the last published L1 block only when the
        # cursor file outlives the container.
        self.run_deploy(check=True)
        task = self.api.state['plans']['da-watcher']['TaskGroups'][0]['Tasks'][0]
        self.assertIn('/opt/kardamom/da-watcher:/opt/kardamom/da-watcher', task['Config']['volumes'])
        args = task['Config']['args']
        self.assertEqual(args[args.index('--l1-cursor-file') + 1], '/opt/kardamom/da-watcher/l1-cursor')
        self.assertEqual(self.api.state['writes'], [])

    def test_the_da_watcher_follows_the_sealer_boundaries(self):
        # The watcher confirms its epochs by the sealer's boundaries, so it
        # needs a cluster session: the [cluster] config and its own egress.
        self.run_deploy(check=True)
        group = self.api.state['plans']['da-watcher']['TaskGroups'][0]
        task = group['Tasks'][0]
        args = task['Config']['args']
        self.assertEqual(args[args.index('--config') + 1], '/local/da-watcher.toml')
        self.assertEqual(args[args.index('--cluster-egress-endpoint') + 1],
                         '${meta.node_ip}:${NOMAD_HOST_PORT_egress}')
        templates = {t['DestPath']: t['EmbeddedTmpl'] for t in task['Templates']}
        self.assertIn('[cluster]', templates['local/da-watcher.toml'])
        self.assertIn('sealer-0.node.consul', templates['local/da-watcher.toml'])
        ports = [p['Label'] for n in group['Networks'] for p in n.get('DynamicPorts') or []]
        self.assertIn('egress', ports)
        self.assertEqual(self.api.state['writes'], [])

    def test_priority_fees_default_off_on_every_role(self):
        self.run_deploy(check=True)
        plans = self.api.state['plans']
        self.assertIn('-Dkardamom.cluster.orderingWindow=0', json.dumps(plans['cluster']))
        self.assertEqual(self.sequencer_env(plans)['KARDAMOM_PRIORITY_FEES'], 'false')
        for name in ('executor', 'validator'):
            self.assertNotIn('base_fee_initial', self.genesis_template(plans[name]), name)

    def test_the_exec_cursor_switch_defaults_off_and_reaches_the_executor(self):
        for switch, expected in (('', 'false'), ('on', 'true')):
            with self.subTest(switch=switch):
                self.run_deploy({'workloads_exec_cursor': switch}, check=True)
                env = self.api.state['plans']['executor']['TaskGroups'][0]['Tasks'][0]['Env']
                self.assertEqual(env['KARDAMOM_EXEC_CURSOR'], expected)

    def test_every_aeron_party_takes_the_stall_tolerance(self):
        for tolerance in (10000, 30000):
            with self.subTest(tolerance=tolerance):
                environ = {} if tolerance == 10000 else {'AERON_STALL_TOLERANCE_MS': str(tolerance)}
                self.run_deploy(check=True, environ=environ)
                self.assert_aeron_parties(self.api.state['plans'], tolerance)

    def assert_aeron_parties(self, plans, tolerance):
        """Every task that maps the Aeron directory carries the tolerance:
        a Rust client as its driver timeout, a JVM with a media driver as
        its driver timeout, client liveness, and a publication unblock
        timeout above the liveness."""
        jvm_options = {'aeron': '_JAVA_OPTIONS', 'cluster': 'JAVA_TOOL_OPTIONS'}
        parties = [(name, task) for name, job in plans.items() for group in job['TaskGroups']
                   for task in group['Tasks'] if 'aeron-mount' in json.dumps(task['Config'].get('volumes', []))]
        self.assertEqual({name for name, _ in parties}, {
            'aeron', 'cluster', 'sequencer', 'ingress', 'executor', 'validator', 'da-watcher', 'batcher',
            'state-mirror', 'notifier', 'l1-indexer'})
        liveness_ns = tolerance * 1_000_000
        for name, task in parties:
            if name not in jvm_options:
                self.assertEqual(task['Env']['AERON_DRIVER_TIMEOUT'], str(tolerance), name)
                continue
            options = task['Env'][jvm_options[name]].split()
            self.assertIn(f'-Daeron.driver.timeout={tolerance}', options, name)
            self.assertIn(f'-Daeron.client.liveness.timeout={liveness_ns}', options, name)
            unblock = [o for o in options if o.startswith('-Daeron.publication.unblock.timeout=')]
            self.assertEqual(len(unblock), 1, name)
            self.assertGreater(int(unblock[0].split('=')[1]), liveness_ns, name)
        # A sealer member loads its snapshots at a start through the
        # archive waits, so they scale with the tolerance too.
        cluster_options = dict(parties)['cluster']['Env']['JAVA_TOOL_OPTIONS'].split()
        self.assertIn(f'-Daeron.archive.message.timeout={tolerance}ms', cluster_options)
        self.assertIn(f'-Daeron.archive.connect.timeout={tolerance // 2}ms', cluster_options)
        # A restarted driver waits out the active-driver window of its
        # dead predecessor: Nomad's 15 s default at the 10 s tolerance.
        delay_ns = plans['aeron']['TaskGroups'][0]['RestartPolicy']['Delay']
        self.assertEqual(delay_ns, (tolerance // 1000 + 5) * 1_000_000_000)

    @staticmethod
    def sequencer_env(plans):
        return plans['sequencer']['TaskGroups'][0]['Tasks'][0]['Env']

    @staticmethod
    def genesis_template(job):
        templates = [t for g in job['TaskGroups'] for t in g['Tasks'][0]['Templates']
                     if t['DestPath'] == 'local/genesis.toml']
        return templates[0]['EmbeddedTmpl']

    def test_resize_reuses_deployment_inputs_and_image_pins(self):
        settings = {
            'workloads_resize_job': 'sequencer',
            'workloads_resize_vars': {'shard_table': json.dumps([v % 3 for v in range(256)])},
            'tx_ttl_ms': 45001,
            'datacenter': 'routing-test',
        }
        self.run_deploy(settings, playbook='resize.yml')
        job = self.api.state['jobs']['sequencer']
        self.assertEqual(job['Datacenters'], ['routing-test'])
        self.assertEqual(len(job['TaskGroups']), 3)
        for group in job['TaskGroups']:
            task = group['Tasks'][0]
            self.assertTrue(task['Config']['image'].endswith('@sha256:' + 'a' * 64))
            args = task['Config']['args']
            self.assertEqual(args[args.index('--tx-ttl-ms') + 1], '45001')
        self.run_deploy(settings, playbook='resize.yml')
        self.assertEqual(self.api.state['writes'], ['sequencer'])
        self.run_deploy({'workloads_resize_job': 'ingress', 'tx_ttl_ms': 45001}, playbook='resize.yml')
        task = self.api.state['jobs']['ingress']['TaskGroups'][0]['Tasks'][0]
        args = task['Config']['args']
        self.assertEqual(args[args.index('--pending-receipt-timeout-ms') + 1], '45001')
        self.assertEqual(task['KillTimeout'], 56_000_000_000)

    def test_resize_rejects_an_unverified_manifest_before_submission(self):
        self.manifest.write_text('')
        self.run_deploy({'workloads_resize_job': 'sequencer', 'workloads_require_signed': True},
                        playbook='resize.yml', success=False)
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

    def test_a_deployment_is_waited_to_its_verdict(self):
        self.api.state['deployments']['ingress'] = Deployment('ingress', ['running', 'running', 'successful'])
        self.run_deploy()
        self.assertEqual(self.api.state['deployments']['ingress'].polls, 3)
        self.assertIn('executor', self.api.state['writes'])

    def test_a_failed_deployment_stops_the_deploy_at_its_job(self):
        self.api.state['deployments']['ingress'] = Deployment('ingress', ['running', 'failed'])
        output = self.run_deploy(success=False)
        self.assertIn('scripted failed', output)
        self.assertNotIn('executor', self.api.state['writes'])
        self.assertFalse((self.record_dir / 'images.digests').exists(), 'a failed deploy records nothing')

    def test_an_undecided_deployment_fails_when_the_budget_runs_out(self):
        self.api.state['deployments']['executor'] = Deployment('executor', ['running'])
        self.run_deploy(success=False)
        self.assertNotIn('batcher', self.api.state['writes'])

    def test_a_canary_is_smoked_by_its_node_then_promoted(self):
        self.api.state['deployments']['ingress'] = Deployment('ingress', [], canary=True)
        self.run_deploy()
        self.assertTrue(self.api.state['deployments']['ingress'].promoted)
        calls = Path(str(self.smoke) + '.calls').read_text().splitlines()
        self.assertEqual(calls, ['smoke --rpc http://127.0.0.1:8545'])

    def test_a_failed_smoke_leaves_the_canary_unpromoted(self):
        self.api.state['deployments']['sequencer'] = Deployment('sequencer', [], canary=True)
        self.run_deploy({'workloads_cluster_binary': '/usr/bin/false'}, success=False)
        self.assertFalse(self.api.state['deployments']['sequencer'].promoted)
        self.assertNotIn('batcher', self.api.state['writes'])

    def test_a_successful_deploy_records_the_manifest_and_keeps_the_previous_one(self):
        self.run_deploy()
        first = self.manifest.read_text()
        self.assertEqual((self.record_dir / 'images.digests').read_text(), first)
        self.assertFalse((self.record_dir / 'images.digests.previous').exists())
        self.manifest.write_text(first.replace('a' * 64, 'b' * 64))
        self.run_deploy()
        self.assertEqual((self.record_dir / 'images.digests').read_text(), self.manifest.read_text())
        self.assertEqual((self.record_dir / 'images.digests.previous').read_text(), first)



class RecordTest(Deploys):
    """The deploy record and the release gate."""

    def test_the_record_follows_the_attempt_to_the_accepted_release(self):
        self.run_deploy()
        attempt = self.record()['attempt']
        self.assertEqual(self.record()['accepted'], attempt)
        self.assertEqual(attempt['status'], 'accepted')
        self.assertEqual((attempt['env'], attempt['operator'], attempt['target']['revision']), ('local', 'tester', 'rev-test'))
        self.assertEqual(attempt['target']['formats'], FORMATS)
        self.assertEqual(attempt['target']['manifest']['ingress'], f'registry.example:5000/kardamom-ingress:test@sha256:{"a" * 64}')
        self.assertEqual(attempt['changed'], self.api.state['writes'])
        # Every job was new: no version before, version 0 after.
        self.assertEqual(attempt['before']['jobs']['ingress'], {'before': None, 'after': 0})
        self.assertEqual((attempt['before']['manifest'], attempt['floor']), ({}, {}))
        # The record is written at the start, before and after each
        # registration, and at the acceptance; the mirror follows it.
        self.assertEqual(self.api.state['record_writes'], ['started'] * (2 * len(self.api.state['writes']) + 1) + ['accepted'])
        self.assertEqual(json.loads((self.record_dir / 'attempt.json').read_text()), attempt)
        self.assertEqual(json.loads((self.record_dir / 'accepted.json').read_text()), attempt)
        self.assertEqual((self.record_dir / 'accepted.formats.toml').read_text(), FORMATS)
        # A new release records the versions before it, and only the jobs it changed.
        self.repin('b')
        self.api.state['writes'] = []
        self.run_deploy()
        attempt = self.record()['attempt']
        self.assertEqual(attempt['changed'], self.REPINNED)
        self.assertEqual(attempt['before']['jobs']['ingress'], {'before': 0, 'after': 1})
        self.assertEqual(attempt['before']['jobs']['cluster'], {'before': 0, 'after': 3}, 'the sealer roll registers three versions')
        self.assertEqual(attempt['before']['jobs']['anvil'], {'before': 0})
        self.assertEqual(attempt['before']['manifest']['ingress'], f'registry.example:5000/kardamom-ingress:test@sha256:{"a" * 64}')

    def test_a_failed_attempt_stays_distinct_from_the_accepted_release(self):
        self.run_deploy()
        accepted = self.record()['accepted']
        self.repin('b')
        self.api.state['deployments']['ingress'] = Deployment('ingress', ['failed'])
        self.run_deploy(success=False)
        attempt = self.record()['attempt']
        self.assertEqual(attempt['status'], 'started')
        self.assertEqual(attempt['changed'], ['aeron', 'cluster', 'sequencer', 'redis', 'ingress'])
        self.assertEqual(self.record()['accepted'], accepted, 'a failed attempt is never the accepted release')
        self.assertEqual(json.loads((self.record_dir / 'accepted.json').read_text()), accepted)
        # The attempt is still started: it runs, or it died. Only the
        # operator can tell, so a deploy over it names that choice.
        del self.api.state['deployments']['ingress']
        self.api.state['writes'] = []
        output = self.run_deploy(success=False)
        self.assertIn(f"the attempt of {attempt['started_at']} by tester is still started", output)
        self.assertEqual(self.api.state['writes'], [])
        self.run_deploy(environ={'KARDAMOM_REPLACE_ATTEMPT': '1'})
        self.assertEqual(self.record()['attempt']['status'], 'accepted')
        self.assertEqual(self.record()['attempt']['before']['jobs']['ingress']['before'], 2,
                         'the record holds the mixed versions that ran: the failed release of the ingress')

    def test_a_record_that_another_controller_changed_stops_the_deploy(self):
        self.run_deploy()
        self.repin('b')
        self.api.state['writes'] = []
        self.api.state['record_clobber'] = True
        output = self.run_deploy(success=False)
        self.assertIn('changed under this run', output)
        self.assertEqual(self.api.state['writes'], [], 'the first write of the attempt is the check-and-set that fails')

    def test_a_halted_chain_refuses_the_release(self):
        self.run_deploy()
        self.api.state['writes'] = []
        self.api.state['record_writes'] = []
        self.api.state['chain_status'] = HEALTHY_CHAIN | {
            'roots': [{'service': 'sealer', 'instance': 'cluster', 'cause': 'da_lag', 'runbook': 'docs/runbooks/da_lag.md'}],
            'ingress': {'pause': {'reason': 'upstream'}}}
        output = self.run_deploy(success=False)
        self.assertIn('the chain stands on a halt or a pause', output)
        self.assertIn('da_lag', output)
        self.assertEqual(self.api.state['writes'], [])
        self.assertEqual(self.api.state['record_writes'], [], 'a refused release starts no attempt')
        self.api.state['chain_status'] = HEALTHY_CHAIN | {'services': [
            {'service': 'batcher', 'instance': '0', 'state': 'paused', 'pause': {'note': 'disk-swap'}, 'halt': None}]}
        output = self.run_deploy(success=False)
        self.assertIn("Paused: ['batcher']", output)
        self.assertEqual(self.api.state['writes'], [])
        # An ingress job without a running allocation is what a failed
        # release leaves; the chain status cannot be read, so no deploy
        # goes over it.
        del self.api.state['chain_status']
        self.api.state['not_running'] = {'ingress'}
        output = self.run_deploy(success=False)
        self.assertIn('the ingress job is registered and no allocation of it runs', output)
        self.assertEqual(self.api.state['writes'], [])

    def test_a_missing_image_refuses_the_release(self):
        self.api.state['missing_images'] = {'kardamom-executor'}
        output = self.run_deploy(success=False)
        self.assertIn('does not hold the image of executor', output)
        self.assertEqual(self.api.state['writes'], [])
        self.assertFalse(Path(str(self.smoke) + '.calls').exists(), 'the gate reads the registry before the formats')
        # Only the local profile skips the check.
        output = self.run_deploy({'workloads_registry_url': 'off', 'deployment_profile': 'production'}, success=False)
        self.assertIn('the production profile does not allow that', output)
        self.assertEqual(self.api.state['writes'], [])

    def test_a_coordinated_format_change_refuses_the_rolling_path(self):
        self.gate_formats([{'rule': 'rollback', 'id': 'sealer-snapshot', 'head': 11, 'base': 10},
                           {'rule': 'mixed_fleet', 'id': 'sealer-snapshot', 'head': 11, 'base': 10}])
        output = self.run_deploy(success=False)
        self.assertIn('coordinated format change: sealer-snapshot', output)
        self.assertEqual(self.api.state['writes'], [])
        # The comparison ran the library against the accepted registry and this tree.
        calls = Path(str(self.smoke) + '.calls').read_text().splitlines()
        self.assertEqual(calls, [f'formats --base {self.record_dir}/accepted.formats.toml --root {ANSIBLE.parent}/../..'])

    def test_a_one_way_format_change_needs_the_allowance(self):
        self.gate_formats([{'rule': 'rollback', 'id': 'state-db', 'head': 4, 'base': 3}])
        output = self.run_deploy(success=False)
        self.assertIn('KARDAMOM_ALLOW_ONE_WAY=state-db', output)
        self.assertEqual(self.api.state['writes'], [])
        output = self.run_deploy(success=False, environ={'KARDAMOM_ALLOW_ONE_WAY': 'state-db,kar1-batch'})
        self.assertIn('names kar1-batch, and the release has no one-way change of it', output)
        self.assertEqual(self.api.state['writes'], [])
        self.run_deploy(environ={'KARDAMOM_ALLOW_ONE_WAY': 'state-db'})
        self.assertEqual(sorted(set(self.api.state['writes'])), sorted(self.REPINNED))
        self.assertEqual(self.record()['accepted']['floor'], {'formats': {'state-db': 4}, 'revision': 'rev-test'})

    def test_a_must_match_sealer_setting_refuses_the_rolling_path(self):
        self.run_deploy()
        self.api.state['writes'] = []
        output = self.run_deploy({'workloads_da_lag_budget_blocks': '5000'}, success=False)
        self.assertIn('every member must match: daLagBudgetBlocks.', output)
        self.assertEqual(self.api.state['writes'], [])
        output = self.run_deploy({'workloads_da_lag_budget_blocks': '5000'}, success=False,
                                 environ={'KARDAMOM_ALLOW_MUST_MATCH': 'daLagBudgetBlocks,remoteOrigins'})
        self.assertIn('names remoteOrigins, and the release does not change it', output)
        self.assertEqual(self.api.state['writes'], [])
        # A documented procedure names the settings it changes.
        self.run_deploy({'workloads_da_lag_budget_blocks': '5000'}, environ={'KARDAMOM_ALLOW_MUST_MATCH': 'daLagBudgetBlocks'})
        self.assertEqual(self.api.state['writes'], ['cluster', 'cluster', 'cluster'])

    def test_a_shard_map_change_refuses_the_rolling_path(self):
        self.run_deploy()
        self.api.state['writes'] = []
        task = self.api.state['jobs']['ingress']['TaskGroups'][0]['Tasks'][0]
        template = next(t for t in task['Templates'] if t['DestPath'] == 'local/shard-map.toml')
        template['EmbeddedTmpl'] = template['EmbeddedTmpl'].replace('version = 0', 'version = 1')
        output = self.run_deploy(success=False)
        self.assertIn('differs from the shard map of the registered ingress (version 1)', output)
        self.assertEqual(self.api.state['writes'], [])



class RollbackTest(Deploys):
    """`just rollback`: the targets, the floor, the re-render, the resume."""

    def test_a_rollback_reverts_the_accepted_release_in_reverse_order(self):
        self.run_deploy()
        self.repin('b')
        self.api.state['roles'] = {'node-cluster-0': 'FOLLOWER', 'node-cluster-1': 'LEADER', 'node-cluster-2': 'FOLLOWER'}
        self.run_deploy()
        self.api.state['writes'] = []
        self.api.state['roll_marker'] = 'a' * 64
        self.run_rollback()
        # Every job goes to its version before the release, last job first;
        # the sealer rolls back member by member, followers first.
        expected = [f'revert:{job}:0' for job in reversed(self.REPINNED) if job != 'cluster']
        expected[expected.index('revert:aeron:0'):expected.index('revert:aeron:0')] = ['cluster'] * 3
        self.assertEqual(self.api.state['writes'], expected)
        self.assertEqual(self.api.state['rolled'][-3:],
                         [['cluster-0'], ['cluster-0', 'cluster-2'], ['cluster-0', 'cluster-1', 'cluster-2']])
        self.assertTrue(all(t['Config']['image'].endswith('a' * 64) for g in self.api.state['jobs']['ingress']['TaskGroups']
                            for t in g['Tasks']), 'the ingress runs the release before')
        record = self.record()
        self.assertEqual(record['attempt']['status'], 'rolled_back')
        self.assertEqual(record['accepted']['restored_from'], record['attempt']['started_at'])
        self.assertEqual(record['accepted']['target']['manifest'], record['attempt']['before']['manifest'])
        self.assertEqual(record['accepted']['target']['formats'], FORMATS)
        output = self.run_rollback(success=False)
        self.assertIn('is already rolled back', output)

    def test_a_rollback_after_a_failed_attempt_targets_the_accepted_release(self):
        self.run_deploy()
        self.repin('b')
        self.api.state['deployments']['executor'] = Deployment('executor', ['failed'])
        self.run_deploy(success=False)
        self.api.state['writes'] = []
        del self.api.state['deployments']['executor']
        self.run_rollback()
        # The followers register before the role waits for the executor, so
        # the attempt reached them too.
        reverted = [w for w in self.api.state['writes'] if w.startswith('revert:')]
        self.assertEqual(reverted, ['revert:da-watcher:0', 'revert:validator:0', 'revert:notifier:0', 'revert:state-mirror:0',
                                    'revert:executor:0', 'revert:ingress:0', 'revert:redis:0', 'revert:sequencer:0',
                                    'revert:aeron:0'])
        self.assertEqual(self.api.state['writes'].count('cluster'), 3)
        self.assertNotIn('revert:batcher:0', self.api.state['writes'], 'the failed attempt never reached the batcher')
        self.assertEqual(self.record()['accepted']['target']['manifest']['executor'],
                         f'registry.example:5000/kardamom-executor:test@sha256:{"a" * 64}')

    def test_a_rollback_passes_a_job_that_nomad_reverted_itself(self):
        # The ingress has auto_revert: its failed deployment makes Nomad
        # register the pre-release definition as a new version. The
        # rollback finds it there and does not fail the EnforcePriorVersion
        # check on it.
        self.run_deploy()
        self.repin('b')
        self.api.state['deployments']['ingress'] = Deployment('ingress', ['failed'])
        self.run_deploy(success=False)
        self.assertIn('auto-revert:ingress', self.api.state['writes'])
        self.assertEqual(self.record()['attempt']['before']['jobs']['ingress'], {'before': 0, 'after': 1})
        self.api.state['writes'] = []
        del self.api.state['deployments']['ingress']
        output = self.run_rollback()
        self.assertIn('ingress is at version 2, the pre-release definition', output)
        self.assertNotIn('revert:ingress:0', self.api.state['writes'])
        self.assertIn('revert:redis:0', self.api.state['writes'])
        self.assertEqual(self.record()['attempt']['rolled_back'], ['ingress', 'redis', 'sequencer', 'cluster', 'aeron'])
        self.assertEqual(self.record()['attempt']['status'], 'rolled_back')

    def test_a_rollback_that_stops_resumes_after_the_jobs_it_reverted(self):
        self.run_deploy()
        self.repin('b')
        self.run_deploy()
        self.api.state['writes'] = []
        # The revert of the executor fails: the rollback stops there, with
        # the jobs before it recorded.
        self.api.state['deployments']['executor'] = Deployment('executor', ['failed'])
        self.run_rollback(success=False)
        done = ['batcher', 'da-store', 'da-watcher', 'validator', 'notifier', 'state-mirror']
        self.assertEqual(self.record()['attempt']['rolled_back'], done)
        self.assertEqual(self.record()['attempt']['status'], 'accepted', 'the rollback is not complete')
        self.assertEqual(self.api.state['writes'], [f'revert:{job}:0' for job in done + ['executor']])
        # The second run skips the recorded jobs and the executor, which is
        # at its pre-release definition already, and finishes the rest.
        del self.api.state['deployments']['executor']
        self.api.state['writes'] = []
        output = self.run_rollback()
        self.assertIn('executor is at version 2, the pre-release definition', output)
        self.assertEqual(self.api.state['writes'],
                         ['revert:ingress:0', 'revert:redis:0', 'revert:sequencer:0', 'cluster', 'cluster', 'cluster', 'revert:aeron:0'])
        self.assertEqual(self.record()['attempt']['status'], 'rolled_back')
        self.assertEqual(self.record()['attempt']['rolled_back'], done + ['executor', 'ingress', 'redis', 'sequencer', 'cluster', 'aeron'])

    def test_a_rollback_does_not_cross_the_floor(self):
        self.gate_formats([{'rule': 'rollback', 'id': 'state-db', 'head': 4, 'base': 3}])
        self.run_deploy(environ={'KARDAMOM_ALLOW_ONE_WAY': 'state-db'})
        self.api.state['writes'] = []
        output = self.run_rollback(success=False)
        self.assertIn("writes {'state-db': 4}", output)
        self.assertIn('KARDAMOM_ROLLBACK_BELOW_FLOOR=state-db', output)
        self.assertEqual(self.api.state['writes'], [])
        self.run_rollback(environ={'KARDAMOM_ROLLBACK_BELOW_FLOOR': 'state-db'})
        self.assertIn('revert:batcher:0', self.api.state['writes'])

    def test_a_rollback_to_a_dropped_version_asks_for_a_re_render(self):
        self.run_deploy()
        self.repin('b')
        self.run_deploy()
        self.api.state['writes'] = []
        self.api.state['dropped'] = {'ingress': [0]}
        output = self.run_rollback(success=False)
        self.assertIn('Nomad no longer holds the pre-release version of ingress', output)
        self.assertIn('just rollback-rerender local', output)
        self.assertEqual(self.api.state['writes'], [], 'no job changes before every version is confirmed')
        rerender = (self.record_dir / 'rollback.digests').read_text()
        self.assertIn(f'ingress registry.example:5000/kardamom-ingress:test@sha256:{"a" * 64}\n', rerender)
        self.assertEqual(len(rerender.splitlines()), len(MANIFEST))

    def test_a_rollback_without_a_record_refuses(self):
        output = self.run_rollback(success=False)
        self.assertIn('nothing to roll back', output)



class SealerTest(Deploys):
    """The staged roll of the sealer and its bootstrap."""

    def sealer_roll(self, leader):
        """Deploy twice: the second deploy edits the sealer, with `leader` leading."""
        self.run_deploy()
        self.api.state['roles'] = {f'node-cluster-{i}': 'LEADER' if i == leader else 'FOLLOWER' for i in range(3)}
        self.api.state['deployments']['cluster'] = Deployment('cluster', ['successful'])
        self.api.state['writes'] = []
        self.run_deploy({'workloads_cluster_retention': '4096'})
        return [g['Name'] for g in self.api.state['jobs']['cluster']['TaskGroups']]

    def test_the_sealer_rolls_followers_first_and_the_leader_last(self):
        groups = self.sealer_roll(leader=1)
        self.assertEqual(self.api.state['writes'], ['cluster', 'cluster', 'cluster'])
        self.assertEqual(sorted(groups), ['cluster-0', 'cluster-1', 'cluster-2'])
        # Every step holds the current definition of the members not rolled
        # yet, so the retention reaches the groups in the roll order.
        retention = [('-Dkardamom.cluster.retention=4096' in json.dumps(g)) for g in
                     self.api.state['jobs']['cluster']['TaskGroups']]
        self.assertTrue(all(retention), retention)
        self.assertEqual(self.api.state['deployments']['cluster'].polls, 3)
        # Member 1 leads: the followers 0 and 2 roll first, then the leader.
        self.assertEqual(self.api.state['rolled'][1:],
                         [['cluster-0'], ['cluster-0', 'cluster-2'], ['cluster-0', 'cluster-1', 'cluster-2']])

    def test_a_fresh_sealer_registers_in_one_step(self):
        self.run_deploy()
        self.assertEqual(self.api.state['writes'].count('cluster'), 1)
        self.assertNotIn('bootstrap-open', self.api.state['writes'])

    def test_the_bootstrap_lives_only_while_a_new_cluster_comes_up(self):
        self.run_deploy({'workloads_cluster_bootstrap': True})
        writes = self.api.state['writes']
        cluster = writes.index('cluster')
        self.assertEqual(writes[cluster - 1:cluster + 2], ['bootstrap-open', 'cluster', 'bootstrap-close'])
        # A re-deploy of the registered cluster keeps the flag and still
        # opens no bootstrap.
        self.api.state['deployments']['cluster'] = Deployment('cluster', ['successful'])
        self.api.state['writes'] = []
        self.run_deploy({'workloads_cluster_bootstrap': True, 'workloads_cluster_retention': '4096'})
        self.assertEqual(self.api.state['writes'], ['cluster', 'cluster', 'cluster'])

    def test_a_failed_bootstrap_closes_the_bootstrap(self):
        self.api.state['deployments']['cluster'] = Deployment('cluster', ['failed'])
        self.run_deploy({'workloads_cluster_bootstrap': True}, success=False)
        self.assertEqual(self.api.state['writes'][-3:], ['bootstrap-open', 'cluster', 'bootstrap-close'])


if __name__ == '__main__':
    unittest.main()
