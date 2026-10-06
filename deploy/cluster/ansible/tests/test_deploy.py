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
SERVICES = ['aeron', 'cluster', 'redis', 'sequencer', 'ingress', 'executor', 'validator', 'da-watcher',
            'batcher', 'state-mirror', 'notifier', 'da-store']
# The images the manifest pins beyond the default deployment: the jobs a
# real L1 or the chaos-l1 shard adds.
MANIFEST = SERVICES + ['l1-indexer', 'l1-fault-proxy']


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
            if body['Job']['ID'] in state['deployments']:
                state['deployments'][body['Job']['ID']].registered(body['Job'])
            # The sealer groups that carry the roll's new retention, per
            # registration: the staged roll adds one group per step.
            if body['Job']['ID'] == 'cluster':
                state.setdefault('rolled', []).append(sorted(
                    g['Name'] for g in body['Job']['TaskGroups'] if 'retention=4096' in json.dumps(g)))
            self.respond({'JobModifyIndex': 1})
        elif self.path.startswith('/v1/deployment/promote/'):
            name = self.path.split('/')[4].split('?')[0]
            assert body['All'] is True
            state['deployments'][name].promote()
            self.respond({'EvalID': 'promoted'})
        else:
            raise AssertionError(self.path)

    def do_PUT(self):
        # The bootstrap variable of a new sealer cluster.
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        assert self.path.startswith('/v1/var/nomad/jobs/cluster?'), self.path
        assert body['Path'] == 'nomad/jobs/cluster' and body['Items'] == {'bootstrap': 'true'}, body
        self.server.state['writes'].append('bootstrap-open')
        self.respond(body)

    def do_DELETE(self):
        assert self.path.startswith('/v1/var/nomad/jobs/cluster?'), self.path
        self.server.state['writes'].append('bootstrap-close')
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        parts = self.path.split('?')[0].split('/')
        parts += [''] * (6 - len(parts))
        state = self.server.state
        if parts[2] == 'deployment' and parts[3] == 'allocations':
            allocs = self.allocations(parts[4])
            allocs[0]['DeploymentStatus']['Canary'] = True
            self.respond(allocs)
        elif parts[2] == 'deployment':
            # The deployment's verdict, by the job's scripted outcome.
            self.respond(state['deployments'][parts[3]].state())
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
            self.respond(self.allocations(parts[3]))
        elif parts[2] == 'job':
            self.respond(state['jobs'].get(parts[3], {}) | {'Version': 1})
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
                allocs.append({'TaskGroup': group['Name'], 'JobVersion': 0 if stale else 1,
                               'DesiredStatus': 'run', 'ClientStatus': 'running',
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
class DeployTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='kardamom-deploy-test-')
        self.addCleanup(self.tmp.cleanup)
        self.manifest = Path(self.tmp.name) / 'images.digests'
        self.manifest.write_text(''.join(
            f'{s} registry.example:5000/kardamom-{s}:test@sha256:{"a" * 64}\n' for s in MANIFEST))
        # Every loopback address, so the sealer nodes' 127.0.0.<n> resolve here.
        self.api = ThreadingHTTPServer(('0.0.0.0', 0), NomadAPI)
        self.api.state = {'jobs': {}, 'writes': [], 'deployments': {}, 'roles': {}}
        self.record_dir = Path(self.tmp.name) / 'deployed'
        self.smoke = Path(self.tmp.name) / 'smoke.sh'
        self.smoke.write_text('#!/bin/sh\necho "$@" >> "$0.calls"\n')
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
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
        self.assertEqual(result.returncode == 0, success, result.stdout)
        return result.stdout

    def test_deploy_order_pinning_and_repeat(self):
        self.run_deploy()
        expected = ['aeron', 'anvil', 'cluster', 'sequencer', 'redis', 'ingress', 'executor',
                    'state-mirror', 'notifier', 'validator', 'da-watcher', 'node-exporter', 'monitoring',
                    'da-store', 'batcher']
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
            'workloads_cluster_file_sync_level': '2',
            'workloads_remote_origins': '412399',
            'workloads_priority_fees': 'on',
        }, check=True)
        plans = self.api.state['plans']
        self.assertIn('l1-light-client', plans)
        self.assertTrue(all(job['Namespace'] == 'staging' for job in plans.values()))
        validator = json.dumps(plans['validator'])
        self.assertIn('http://kardamom-l1-light-client.service.dc1.consul:8548', validator)
        self.assertNotIn('http://execution.example', validator)
        # The indexer follows the light client and reads the payloads from
        # the DA proxy; the batcher resumes from the indexer.
        indexer = json.dumps(plans['l1-indexer'])
        self.assertIn('http://kardamom-l1-light-client.service.dc1.consul:8548', indexer)
        self.assertIn('http://kardamom-da-proxy.service.consul:3100', indexer)
        self.assertIn('http://kardamom-l1-indexer.service.dc1.consul:8549', json.dumps(plans['batcher']))
        self.assertIn('8192', json.dumps(plans['cluster']))
        self.assertIn('-Dkardamom.cluster.fileSyncLevel=2', json.dumps(plans['cluster']))
        # One value turns priority fees on for every role that has a say.
        self.assertIn('-Dkardamom.cluster.orderingWindow=20', json.dumps(plans['cluster']))
        self.assertEqual(self.sequencer_env(plans)['KARDAMOM_PRIORITY_FEES'], 'true')
        for name in ('executor', 'validator'):
            self.assertIn('base_fee_initial', self.genesis_template(plans[name]), name)
        self.assertEqual(self.api.state['writes'], [])

    def test_fault_proxy_routes_the_followers_through_it(self):
        self.run_deploy({'workloads_l1_fault_proxy': True, 'workloads_indexer_poll_s': '2'}, check=True)
        plans = self.api.state['plans']
        proxy = 'http://kardamom-l1-fault-proxy.service.dc1.consul:8547'
        self.assertIn('http://anvil.service.consul:8546', json.dumps(plans['l1-fault-proxy']))
        anvil = plans['anvil']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertEqual(anvil[anvil.index('--slots-in-an-epoch') + 1], '1')
        for job in ('batcher', 'da-watcher', 'l1-indexer'):
            self.assertIn(proxy, json.dumps(plans[job]), job)
        indexer = plans['l1-indexer']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertEqual(indexer[indexer.index('--poll-interval-secs') + 1], '2')
        self.assertEqual(indexer[indexer.index('--start-block') + 1], '1')
        self.assertEqual(indexer[indexer.index('--lockbox') + 1], '0x' + '0' * 40)
        self.assertIn('http://kardamom-l1-indexer.service.dc1.consul:8549', json.dumps(plans['batcher']))
        self.assertEqual(self.api.state['writes'], [])

    def test_the_da_watcher_keeps_its_l1_cursor_on_the_node(self):
        # A restart resumes after the last published L1 block only when the
        # cursor file outlives the container.
        self.run_deploy(check=True)
        task = self.api.state['plans']['da-watcher']['TaskGroups'][0]['Tasks'][0]
        self.assertIn('/opt/kardamom/da-watcher:/opt/kardamom/da-watcher', task['Config']['volumes'])
        args = task['Config']['args']
        self.assertEqual(args[args.index('--l1-cursor-file') + 1], '/opt/kardamom/da-watcher/l1-cursor')
        self.assertEqual(self.api.state['writes'], [])

    def test_priority_fees_default_off_on_every_role(self):
        self.run_deploy(check=True)
        plans = self.api.state['plans']
        self.assertIn('-Dkardamom.cluster.orderingWindow=0', json.dumps(plans['cluster']))
        self.assertEqual(self.sequencer_env(plans)['KARDAMOM_PRIORITY_FEES'], 'false')
        for name in ('executor', 'validator'):
            self.assertNotIn('base_fee_initial', self.genesis_template(plans[name]), name)

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
            'state-mirror', 'notifier'})
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
