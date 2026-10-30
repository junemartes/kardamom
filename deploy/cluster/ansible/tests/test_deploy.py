"""Exercise the actual playbook and Nomad HCL compiler against an isolated API.

Run with: python3 -m unittest discover -s deploy/cluster/ansible/tests -v
Requires ansible-playbook and nomad on PATH; never connects to a real cluster.
"""
import copy
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import socket
import time
import urllib.parse
import urllib.request
import subprocess
import tempfile
import threading
import tomllib
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import yaml

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
# The SHA-256 of the genesis that staging started from. A live chain
# refuses a changed genesis, so a deployment without the local canary
# renders exactly this file.
STAGING_GENESIS_SHA256 = '1f9b08a455960c3bb26469765baf0dd46cfd68843eda9ee735fb9291aaaad1fa'
# The first account of the canary's fragment (config/genesis/canary.toml).
CANARY_RING_ACCOUNT = '0x1aac82773CB722166D7dA0d5b0FA35B0307dD99D'
# The chain status of a chain that stands on nothing.
HEALTHY_CHAIN = {'roots': [], 'sealer': {'pause': None}, 'ingress': {'pause': None}, 'services': []}
# The chain status while no sealer member runs: the ingress halts the
# sealer on a lost quorum and pauses on it.
NO_QUORUM_ROOT = {'service': 'sealer', 'instance': 'cluster', 'cause': 'sealer_no_quorum',
                  'runbook': 'docs/runbooks/sealer_no_quorum.md'}
NO_QUORUM_CHAIN = HEALTHY_CHAIN | {'roots': [NO_QUORUM_ROOT],
                                   'ingress': {'pause': {'reason': 'upstream', 'root': NO_QUORUM_ROOT}}}
# The carry fields of the status of a member of this release, before the
# carry window: decision version 5, the heap store, no feed.
MEMBER_CARRY = {'decisionVersion': 5, 'retainedStore': 'heap', 'carryWindow': False, 'feed': 'off',
                'retainedFrames': 0, 'retainedBytes': 0}
# The format registry of the repository: the formats of every target.
FORMATS = (ANSIBLE.parents[2] / 'formats.toml').read_text()
# The filter of the notifier node preflight, and its meaning on a node.
# A Nomad 1.9 agent evaluates the filter on the whole node.
ROLE_NODE_FILTER = ('Meta.roles matches "(^|,) *{role} *(,|$)" and Status == "ready" '
                    'and SchedulingEligibility == "eligible"')


def role_node(node, role):
    """Whether ROLE_NODE_FILTER of `role` admits the node."""
    return (re.search(rf'(^|,) *{role} *(,|$)', node['Meta'].get('roles', '')) is not None
            and node['Status'] == 'ready' and node['SchedulingEligibility'] == 'eligible')


def nomad_node(name, roles, status='ready', eligibility='eligible'):
    return {'Name': name, 'Meta': {'roles': roles}, 'Status': status, 'SchedulingEligibility': eligibility}


# The formats text that an accepted release holds in the record. The deploy
# binary of the tests scripts its findings, so the text is a small fixed
# registry: a record holds it up to four times within Nomad's 64 KiB limit.
RECORD_FORMATS = ('[format.spool]\nwrites = 1\nreads_min = 1\nreads_max = 1\n'
                  'shared = false\ncode = ["a.rs#A"]\n')


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
            state.setdefault('registrations', []).append(body['Job'])
            # The sealer groups that carry the roll's marker (the new
            # retention, or the digest a rollback restores), per
            # registration: the staged roll adds one group per step.
            if body['Job']['ID'] == 'cluster':
                marker = state.get('roll_marker', 'retention=131072')
                state.setdefault('rolled', []).append(sorted(
                    g['Name'] for g in body['Job']['TaskGroups'] if marker in json.dumps(g)))
            self.respond({'JobModifyIndex': 1, 'EvalID': self.eval_id(body['Job']['ID'])})
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
            self.respond({'EvalID': self.eval_id(name), 'JobModifyIndex': len(versions)})
        else:
            raise AssertionError(self.path)

    def eval_id(self, name):
        """The id of the evaluation of a new version of the job `name`."""
        return f'eval:{name}:{len(self.server.state.get("versions", {}).get(name, []))}'

    def register(self, job):
        """Store `job` as the current definition and as a new version. A down group starts when its
        definition changes: a member that crash-loops on its release starts on another one."""
        state = self.server.state
        old = {g['Name']: g for g in state['jobs'].get(job['ID'], {}).get('TaskGroups', [])}
        new = {g['Name']: g for g in job['TaskGroups']}
        if 'down_groups' in state:
            state['down_groups'] = [name for name in state['down_groups'] if old.get(name) == new.get(name)]
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
        token = self.server.state.get('registry_token')
        if token and self.headers.get('Authorization') != f'Bearer {token}':
            # A registry such as ghcr.io: no manifest without a pull token,
            # also for a public image.
            self.send_response(401)
            self.send_header('WWW-Authenticate', f'Bearer realm="http://127.0.0.1:{self.server.server_port}/token",'
                                                 f'service="registry.example",scope="repository:{name}:pull"')
            self.end_headers()
            return
        self.send_response(404 if name in self.server.state.get('missing_images', ()) else 200)
        self.end_headers()

    def do_PUT(self):
        # The bootstrap variable of a new sealer cluster, or the secrets
        # of a job.
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        path = self.path.split('?')[0].removeprefix('/v1/var/')
        assert self.path.startswith(('/v1/var/nomad/jobs/', '/v1/var/kardamom/deploys/')) \
            and body['Path'] == path, (self.path, body)
        # Nomad refuses a variable over 64 KiB.
        assert len(json.dumps(body['Items'])) <= 64 * 1024, (path, len(json.dumps(body['Items'])))
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
        if self.path.startswith('/token?'):
            # The token service of the registry: an anonymous pull token
            # for the scope of the challenge.
            query = urllib.parse.parse_qs(self.path.split('?')[1])
            assert query['service'] == ['registry.example'] and query['scope'][0].endswith(':pull'), query
            self.server.state.setdefault('token_scopes', []).append(query['scope'][0])
            self.respond({'token': self.server.state['registry_token']})
            return
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
        elif parts[2] == 'evaluation':
            # The evaluation of a registration: complete, with the
            # deployment of the job when the test scripts one.
            job = parts[3].split(':')[1]
            self.respond({'ID': parts[3], 'Status': 'complete',
                          'DeploymentID': job if job in state['deployments'] else ''})
        elif parts[2] == 'nodes':
            # The node list of the preflight of a moved role (the notifier,
            # the egress): the stubs of the nodes that the filter admits.
            [query] = urllib.parse.parse_qs(self.path.split('?')[1])['filter']
            role = re.search(r'\* *([a-z-]+) \*', query).group(1)
            assert query == ROLE_NODE_FILTER.format(role=role), query
            self.respond([{'Name': n['Name']} for n in state['nodes'] if role_node(n, role)])
        elif parts[2] == 'node':
            # Every node advertises an address of this fake API, so the
            # member status and the smoke target resolve back here. A
            # sealer node gets its own loopback address, so the status
            # request names the member through the Host header.
            self.respond({'ID': parts[3], 'HTTPAddr': f'{self.node_ip(parts[3])}:{self.server.server_port}'})
        elif parts[1] == 'status':
            # The sealer member status of the node the Host header names;
            # member 0 leads unless the test says otherwise.
            node = self.host_node()
            octet = int(node.rsplit('-', 1)[1]) + 1
            default = 'LEADER' if node.endswith('-0') else 'FOLLOWER'
            # A snapshotLatest of None stands for a member of a release
            # without the field.
            latest = state.get('snapshot_latest', {}).get(node, 0)
            self.respond({'memberId': octet - 1, 'role': state['roles'].get(node, default), 'election': 'CLOSED',
                          'snapshotReadsMax': state.get('reads_max', {}).get(node, 15)}
                         | ({} if latest is None else {'snapshotLatest': latest})
                         | MEMBER_CARRY | state.get('status', {}).get(node, {}))
        elif parts[1] == 'metrics':
            # The Prometheus page of the node the Host header names. A node
            # without a page does not answer.
            page = state.get('metrics', {}).get(self.host_node())
            if page is None:
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(page.encode())
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

    def metric_nodes(self):
        """The nodes with a metrics page that are not sealer nodes, each on
        its own loopback address 127.0.2.<n>."""
        return sorted(n for n in self.server.state.get('metrics', {}) if not n.startswith('node-cluster-'))

    def node_ip(self, node):
        if node.startswith('node-cluster-'):
            return f'127.0.0.{int(node.rsplit("-", 1)[1]) + 1}'
        if node in self.metric_nodes():
            return f'127.0.2.{self.metric_nodes().index(node) + 1}'
        return '127.0.0.1'

    def host_node(self):
        """The node that the Host header of the request names."""
        address = self.headers['Host'].split(':')[0]
        prefix, last = address.rsplit('.', 1)
        if prefix == '127.0.2':
            return self.metric_nodes()[int(last) - 1]
        return f'node-cluster-{int(last) - 1}'

    def allocations(self, name):
        state = self.server.state
        allocs = []
        job = state['jobs'][name]
        for group in job['TaskGroups']:
            # The scheduler places a system or sysbatch group on the nodes
            # its constraints admit; the playbook must not assume a node count.
            per_node = job['Type'] in ('system', 'sysbatch')
            count = 8 if per_node else group['Count']
            # A sysbatch allocation that ends well is complete; the others run.
            done = 'complete' if job['Type'] == 'sysbatch' else 'running'
            for i in range(count):
                # Historical/stopping allocations must not satisfy readiness.
                stale = state.get('missing_replica') == name and i > 0
                # A job the test names has no running allocation: a crash loop.
                running = name not in state.get('not_running', ()) \
                    and group['Name'] not in state.get('down_groups', ())
                # A node that the test takes down holds the first allocation
                # of the job. Nomad stops the allocation of a down node and
                # marks it lost.
                lost = name in state.get('down_node_jobs', ()) and i == 0
                # Each instance of a group has its own node.
                allocs.append({'TaskGroup': group['Name'], 'JobVersion': self.version(name) - int(stale),
                               'DesiredStatus': 'stop' if lost else 'run',
                               'ClientStatus': 'lost' if lost else done if running else 'pending',
                               'NodeID': f'node-{group["Name"]}' + (f'.{i}' if i else ''),
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


class Dependency(Deployment):
    """A deployment that becomes ready only after the jobs `needs` are registered."""

    def __init__(self, name, jobs, needs):
        super().__init__(name, ['running'])
        self.jobs = jobs
        self.needs = needs

    def state(self, poll=True):
        ready = self.needs <= self.jobs.keys()
        self.outcomes = ['successful' if ready else 'running']
        return super().state(poll)


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
                          'record_writes': [],
                          'nodes': [nomad_node('ingress-0', 'ingress,redis'), nomad_node('egress-0', 'egress'),
                                    nomad_node('egress-1', 'egress'), nomad_node('notifier-0', 'notifier'),
                                    nomad_node('notifier-1', 'notifier')]}
        self.record_dir = Path(self.tmp.name) / 'deployed'
        # The target tree: a git checkout with the formats.toml of this
        # repository. A deploy records its HEAD as the revision, which must
        # be a commit of the checkout.
        self.repo = Path(self.tmp.name) / 'repo'
        self.repo.mkdir()
        self.git('init', '-q')
        (self.repo / 'formats.toml').write_text(FORMATS)
        self.revision = self.commit('the release')
        # One stub for each operator binary, so that a call to the wrong
        # binary fails the deploy. A release ships only the deploy binary,
        # so the cluster binary fails every call. The deploy binary prints
        # the format findings of the release gate, which a test scripts in
        # `deploy.sh.findings`, and smokes a canary; the smoke fails when
        # `deploy.sh.smoke-fails` exists. It fails every other command.
        # Each stub logs its calls in `<stub>.calls`.
        self.cluster_bin = Path(self.tmp.name) / 'cluster.sh'
        self.cluster_bin.write_text('#!/bin/sh\necho "$@" >> "$0.calls"\nexit 1\n')
        self.cluster_bin.chmod(0o755)
        self.deploy_bin = Path(self.tmp.name) / 'deploy.sh'
        self.deploy_bin.write_text('#!/bin/sh\necho "$@" >> "$0.calls"\n'
                                   'case "$1" in\n'
                                   'formats) cat "$0.findings" 2>/dev/null || echo "[]" ;;\n'
                                   'smoke) [ ! -e "$0.smoke-fails" ] ;;\n'
                                   '*) exit 1 ;;\n'
                                   'esac\n')
        self.deploy_bin.chmod(0o755)
        self.thread = threading.Thread(target=self.api.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.api.server_close)
        self.addCleanup(self.api.shutdown)

    def git(self, *args):
        # A fixed date gives each commit the same hash in every test.
        date = {'GIT_AUTHOR_DATE': '2026-01-01T00:00:00Z', 'GIT_COMMITTER_DATE': '2026-01-01T00:00:00Z'}
        return subprocess.run(['git', '-C', str(self.repo), '-c', 'user.name=t', '-c', 'user.email=t@t',
                               *args], check=True, capture_output=True, text=True, env=os.environ | date).stdout.strip()

    def commit(self, message):
        """Commit the target tree and return the new revision."""
        self.git('add', '-A')
        self.git('commit', '-q', '--allow-empty', '-m', message)
        return self.git('rev-parse', 'HEAD')

    def run_deploy(self, extra=None, check=False, success=True, playbook="deploy.yml", environ=None,
                   inventory='localhost,'):
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
            'workloads_cluster_binary': str(self.cluster_bin),
            'workloads_deploy_binary': str(self.deploy_bin),
            'workloads_sealer_admin_port': self.api.server_port,
            'workloads_metrics_ports': dict.fromkeys(('executor', 'batcher', 'validator', 'egress'),
                                                     self.api.server_port),
            'workloads_cluster_bootstrap': False,
            'workloads_ingress_rpc_port': self.api.server_port,
            'workloads_registry_url': f'http://127.0.0.1:{self.api.server_port}',
            'workloads_repo_root': str(self.repo),
            'workloads_revision': '',
            'workloads_operator': 'tester',
        } | (extra or {})
        env = {k: v for k, v in os.environ.items() if not k.startswith(('ANSIBLE_', 'NOMAD_'))}
        env.update(ANSIBLE_NOCOLOR='1', ANSIBLE_STDOUT_CALLBACK='default',
                   ANSIBLE_LOCAL_TEMP=self.tmp.name + '/ansible',
                   OBJC_DISABLE_INITIALIZE_FORK_SAFETY='YES')
        env.pop('AERON_STALL_TOLERANCE_MS', None)
        env.pop('KARDAMOM_IDLE_CAP_US', None)
        env.pop('KARDAMOM_GENESIS_CARRY', None)
        env.update(environ or {})
        cmd = ['ansible-playbook', '-i', inventory, str(ANSIBLE / playbook),
               '-e', json.dumps(variables)] + (['--check'] if check else [])
        result = subprocess.run(cmd, cwd=ANSIBLE.parent, env=env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=900)
        self.assertEqual(result.returncode == 0, success, result.stdout)
        return result.stdout

    # The jobs a manifest with new digests changes, in deploy order: every
    # pinned image, and not anvil or the monitoring. The data directory job
    # runs the Aeron image of the manifest.
    REPINNED = ['data-dirs', 'aeron', 'cluster', 'redis', 'ingress', 'executor', 'state-mirror', 'notifier',
                'validator', 'da-watcher', 'da-store', 'l1-indexer', 'batcher', 'sequencer']

    def record(self):
        return self.api.state['records']['kardamom/deploys/local']

    def repin(self, digest, tag=None):
        """Point every manifest line at `digest`: a new release. A release tags its images
        main-<12 hex of its commit>, and `tag` defaults to that tag of HEAD."""
        text = self.manifest.read_text()
        old = re.search(r'@sha256:([0-9a-f]{64})', text).group(1)
        tag = tag or 'main-' + self.git('rev-parse', 'HEAD')[:12]
        self.manifest.write_text(re.sub(rf':[^:@\s]+@sha256:{old}', f':{tag}@sha256:{digest * 64}', text))

    def upgrade_check(self, success=True, feature='9', activation_ms=''):
        """Run the upgrade check for an upgrade transaction of `feature` at `activation_ms`."""
        return self.run_deploy(playbook='upgrade-check.yml', success=success,
                               environ={'KARDAMOM_UPGRADE_FEATURE': feature,
                                        'KARDAMOM_UPGRADE_ACTIVATION_MS': activation_ms})

    def add_egress_image(self):
        """Pin the image of the egress in the manifest."""
        self.manifest.write_text(self.manifest.read_text()
                                 + f'egress registry.example:5000/kardamom-egress:test@sha256:{"a" * 64}\n')

    def run_rollback(self, success=True, environ=None):
        return self.run_deploy(playbook='rollback.yml', success=success, environ=environ)

    # The results of the deploys on a fresh fixture, kept per test class and
    # per deploy setting: the state of the fake API, and a directory with a
    # copy of the record directory.
    deployed = None
    # The environment of the default deploy.
    DEPLOY_ENVIRON = {}

    def deploy(self, extra=None, environ=None):
        """Run a deploy on the fresh fixture of this test: the default deploy,
        or the one of the extra vars `extra` and the environment `environ`.

        A deploy of the whole pipeline takes minutes on a loaded runner. The
        first test of a class runs it. Every later test restores the state
        and the files that it left. The copy is valid only while the first
        deploy gives the same result in every test: the same fixtures, the
        same revision, and no `.calls` file of a stub.
        """
        cls = type(self)
        if cls.deployed is None:
            cls.deployed = {}
        environ = self.DEPLOY_ENVIRON if environ is None else environ
        key = json.dumps([extra, environ], sort_keys=True)
        if key not in cls.deployed:
            self.run_deploy(extra, environ=environ)
            files = tempfile.TemporaryDirectory(prefix='kardamom-deploy-snapshot-')
            cls.addClassCleanup(files.cleanup)
            shutil.copytree(self.record_dir, Path(files.name) / 'deployed')
            cls.deployed[key] = (copy.deepcopy(self.api.state), files)
            return
        state, files = cls.deployed[key]
        self.api.state = copy.deepcopy(state)
        shutil.copytree(Path(files.name) / 'deployed', self.record_dir, dirs_exist_ok=True)

    def gate_formats(self, findings):
        """Deploy once, give the accepted release the formats text that the
        gate compares with, then script the format findings of the next
        deploy."""
        self.deploy()
        self.record()['accepted']['target']['formats'] = RECORD_FORMATS
        self.api.state['writes'] = []
        self.api.state['record_writes'] = []
        Path(str(self.deploy_bin) + '.findings').write_text(json.dumps(findings))
        self.repin('b')

    def target_snapshot_reads_max(self, version):
        """Commit a target release with a sealer snapshot reads_max of `version`, and return its revision."""
        formats = self.repo / 'formats.toml'
        formats.write_text(re.sub(r'(\[format\.sealer-snapshot\][^\[]*?reads_max = )\d+', rf'\g<1>{version}',
                                  formats.read_text()))
        return self.commit(f'the release that reads snapshot version {version}')

    def two_sealer_releases(self):
        """Deploy release a, which reads snapshot version 16, then release b, which reads 17. A member then
        holds a snapshot of version 17. Return the revision of release a."""
        revision = self.target_snapshot_reads_max(16)
        self.run_deploy()
        self.target_snapshot_reads_max(17)
        self.repin('b')
        self.run_deploy()
        self.api.state['snapshot_latest'] = {'node-cluster-1': 17}
        self.api.state['writes'] = []
        return revision

class DeployTest(Deploys):
    """The deploy: order, pins, secrets, waits, canary, sealer roll."""

    def test_a_da_paused_sequencer_gets_its_posting_dependencies_before_readiness(self):
        # A paused sequencer becomes ready only after the posting path exists.
        self.api.state['deployments']['sequencer'] = Dependency(
            'sequencer', self.api.state['jobs'], {'da-store', 'l1-indexer', 'batcher'})
        self.run_deploy({'workloads_da_lag_budget_blocks': '8'})
        writes = self.api.state['writes']
        for dependency in ('da-store', 'l1-indexer', 'batcher'):
            self.assertLess(writes.index(dependency), writes.index('sequencer'))
        definition = self.api.state['jobs']['sequencer']
        for group in definition['TaskGroups']:
            self.assertEqual(group['Update']['HealthyDeadline'], 180_000_000_000)
            self.assertEqual(group['Update']['ProgressDeadline'], 600_000_000_000)
        self.assertEqual(self.api.state['deployments']['sequencer'].state()['Status'], 'successful')

    def test_deploy_order_pinning_and_repeat(self):
        # The local canary runs only on request: the CI shards count
        # transactions.
        local_canary = {'CANARY_LOCAL': '1'}
        self.run_deploy(environ=local_canary)
        self.assertIn('-Daeron.archive.file.sync.level=1', json.dumps(self.api.state['jobs']['aeron']))
        expected = ['data-dirs', 'aeron', 'anvil', 'cluster', 'redis', 'ingress', 'executor',
                    'state-mirror', 'notifier', 'validator', 'da-watcher', 'node-exporter', 'monitoring',
                    'da-store', 'l1-indexer', 'batcher', 'sequencer', 'canary']
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
        # Every deployment runs the follower; anvil has no beacon chain,
        # so it reads every second.
        follower = self.api.state['jobs']['l1-indexer']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertEqual(follower[follower.index('--poll-interval-secs') + 1], '1')
        # A halt or a pause fails /ready, so Consul marks the service
        # critical. Prometheus takes every kardamom service in every health
        # state: a healthy-only list drops it exactly when its halt alert
        # needs it. The Nomad agents' service is not a kardamom job.
        templates = [t['EmbeddedTmpl'] for g in self.api.state['jobs']['monitoring']['TaskGroups']
                     for t in g['Tasks'] for t in (t.get('Templates') or [])]
        services = re.findall(r'service "(kardamom-[^"]*)"', '\n'.join(templates))
        self.assertIn('kardamom-validator|any', services)
        self.assertEqual([s for s in services if not s.endswith(('|any', '-nomad'))], [])
        self.assertEqual(variables['nomad/jobs/batcher']['KARDAMOM_L1_RPC'], 'http://anvil.service.consul:8546')
        self.assertEqual(variables['nomad/jobs/batcher']['KARDAMOM_L1_KEY'][:10], '0x5de4111a')
        self.assertEqual(variables['nomad/jobs/canary'], {
            'KARDAMOM_CANARY_MNEMONIC': 'test test test test test test test test test test test junk',
            'KARDAMOM_L1_RPC': 'http://anvil.service.consul:8546'})
        canary = self.api.state['jobs']['canary']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertEqual(str(canary[canary.index('--ring-offset') + 1]), '34')
        # The local canary's ring is funded by its own genesis fragment.
        for name in ('executor', 'validator'):
            self.assertIn(CANARY_RING_ACCOUNT, self.genesis_template(self.api.state['jobs'][name]), name)
        self.assertEqual(sorted(self.api.state['variable_writes']),
                         ['nomad/jobs/batcher', 'nomad/jobs/canary', 'nomad/jobs/l1-indexer'])
        self.run_deploy(environ=local_canary)
        # An unchanged redeploy registers only the data directory job: each
        # deploy gives it a new version, so it runs again on every node.
        self.assertEqual(self.api.state['writes'], expected + ['data-dirs'],
                         'unchanged redeploy must register no job but the data directories')
        self.assertEqual(len(self.api.state['variable_writes']), 3, 'unchanged redeploy must not write secrets')

    def test_the_waves_of_a_new_cluster_start_a_wave_before_its_waits(self):
        # The ingress becomes ready only after the last job of its wave
        # registers. A deploy that waits for each job before the next one
        # never gets there.
        self.api.state['deployments']['ingress'] = Dependency('ingress', self.api.state['jobs'], {'l1-indexer'})
        self.run_deploy(environ={'KARDAMOM_DEPLOY_WAVES': '1', 'CANARY_LOCAL': '1'})
        expected = ['data-dirs', 'aeron', 'anvil', 'cluster', 'redis', 'ingress', 'executor',
                    'state-mirror', 'notifier', 'validator', 'da-watcher', 'node-exporter', 'monitoring',
                    'da-store', 'l1-indexer', 'batcher', 'sequencer', 'canary']
        self.assertEqual(self.api.state['writes'], expected, 'the waves keep the registration order')
        self.assertEqual(self.api.state['deployments']['ingress'].state(poll=False)['Status'], 'successful')

    def test_a_failed_job_of_a_wave_stops_the_deploy_at_the_end_of_the_wave(self):
        self.api.state['deployments']['ingress'] = Deployment('ingress', ['failed'])
        output = self.run_deploy(success=False, environ={'KARDAMOM_DEPLOY_WAVES': '1'})
        self.assertIn('scripted failed', output)
        self.assertIn('l1-indexer', self.api.state['writes'])
        self.assertNotIn('batcher', self.api.state['writes'])

    def test_a_deploy_over_a_record_waits_for_each_job_before_the_next(self):
        self.deploy()
        self.repin('b')
        self.api.state['writes'] = []
        self.api.state['deployments']['ingress'] = Deployment('ingress', ['failed'])
        self.run_deploy(success=False, environ={'KARDAMOM_DEPLOY_WAVES': '1'})
        self.assertIn('ingress', self.api.state['writes'])
        self.assertNotIn('executor', self.api.state['writes'])

    def chaos_knobs(self):
        """The values of the five chaos knobs in the planned jobs."""
        plans = self.api.state['plans']
        sealer = plans['cluster']['TaskGroups'][0]['Tasks'][0]['Env']['JAVA_TOOL_OPTIONS'].split()
        executor = [t['Config']['args'] for g in plans['executor']['TaskGroups'] for t in g['Tasks']
                    if '--checkpoint-interval-secs' in (t['Config'].get('args') or [])][0]
        rules = [t['EmbeddedTmpl'] for g in plans['monitoring']['TaskGroups'] for t in g['Tasks']
                 for t in (t.get('Templates') or []) if t['DestPath'] == 'local/alerts.yml'][0]
        stalled = re.search(r'alert: KardamomCanaryStalled\n.*?outcome="success"}\[(\w+)\]', rules, re.S).group(1)
        canary = plans['canary']['TaskGroups'][0]['Tasks'][0]['Config']
        return {'canvass': [o for o in sealer if 'canvass' in o],
                'checkpoint': str(executor[executor.index('--checkpoint-interval-secs') + 1]),
                'stalled': stalled, 'force_pull': canary['force_pull'],
                'rules_unchanged': rules == (ANSIBLE.parents[1] / 'alerts.yml').read_text()}

    def test_the_chaos_knobs_keep_todays_values_by_default(self):
        self.run_deploy(check=True, environ={'CANARY_LOCAL': '1'})
        self.assertEqual(self.chaos_knobs(), {'canvass': [], 'checkpoint': '20', 'stalled': '5m',
                                              'force_pull': True, 'rules_unchanged': True})

    def test_the_chaos_knobs_render_the_chaos_values(self):
        self.run_deploy(check=True, environ={
            'CANARY_LOCAL': '1', 'KARDAMOM_CHECKPOINT_INTERVAL_S': '5', 'KARDAMOM_CANARY_STALLED_WINDOW': '1m',
            'KARDAMOM_CANARY_FORCE_PULL': 'off',
            'KARDAMOM_CLUSTER_JVM_OPTS': '-Daeron.cluster.startup.canvass.timeout=15s'})
        self.assertEqual(self.chaos_knobs(), {
            'canvass': ['-Daeron.cluster.startup.canvass.timeout=15s'], 'checkpoint': '5', 'stalled': '1m',
            'force_pull': False, 'rules_unchanged': False})

    def test_the_local_profile_runs_no_canary_by_default(self):
        self.run_deploy(check=True)
        self.assertNotIn('canary', self.api.state['plans'])

    def upstream_images(self):
        """The image of each task of the jobs that run an upstream image, by task name."""
        return {t['Name']: t['Config']['image'] for name in ('anvil', 'node-exporter', 'monitoring')
                for g in self.api.state['jobs'][name]['TaskGroups'] for t in g['Tasks']}

    def test_upstream_images_run_the_pins_or_the_copies_of_the_manifest(self):
        pins = re.findall(r'^  (\w+_image): "(\S+)"$', (ANSIBLE / 'group_vars/all.yml').read_text(), re.M)
        tasks = {'anvil_image': 'anvil', 'node_exporter_image': 'node-exporter', 'prometheus_image': 'prometheus',
                 'alertmanager_image': 'alertmanager', 'grafana_image': 'grafana'}
        # A manifest without copies (a release for a real registry): the pins.
        self.deploy()
        self.assertEqual(self.upstream_images(), {tasks[var]: ref for var, ref in pins})
        # The container path records a copy of each one: the jobs run the copies.
        copies = {var: f'registry.example:5000/kardamom-upstream-{var.replace("_", "-")}:test@sha256:{"c" * 64}'
                  for var, _ in pins}
        with self.manifest.open('a') as manifest:
            manifest.writelines(f'upstream-{var.replace("_", "-")} {ref}\n' for var, ref in copies.items())
        self.run_deploy()
        self.assertEqual(self.upstream_images(), {tasks[var]: ref for var, ref in copies.items()})

    def test_secrets_reach_tasks_only_through_nomad_variables(self):
        # Every keyed URL and key the deploy gets, by the environment the
        # deploy workflow sets.
        l1 = f'https://l1.example/v3/{SENTINEL}-L1'
        followers = f'https://a.example/v2/{SENTINEL}-A,https://b.example/v3/{SENTINEL}-B'
        batcher = f'https://primary.example/v2/{SENTINEL}-P,https://fallback.example/v3/{SENTINEL}-F'
        key = f'0x{SENTINEL}-KEY'
        # An Alertmanager configuration with a receiver token, and its own
        # Go templates, which must arrive as data.
        alertmanager = Path(self.tmp.name) / 'alertmanager.yml'
        alertmanager.write_text(
            'route:\n  receiver: telegram\nreceivers:\n  - name: telegram\n    telegram_configs:\n'
            f'      - bot_token: "{SENTINEL}-BOT"\n        chat_id: 1\n'
            '        message: \'{{ .CommonLabels.alertname }} {{ range .Alerts }}{{ .Annotations.runbook }}{{ end }}\'\n')
        output = self.run_deploy(environ={
            'L1_RPC': l1, 'L1_FOLLOWERS_RPC': followers, 'BATCHER_L1_RPC': batcher, 'BATCHER_KEY': key,
            'L1_OWNER_KEY': f'0x{SENTINEL}-OWNER', 'EIGENDA_NETWORK': 'sepolia_testnet',
            'CANARY_MNEMONIC': f'{SENTINEL}-MNEMONIC', 'CANARY_L1_KEY': f'0x{SENTINEL}-CANARY',
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
            'nomad/jobs/batcher': {'KARDAMOM_L1_RPC': batcher, 'KARDAMOM_L1_KEY': key},
            'nomad/jobs/canary': {'KARDAMOM_CANARY_MNEMONIC': f'{SENTINEL}-MNEMONIC',
                                  'KARDAMOM_CANARY_L1_KEY': f'0x{SENTINEL}-CANARY', 'KARDAMOM_L1_RPC': l1},
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

    def test_the_backup_sealer_runs_only_with_a_backup_node(self):
        # Without a node of role backup, the deploy registers no backup job.
        self.deploy()
        self.assertNotIn('cluster-backup', self.api.state['plans'])
        # With one, it registers the job on the sealer image, as a backup.
        inventory = Path(self.tmp.name) / 'backup-hosts.ini'
        inventory.write_text('localhost ansible_connection=local\n[backup]\nbackup-0 ansible_connection=local\n')
        self.run_deploy({'workloads_backup_file_sync_level': '1'}, inventory=str(inventory))
        plans = self.api.state['plans']
        self.assertIn('cluster-backup', plans)
        task = plans['cluster-backup']['TaskGroups'][0]['Tasks'][0]
        self.assertEqual(task['Config']['image'], plans['cluster']['TaskGroups'][0]['Tasks'][0]['Config']['image'])
        options = task['Env']['JAVA_TOOL_OPTIONS'].split()
        self.assertIn('-Dkardamom.cluster.backup=true', options)
        self.assertIn('-Dkardamom.backup.fileSyncLevel=1', options)
        self.assertIn('-Dkardamom.backup.sourceType=FOLLOWER', options)
        self.assertEqual(plans['cluster-backup']['Constraints'][0]['RTarget'], 'backup')

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
            'workloads_cluster_file_sync_level': '1',
            'workloads_cluster_heap_mb': '2560',
            'workloads_prometheus_retention_time': '7d',
            'workloads_prometheus_retention_size': '40GB',
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
        self.assertIn('-Dkardamom.cluster.fileSyncLevel=1', json.dumps(plans['cluster']))
        # The heap reaches the image entrypoint, and the job memory follows it.
        cluster_task = plans['cluster']['TaskGroups'][0]['Tasks'][0]
        self.assertEqual(cluster_task['Env']['KARDAMOM_CLUSTER_HEAP_MB'], '2560')
        self.assertEqual(cluster_task['Resources']['MemoryMB'], 3200)
        prometheus = next(t for t in plans['monitoring']['TaskGroups'][0]['Tasks'] if t['Name'] == 'prometheus')
        self.assertIn('--storage.tsdb.retention.time=7d', prometheus['Config']['args'])
        self.assertIn('--storage.tsdb.retention.size=40GB', prometheus['Config']['args'])
        self.assertTrue(plans['monitoring']['TaskGroups'][0]['EphemeralDisk']['Sticky'])
        self.assertIn('-Dkardamom.cluster.logPurgeKeepSnapshots=5', json.dumps(plans['cluster']))
        self.assertIn('-Daeron.archive.file.sync.level=0', json.dumps(plans['aeron']))
        self.assertIn('-Daeron.archive.catalog.file.sync.level=0', json.dumps(plans['aeron']))
        # One value turns priority fees on for every role that has a say.
        self.assertIn('-Dkardamom.cluster.orderingWindow=20', json.dumps(plans['cluster']))
        self.assertEqual(self.sequencer_env(plans)['KARDAMOM_PRIORITY_FEES'], 'true')
        for name in ('executor', 'validator'):
            self.assertIn('base_fee_initial', self.genesis_template(plans[name]), name)

    def test_fault_proxy_routes_the_followers_through_it(self):
        # Staging-like batcher values ride on this run: a 12 h idle flush
        # needs a budget above 43200 blocks.
        self.run_deploy({'workloads_l1_fault_proxy': True, 'workloads_indexer_poll_s': '2',
                         'workloads_batcher_idle_flush_ms': str(12 * 3600 * 1000),
                         'workloads_da_lag_budget_blocks': '100000'})
        plans = self.api.state['plans']
        # The batcher gets the cluster's budget and posts at half of it.
        batcher = plans['batcher']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertEqual(batcher[batcher.index('--da-lag-budget-blocks') + 1], '100000')
        self.assertIn('-Dkardamom.cluster.daLagBudgetBlocks=100000', json.dumps(plans['cluster']))
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
        # Two follower instances on two nodes, each on the node's Aeron
        # driver and recording l1_blocks; anvil has no beacon chain, so no
        # schedule.
        group = plans['l1-indexer']['TaskGroups'][0]
        self.assertEqual(group['Count'], 2)
        self.assertIn('distinct_hosts', json.dumps(plans['l1-indexer'].get('Constraints') or []))
        task = group['Tasks'][0]
        self.assertIn('--archive-durability', indexer)
        self.assertEqual(indexer[indexer.index('--log-config') + 1], '/local/channels.toml')
        self.assertEqual(indexer[indexer.index('--aeron-dir') + 1], '/opt/kardamom/aeron-mount/dir')
        self.assertNotIn('--beacon-api', indexer)
        self.assertIn('/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount', task['Config']['volumes'])
        # The da-watcher has no L1 endpoint; its history reads replay the
        # follower archives onto its own ports and fall back to the
        # follower's API.
        watcher = plans['da-watcher']['TaskGroups'][0]['Tasks'][0]['Config']['args']
        self.assertIn('--l1-blocks', watcher)
        self.assertNotIn('--l1-rpc', watcher)
        self.assertNotIn('kardamom-l1-fault-proxy', json.dumps(plans['da-watcher']))
        self.assertEqual(watcher[watcher.index('--indexer-url') + 1], 'http://kardamom-l1-indexer.service.consul:8549')
        self.assertIn('--replay-destination-endpoint', watcher)
        self.assertIn('--archive-control-response-endpoint', watcher)

    def test_the_follower_takes_its_count_and_its_log_range(self):
        self.run_deploy({'workloads_l1_fault_proxy': True, 'workloads_indexer_count': '1',
                         'workloads_indexer_max_log_range': '2000'}, check=True)
        group = self.api.state['plans']['l1-indexer']['TaskGroups'][0]
        self.assertEqual(group['Count'], 1)
        args = group['Tasks'][0]['Config']['args']
        self.assertEqual(args[args.index('--max-log-range') + 1], '2000')

    def test_the_batcher_posts_before_the_da_lag_guard_halts_an_idle_chain(self):
        # The budget below the idle interval in blocks halts an idle chain:
        # the role refuses it before any job. The accepted case rides on
        # the fault proxy test's deploy run.
        output = self.run_deploy({'workloads_batcher_idle_flush_ms': str(12 * 3600 * 1000)},
                                 check=True, success=False)
        self.assertIn('must be below the DA-lag budget', output)

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

    def test_every_redis_instance_names_a_primary_on_a_cold_start(self):
        # A sentinel stops at start on a config without a `sentinel
        # monitor` line. So the seed always names a primary: the
        # registered one, else the node that the placement rule of the
        # primary group picks. The replica follows the same node.
        self.run_deploy(check=True)
        groups = {g['Name']: g for g in self.api.state['plans']['redis']['TaskGroups']}
        self.assertEqual(groups['primary']['Constraints'],
                         [{'LTarget': '${meta.roles}', 'RTarget': 'redis-primary', 'Operand': 'set_contains'}])
        sentinel = groups['sentinel']['Tasks'][0]
        seed = {t['DestPath']: t['EmbeddedTmpl'] for t in sentinel['Templates']}['local/sentinel.seed.conf']
        monitor = re.search(r'^sentinel monitor kardamom (.*)\.node\.dc1\.consul 6379 2$', seed, re.MULTILINE)
        self.assertIsNotNone(monitor, seed)
        primary = monitor.group(1)
        self.assertTrue(primary.startswith('{{ $primary := "" }}{{ with service "redis-primary" }}'), primary)
        self.assertIn('(.Meta.roles | split "," | contains "redis-primary")', primary)
        self.assertTrue(primary.endswith('{{ $primary }}'), primary)
        replica = groups['replica']['Tasks'][0]
        self.assertEqual(replica['Templates'][0]['EmbeddedTmpl'], f'REDIS_PRIMARY_NODE={primary}\n')
        self.assertEqual(replica['Config']['args'][:2], ['sh', '-c'])
        self.assert_replica_start(replica['Config']['args'][2])
        self.assertEqual(sentinel['Config']['args'][:2], ['sh', '-c'])
        self.assert_sentinel_start(sentinel['Config']['args'][2])
        self.assertEqual(self.api.state['writes'], [])

    @staticmethod
    def run_task_script(root, script, program, files):
        """Run the start script of a task in `root`: its /local is
        `root`/local with `files` in it (a path such as /usr/local stays),
        and `program` is a stub that prints `started` and its arguments."""
        (root / 'local').mkdir()
        (root / 'bin').mkdir()
        stub = root / 'bin' / program
        stub.write_text('#!/bin/sh\necho "started $*"\n')
        stub.chmod(0o755)
        for name, text in files.items():
            (root / 'local' / name).write_text(text)
        return subprocess.run(['sh', '-c', re.sub(r'(?<![\w/])/local/', f'{root}/local/', script)],
                              env={'PATH': f'{root}/bin:/usr/bin:/bin'}, text=True,
                              capture_output=True, timeout=10)

    def assert_replica_start(self, script):
        """The replica start script reads the rendered primary node at
        each start. An empty node stops the task with the cause, and a
        set node starts Redis as a replica of that node."""
        script = script.replace('${node.unique.name}', 'ingress-1')
        with self.subTest('no primary'), tempfile.TemporaryDirectory() as tmp:
            result = self.run_task_script(Path(tmp), script, 'docker-entrypoint.sh',
                                          {'primary.env': 'REDIS_PRIMARY_NODE=\n'})
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertEqual(result.stdout, '')
            self.assertIn('no primary to follow: no redis-primary service passes, '
                          'and no Consul node meta roles holds redis-primary', result.stderr)
        with self.subTest('a primary'), tempfile.TemporaryDirectory() as tmp:
            result = self.run_task_script(Path(tmp), script, 'docker-entrypoint.sh',
                                          {'primary.env': 'REDIS_PRIMARY_NODE=aux-0\n'})
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(result.stdout, 'started redis-server /usr/local/etc/redis/redis.conf --dir /local '
                             '--replicaof aux-0.node.dc1.consul 6379 '
                             '--replica-announce-ip ingress-1.node.dc1.consul\n')

    def assert_sentinel_start(self, script):
        """Run the start script of a sentinel task on each state of its
        files. The task keeps a live config that Sentinel rewrote (it has
        `sentinel myid`), copies a seed that names a primary over any
        other live config, and refuses a seed that names none without
        writing the live config."""
        good = 'sentinel monitor kardamom aux-0.node.dc1.consul 6379 2\n'
        empty = 'sentinel monitor kardamom .node.dc1.consul 6379 2\n'
        rewritten = 'sentinel monitor kardamom ingress-1.node.dc1.consul 6379 2\nsentinel myid abc\n'
        cases = [
            ('a cold start', good, None, 0, good),
            ('a seed that names no primary', empty, None, 1, None),
            ('a restart after Sentinel rewrote its config', empty, rewritten, 0, rewritten),
            ('a restart after a refused start, with a good seed now', good, empty, 0, good),
        ]
        for name, seed, live, code, after in cases:
            with self.subTest(name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                files = {'sentinel.seed.conf': seed} | ({'sentinel.conf': live} if live is not None else {})
                result = self.run_task_script(root, script, 'redis-sentinel', files)
                self.assertEqual(result.returncode, code, result.stdout + result.stderr)
                config = root / 'local/sentinel.conf'
                self.assertEqual(config.read_text() if config.exists() else None, after)
                if code:
                    self.assertIn('the seed names no primary', result.stderr)
                else:
                    self.assertEqual(result.stdout, f'started {root}/local/sentinel.conf\n')

    def test_the_default_plans_turn_priority_fees_off_and_skip_the_append_sync(self):
        self.run_deploy(check=True)
        plans = self.api.state['plans']
        self.assertIn('-Dkardamom.cluster.orderingWindow=0', json.dumps(plans['cluster']))
        # A sealer commit waits for no disk sync; the kernel writes the log back.
        self.assertIn('-Dkardamom.cluster.fileSyncLevel=0', json.dumps(plans['cluster']))
        self.assertEqual(self.sequencer_env(plans)['KARDAMOM_PRIORITY_FEES'], 'false')
        for name in ('executor', 'validator'):
            self.assertNotIn('base_fee_initial', self.genesis_template(plans[name]), name)
            # A deployment without the local canary keeps dev.toml as it
            # is: a live chain refuses a changed genesis.
            template = self.genesis_template(plans[name]).encode()
            self.assertEqual(hashlib.sha256(template).hexdigest(), STAGING_GENESIS_SHA256, name)

    def test_the_driver_threading_modes_default_shared_and_reach_the_drivers(self):
        for mode in ('', 'DEDICATED'):
            with self.subTest(mode=mode):
                self.run_deploy({'workloads_aeron_threading_mode': mode,
                                 'workloads_cluster_driver_threading_mode': mode}, check=True)
                plans = self.api.state['plans']
                expected = mode or 'SHARED'
                aeron = plans['aeron']['TaskGroups'][0]['Tasks'][0]['Env']
                self.assertEqual(aeron['AERON_THREADING_MODE'], expected)
                cluster = plans['cluster']['TaskGroups'][0]['Tasks'][0]['Env']['JAVA_TOOL_OPTIONS'].split()
                self.assertIn(f'-Dkardamom.cluster.driverThreadingMode={expected}', cluster)

    def test_the_exec_cursor_switch_defaults_off_and_reaches_the_executor(self):
        for switch, expected in (('', 'false'), ('on', 'true')):
            with self.subTest(switch=switch):
                self.run_deploy({'workloads_exec_cursor': switch}, check=True)
                env = self.api.state['plans']['executor']['TaskGroups'][0]['Tasks'][0]['Env']
                self.assertEqual(env['KARDAMOM_EXEC_CURSOR'], expected)

    # Every egress switch on: the egress job and each reader's flag.
    EGRESS_ON = {'workloads_egress': True, 'workloads_exec_frames': 'on', 'workloads_validator_egress': 'on',
                 'workloads_batcher_egress': 'on', 'workloads_sequencer_receipts_from_egress': 'on',
                 'workloads_ingress_receipts_from_egress': 'on', 'workloads_notifier_receipts_from_egress': 'on'}
    EGRESS_URL = 'http://kardamom-egress.service.dc1.consul:8548'

    @staticmethod
    def job_args(job):
        """The arguments of every task of `job`, in one list."""
        return [a for g in job['TaskGroups'] for t in g['Tasks'] for a in t['Config'].get('args') or []]

    @staticmethod
    def job_templates(job):
        """The embedded templates of every task of `job`, in one text."""
        return '\n'.join(t['EmbeddedTmpl'] for g in job['TaskGroups'] for task in g['Tasks']
                         for t in task.get('Templates') or [])

    def test_the_sequencer_admission_switch_adds_the_v2_section(self):
        """Empty keeps the v1 loop; on appends the [admission] table."""
        for switch, present in (('', False), ('on', True)):
            with self.subTest(switch=switch):
                self.run_deploy({'workloads_sequencer_admission': switch}, check=True)
                templates = self.job_templates(self.api.state['plans']['sequencer'])
                self.assertEqual('[admission]\nenabled = true\nmax_parked = 512\nmax_forwarded = 512' in templates,
                                 present)

    def test_the_egress_switches_default_off(self):
        """With every switch off, no egress job and no reader flag."""
        self.run_deploy(check=True)
        plans = self.api.state['plans']
        self.assertNotIn('egress', plans)
        flags = {'--exec-frames', '--egress', '--receipt-query-endpoints', '--receipts-from-egress', '--events',
                 '--executed'}
        for name in ('executor', 'validator', 'batcher', 'ingress', 'notifier'):
            self.assertEqual(flags & set(self.job_args(plans[name])), set(), name)
        self.assertNotIn('receipts_from_egress', self.job_templates(plans['sequencer']))
        self.assertNotIn('service "kardamom-egress-metrics|any"', self.job_templates(plans['monitoring']))

    def test_the_egress_switches_deploy_the_egress_and_reach_each_reader(self):
        self.add_egress_image()
        # A new reader starts on the egress only while every egress
        # instance holds no reference (gate 8b). So the egress runs first,
        # and the readers are new jobs.
        readers = ('workloads_validator_egress', 'workloads_batcher_egress')
        self.run_deploy({k: v for k, v in self.EGRESS_ON.items() if k not in readers})
        for job in ('batcher', 'validator'):
            del self.api.state['jobs'][job]
        for node in ('node-egress', 'node-egress.1'):
            self.api.state.setdefault('metrics', {})[node] = (
                'kardamom_egress_ref_frame_max_index 0\nkardamom_egress_ref_frame_max_block 0\n')
        self.run_deploy(self.EGRESS_ON, check=True)
        plans = self.api.state['plans']
        # After the executors that it reads, before the readers after it.
        order = list(plans)
        self.assertLess(order.index('executor'), order.index('egress'))
        for reader in ('notifier', 'validator', 'batcher'):
            self.assertLess(order.index('egress'), order.index(reader), reader)
        egress = plans['egress']
        self.assertTrue(egress['TaskGroups'][0]['Tasks'][0]['Config']['image'].endswith('@sha256:' + 'a' * 64))
        self.assertEqual(egress['TaskGroups'][0]['Count'], 2)
        egress_args = self.job_args(egress)
        self.assertEqual(egress_args[egress_args.index('--peers') + 1],
                         ','.join(f'http://executor-{i}.node.dc1.consul:8548' for i in range(3)))
        services = {s['Name']: s for t in egress['TaskGroups'][0]['Tasks'] for s in t['Services']}
        self.assertEqual(services['kardamom-egress']['Checks'][0]['Path'], '/health')
        self.assertEqual(services['kardamom-egress-metrics']['Tags'], ['metrics'])
        self.assertIn('--exec-frames', self.job_args(plans['executor']))
        for name, flag in (('validator', '--egress'), ('batcher', '--egress'),
                           ('ingress', '--receipt-query-endpoints')):
            args = self.job_args(plans[name])
            self.assertEqual(args[args.index(flag) + 1], self.EGRESS_URL, name)
        self.assertIn('--receipts-from-egress', self.job_args(plans['notifier']))
        self.assertIn('receipts_from_egress = true', self.job_templates(plans['sequencer']))
        self.assertIn('service "kardamom-egress-metrics|any"', self.job_templates(plans['monitoring']))

    def test_the_egress_reads_the_output_oracle_only_when_one_is_given(self):
        self.add_egress_image()
        switches = {k: v for k, v in self.EGRESS_ON.items()
                    if k not in ('workloads_validator_egress', 'workloads_batcher_egress')}
        self.run_deploy(switches, check=True)
        args = self.job_args(self.api.state['plans']['egress'])
        self.assertEqual({'--l1-rpc-url', '--withdrawal-oracle'} & set(args), set())
        # The window of the readers that resume: one challenge window.
        self.assertEqual(args[args.index('--keep-blocks') + 1], '43200')
        oracle = '0x' + '3' * 40
        cases = (
            ('the in-cluster anvil', {}, 'http://anvil.service.consul:8546'),
            ('the light client', {'workloads_light_execution_rpc': 'https://execution.example',
                                  'workloads_light_consensus_rpc': 'https://consensus.example',
                                  'workloads_light_checkpoint': '0x' + 'a' * 64,
                                  'workloads_lockbox_address': '0x' + '2' * 40},
             'http://kardamom-l1-light-client.service.dc1.consul:8548'),
        )
        for name, extra, l1 in cases:
            with self.subTest(name):
                self.run_deploy({**switches, **extra, 'workloads_withdrawal_oracle_address': oracle},
                                check=True)
                args = self.job_args(self.api.state['plans']['egress'])
                self.assertEqual(args[args.index('--withdrawal-oracle') + 1], oracle)
                self.assertEqual(args[args.index('--l1-rpc-url') + 1], l1)

    def test_the_notifier_events_path_takes_its_two_switches(self):
        """KARDAMOM_NOTIFIER_EVENTS and KARDAMOM_NOTIFIER_EXECUTED reach the notifier, and only the notifier."""
        self.run_deploy({'workloads_notifier_events': 'on'}, check=True)
        args = self.job_args(self.api.state['plans']['notifier'])
        self.assertEqual(args[args.index('--events') + 1], 'on')
        self.assertEqual(args[args.index('--executed') + 1], 'off')
        self.run_deploy({'workloads_notifier_events': 'on', 'workloads_notifier_executed': 'on'}, check=True)
        args = self.job_args(self.api.state['plans']['notifier'])
        self.assertEqual(args[args.index('--executed') + 1], 'on')
        for name in ('ingress', 'sequencer', 'executor'):
            self.assertNotIn('--events', self.job_args(self.api.state['plans'][name]), name)

    def test_the_egress_nodes_switch_moves_only_the_egress(self):
        """Off, the egress places on the executor nodes. On, it places on
        the nodes with egress in meta.roles, and its peers name the egress
        nodes and the executor nodes, so a moved instance fills its store
        from the one that did not move. No other job changes."""
        self.add_egress_image()
        # A new reader starts on the egress only while every egress
        # instance holds no reference (gate 8b). So the egress runs first,
        # and the readers are new jobs, as in the egress switch test.
        readers = ('workloads_validator_egress', 'workloads_batcher_egress')
        self.run_deploy({k: v for k, v in self.EGRESS_ON.items() if k not in readers})
        for job in ('batcher', 'validator'):
            del self.api.state['jobs'][job]
        for node in ('node-egress', 'node-egress.1'):
            self.api.state.setdefault('metrics', {})[node] = (
                'kardamom_egress_ref_frame_max_index 0\nkardamom_egress_ref_frame_max_block 0\n')
        plans = {}
        cases = (
            ({}, ('${meta.role}', '=', 'executor'), [f'executor-{i}' for i in range(3)]),
            ({'KARDAMOM_EGRESS_NODES': 'on'}, ('${meta.roles}', 'set_contains', 'egress'),
             ['egress-0', 'egress-1'] + [f'executor-{i}' for i in range(3)]),
        )
        for environ, constraint, peers in cases:
            with self.subTest(environ=environ):
                self.run_deploy(self.EGRESS_ON, check=True, environ=environ)
                plans[bool(environ)] = self.api.state['plans']
                egress = plans[bool(environ)]['egress']
                [placed] = egress['Constraints']
                self.assertEqual((placed['LTarget'], placed['Operand'], placed['RTarget']), constraint)
                self.assertIn({'LTarget': '', 'RTarget': 'true', 'Operand': 'distinct_hosts'},
                              egress['TaskGroups'][0]['Constraints'])
                args = self.job_args(egress)
                self.assertEqual(args[args.index('--peers') + 1],
                                 ','.join(f'http://{n}.node.dc1.consul:8548' for n in peers))
        self.assertEqual({n: j for n, j in plans[True].items() if n != 'egress'},
                         {n: j for n, j in plans[False].items() if n != 'egress'})

    def test_the_preflight_refuses_the_egress_nodes_switch_without_two_egress_nodes(self):
        """With the switch on, each egress instance needs a ready, eligible
        node with egress in meta.roles, before any job changes. Without the
        egress (KARDAMOM_EGRESS empty) the switch counts no node."""
        self.api.state['nodes'] = [nomad_node('egress-0', 'egress'), nomad_node('egress-1', 'egress', status='down')]
        output = self.run_deploy(self.EGRESS_ON, check=True, success=False, environ={'KARDAMOM_EGRESS_NODES': 'on'})
        self.assertIn("The egress count (2) is above the count of the ready, eligible Nomad nodes "
                      "with egress in meta.roles (['egress-0'])", output)
        self.assertNotIn('plans', self.api.state)
        self.run_deploy(check=True, environ={'KARDAMOM_EGRESS_NODES': 'on'})

    def test_the_egress_switches_refuse_a_reader_before_its_source(self):
        cases = (
            ('a reader without the egress', {'workloads_validator_egress': 'on'}),
            ('the egress without the frames', {'workloads_egress': True}),
            ('a value that is not on or off', {'workloads_exec_frames': 'yes'}),
        )
        for name, switches in cases:
            with self.subTest(name):
                output = self.run_deploy(switches, check=True, success=False)
                self.assertIn('Each egress switch is on, off or empty', output)
                self.assertNotIn('plans', self.api.state)

    def test_the_checkpoint_log_switch_defaults_off_and_reaches_the_executor(self):
        for switch, expected in (('', 'false'), ('on', 'true')):
            with self.subTest(switch=switch):
                self.run_deploy({'workloads_checkpoint_log': switch}, check=True)
                env = self.api.state['plans']['executor']['TaskGroups'][0]['Tasks'][0]['Env']
                self.assertEqual(env['KARDAMOM_CHECKPOINT_LOG'], expected)

    def test_the_ingress_ack_defaults_to_the_receipt_and_reaches_the_ingress(self):
        for switch, expected in (('', 'receipt'), ('validated', 'validated')):
            with self.subTest(switch=switch):
                self.run_deploy({'workloads_ingress_ack': switch}, check=True)
                args = self.api.state['plans']['ingress']['TaskGroups'][0]['Tasks'][0]['Config']['args']
                self.assertEqual(args[args.index('--ack') + 1], expected)

    # The jobs that the switches of the carry window change.
    CARRY_JOBS = ('aeron', 'cluster', 'executor', 'state-mirror', 'sequencer', 'ingress', 'batcher',
                  'validator', 'monitoring')
    # Every switch of the carry window, off.
    CARRY_OFF = {'KARDAMOM_MEMBER_FEED': 'off', 'KARDAMOM_EXEC_ON_SEALERS': 'off', 'KARDAMOM_CARRY_EXECUTORS': '',
                 'KARDAMOM_SEALER_VERSION_FLOOR': '0'}
    MEMBER_FEED = {'KARDAMOM_MEMBER_FEED': 'on'}
    ON_SEALERS = MEMBER_FEED | {'KARDAMOM_EXEC_ON_SEALERS': 'on'}

    def test_the_carry_switches_off_render_every_job_as_before(self):
        """An explicit off and unset switches render the same jobs: no feed,
        the executors on the executor nodes, no floor and no new meta."""
        self.run_deploy(check=True)
        unset = self.api.state['plans']
        self.run_deploy(check=True, environ=self.CARRY_OFF)
        self.assertEqual(self.api.state['plans'], unset)
        executor = unset['executor']
        self.assertEqual([(g['Name'], g['Count']) for g in executor['TaskGroups']], [('executor', 3)])
        self.assertEqual(executor['Constraints'], [{'LTarget': '${meta.role}', 'RTarget': 'executor', 'Operand': '='}])
        self.assertEqual(executor['TaskGroups'][0]['Constraints'],
                         [{'LTarget': '', 'RTarget': 'true', 'Operand': 'distinct_hosts'}])
        self.assertEqual({'--carry-bytes', '--egress'} & set(self.job_args(executor)), set())
        self.assertNotIn('carryBytes', json.dumps(unset['cluster']))
        self.assertNotIn('feedAeronDir', json.dumps(unset['cluster']))
        self.assertIn({'LTarget': '${meta.role}', 'RTarget': 'sealer', 'Operand': '!='},
                      unset['aeron']['Constraints'])
        self.assertNotIn('--sealer-version-floor', self.job_args(unset['sequencer']))
        for name in ('batcher', 'validator'):
            self.assertNotIn('reads_egress', unset[name]['Meta'], name)
        for name in self.CARRY_JOBS:
            self.assertNotIn('sealer-0.node.dc1.consul:90', json.dumps(unset[name]), name)

    def test_the_member_feed_reaches_the_members_and_the_shared_driver(self):
        self.run_deploy(check=True, environ=self.MEMBER_FEED)
        plans = self.api.state['plans']
        for group in plans['cluster']['TaskGroups']:
            task = group['Tasks'][0]
            options = task['Env']['JAVA_TOOL_OPTIONS'].split()
            self.assertIn('-Dkardamom.cluster.feedAeronDir=/opt/kardamom/aeron-mount/dir', options)
            self.assertIn('-Dkardamom.cluster.feedRingBytes=16777216', options)
            # No retained file and no token: the feed walks the heap.
            self.assertNotIn('carryBytes', task['Env']['JAVA_TOOL_OPTIONS'])
            self.assertEqual([t['DestPath'] for t in task['Templates'] if t['Envvars']], [])
        self.assertNotIn('sealer', json.dumps(plans['aeron']['Constraints']))
        self.assertEqual([g['Name'] for g in plans['executor']['TaskGroups']], ['executor'])

    def test_the_executors_on_the_sealer_nodes_pin_one_group_to_each_member(self):
        self.run_deploy(check=True, environ=self.ON_SEALERS)
        plans = self.api.state['plans']
        # Executor <i> runs on the node of member <i>, with the member index
        # as its id, in reference mode. The role constraint is in each group,
        # so a step of the move holds the group of the executor nodes too.
        executor = plans['executor']
        self.assertIsNone(executor['Constraints'])
        # The executor nodes stay peers: the first executor that moves has
        # no state, and only the executors there hold a checkpoint.
        peers = ','.join([f'sealer-{i}.node.dc1.consul:9014' for i in range(3)]
                         + [f'executor-{i}.node.dc1.consul:9014' for i in range(3)])
        for i, group in enumerate(executor['TaskGroups']):
            self.assertEqual((group['Name'], group['Count']), (f'executor-{i}', 1))
            self.assertEqual(group['Constraints'],
                             [{'LTarget': '${meta.node_index}', 'RTarget': str(i), 'Operand': '='},
                              {'LTarget': '${meta.role}', 'RTarget': 'sealer', 'Operand': '='}])
            task = group['Tasks'][0]
            args = task['Config']['args']
            self.assertEqual({'--carry-bytes', '--egress'} & set(args), set())
            self.assertEqual(args[args.index('--aeron-dir') + 1], '/opt/kardamom/aeron-mount/dir')
            self.assertEqual(args[args.index('--recorder-id') + 1], str(i))
            self.assertEqual(args[args.index('--void-voter-id') + 1], str(i))
            self.assertEqual(args[args.index('--checkpoint-peers') + 1], peers)
            self.assertEqual(task['Env']['KARDAMOM_HOST_ID'], f'executor-{i}')
        # The state mirror and every reader of an executor follow the
        # executors to the sealer nodes.
        self.assertEqual(plans['state-mirror']['Constraints'][0]['RTarget'], 'sealer')
        for name in ('sequencer', 'ingress', 'batcher', 'validator', 'monitoring'):
            text = json.dumps(plans[name])
            self.assertIn('sealer-2.node.dc1.consul:90', text, name)
            self.assertNotIn('executor-0.node.dc1.consul:90', text, name)
        self.assertEqual(self.api.state['variables'], {}, 'no Nomad variable for a member group')

    def test_the_carry_switches_refuse_an_incomplete_setting(self):
        egress = {'KARDAMOM_EGRESS': '1', 'KARDAMOM_EXEC_FRAMES': 'on'}
        cases = (
            ('a feed value that is not on or off', {'KARDAMOM_MEMBER_FEED': 'yes'},
             'KARDAMOM_MEMBER_FEED and KARDAMOM_EXEC_ON_SEALERS are on, off or empty'),
            ('the executors on the sealer nodes without the feed', {'KARDAMOM_EXEC_ON_SEALERS': 'on'},
             'KARDAMOM_EXEC_ON_SEALERS=on needs'),
            ('a carry executor on the executor nodes', egress | {'KARDAMOM_CARRY_EXECUTORS': '0'},
             'is empty or a list of distinct member indexes'),
            ('a carry executor without the egress', self.ON_SEALERS | {'KARDAMOM_CARRY_EXECUTORS': '0'},
             'is empty or a list of distinct member indexes'),
            ('a carry executor with the recorded cursor',
             self.ON_SEALERS | egress | {'KARDAMOM_CARRY_EXECUTORS': '0', 'KARDAMOM_EXEC_CURSOR': 'on'},
             'is empty or a list of distinct member indexes'),
            ('a repeated carry executor', self.ON_SEALERS | egress | {'KARDAMOM_CARRY_EXECUTORS': '0,0'},
             'is empty or a list of distinct member indexes'),
            ('a carry executor without a member', self.ON_SEALERS | egress | {'KARDAMOM_CARRY_EXECUTORS': '3'},
             'is empty or a list of distinct member indexes'),
            ('a carry executor without the egress URL', self.ON_SEALERS | egress | {'KARDAMOM_CARRY_EXECUTORS': '0'},
             'with the egress URL of the executors', {'workloads_egress_url': ''}),
            ('a floor above decision version 5', {'KARDAMOM_SEALER_VERSION_FLOOR': '6'},
             'KARDAMOM_SEALER_VERSION_FLOOR ("6") is empty or a decision version from 0 to 5'),
            ('a retention above the cap', {'KARDAMOM_CLUSTER_RETENTION': '131073'},
             'KARDAMOM_CLUSTER_RETENTION ("131073") is empty or a frame count from 1 to 131072'),
            ('a retention of 0', {'KARDAMOM_CLUSTER_RETENTION': '0'}, 'is empty or a frame count from 1 to 131072'),
        )
        self.add_egress_image()
        for name, environ, message, *extra in cases:
            with self.subTest(name):
                output = self.run_deploy(*extra, check=True, success=False, environ=environ)
                self.assertIn(message, output)
                self.assertNotIn('plans', self.api.state)

    # The carry from genesis.
    GENESIS = {'KARDAMOM_GENESIS_CARRY': 'on'}

    def test_the_genesis_carry_unset_renders_every_job_as_before(self):
        """Unset, empty and off render the same jobs as before: no genesis
        features in the sealer job or in the genesis of the executors and
        the validator, and every carry switch at its default."""
        self.run_deploy(check=True)
        unset = self.api.state['plans']
        for value in ('', 'off'):
            with self.subTest(value=value):
                self.run_deploy(check=True, environ={'KARDAMOM_GENESIS_CARRY': value})
                self.assertEqual(self.api.state['plans'], unset)
        text = json.dumps(unset)
        self.assertNotIn('genesisFeatures', text)
        self.assertNotIn('genesis_features', text)
        for group in unset['cluster']['TaskGroups']:
            options = group['Tasks'][0]['Env']['JAVA_TOOL_OPTIONS']
            self.assertTrue(options.endswith(' -Dkardamom.cluster.orderingWindow=0'), options)
        for name in ('executor', 'validator'):
            template = self.genesis_template(unset[name]).encode()
            self.assertEqual(hashlib.sha256(template).hexdigest(), STAGING_GENESIS_SHA256, name)
        self.assertNotIn('egress', unset)
        self.assertNotIn('--sealer-version-floor', self.job_args(unset['sequencer']))

    def test_the_genesis_carry_sets_every_carry_switch(self):
        """On a new cluster: the genesis features in every member and in the
        genesis, and every carry switch on."""
        self.add_egress_image()
        self.run_deploy({'workloads_cluster_bootstrap': True}, check=True, environ=self.GENESIS)
        plans = self.api.state['plans']
        for group in plans['cluster']['TaskGroups']:
            options = group['Tasks'][0]['Env']['JAVA_TOOL_OPTIONS'].split()
            self.assertIn('-Dkardamom.cluster.genesisFeatures=3,4,5', options)
            self.assertIn('-Dkardamom.cluster.feedAeronDir=/opt/kardamom/aeron-mount/dir', options)
        executor = plans['executor']
        self.assertEqual([g['Name'] for g in executor['TaskGroups']], ['executor-0', 'executor-1', 'executor-2'])
        for group in executor['TaskGroups']:
            args = group['Tasks'][0]['Config']['args']
            self.assertIn('--carry-bytes', args)
            self.assertIn('--exec-frames', args)
            self.assertEqual(args[args.index('--egress') + 1], self.EGRESS_URL)
        self.assertIn('egress', plans)
        for name in ('batcher', 'validator'):
            args = self.job_args(plans[name])
            self.assertEqual(args[args.index('--egress') + 1], self.EGRESS_URL, name)
        args = self.job_args(plans['sequencer'])
        self.assertEqual(args[args.index('--sealer-version-floor') + 1], '5')
        self.assertIn('[admission]\nenabled = true', self.job_templates(plans['sequencer']))
        genesis = ANSIBLE.parent / 'config/genesis'
        dev = tomllib.loads((genesis / 'dev.toml').read_text())
        for name in ('executor', 'validator'):
            template = self.genesis_template(plans[name])
            self.assertTrue(template.startswith((genesis / 'carry.toml').read_text()), name)
            self.assertEqual(tomllib.loads(template), dev | {'genesis_features': [3, 4, 5]}, name)

    def test_the_genesis_carry_refuses_another_carry_switch(self):
        cases = (
            ('a value that is not on or off', {'KARDAMOM_GENESIS_CARRY': 'yes'},
             'KARDAMOM_GENESIS_CARRY ("yes") is on, off or empty'),
            ('the recorded cursor', self.GENESIS | {'KARDAMOM_EXEC_CURSOR': 'on'},
             'KARDAMOM_EXEC_CURSOR ("on") must not be on'),
            ('a floor below 5', self.GENESIS | {'KARDAMOM_SEALER_VERSION_FLOOR': '4'},
             "['KARDAMOM_SEALER_VERSION_FLOOR'] are ['4'], and the carry from genesis needs ['5']"),
            ('a partial carry executor list', self.GENESIS | {'KARDAMOM_CARRY_EXECUTORS': '0,1'},
             "['KARDAMOM_CARRY_EXECUTORS'] are ['0,1'], and the carry from genesis needs ['0,1,2']"),
            ('the member feed off', self.GENESIS | {'KARDAMOM_MEMBER_FEED': 'off'},
             "['KARDAMOM_MEMBER_FEED'] are ['off']"),
            ('the v1 loop', self.GENESIS | {'KARDAMOM_SEQUENCER_ADMISSION': 'off'},
             "['KARDAMOM_SEQUENCER_ADMISSION'] are ['off']"),
            ('no egress', self.GENESIS | {'KARDAMOM_EGRESS': '0'}, "['KARDAMOM_EGRESS'] are ['0']"),
        )
        self.add_egress_image()
        for name, environ, message in cases:
            with self.subTest(name):
                output = self.run_deploy({'workloads_cluster_bootstrap': True}, check=True, success=False,
                                         environ=environ)
                self.assertIn(message, output)
                self.assertNotIn('plans', self.api.state)

    def test_a_floor_on_a_new_chain_is_refused(self):
        output = self.run_deploy(check=True, success=False, environ={'KARDAMOM_SEALER_VERSION_FLOOR': '5'})
        self.assertIn('Nomad does not know the sealer job. A new chain starts on decision version 3', output)
        self.assertNotIn('sequencer', self.api.state.get('plans', {}))

    def test_every_aeron_party_takes_the_deploy_values(self):
        """Each Aeron deploy value: its environment variable, its default,
        a second value, and the check of the plans."""
        cases = (('AERON_STALL_TOLERANCE_MS', 10000, 30000, self.assert_aeron_parties),
                 ('KARDAMOM_IDLE_CAP_US', 50, 200, self.assert_idle_cap))
        for variable, default, other, check in cases:
            for value in (default, other):
                with self.subTest(variable=variable, value=value):
                    environ = {} if value == default else {variable: str(value)}
                    self.run_deploy(check=True, environ=environ)
                    check(self.api.state['plans'], value)

    def test_a_bad_idle_cap_fails_the_deploy(self):
        for value in ('0', 'abc', '1000001'):
            with self.subTest(value=value):
                self.run_deploy(check=True, success=False, environ={'KARDAMOM_IDLE_CAP_US': value})

    # The plan of the notifier and of the media drivers for each value of
    # the notifier switch: the job constraint, the count, the memory, the
    # ring cap, and each driver group with its node class
    # constraint and its idle cap. The fake cluster has two notifier nodes.
    NOTIFIER_PLACEMENTS = (
        ({}, {
            'constraint': ('${meta.role}', '=', 'ingress'), 'count': 2, 'memory': 1024, 'ring': 1000000,
            'drivers': {'aeron': (None, '50')}}),
        ({'KARDAMOM_NOTIFIER_NODES': 'off'}, {
            'constraint': ('${meta.role}', '=', 'ingress'), 'count': 2, 'memory': 1024, 'ring': 1000000,
            'drivers': {'aeron': (None, '50')}}),
        ({'KARDAMOM_NOTIFIER_NODES': 'on'}, {
            'constraint': ('${meta.roles}', 'set_contains', 'notifier'), 'count': 1, 'memory': 512, 'ring': 250000,
            'drivers': {'aeron': ('!=', '50'), 'aeron-notifier': ('=', '1000')}}),
        ({'KARDAMOM_NOTIFIER_NODES': 'on', 'KARDAMOM_NOTIFIER_COUNT': '2'}, {
            'constraint': ('${meta.roles}', 'set_contains', 'notifier'), 'count': 2, 'memory': 512, 'ring': 250000,
            'drivers': {'aeron': ('!=', '50'), 'aeron-notifier': ('=', '1000')}}),
    )

    def test_the_notifier_nodes_switch_defaults_off_and_moves_the_notifier(self):
        """Off, every plan is the default plan. On, only the notifier and
        the media drivers change: the monitoring and the canary find the
        notifier through its Consul service records. The local canary
        runs, so its plan is in the comparison."""
        plans = {}
        for environ, expected in self.NOTIFIER_PLACEMENTS:
            with self.subTest(environ=environ):
                self.run_deploy(check=True, environ=environ | {'CANARY_LOCAL': '1'})
                plans[json.dumps(environ)] = self.api.state['plans']
                self.assert_notifier_placement(self.api.state['plans'], expected)
        default = plans['{}']
        for environ, current in plans.items():
            moved = {'notifier', 'aeron'} if '"on"' in environ else set()
            self.assertEqual({n: j for n, j in current.items() if n not in moved},
                             {n: j for n, j in default.items() if n not in moved}, environ)
        self.assertIn('service "kardamom-notifier-metrics|any"', self.job_templates(default['monitoring']))
        self.assertIn('ws://kardamom-notifier.service.consul:8547', self.job_args(default['canary']))

    def test_a_bad_notifier_switch_fails_the_deploy(self):
        for environ in ({'KARDAMOM_NOTIFIER_NODES': 'yes'}, {'KARDAMOM_NOTIFIER_COUNT': '0'}):
            with self.subTest(environ=environ):
                self.run_deploy(check=True, success=False, environ=environ)

    def test_the_preflight_refuses_more_notifiers_than_notifier_nodes(self):
        """The preflight counts the ready, eligible nodes whose role set
        holds notifier, before any job changes. A down node, a drained node
        and a role that only starts with notifier do not count."""
        self.api.state['nodes'] = [
            nomad_node('notifier-0', 'redis, notifier'), nomad_node('notifier-1', 'notifier', status='down'),
            nomad_node('notifier-2', 'notifier', eligibility='ineligible'), nomad_node('aux-0', 'notifiers')]
        output = self.run_deploy(check=True, success=False,
                                 environ={'KARDAMOM_NOTIFIER_NODES': 'on', 'KARDAMOM_NOTIFIER_COUNT': '2'})
        self.assertIn("KARDAMOM_NOTIFIER_COUNT (2) is above the count of the ready, eligible Nomad nodes "
                      "with notifier in meta.roles (['notifier-0'])", output)
        self.assertNotIn('plans', self.api.state)
        self.api.state['nodes'] = []
        output = self.run_deploy(check=True, success=False, environ={'KARDAMOM_NOTIFIER_NODES': 'on'})
        self.assertIn('KARDAMOM_NOTIFIER_COUNT (1) is above the count', output)
        self.run_deploy(check=True, environ={'KARDAMOM_NOTIFIER_NODES': 'off'})

    def assert_notifier_placement(self, plans, expected):
        notifier = plans['notifier']
        group = notifier['TaskGroups'][0]
        task = group['Tasks'][0]
        args = task['Config']['args']
        [constraint] = notifier['Constraints']
        self.assertEqual((constraint['LTarget'], constraint['Operand'], constraint['RTarget']),
                         expected['constraint'])
        self.assertEqual(group['Count'], expected['count'])
        # The notifier keeps no state, so the instances need no count and
        # no peers.
        self.assertNotIn('--instance-count', args)
        self.assertNotIn('--peers', args)
        self.assertIn({'LTarget': '', 'RTarget': 'true', 'Operand': 'distinct_hosts'}, group['Constraints'])
        self.assertEqual(task['Resources']['MemoryMB'], expected['memory'])
        self.assertEqual(args[args.index('--ring-max-events') + 1], expected['ring'])
        drivers = {g['Name']: (g['Constraints'], g['Tasks'][0]['Env']['KARDAMOM_IDLE_CAP_US'])
                   for g in plans['aeron']['TaskGroups']}
        self.assertEqual(drivers, {
            name: (None if operator is None else [{'LTarget': '${meta.role}', 'RTarget': 'notifier',
                                                    'Operand': operator}], cap)
            for name, (operator, cap) in expected['drivers'].items()})

    @staticmethod
    def aeron_tasks(plans):
        """Every task that maps the Aeron directory, by job name."""
        return [(name, task) for name, job in plans.items() for group in job['TaskGroups']
                for task in group['Tasks'] if 'aeron-mount' in json.dumps(task['Config'].get('volumes', []))]

    def assert_idle_cap(self, plans, cap):
        """The hot-path jobs carry the idle cap. The other Aeron parties
        do not, so they keep the waits of the code."""
        hot = {'aeron', 'cluster', 'sequencer', 'ingress', 'executor'}
        for name, task in self.aeron_tasks(plans):
            expected = str(cap) if name in hot else None
            self.assertEqual(task['Env'].get('KARDAMOM_IDLE_CAP_US'), expected, name)

    def assert_aeron_parties(self, plans, tolerance):
        """Every task that maps the Aeron directory carries the tolerance:
        a Rust client as its driver timeout, a JVM with a media driver as
        its driver timeout, client liveness, image liveness, and a
        publication unblock timeout above the client liveness."""
        jvm_options = {'aeron': '_JAVA_OPTIONS', 'cluster': 'JAVA_TOOL_OPTIONS'}
        parties = self.aeron_tasks(plans)
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
            # A receiving driver keeps the image of a stalled publisher as
            # long: an archive keeps its recording whole through a stall.
            self.assertIn(f'-Daeron.image.liveness.timeout={liveness_ns}', options, name)
            unblock = [o for o in options if o.startswith('-Daeron.publication.unblock.timeout=')]
            self.assertEqual(len(unblock), 1, name)
            self.assertGreater(int(unblock[0].split('=')[1]), liveness_ns, name)
        # A sealer member loads its snapshots at a start through the
        # archive waits, so they scale with the tolerance too.
        cluster_options = dict(parties)['cluster']['Env']['JAVA_TOOL_OPTIONS'].split()
        self.assertIn(f'-Daeron.archive.message.timeout={tolerance}ms', cluster_options)
        self.assertIn(f'-Daeron.archive.connect.timeout={tolerance // 2}ms', cluster_options)
        # Each Aeron agent of a sealer counts its work cycles above 5 ms.
        self.assertIn('-Daeron.driver.sender.cycle.threshold=5ms', cluster_options)
        self.assertIn('-Daeron.cluster.service.cycle.threshold=5ms', cluster_options)
        # Each GC pause and safepoint goes to a rotated file with a UTC time
        # stamp, written asynchronously.
        self.assertIn('-Xlog:async', cluster_options)
        self.assertIn('-Xlog:gc,safepoint:file=/opt/kardamom/cluster/jvm-pauses.log:utctime:filecount=5,filesize=20m', cluster_options)
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

    def test_the_deploy_makes_every_data_directory_on_every_node_first(self):
        self.run_deploy(environ={'CANARY_LOCAL': '1'})
        self.assertEqual(self.api.state['writes'][0], 'data-dirs')
        job = self.api.state['jobs']['data-dirs']
        self.assertEqual((job['Type'], job['NodePool']), ('sysbatch', 'all'))
        task = job['TaskGroups'][0]['Tasks'][0]
        config = task['Config']
        # The signed Aeron image of the manifest, as root: no other image.
        self.assertEqual(config['image'], f'registry.example:5000/kardamom-aeron:test@sha256:{"a" * 64}')
        self.assertEqual(task['User'], 'root')
        self.assertEqual(config['volumes'], ['/opt/kardamom:/opt/kardamom'])
        self.assertEqual(config['entrypoint'], ['/bin/sh', '-c', 'mkdir -p "$@" && chmod 0777 "$@"', 'data-dirs'])
        dirs = config['args']
        self.assertIn('/opt/kardamom/canary', dirs)
        # One list: the common role makes the same directories.
        group_vars = yaml.safe_load((ANSIBLE / 'group_vars/all.yml').read_text())
        listed = [group_vars['paths'][re.fullmatch(r'\{\{ paths\.(\w+) \}\}', d).group(1)]
                  for d in group_vars['node_data_dirs']['paths']]
        self.assertEqual(dirs, listed)
        common = yaml.safe_load((ANSIBLE / 'roles/common/tasks/main.yml').read_text())
        task = next(t for t in common if t['name'] == 'Create on-node kardamom directories')
        self.assertEqual((task['loop'], task['ansible.builtin.file']['mode']),
                         ('{{ node_data_dirs.paths }}', '{{ node_data_dirs.mode }}'))
        # Every host directory that a job mounts is in the list. The
        # exceptions: the Aeron tmpfs, which the common role mounts, and the
        # directories of the sealer, whose image runs as root.
        exempt = {'/opt/kardamom/aeron-mount', '/opt/kardamom/cluster', '/opt/kardamom/seed'}
        mounted = {volume.split(':')[0] for job in self.api.state['jobs'].values()
                   for group in job['TaskGroups'] for task in group['Tasks']
                   for volume in (task['Config'].get('volumes') or [])}
        self.assertEqual({d for d in mounted if d.startswith('/opt/kardamom/')} - exempt - set(dirs), set())

    def test_a_second_deploy_of_the_same_release_runs_the_data_directory_job_again(self):
        # A sysbatch job runs once for each version, and Nomad can remove the
        # allocations of the last run. So each deploy gives the job a new
        # version, and the wait finds allocations of that version.
        self.run_deploy()
        first = self.api.state['jobs']['data-dirs']['Meta']['run']
        self.api.state['writes'] = []
        self.run_deploy()
        self.assertEqual(self.api.state['writes'][0], 'data-dirs')
        self.assertNotEqual(self.api.state['jobs']['data-dirs']['Meta']['run'], first)

    def test_a_down_node_does_not_hold_the_data_directory_job(self):
        self.api.state['down_node_jobs'] = ['data-dirs']
        self.run_deploy()
        self.assertEqual(self.api.state['writes'][:2], ['data-dirs', 'aeron'])

    def test_an_incomplete_data_directory_job_stops_the_deploy_before_any_service(self):
        self.api.state['not_running'] = ['data-dirs']
        self.run_deploy(success=False)
        self.assertEqual(self.api.state['writes'], ['data-dirs'])

    def test_an_unhealthy_canary_job_fails_the_deploy(self):
        self.api.state['deployments']['canary'] = Deployment('canary', ['running', 'failed'])
        output = self.run_deploy(environ={'CANARY_LOCAL': '1'}, success=False)
        self.assertIn('scripted failed', output)
        self.assertEqual(self.api.state['writes'][-1], 'canary')
        self.assertFalse((self.record_dir / 'images.digests').exists(), 'a failed deploy records nothing')

    def test_a_canary_is_smoked_by_its_node_then_promoted(self):
        self.api.state['deployments']['ingress'] = Deployment('ingress', [], canary=True)
        self.run_deploy()
        self.assertTrue(self.api.state['deployments']['ingress'].promoted)
        # The deploy binary of the release smokes the canary. The cluster
        # binary, which no release ships, does not run.
        calls = Path(str(self.deploy_bin) + '.calls').read_text().splitlines()
        self.assertEqual(calls, ['smoke --rpc-url http://127.0.0.1:8545 --chain-id 412346'])
        self.assertFalse(Path(str(self.cluster_bin) + '.calls').exists())

    def test_a_failed_smoke_leaves_the_canary_unpromoted(self):
        self.api.state['deployments']['sequencer'] = Deployment('sequencer', [], canary=True)
        Path(str(self.deploy_bin) + '.smoke-fails').touch()
        self.run_deploy(environ={'CANARY_LOCAL': '1'}, success=False)
        calls = Path(str(self.deploy_bin) + '.calls').read_text().splitlines()
        self.assertEqual(calls, ['smoke --rpc-url http://127.0.0.1:8545 --chain-id 412346'])
        self.assertFalse(self.api.state['deployments']['sequencer'].promoted)
        self.assertEqual(self.api.state['writes'][-4:], ['da-store', 'l1-indexer', 'batcher', 'sequencer'])
        self.assertNotIn('canary', self.api.state['writes'])
        self.assertEqual(self.record()['attempt']['status'], 'started')
        self.assertNotIn('accepted', self.record())
        self.assertFalse((self.record_dir / 'images.digests').exists())

    def test_a_successful_deploy_records_the_manifest_and_keeps_the_previous_one(self):
        self.run_deploy()
        first = self.manifest.read_text()
        self.assertEqual((self.record_dir / 'images.digests').read_text(), first)
        self.assertFalse((self.record_dir / 'images.digests.previous').exists())
        self.repin('b')
        self.run_deploy()
        self.assertEqual((self.record_dir / 'images.digests').read_text(), self.manifest.read_text())
        self.assertEqual((self.record_dir / 'images.digests.previous').read_text(), first)



class RecordTest(Deploys):
    """The deploy record and the release gate."""

    def test_the_record_follows_the_attempt_to_the_accepted_release(self):
        self.deploy()
        attempt = self.record()['attempt']
        self.assertEqual(self.record()['accepted'], attempt)
        self.assertEqual(attempt['status'], 'accepted')
        self.assertEqual((attempt['env'], attempt['operator'], attempt['target']['revision']), ('local', 'tester', self.revision))
        self.assertNotIn('formats', attempt['target'])
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
        # The record holds no formats text, so the mirror writes no registry.
        self.assertFalse((self.record_dir / 'accepted.formats.toml').exists())
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
        self.deploy()
        accepted = self.record()['accepted']
        self.repin('b')
        self.api.state['deployments']['ingress'] = Deployment('ingress', ['failed'])
        self.run_deploy(success=False)
        attempt = self.record()['attempt']
        self.assertEqual(attempt['status'], 'started')
        self.assertEqual(attempt['changed'], ['data-dirs', 'aeron', 'cluster', 'redis', 'ingress'])
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
        self.deploy()
        self.repin('b')
        self.api.state['writes'] = []
        self.api.state['record_clobber'] = True
        output = self.run_deploy(success=False)
        self.assertIn('changed under this run', output)
        self.assertEqual(self.api.state['writes'], [], 'the first write of the attempt is the check-and-set that fails')

    def test_a_halted_chain_refuses_the_release(self):
        self.deploy()
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

    def test_a_purged_sealer_excuses_only_its_own_no_quorum_halt(self):
        self.deploy()
        self.api.state['writes'] = []
        self.api.state['chain_status'] = NO_QUORUM_CHAIN
        output = self.run_deploy(success=False)
        self.assertIn('the chain stands on a halt or a pause', output, 'a registered sealer gets no excuse')
        self.assertEqual(self.api.state['writes'], [])
        # The coordinated restart: nomad job stop -purge cluster.
        del self.api.state['jobs']['cluster']
        da_lag = {'service': 'sealer', 'instance': 'cluster', 'cause': 'da_lag', 'runbook': 'docs/runbooks/da_lag.md'}
        self.api.state['chain_status'] = NO_QUORUM_CHAIN | {'roots': [NO_QUORUM_ROOT, da_lag]}
        output = self.run_deploy(success=False)
        self.assertIn('da_lag', output, 'the purge excuses no other root')
        self.assertEqual(self.api.state['writes'], [])
        self.api.state['chain_status'] = NO_QUORUM_CHAIN | {
            'ingress': {'pause': {'reason': 'operator', 'root': None, 'note': 'decision-version'}}}
        output = self.run_deploy(success=False)
        self.assertIn("Paused: ['ingress']", output, 'the purge excuses no operator pause')
        self.assertEqual(self.api.state['writes'], [])
        self.api.state['chain_status'] = NO_QUORUM_CHAIN
        self.run_deploy()
        self.assertEqual(self.api.state['writes'], ['data-dirs', 'cluster'], 'the sealer registers in one step')

    def test_a_missing_image_refuses_the_release(self):
        self.api.state['missing_images'] = {'kardamom-executor'}
        output = self.run_deploy(success=False)
        self.assertIn('does not hold the image of executor', output)
        self.assertEqual(self.api.state['writes'], [])
        self.assertFalse(Path(str(self.deploy_bin) + '.calls').exists(), 'the gate reads the registry before the formats')
        # Only the local profile skips the check.
        output = self.run_deploy({'workloads_registry_url': 'off', 'deployment_profile': 'production'}, success=False)
        self.assertIn('the production profile does not allow that', output)
        self.assertEqual(self.api.state['writes'], [])

    def test_the_image_check_reads_a_registry_that_wants_a_pull_token(self):
        self.api.state['registry_token'] = 'anonymous-pull'
        self.api.state['missing_images'] = {'kardamom-executor'}
        output = self.run_deploy(success=False)
        self.assertIn('does not hold the image of executor', output)
        self.assertIn('repository:kardamom-executor:pull', self.api.state['token_scopes'])
        self.assertEqual(self.api.state['writes'], [])
        self.api.state['missing_images'] = set()
        self.run_deploy()
        self.assertIn('cluster', self.api.state['writes'])

    def test_a_coordinated_format_change_refuses_the_rolling_path(self):
        self.gate_formats([{'rule': 'rollback', 'id': 'sealer-snapshot', 'head': 11, 'base': 10},
                           {'rule': 'mixed_fleet', 'id': 'sealer-snapshot', 'head': 11, 'base': 10}])
        output = self.run_deploy(success=False)
        self.assertIn('coordinated format change: sealer-snapshot', output)
        self.assertEqual(self.api.state['writes'], [])
        # The deploy binary ran the comparison against the accepted registry
        # and this tree. The cluster binary did not run.
        calls = Path(str(self.deploy_bin) + '.calls').read_text().splitlines()
        self.assertEqual(calls, [f'formats --base {self.record_dir}/accepted.formats.toml --root {self.repo}'])
        self.assertFalse(Path(str(self.cluster_bin) + '.calls').exists())

    def test_a_retired_format_needs_the_allowance(self):
        self.gate_formats([{'rule': 'retired', 'id': 'notifier-outbox', 'head': 1, 'base': 1,
                            'shared': False, 'permanent': False}])
        output = self.run_deploy(success=False)
        self.assertIn('coordinated format change: notifier-outbox.', output)
        self.assertIn('set KARDAMOM_ALLOW_RETIRED=notifier-outbox', output)
        self.assertEqual(self.api.state['writes'], [])
        output = self.run_deploy(success=False, environ={'KARDAMOM_ALLOW_RETIRED': 'notifier-outbox,state-db'})
        self.assertIn('KARDAMOM_ALLOW_RETIRED names state-db, and the release retires no format', output)
        self.assertEqual(self.api.state['writes'], [])
        output = self.run_deploy(environ={'KARDAMOM_ALLOW_RETIRED': 'notifier-outbox'})
        self.assertIn('KARDAMOM_ALLOW_RETIRED accepts the retirement of notifier-outbox.', output)
        self.assertEqual(sorted(set(self.api.state['writes'])), sorted(self.REPINNED))
        self.assertEqual(self.record()['accepted']['floor'], {})
        self.assertEqual(self.record()['accepted']['retired'], ['notifier-outbox'])

    def test_a_retired_shared_or_permanent_format_refuses_the_rolling_path(self):
        # Shared, permanent, and a finding without the flags: the allowance
        # covers none of them.
        self.gate_formats([{'rule': 'retired', 'id': 'aeron-stream-records', 'head': 1, 'base': 1,
                            'shared': True, 'permanent': False},
                           {'rule': 'retired', 'id': 'kar1-batch', 'head': 1, 'base': 1,
                            'shared': False, 'permanent': True},
                           {'rule': 'retired', 'id': 'old-spool', 'head': 2, 'base': 2}])
        output = self.run_deploy(success=False, environ={
            'KARDAMOM_ALLOW_RETIRED': 'aeron-stream-records,kar1-batch,old-spool'})
        self.assertIn('coordinated format change: aeron-stream-records, kar1-batch, old-spool.', output)
        self.assertNotIn('set KARDAMOM_ALLOW_RETIRED', output)
        self.assertEqual(self.api.state['writes'], [])

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
        self.assertEqual(self.record()['accepted']['floor'], {'formats': {'state-db': 4}, 'revision': self.revision})

    def test_a_must_match_sealer_setting_refuses_the_rolling_path(self):
        self.deploy()
        self.api.state['writes'] = []
        output = self.run_deploy({'workloads_da_lag_budget_blocks': '5000'}, success=False)
        self.assertIn('every member must match: daLagBudgetBlocks.', output)
        self.assertEqual(self.api.state['writes'], [])
        output = self.run_deploy({'workloads_da_lag_budget_blocks': '5000'}, success=False,
                                 environ={'KARDAMOM_ALLOW_MUST_MATCH': 'daLagBudgetBlocks,remoteOrigins'})
        self.assertIn('names remoteOrigins, and the release does not change it', output)
        self.assertEqual(self.api.state['writes'], [])
        # A documented procedure names the settings it changes.
        # The batcher takes the same budget, so it rolls after the sealer.
        self.run_deploy({'workloads_da_lag_budget_blocks': '5000'}, environ={'KARDAMOM_ALLOW_MUST_MATCH': 'daLagBudgetBlocks'})
        self.assertEqual(self.api.state['writes'], ['data-dirs', 'cluster', 'cluster', 'cluster', 'batcher'])

    def test_a_sealer_job_that_names_a_decision_version_rolls(self):
        """The job of the release before names decisionVersion; this release names none."""
        self.deploy()
        self.api.state['writes'] = []
        for group in self.api.state['jobs']['cluster']['TaskGroups']:
            env = group['Tasks'][0]['Env']
            env['JAVA_TOOL_OPTIONS'] += ' -Dkardamom.cluster.decisionVersion=2'
        self.run_deploy()
        self.assertEqual(self.api.state['writes'].count('cluster'), 3, 'the sealer rolls member by member')

    def test_a_carry_bytes_flip_refuses_the_rolling_path(self):
        self.deploy()
        self.api.state['writes'] = []
        for group in self.api.state['jobs']['cluster']['TaskGroups']:
            env = group['Tasks'][0]['Env']
            env['JAVA_TOOL_OPTIONS'] += ' -Dkardamom.cluster.carryBytes=true'
        output = self.run_deploy(success=False)
        self.assertIn('every member must match: carryBytes.', output)
        self.assertEqual(self.api.state['writes'], [])

    def test_a_sealer_job_that_started_on_decision_version_three_is_refused(self):
        """A cluster that ran a build with decisionVersion=3 must not roll onto version 2."""
        self.deploy()
        self.api.state['writes'] = []
        for group in self.api.state['jobs']['cluster']['TaskGroups']:
            env = group['Tasks'][0]['Env']
            env['JAVA_TOOL_OPTIONS'] += ' -Dkardamom.cluster.decisionVersion=3'
        output = self.run_deploy(success=False)
        self.assertIn('the registered sealer job names decisionVersion=3', output)
        self.assertEqual(self.api.state['writes'], [])

    def test_a_release_that_cannot_read_the_newest_sealer_snapshot_is_refused(self):
        self.deploy()
        self.api.state['writes'] = []
        self.api.state['record_writes'] = []
        self.target_snapshot_reads_max(16)
        self.repin('b')
        self.api.state['snapshot_latest'] = {'node-cluster-0': 16, 'node-cluster-1': 17}
        output = self.run_deploy(success=False)
        self.assertIn('reads sealer snapshots only up to version 16', output)
        self.assertIn('cluster-1 at version 17.', output)
        self.assertIn('Deploy a release that reads snapshot version 17.', output)
        self.assertEqual(self.api.state['writes'], [])
        self.assertEqual(self.api.state['record_writes'], [], 'a refused release starts no attempt')
        # The gate cannot read the snapshot of a member that does not run.
        self.api.state['snapshot_latest'] = {}
        self.api.state['down_groups'] = ['cluster-2']
        output = self.run_deploy(success=False)
        self.assertIn('the sealer members cluster-2 do not run', output)
        self.assertEqual(self.api.state['writes'], [])
        del self.api.state['down_groups']
        # A member of a release without snapshotLatest reads as 0.
        self.api.state['snapshot_latest'] = {'node-cluster-0': None, 'node-cluster-1': 16, 'node-cluster-2': 16}
        self.run_deploy()
        self.assertEqual(sorted(set(self.api.state['writes'])), sorted(self.REPINNED))

    def test_a_release_that_does_not_change_the_sealer_skips_the_snapshot_check(self):
        """A running member reads no snapshot, and the release can be the fix of the outage of a member."""
        self.deploy()
        self.api.state['writes'] = []
        self.target_snapshot_reads_max(16)
        self.api.state['snapshot_latest'] = {'node-cluster-0': 17}
        self.api.state['down_groups'] = ['cluster-2']
        self.manifest.write_text(re.sub(r'^(executor .*@sha256:)[0-9a-f]{64}$', r'\g<1>' + 'b' * 64,
                                        self.manifest.read_text(), flags=re.M))
        output = self.run_deploy()
        self.assertIn('The sealer job does not change, so no sealer member restarts on the target release.', output)
        self.assertEqual(self.api.state['writes'], ['data-dirs', 'executor'])

    def test_a_new_sealer_cluster_skips_the_snapshot_check(self):
        self.target_snapshot_reads_max(16)
        self.api.state['snapshot_latest'] = {'node-cluster-0': 17}
        output = self.run_deploy(check=True)
        self.assertRegex(output, r'Check the sealer snapshot against the target\] \*+\nskipping')
        self.assertIn('cluster', self.api.state['plans'])

    def test_the_upgrade_check_passes_on_a_release_that_takes_the_upgrade(self):
        self.deploy()
        output = self.upgrade_check()
        self.assertIn('The upgrade check passed: 3 sealer members read snapshot version 14', output)
        self.assertIn('the live allocations of sequencer, executor, validator, ingress', output)
        self.assertIn('The upgrade transaction of feature 9 may go to the Safe', output)

    def test_the_upgrade_check_allows_the_sealer_feature_only_on_at_once(self):
        """A new network turns FEATURE_SEALER_V3 on at once. Off, or on at a later time, is refused."""
        self.deploy()
        output = self.upgrade_check(feature='2', activation_ms='0')
        self.assertIn('The upgrade transaction of feature 2 may go to the Safe', output)
        for activation_ms in ('18446744073709551615', '1700000000000', ''):
            output = self.upgrade_check(success=False, feature='2', activation_ms=activation_ms)
            self.assertIn('Only the time 0 (on at once) is allowed', output)

    def test_the_upgrade_check_refuses_feature_three_before_the_executor_keeps_the_first_receipt(self):
        """Decision version 4 orders a transaction again: the executor must keep the receipt that used the nonce."""
        self.deploy()
        output = self.upgrade_check(feature='3', activation_ms='0')
        self.assertIn('3 sealer members read snapshot version 15', output)
        self.assertIn('The upgrade transaction of feature 3 may go to the Safe', output)
        for job in ('executor', 'validator'):
            del self.api.state['jobs'][job]['Meta']['first_receipt']
            output = self.upgrade_check(success=False, feature='3', activation_ms='0')
            self.assertIn(f'the {job} job does not keep the receipt that used the nonce', output)
            self.assertIn('(job meta first_receipt)', output)
            self.api.state['jobs'][job]['Meta']['first_receipt'] = 'true'

    def test_the_upgrade_check_refuses_feature_three_on_a_member_that_reads_no_version_fifteen(self):
        self.deploy()
        self.api.state['reads_max'] = {'node-cluster-2': 14}
        output = self.upgrade_check(success=False, feature='3', activation_ms='0')
        self.assertIn('needs every sealer member to read snapshot version 15', output)
        output = self.upgrade_check()
        self.assertIn('The upgrade transaction of feature 9 may go to the Safe', output)

    def test_the_upgrade_check_refuses_feature_four_on_a_member_that_reads_no_version_sixteen(self):
        self.deploy()
        output = self.upgrade_check(success=False, feature='4', activation_ms='0')
        self.assertIn('needs every sealer member to read snapshot version 16', output)
        for reads_max in (16, 17):
            self.api.state['reads_max'] = {f'node-cluster-{member}': reads_max for member in range(3)}
            output = self.upgrade_check(feature='4', activation_ms='0')
            self.assertIn('3 sealer members read snapshot version 16', output)
            self.assertIn('The upgrade transaction of feature 4 may go to the Safe', output)

    def test_the_upgrade_check_refuses_feature_three_on_a_sequencer_that_sends_no_position(self):
        self.deploy()
        del self.api.state['jobs']['sequencer']['Meta']['sealer_v4']
        output = self.upgrade_check(success=False, feature='3', activation_ms='0')
        self.assertIn('the sequencer job does not send the activation of decision version 4', output)

    def test_the_upgrade_check_refuses_a_missing_feature(self):
        self.deploy()
        output = self.upgrade_check(success=False, feature='')
        self.assertIn('Name the feature id of the transaction', output)

    def test_the_upgrade_check_refuses_a_member_of_an_older_release(self):
        self.deploy()
        self.api.state['reads_max'] = {'node-cluster-1': 12}
        output = self.upgrade_check(success=False)
        self.assertIn('a sealer member reads only up to snapshot version 12', output)

    def test_the_upgrade_check_refuses_a_member_that_is_down(self):
        self.deploy()
        self.api.state['down_groups'] = ['cluster-2']
        output = self.upgrade_check(success=False)
        self.assertIn('the sealer members cluster-2 do not run', output)

    def test_the_upgrade_check_refuses_a_sequencer_of_an_older_release(self):
        self.deploy()
        meta = self.api.state['jobs']['sequencer'].pop('Meta')
        output = self.upgrade_check(success=False)
        self.assertIn('the sequencer job does not take a skip that uses its nonce (job meta nonce_burn)', output)
        self.api.state['jobs']['sequencer']['Meta'] = meta
        self.api.state['missing_replica'] = 'sequencer'
        output = self.upgrade_check(success=False)
        self.assertIn('an allocation of an older job version still runs', output)

    def test_the_upgrade_check_refuses_an_executor_or_validator_that_does_not_burn(self):
        """An older executor or validator does not use the nonce of a skip, and forks at the first one."""
        self.deploy()
        meta = self.api.state['jobs']['executor'].pop('Meta')
        output = self.upgrade_check(success=False)
        self.assertIn(
            'the executor job does not use the nonce of a skipped transaction and install the chain state code'
            ' (job meta nonce_burn, chain_state_install)',
            output,
        )
        self.api.state['jobs']['executor']['Meta'] = meta
        self.api.state['jobs']['validator'].pop('Meta')
        output = self.upgrade_check(success=False)
        self.assertIn('Deploy the release to every validator', output)
        self.api.state['jobs']['validator']['Meta'] = meta
        self.api.state['jobs']['ingress']['Meta'] = {'nonce_burn': 'false'}
        output = self.upgrade_check(success=False)
        self.assertIn('the ingress job does not take a skip that uses its nonce', output)

    def test_the_upgrade_check_refuses_an_executor_or_validator_that_does_not_install_the_chain_state(self):
        """An older executor or validator runs the first upgrade deposit as a call to an empty account."""
        self.deploy()
        for job in ('executor', 'validator'):
            meta = self.api.state['jobs'][job]['Meta']
            self.api.state['jobs'][job]['Meta'] = {'nonce_burn': 'true'}
            output = self.upgrade_check(success=False)
            self.assertIn(f'the {job} job does not', output)
            self.assertIn('(job meta chain_state_install)', output)
            self.api.state['jobs'][job]['Meta'] = meta
        output = self.upgrade_check()
        self.assertIn('The upgrade check passed', output)

    def test_a_shard_map_change_refuses_the_rolling_path(self):
        self.deploy()
        self.api.state['writes'] = []
        task = self.api.state['jobs']['ingress']['TaskGroups'][0]['Tasks'][0]
        template = next(t for t in task['Templates'] if t['DestPath'] == 'local/shard-map.toml')
        template['EmbeddedTmpl'] = template['EmbeddedTmpl'].replace('version = 0', 'version = 1')
        output = self.run_deploy(success=False)
        self.assertIn('differs from the shard map of the registered ingress (version 1)', output)
        self.assertEqual(self.api.state['writes'], [])



class RecordFormatsTest(Deploys):
    """The revision and the formats text in the deploy record: a deploy
    records a commit of its checkout and no formats text; the gate compares
    with the text of an accepted release that holds it."""

    CLUSTER_ROLES = {'node-cluster-0': 'FOLLOWER', 'node-cluster-1': 'LEADER', 'node-cluster-2': 'FOLLOWER'}

    def formats_calls(self):
        calls = Path(str(self.deploy_bin) + '.calls')
        return calls.read_text().splitlines() if calls.exists() else []

    def next_release(self):
        self.repin('b')
        self.api.state['roles'] = self.CLUSTER_ROLES

    def test_a_record_with_the_text_gates_and_rolls_back_to_it(self):
        # The accepted release holds the formats text in target and before,
        # as the record of staging does.
        self.run_deploy()
        for document in self.record().values():
            document['target']['formats'] = RECORD_FORMATS
            document['before']['formats'] = RECORD_FORMATS
        self.next_release()
        self.run_deploy()
        self.assertEqual(self.formats_calls(),
                         [f'formats --base {self.record_dir}/accepted.formats.toml --root {self.repo}'])
        attempt = self.record()['attempt']
        self.assertNotIn('formats', attempt['target'])
        self.assertEqual(attempt['before']['formats'], RECORD_FORMATS)
        self.api.state['roll_marker'] = 'a' * 64
        (self.record_dir / 'accepted.formats.toml').unlink()
        self.run_rollback()
        self.assertEqual(self.record()['accepted']['target']['formats'], RECORD_FORMATS)
        self.assertEqual((self.record_dir / 'accepted.formats.toml').read_text(), RECORD_FORMATS)

    def test_a_record_without_the_text_has_no_base(self):
        self.run_deploy()
        self.next_release()
        self.run_deploy()
        self.assertEqual(self.formats_calls(), [])
        attempt = self.record()['attempt']
        self.assertEqual((attempt['status'], attempt['before']['revision']), ('accepted', self.revision))
        self.assertNotIn('formats', attempt['before'])

    def test_a_target_revision_outside_the_checkout_stops_the_gate(self):
        output = self.run_deploy({'workloads_revision': 'd' * 40}, success=False)
        self.assertIn(f'the target revision {"d" * 40} is not a commit of the checkout', output)
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
        self.assertEqual(record['accepted']['target']['revision'], self.revision)
        self.assertNotIn('formats', record['accepted']['target'])
        output = self.run_rollback(success=False)
        self.assertIn('is already rolled back', output)

    def test_a_return_after_a_purge_of_the_sealer_starts_the_sealer_before_the_ingress(self):
        self.run_deploy()
        # A purge of the sealer, then release b.
        del self.api.state['jobs']['cluster']
        self.api.state['chain_status'] = NO_QUORUM_CHAIN
        self.repin('b')
        self.run_deploy()
        # The runbook: purge the sealer and the ingress, then deploy the
        # release before. Without an ingress job the gate reads no chain
        # status, so an older gate passes too.
        del self.api.state['jobs']['cluster']
        del self.api.state['jobs']['ingress']
        self.api.state['writes'] = []
        self.repin('a')
        self.run_deploy()
        writes = self.api.state['writes']
        self.assertEqual(writes.count('cluster'), 1, 'the sealer registers in one step')
        self.assertLess(writes.index('cluster'), writes.index('ingress'), 'the sealer starts before the ingress')
        self.assertTrue(all(t['Config']['image'].endswith('a' * 64)
                            for g in self.api.state['jobs']['cluster']['TaskGroups'] for t in g['Tasks']))

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
                                    'revert:executor:0', 'revert:ingress:0', 'revert:redis:0',
                                    'revert:aeron:0', 'revert:data-dirs:0'])
        self.assertEqual(self.api.state['writes'].count('cluster'), 3)
        self.assertNotIn('revert:batcher:0', self.api.state['writes'], 'the failed attempt never reached the batcher')
        self.assertNotIn('revert:sequencer:0', self.api.state['writes'], 'the failed attempt never reached the sequencer')
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
        self.assertEqual(self.record()['attempt']['rolled_back'], ['ingress', 'redis', 'cluster', 'aeron', 'data-dirs'])
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
        done = ['sequencer', 'batcher', 'l1-indexer', 'da-store', 'da-watcher', 'validator', 'notifier', 'state-mirror']
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
                         ['revert:ingress:0', 'revert:redis:0', 'cluster', 'cluster', 'cluster', 'revert:aeron:0',
                          'revert:data-dirs:0'])
        self.assertEqual(self.record()['attempt']['status'], 'rolled_back')
        self.assertEqual(self.record()['attempt']['rolled_back'],
                         done + ['executor', 'ingress', 'redis', 'cluster', 'aeron', 'data-dirs'])

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

    def test_a_rollback_below_the_newest_sealer_snapshot_is_refused(self):
        revision = self.two_sealer_releases()
        output = self.run_rollback(success=False)
        self.assertIn(f'the sealer release of revision {revision} reads sealer snapshots only up to version 16', output)
        self.assertIn('cluster-1 at version 17.', output)
        self.assertEqual(self.api.state['writes'], [], 'the check runs before the first revert')

    def test_a_deploy_of_an_older_sealer_image_reads_the_formats_of_its_revision(self):
        """The manifest of release a, deployed from the checkout of release b: the record names the revision of
        the sealer image."""
        revision = self.two_sealer_releases()
        self.repin('a', tag='test')
        output = self.run_deploy(success=False)
        self.assertIn(f'the sealer release of revision {revision} reads sealer snapshots only up to version 16', output)
        self.assertEqual(self.api.state['writes'], [])

    def test_a_sealer_image_that_no_record_names_needs_its_revision(self):
        """The manifest of a release two back, deployed from the checkout of a newer release: no record names
        the sealer image."""
        revision = self.two_sealer_releases()
        self.repin('c', tag='test')
        output = self.run_deploy(success=False)
        self.assertIn('no deploy record names the sealer image registry.example:5000/kardamom-cluster:test@sha256:', output)
        self.assertIn('Set KARDAMOM_REVISION to the revision of that image', output)
        self.assertEqual(self.api.state['writes'], [])
        output = self.run_deploy({'workloads_revision': revision}, success=False)
        self.assertIn(f'the sealer release of revision {revision} reads sealer snapshots only up to version 16', output)
        # The tag of a release names its commit.
        self.repin('c', tag=f'main-{revision[:12]}')
        output = self.run_deploy(success=False)
        self.assertIn(f'the sealer release of revision {revision[:12]} reads sealer snapshots only up to version 16',
                      output)
        self.assertEqual(self.api.state['writes'], [])

    def test_a_sealer_image_that_only_some_members_run_needs_its_revision(self):
        """A roll that stopped half way: one member runs the target image, so the checkout does not name it."""
        self.two_sealer_releases()
        self.repin('c', tag='test')
        image = re.search(r'\S*kardamom-cluster:test@sha256:[0-9a-f]{64}', self.manifest.read_text()).group(0)
        groups = self.api.state['jobs']['cluster']['TaskGroups']
        groups[0]['Tasks'][0]['Config']['image'] = image
        output = self.run_deploy(success=False)
        self.assertIn('not every registered sealer member runs it', output)
        self.assertEqual(self.api.state['writes'], [])

    def test_a_rollback_rolls_a_down_sealer_member_that_the_operator_names_first(self):
        """A member crash-loops on the release: the main reason to roll back."""
        self.deploy()
        self.repin('b')
        self.run_deploy()
        self.api.state['writes'] = []
        self.api.state['roll_marker'] = 'a' * 64
        self.api.state['down_groups'] = ['cluster-2']
        output = self.run_rollback(success=False)
        self.assertIn('the sealer members cluster-2 do not run', output)
        self.assertIn('set KARDAMOM_ALLOW_SEALER_DOWN=cluster-2', output)
        self.assertEqual(self.api.state['writes'], [], 'the check runs before the first revert')
        output = self.run_rollback(success=False, environ={'KARDAMOM_ALLOW_SEALER_DOWN': 'cluster-1'})
        self.assertIn('the sealer members cluster-2 do not run', output)
        self.assertEqual(self.api.state['writes'], [])
        output = self.run_rollback(environ={'KARDAMOM_ALLOW_SEALER_DOWN': 'cluster-2'})
        self.assertIn('KARDAMOM_ALLOW_SEALER_DOWN accepts the down sealer members cluster-2.', output)
        self.assertEqual(self.api.state['down_groups'], [], 'the member starts on the release before')
        self.assertEqual(self.record()['attempt']['status'], 'rolled_back')
        self.assertEqual(self.api.state['rolled'][-3:],
                         [['cluster-2'], ['cluster-1', 'cluster-2'], ['cluster-0', 'cluster-1', 'cluster-2']])

    def test_a_rollback_without_the_revision_of_the_release_before_is_refused(self):
        """An empty revision makes git read the index of the checkout."""
        self.deploy()
        self.repin('b')
        self.run_deploy()
        self.api.state['writes'] = []
        self.record()['attempt']['before']['revision'] = ''
        output = self.run_rollback(success=False)
        self.assertIn('the sealer release has no revision ("")', output)
        self.assertEqual(self.api.state['writes'], [])

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
        self.run_deploy({'workloads_cluster_retention': '131072'})
        return [g['Name'] for g in self.api.state['jobs']['cluster']['TaskGroups']]

    def test_the_sealer_rolls_followers_first_and_the_leader_last(self):
        groups = self.sealer_roll(leader=1)
        self.assertEqual(self.api.state['writes'], ['data-dirs', 'cluster', 'cluster', 'cluster'])
        self.assertEqual(sorted(groups), ['cluster-0', 'cluster-1', 'cluster-2'])
        # Every step holds the current definition of the members not rolled
        # yet, so the retention reaches the groups in the roll order.
        retention = [('-Dkardamom.cluster.retention=131072' in json.dumps(g)) for g in
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
        self.run_deploy({'workloads_cluster_bootstrap': True, 'workloads_cluster_retention': '131072'})
        self.assertEqual(self.api.state['writes'], ['data-dirs', 'cluster', 'cluster', 'cluster'])

    def test_a_failed_bootstrap_closes_the_bootstrap(self):
        self.api.state['deployments']['cluster'] = Deployment('cluster', ['failed'])
        self.run_deploy({'workloads_cluster_bootstrap': True}, success=False)
        self.assertEqual(self.api.state['writes'][-3:], ['bootstrap-open', 'cluster', 'bootstrap-close'])



class CarryTest(Deploys):
    """The carry window: the staged move and roll of the executors, the
    sequencer version floor, the move of the readers to the egress, the
    carry executors, the retention, and the upgrade check of feature 5."""

    # A chain with the egress, the frames of the executors, and the v2 loop
    # of the sequencers.
    DEPLOY_ENVIRON = {'KARDAMOM_EGRESS': '1', 'KARDAMOM_EXEC_FRAMES': 'on', 'KARDAMOM_SEQUENCER_ADMISSION': 'on'}
    ON_SEALERS = DEPLOY_ENVIRON | DeployTest.ON_SEALERS
    # R of each egress instance, as (index, block): the highest is index
    # 100 at block 50.
    MARKS = {'node-egress': (100, 50), 'node-egress.1': (90, 45)}

    def setUp(self):
        super().setUp()
        self.add_egress_image()

    def metric(self, node, gauges):
        """Give the node a metrics page with `gauges`, by name, in the format
        of the exporters: a sealer member labels each sample with service and
        member_id, a Rust service with service and host_id. The labels of a
        gauge name follow them."""
        own = (f'service="sealer",member_id="{node.rsplit("-", 1)[1]}"' if node.startswith('node-cluster-')
               else f'service="{node.removeprefix("node-").split(".")[0]}",host_id="{node}"')

        def line(name, value):
            bare, _, labels = name.partition('{')
            return f'{bare}{{{own}{"," + labels.rstrip("}") if labels else ""}}} {value}\n'
        self.api.state.setdefault('metrics', {})[node] = '# TYPE x gauge\n' + ''.join(
            line(k, v) for k, v in gauges.items())

    def set_marks(self):
        for node, (index, block) in self.MARKS.items():
            self.metric(node, {'kardamom_egress_ref_frame_max_index': index,
                               'kardamom_egress_ref_frame_max_block': block})

    def refused(self, environ, message):
        """A check deploy that the gate refuses with `message`, and that plans
        no job after the sealer."""
        self.api.state.pop('plans', None)
        output = self.run_deploy(check=True, success=False, environ=environ)
        self.assertIn(message, output)
        self.assertEqual(set(self.api.state.get('plans', {})) - {'cluster'}, set())

    def executor_steps(self):
        """The groups of each registration of the executor job."""
        return [[(g['Name'], g['Count']) for g in job['TaskGroups']]
                for job in self.api.state.get('registrations', []) if job['ID'] == 'executor']

    def test_the_bring_up_of_the_carry_shard_deploys_a_new_chain(self):
        """The bring-up of the chaos-carry shard: the member feed, the
        executors on the sealer nodes, the egress and the v2 loop of the
        sequencers, all on a new chain."""
        self.run_deploy(environ=self.ON_SEALERS | {'KARDAMOM_SEQUENCER_ADMISSION': 'on'})
        jobs = {job['ID']: job for job in self.api.state['registrations']}
        self.assertEqual(sorted(g['Name'] for g in jobs['executor']['TaskGroups']),
                         ['executor-0', 'executor-1', 'executor-2'])
        self.assertIn('[admission]\nenabled = true', DeployTest.job_templates(jobs['sequencer']))
        self.assertIn('egress', jobs)

    def test_the_executors_move_to_the_sealer_nodes_one_at_a_time(self):
        self.deploy()
        self.api.state['writes'] = []
        self.api.state['registrations'] = []
        self.run_deploy(environ=self.ON_SEALERS)
        # Each step first stops the instance of the highest index on the
        # executor nodes, then starts the pinned group of that id. The last
        # step removes the group of the executor nodes.
        self.assertEqual(self.executor_steps(), [
            [('executor', 2)],
            [('executor-2', 1), ('executor', 2)],
            [('executor-2', 1), ('executor', 1)],
            [('executor-1', 1), ('executor-2', 1), ('executor', 1)],
            [('executor-1', 1), ('executor-2', 1)],
            [('executor-0', 1), ('executor-1', 1), ('executor-2', 1)]])
        moving = [job for job in self.api.state['registrations'] if job['ID'] == 'executor'][0]
        self.assertIn({'LTarget': '${meta.role}', 'RTarget': 'executor', 'Operand': '='},
                      moving['TaskGroups'][0]['Constraints'], 'the group of the executor nodes stays there')
        self.assertEqual(self.api.state['writes'].count('cluster'), 3, 'the feed rolls member by member')
        # A new image of the executor rolls the pinned groups one at a time.
        self.api.state['registrations'] = []
        self.manifest.write_text(re.sub(r'^(executor .*@sha256:)[0-9a-f]{64}$', r'\g<1>' + 'c' * 64,
                                        self.manifest.read_text(), flags=re.M))
        self.run_deploy(environ=self.ON_SEALERS)
        rolled = [sorted(g['Name'] for g in job['TaskGroups'] if 'c' * 64 in json.dumps(g))
                  for job in self.api.state['registrations'] if job['ID'] == 'executor']
        self.assertEqual(rolled, [['executor-0'], ['executor-0', 'executor-1'],
                                  ['executor-0', 'executor-1', 'executor-2']])

    def test_a_rollback_rolls_the_pinned_executor_groups_back_one_at_a_time(self):
        self.deploy()
        self.run_deploy(environ=self.ON_SEALERS)
        self.repin('b')
        self.run_deploy(environ=self.ON_SEALERS)
        self.api.state['writes'] = []
        self.api.state['registrations'] = []
        self.run_rollback()
        self.assertNotIn('revert:executor:', ' '.join(self.api.state['writes']))
        rolled = [sorted(g['Name'] for g in job['TaskGroups'] if 'a' * 64 in json.dumps(g))
                  for job in self.api.state['registrations'] if job['ID'] == 'executor']
        self.assertEqual(rolled, [['executor-0'], ['executor-0', 'executor-1'],
                                  ['executor-0', 'executor-1', 'executor-2']])

    def test_the_version_floor_needs_every_reader_of_a_relayed_full_reference(self):
        self.deploy()
        floor = self.DEPLOY_ENVIRON | {'KARDAMOM_SEALER_VERSION_FLOOR': '5'}
        self.run_deploy(check=True, environ=floor)
        for group in self.api.state['plans']['sequencer']['TaskGroups']:
            args = group['Tasks'][0]['Config']['args']
            self.assertEqual(args[args.index('--sealer-version-floor') + 1], '5')
        self.api.state['status'] = {'node-cluster-1': {'decisionVersion': 4}}
        self.refused(floor, 'a sealer member runs decision version 4 ([5, 4, 5])')
        del self.api.state['status']
        self.api.state['down_groups'] = ['cluster-2']
        self.refused(floor, 'the sealer members cluster-2 do not run, so the gate cannot read their decision version')
        del self.api.state['down_groups']
        # The v1 loop offers record type 0 at every floor.
        self.refused(floor | {'KARDAMOM_SEQUENCER_ADMISSION': ''},
                     'and the target runs the v1 loop of the sequencers')
        meta = self.api.state['jobs']['sequencer']['Meta']
        del meta['v2_loop']
        self.refused(floor, 'the sequencer job does not run the v2 loop, the only loop that offers the bytes')
        meta['v2_loop'] = 'true'
        for job in ('batcher', 'egress'):
            meta = self.api.state['jobs'][job]['Meta']
            del meta['relayed_full_ref']
            self.refused(floor, f'the {job} job does not run a relayed full reference (job meta relayed_full_ref)')
            meta['relayed_full_ref'] = 'true'

    def test_the_v2_loop_stays_on_while_a_member_holds_the_carry_window(self):
        """The v1 loop offers record type 0, which the carry window refuses."""
        self.deploy()
        self.api.state['writes'] = []
        v1 = self.DEPLOY_ENVIRON | {'KARDAMOM_SEQUENCER_ADMISSION': ''}
        self.api.state['status'] = {'node-cluster-1': {'carryWindow': True}}
        self.refused(v1, 'the target turns the v2 loop of the sequencers off')
        del self.api.state['status']
        self.metric('node-cluster-2', {'kardamom_sealer_feature_state{feature="5"}': 1})
        self.refused(v1, 'kardamom_sealer_feature_state{feature="5"} [-1.0, -1.0, 1.0]')
        # A member whose /metrics does not answer can hold feature 5.
        self.metric('node-cluster-0', {})
        self.metric('node-cluster-2', {'kardamom_sealer_feature_state{feature="5"}': 0})
        self.refused(v1, "members whose /metrics does not answer ['cluster-1']")
        self.assertEqual(self.api.state['writes'], [])
        # The documented way out.
        self.run_deploy(check=True, environ=v1 | {'KARDAMOM_ALLOW_MUST_MATCH': 'admission'})
        self.assertNotIn('[admission]', DeployTest.job_templates(self.api.state['plans']['sequencer']))

    def test_the_readers_move_to_the_egress_past_the_last_stored_reference(self):
        self.deploy()
        readers = self.DEPLOY_ENVIRON | {'KARDAMOM_BATCHER_EGRESS': 'on', 'KARDAMOM_VALIDATOR_EGRESS': 'on'}
        self.set_marks()
        self.metric('node-batcher', {'kardamom_batcher_last_posted_block': 50})
        self.metric('node-validator', {'validator_committed_block': 60})
        self.refused(readers, 'the batcher moves to the egress, and its block [50.0]')
        self.metric('node-batcher', {'kardamom_batcher_last_posted_block': 51})
        self.run_deploy(check=True, environ=readers)
        for name in ('batcher', 'validator'):
            job = self.api.state['plans'][name]
            self.assertEqual(job['Meta']['reads_egress'], 'true', name)
            args = DeployTest.job_args(job)
            self.assertEqual(args[args.index('--egress') + 1], DeployTest.EGRESS_URL, name)
        # An egress of a release without the mark, or an instance that does
        # not answer: the gate cannot trust R.
        meta = self.api.state['jobs']['egress']['Meta']
        del meta['ref_mark']
        self.refused(readers, 'the egress job does not keep the mark of the highest stored reference')
        meta['ref_mark'] = 'true'
        del self.api.state['metrics']['node-egress.1']
        self.refused(readers, 'Refused: an egress instance does not report its reference mark')
        self.set_marks()
        # A batcher that Nomad does not know has no block to read. It may
        # start on the egress only while the egress holds no reference.
        del self.api.state['jobs']['batcher']
        self.refused(readers, 'Nomad does not know the batcher job, the deploy starts it on the egress (--egress), and'
                              ' the egress holds a reference (kardamom_egress_ref_frame_max_index 100)')
        for node in self.MARKS:
            self.metric(node, {'kardamom_egress_ref_frame_max_index': 0, 'kardamom_egress_ref_frame_max_block': 0})
        self.run_deploy(check=True, environ=readers)
        self.assertEqual(self.api.state['plans']['batcher']['Meta']['reads_egress'], 'true')

    def test_the_carry_executors_move_one_at_a_time_after_the_switch(self):
        self.deploy()
        self.run_deploy(environ=self.ON_SEALERS)
        carry = self.ON_SEALERS | {'KARDAMOM_CARRY_EXECUTORS': '0'}
        ready = {'carryWindow': True, 'feed': 'idle'}
        status = {f'node-cluster-{i}': dict(ready) for i in range(3)}
        self.api.state['status'] = status
        self.set_marks()
        # The committed block minus the compare window of 32 is above the
        # block 50 of R.
        self.metric('node-executor-0', {'kardamom_executor_block_number': 83})
        cases = (
            ('node-cluster-1', 'carryWindow', False, 'needs the rules of the carry window (feature 5) on every member'),
            ('node-cluster-0', 'feed', 'off', 'its member cluster-0 reports the feed off'),
        )
        for node, field, value, message in cases:
            with self.subTest(field):
                status[node][field] = value
                self.refused(carry, message)
                status[node][field] = ready[field]
        self.metric('node-executor-0', {'kardamom_executor_block_number': 82})
        self.refused(carry, 'minus 32 is not above the block 50 of the last stored reference')
        self.metric('node-executor-0', {'kardamom_executor_block_number': 83})
        meta = self.api.state['jobs']['executor']['Meta']
        del meta['carried_ref_fault']
        self.refused(carry, 'the executor job does not stop on a reference that no source can join')
        meta['carried_ref_fault'] = 'true'
        cluster = self.api.state['jobs'].pop('cluster')
        self.api.state['chain_status'] = NO_QUORUM_CHAIN
        self.refused(carry, 'KARDAMOM_CARRY_EXECUTORS lists executor-0, and Nomad does not know the sealer job')
        self.api.state['jobs']['cluster'] = cluster
        del self.api.state['chain_status']
        # The move: only executor-0 changes, so the roll registers one step.
        self.api.state['registrations'] = []
        self.run_deploy(environ=carry)
        self.assertEqual(len(self.executor_steps()), 1)
        for group in self.api.state['jobs']['executor']['TaskGroups']:
            args = group['Tasks'][0]['Config']['args']
            carried = group['Name'] == 'executor-0'
            self.assertEqual('--carry-bytes' in args, carried, group['Name'])
            self.assertEqual(args[args.index('--egress') + 1] if carried else None,
                             DeployTest.EGRESS_URL if carried else None, group['Name'])
        # The next executor: the gate checks only the executor that moves.
        del self.api.state['metrics']['node-executor-0']
        self.metric('node-executor-1', {'kardamom_executor_block_number': 100})
        self.api.state['registrations'] = []
        self.run_deploy(environ=carry | {'KARDAMOM_CARRY_EXECUTORS': '0,1'})
        self.assertEqual(len(self.executor_steps()), 1)

    def test_the_retention_only_rises_and_stays_within_the_cap_in_bytes(self):
        self.deploy()
        lower = self.DEPLOY_ENVIRON | {'KARDAMOM_CLUSTER_RETENTION': '4096'}
        self.refused(lower, 'the target lowers the sealer retention from 65536 to 4096 frames')
        self.run_deploy(check=True, environ=lower | {'KARDAMOM_ALLOW_MUST_MATCH': 'retention'})
        higher = self.DEPLOY_ENVIRON | {'KARDAMOM_CLUSTER_RETENTION': '131072'}
        self.refused(higher | {'KARDAMOM_ALLOW_MUST_MATCH': 'retention'},
                     'KARDAMOM_ALLOW_MUST_MATCH names retention, and the release does not change it')
        # 131072 frames of 400 bytes are above 48 MiB.
        self.api.state['status'] = {f'node-cluster-{i}': {'retainedFrames': 1000, 'retainedBytes': 400000}
                                    for i in range(3)}
        self.refused(higher, 'above the cap of 50331648 bytes')
        self.refused(higher, 'Set at most 125829 frames')
        # 131072 frames of 300 bytes, about 39.3 MB, fit.
        self.api.state['status'] = {f'node-cluster-{i}': {'retainedFrames': 1000, 'retainedBytes': 300000}
                                    for i in range(3)}
        self.run_deploy(check=True, environ=higher)
        self.assertIn('-Dkardamom.cluster.retention=131072', json.dumps(self.api.state['plans']['cluster']))

    # The carry from genesis, and the status of a member that started with
    # it.
    GENESIS = DeployTest.GENESIS
    GENESIS_MEMBER = {'carryWindow': True, 'genesisFeatures': '3,4,5', 'feed': 'idle'}
    BOOTSTRAP = {'workloads_cluster_bootstrap': True}

    def genesis_members(self):
        """Every member reports the rules of the carry from genesis."""
        self.api.state['status'] = {f'node-cluster-{i}': dict(self.GENESIS_MEMBER) for i in range(3)}

    def genesis_start(self):
        """Start a chain with the carry from genesis, on a fresh fixture."""
        self.genesis_members()
        self.deploy(self.BOOTSTRAP, self.GENESIS)
        self.api.state['writes'] = []
        self.api.state['registrations'] = []

    def refused_unwritten(self, message, extra=None, environ=None):
        """A deploy that the gate refuses with `message` before any write."""
        self.api.state['writes'] = []
        self.api.state['record_writes'] = []
        output = self.run_deploy(extra, success=False, environ=environ)
        self.assertIn(message, output)
        self.assertEqual(self.api.state['writes'], [])
        self.assertEqual(self.api.state['record_writes'], [])

    def test_a_genesis_bootstrap_starts_a_chain_with_the_carry_window(self):
        """The gates of the carry window read the genesis facts of the
        rendered sealer job: the floor 5, the readers on the egress, and
        every executor in carry mode, all on a new chain."""
        self.genesis_members()
        output = self.run_deploy(self.BOOTSTRAP, environ=self.GENESIS)
        writes = self.api.state['writes']
        cluster = writes.index('cluster')
        self.assertEqual(writes[cluster - 1:cluster + 2], ['bootstrap-open', 'cluster', 'bootstrap-close'])
        self.assertLess(cluster, writes.index('sequencer'))
        for task in ('Read the rules of every sealer member for the version floor',
                     'Keep the reference mark of a chain that starts with the carry window',
                     'Read the rules of every sealer member for the carry executors',
                     'Refuse a new sealer member without the genesis rules'):
            self.assertIn(task, output)
        jobs = {job['ID']: job for job in self.api.state['registrations']}
        for group in jobs['executor']['TaskGroups']:
            self.assertIn('--carry-bytes', group['Tasks'][0]['Config']['args'], group['Name'])
        for name in ('batcher', 'validator'):
            self.assertEqual(jobs[name]['Meta']['reads_egress'], 'true', name)
        self.assertEqual(jobs['sequencer']['Meta']['v2_loop'], 'true')
        # A redeploy without the bootstrap: the registered sealer job carries
        # the genesis features, so only the data directory job registers.
        self.api.state['writes'] = []
        self.run_deploy(environ=self.GENESIS)
        self.assertEqual(self.api.state['writes'], ['data-dirs'])

    def test_a_genesis_bootstrap_stops_at_a_member_without_the_genesis_rules(self):
        """A sealer image that does not take the genesis features starts on
        decision version 3, and no other job registers."""
        self.genesis_members()
        self.api.state['status']['node-cluster-1'] = {'carryWindow': False}
        output = self.run_deploy(self.BOOTSTRAP, success=False, environ=self.GENESIS)
        self.assertIn('the new sealer members do not all report the genesis rules', output)
        self.assertIn("genesisFeatures ['3,4,5', 'missing', '3,4,5']", output)
        self.assertEqual(self.api.state['writes'][-3:], ['bootstrap-open', 'cluster', 'bootstrap-close'])

    def test_the_genesis_carry_is_refused_on_a_chain_that_did_not_start_with_it(self):
        self.deploy()
        no_new_chain = 'and the deploy starts no new chain'
        self.refused_unwritten(no_new_chain, environ=self.GENESIS)
        self.refused_unwritten(no_new_chain, self.BOOTSTRAP, environ=self.GENESIS)
        # A purged sealer job: Nomad knows the other jobs of the chain.
        self.api.state['jobs'].pop('cluster')
        self.api.state['chain_status'] = NO_QUORUM_CHAIN
        self.refused_unwritten("Nomad knows ['sequencer', 'executor', 'batcher', 'validator', 'egress']",
                               self.BOOTSTRAP, environ=self.GENESIS)
        self.refused_unwritten(no_new_chain, environ=self.GENESIS)

    def test_a_chain_that_started_with_the_carry_window_keeps_it(self):
        """No deploy removes the genesis features, and none adds a seed or the
        retained file to them."""
        self.genesis_start()
        removed = 'the target changes genesisFeatures of the registered sealer job'
        self.refused_unwritten(removed, environ=self.DEPLOY_ENVIRON)
        self.refused_unwritten(removed, environ=self.DEPLOY_ENVIRON | {'KARDAMOM_ALLOW_MUST_MATCH': 'genesisFeatures'})
        for option in ('-Dkardamom.cluster.seedSnapshot=/opt/kardamom/seed/seed.bin',
                       '-Dkardamom.cluster.carryBytes=true'):
            with self.subTest(option):
                self.genesis_start()
                for group in self.api.state['jobs']['cluster']['TaskGroups']:
                    group['Tasks'][0]['Env']['JAVA_TOOL_OPTIONS'] += ' ' + option
                self.refused_unwritten(f'the target or the registered sealer job names {option}', environ=self.GENESIS)

    def test_the_upgrade_check_reads_a_chain_that_started_with_the_carry_window(self):
        """The upgrade check reads the live members of a chain with the carry
        from genesis, with no allowance: feature 5 off and on passes, and
        feature 3 or 4 off is refused, since every member holds feature 5."""
        self.genesis_start()
        self.api.state['reads_max'] = {f'node-cluster-{i}': 18 for i in range(3)}
        for node in self.MARKS:
            self.metric(node, {'kardamom_egress_ref_frame_max_index': 0, 'kardamom_egress_ref_frame_max_block': 0})
        self.metric('node-batcher', {'kardamom_batcher_egress_cursor_index': 1})
        self.metric('node-validator', {'kardamom_validator_egress_cursor_index': 1})
        for activation_ms in ('18446744073709551615', '1700000000000'):
            output = self.upgrade_check(feature='5', activation_ms=activation_ms)
            self.assertIn('The upgrade transaction of feature 5 may go to the Safe', output)
        for i in range(3):
            self.metric(f'node-cluster-{i}', {'kardamom_sealer_feature_state{feature="5"}': 2})
        output = self.upgrade_check(success=False, feature='4', activation_ms='18446744073709551615')
        self.assertIn('can turn decision version 5 off, and a member holds feature 5', output)

    def window_ready(self):
        """A chain that takes feature 5: every member reads version 18, and
        every reader of the egress is past R."""
        self.deploy()
        for name in ('batcher', 'validator'):
            self.api.state['jobs'][name]['Meta']['reads_egress'] = 'true'
        self.api.state['reads_max'] = {f'node-cluster-{i}': 18 for i in range(3)}
        self.set_marks()
        self.metric('node-batcher', {'kardamom_batcher_egress_cursor_index': 101})
        self.metric('node-validator', {'kardamom_validator_egress_cursor_index': 150})

    def test_the_upgrade_check_of_feature_five(self):
        self.window_ready()
        for activation_ms in ('1700000000000', '18446744073709551615'):
            output = self.upgrade_check(feature='5', activation_ms=activation_ms)
            self.assertIn('3 sealer members read snapshot version 18', output)
            self.assertIn('The upgrade transaction of feature 5 may go to the Safe', output)
        cases = (
            ('reads_max', lambda state: state['reads_max'].update({'node-cluster-2': 17}),
             'needs every sealer member to read snapshot version 18'),
            ('decision version', lambda state: state.update(status={'node-cluster-1': {'decisionVersion': 4}}),
             'feature 5 needs decision version 5 on every member, and the members run [5, 4, 5]'),
            ('retained file', lambda state: state.update(status={'node-cluster-0': {'retainedStore': 'file'}}),
             "the stores ['file', 'heap', 'heap']"),
            ('sequencer', lambda state: state['jobs']['sequencer']['Meta'].pop('sealer_version_kept'),
             'the sequencer job does not keep the highest sealer version across its sessions'),
            ('v1 loop', lambda state: state['jobs']['sequencer']['Meta'].pop('v2_loop'),
             'the sequencer job does not run the v2 loop, the only loop that offers the bytes of a transaction'),
            ('batcher', lambda state: state['jobs']['batcher']['Meta'].pop('reads_egress'),
             'the batcher job does not read the canonical stream from the egress'),
            ('egress', lambda state: state['jobs']['egress']['Meta'].pop('ref_mark'),
             'the egress job does not keep the mark of the highest stored reference'),
            ('older egress', lambda state: state.update(missing_replica='egress'),
             'an allocation of an older job version still runs'),
            ('egress down', lambda state: state['metrics'].pop('node-egress'),
             'Refused: an egress instance does not report its reference mark'),
            ('reader at R', lambda state: self.metric('node-validator', {'kardamom_validator_egress_cursor_index': 100}),
             'the validator cursors [100.0] (kardamom_validator_egress_cursor_index; -1 is no answer) are not all above'
             ' the index 100'),
            ('reader down', lambda state: state['metrics'].pop('node-batcher'), 'the batcher cursors [-1.0]'),
        )
        for name, change, message in cases:
            with self.subTest(name):
                self.window_ready()
                change(self.api.state)
                output = self.upgrade_check(success=False, feature='5', activation_ms='1700000000000')
                self.assertIn(message, output)

    def test_feature_three_or_four_off_is_refused_while_a_member_holds_feature_five(self):
        self.deploy()
        self.api.state['reads_max'] = {f'node-cluster-{i}': 16 for i in range(3)}
        # A member of an older release reports no gauge of feature 5.
        self.metric('node-cluster-0', {})
        self.metric('node-cluster-2', {})
        self.metric('node-cluster-1', {'kardamom_sealer_feature_state{feature="4"}': 2,
                                        'kardamom_sealer_feature_state{feature="5"}': 1})
        held = self.api.state['metrics']['node-cluster-1']
        self.assertIn('kardamom_sealer_feature_state{service="sealer",member_id="1",feature="5"} 1', held)
        for feature in ('3', '4'):
            for activation_ms in ('18446744073709551615', '1700000000000'):
                with self.subTest(feature=feature, activation_ms=activation_ms):
                    output = self.upgrade_check(success=False, feature=feature, activation_ms=activation_ms)
                    self.assertIn('can turn decision version 5 off, and a member holds feature 5', output)
        output = self.upgrade_check(feature='4', activation_ms='0')
        self.assertIn('The upgrade transaction of feature 4 may go to the Safe', output)
        self.metric('node-cluster-1', {'kardamom_sealer_feature_state{feature="5"}': 0})
        output = self.upgrade_check(feature='4', activation_ms='18446744073709551615')
        self.assertIn('The upgrade transaction of feature 4 may go to the Safe', output)
        # A member whose /metrics does not answer can hold feature 5.
        del self.api.state['metrics']['node-cluster-2']
        output = self.upgrade_check(success=False, feature='4', activation_ms='18446744073709551615')
        self.assertIn("members whose /metrics does not answer: ['cluster-2']", output)


def load_tests(loader, tests, pattern):
    """Keep one shard of each class when DEPLOY_TEST_SHARD is `i/n`: the
    tests whose place in the sorted names of their class is i modulo n. CI
    runs each shard of a long class on its own runner, so a class with
    more tests needs only a larger n, and a new test lands in a shard by
    itself."""
    shard = os.environ.get('DEPLOY_TEST_SHARD', '')
    if not shard:
        return tests
    index, count = (int(part) for part in shard.split('/'))
    kept = unittest.TestSuite()
    places = {}
    for test in iter_tests(tests):
        place = places.setdefault(type(test), 0)
        places[type(test)] = place + 1
        if place % count == index:
            kept.addTest(test)
    return kept


def iter_tests(suite):
    """Every test of `suite`, in its order, with the nested suites flat."""
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from iter_tests(item)
        else:
            yield item


if __name__ == '__main__':
    unittest.main()
