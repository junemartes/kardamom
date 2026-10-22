"""Compile the static Nomad job and verify lane ownership across resize phases."""
import json
from pathlib import Path
import shutil
import subprocess
import unittest

CLUSTER = Path(__file__).resolve().parents[2]


@unittest.skipUnless(shutil.which('nomad'), 'nomad required')
class SequencerTest(unittest.TestCase):
    def compile(self, table, previous=None, success=True, extra=()):
        args = ['nomad', 'job', 'run', '-output', '-var=shard_table=' + json.dumps(table), *extra]
        if previous is not None:
            args.append('-var=previous_shard_table=' + json.dumps(previous))
        result = subprocess.run(args + ['nomad/sequencer.nomad.hcl'], cwd=CLUSTER,
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode == 0, success, result.stderr)
        if success:
            return {g['Name']: g for g in json.loads(result.stdout)['Job']['TaskGroups']}
        return None

    def flags(self, group):
        args = group['Tasks'][0]['Config']['args']
        return dict(zip(args[::2], args[1::2]))

    def test_steady_lane_counts_and_placement(self):
        for count in range(1, 9):
            table = [v % count for v in range(256)]
            groups = self.compile(table)
            self.assertEqual(set(groups), {f'seq-{lane}' for lane in range(count)})
            for lane in range(count):
                group = groups[f'seq-{lane}']
                flags = self.flags(group)
                self.assertEqual(group['Count'], 2)
                self.assertEqual(group['Constraints'][0]['Operand'], 'distinct_hosts')
                self.assertEqual(group['Update']['MaxParallel'], 1)
                self.assertEqual(flags['--lane'], str(lane))
                self.assertEqual(flags['--partition-count'], str(count))
                self.assertEqual(flags['--vslots'], ','.join(str(v) for v in range(256) if table[v] == lane))
                self.assertNotIn('--shadow-vslots', flags)
                self.assertEqual(group['Tasks'][0]['Env']['KARDAMOM_METRICS_ADDR'], f'0.0.0.0:{9001 + 10 * lane}')

    def test_overlap_preserves_losing_groups_and_shadows_incoming_slots(self):
        old = [v % 2 for v in range(256)]
        new = [2 if v < 84 or v == 85 else old[v] for v in range(256)]
        for current, target in [(old, new), (new, old), ([1 - lane for lane in old], new)]:
            steady = self.compile(current)
            overlap = self.compile(target, current)
            for lane in set(current + target):
                incoming = [v for v in range(256) if target[v] == lane and current[v] != lane]
                group = overlap[f'seq-{lane}']
                if not incoming:
                    self.assertEqual(group, steady[f'seq-{lane}'])
                else:
                    flags = self.flags(group)
                    self.assertEqual(flags['--shadow-vslots'], ','.join(map(str, incoming)))
                    self.assertEqual(flags['--extra-lanes'], ','.join(map(str, sorted({current[v] for v in incoming}))))
                    self.assertEqual(flags['--vslots'], ','.join(str(v) for v in range(256) if target[v] == lane))

    def test_the_ingress_is_off_by_default(self):
        for group in self.compile([v % 4 for v in range(256)]).values():
            task = group['Tasks'][0]
            self.assertNotIn('ingress', task['Config']['args'])
            self.assertEqual([s['Name'] for s in group['Services']], ['kardamom-sequencer'])
            self.assertEqual([p['Label'] for p in group['Networks'][0]['ReservedPorts']], ['metrics'])
            self.assertEqual(task['Config']['ulimit'], [])
            self.assertEqual([t['DestPath'] for t in task['Templates']], ['local/channels.toml', 'local/sequencer.toml'])

    def test_the_ingress_runs_in_each_sequencer_on_a_port_of_its_lane(self):
        groups = self.compile([v % 4 for v in range(256)], extra=['-var=ingress=true', '-var=ingress_archive_durability=true'])
        for lane in range(4):
            group = groups[f'seq-{lane}']
            task = group['Tasks'][0]
            args = task['Config']['args']
            ingress = args[args.index('ingress') + 1:]
            flags = dict(zip(ingress[::2], ingress[1::2]))
            self.assertEqual(ingress[-1], '--archive-durability')
            self.assertEqual(flags['--ingress-id'], f'{lane + 1}${{NOMAD_ALLOC_INDEX}}')
            self.assertEqual(flags['--jsonrpc-bind'], '0.0.0.0:${NOMAD_PORT_jsonrpc}')
            self.assertEqual(flags['--shards'], '4')
            self.assertEqual(flags['--pending-receipt-timeout-ms'], '30000')
            self.assertEqual(flags['--cluster-egress-endpoint'], '${meta.node_ip}:${NOMAD_HOST_PORT_ingress_egress}')
            self.assertIn('ingress_egress', [p['Label'] for p in group['Networks'][0]['DynamicPorts']])
            ports = {p['Label']: p['Value'] for p in group['Networks'][0]['ReservedPorts']}
            self.assertEqual(ports['jsonrpc'], 8550 + lane)
            service = next(s for s in group['Services'] if s['Name'] == 'ingress-jsonrpc')
            self.assertEqual((service['PortLabel'], service['Checks'][0]['Path']), ('jsonrpc', '/health'))
            self.assertEqual(task['KillTimeout'], 40 * 10**9)
            self.assertIn('local/shard-map.toml', [t['DestPath'] for t in task['Templates']])

    def test_rejects_malformed_maps(self):
        for table in [[0], [8] * 256, [-1] * 256, [0.5] * 256]:
            self.compile(table, success=False)
            self.compile([0] * 256, previous=table, success=False)
