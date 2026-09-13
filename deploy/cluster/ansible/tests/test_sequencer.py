"""Compile the static Nomad job and verify lane ownership across resize phases."""
import json
from pathlib import Path
import shutil
import subprocess
import unittest

CLUSTER = Path(__file__).resolve().parents[2]


@unittest.skipUnless(shutil.which('nomad'), 'nomad required')
class SequencerTest(unittest.TestCase):
    def compile(self, table, previous=None, success=True):
        args = ['nomad', 'job', 'run', '-output', '-var=shard_table=' + json.dumps(table)]
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

    def test_rejects_malformed_maps(self):
        for table in [[0], [8] * 256, [-1] * 256, [0.5] * 256]:
            self.compile(table, success=False)
            self.compile([0] * 256, previous=table, success=False)
