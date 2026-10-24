"""The jobs of the carry window with every switch off, against main.

The fixture fixtures/carry-off-jobs.json holds, for each job, the job
metas and the SHA-256 of the canonical JSON of the rest of the compiled job
of main before the switches of the carry window (`nomad job run -output`
with the job defaults). The monitoring job embeds the dashboards, so the
fixture keeps digests, not the jobs. With every switch off, each job of
this checkout must compile to the same job. Its metas may only add the
metas that state the release.

A change that edits one of these jobs on purpose writes the fixture again
from the tree that holds the new jobs:

    python3 deploy/cluster/ansible/tests/test_off_identity.py --write <tree>

Run the tests with: python3 -m unittest discover -s deploy/cluster/ansible/tests -p test_off_identity.py
"""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import unittest

CLUSTER = Path(__file__).resolve().parents[2]
FIXTURE = Path(__file__).resolve().parent / 'fixtures' / 'carry-off-jobs.json'
# The jobs that the switches change, by file.
JOBS = ('aeron.system', 'batcher', 'cluster', 'egress', 'executor', 'ingress', 'monitoring', 'sequencer',
        'state-mirror', 'validator')
# The value of each switch when it is off, for the jobs that declare it.
OFF = {'member_feed': 'off', 'exec_on_sealers': 'off', 'carry_executors': '', 'sealer_version_floor': '0',
       'admission': 'off'}
# The metas that state what the release does. They are on with every
# switch off.
RELEASE_METAS = {'sealer_version_kept', 'carried_ref_fault', 'ref_mark'}


def compile_job(cluster, name, variables=()):
    """The compiled job of nomad/<name>.nomad.hcl under `cluster`."""
    out = subprocess.run(['nomad', 'job', 'run', '-output', *variables, f'nomad/{name}.nomad.hcl'],
                         cwd=cluster, check=True, capture_output=True, text=True).stdout
    return json.loads(out)['Job']


def digest(job):
    """The SHA-256 of the canonical JSON of `job`."""
    return hashlib.sha256(json.dumps(job, sort_keys=True).encode()).hexdigest()


def off_variables(name):
    """The -var arguments that set each switch that the job declares to off."""
    text = (CLUSTER / 'nomad' / f'{name}.nomad.hcl').read_text()
    return [f'-var={key}={value}' for key, value in OFF.items() if f'variable "{key}"' in text]


@unittest.skipUnless(shutil.which('nomad'), 'nomad required')
class OffIdentity(unittest.TestCase):
    """Every switch off compiles each job as main did."""

    def test_every_job_with_the_switches_off_compiles_as_main(self):
        fixture = json.loads(FIXTURE.read_text())
        self.assertEqual(sorted(fixture), sorted(JOBS))
        for name in JOBS:
            for variables in ([], off_variables(name)):
                with self.subTest(job=name, variables=variables):
                    job = compile_job(CLUSTER, name, variables)
                    meta = job.pop('Meta', None) or {}
                    self.assertEqual(digest(job), fixture[name]['sha256'],
                                     f'{name} compiles to another job than main; compare with nomad job run -output')
                    self.assertEqual({k: v for k, v in meta.items() if k not in RELEASE_METAS}, fixture[name]['meta'])
                    self.assertEqual({meta[k] for k in RELEASE_METAS & set(meta)} - {'true'}, set())


def write(tree):
    """Write the fixture from the jobs of `tree`, a checkout of main."""
    cluster = Path(tree) / 'deploy' / 'cluster'
    jobs = {}
    for name in JOBS:
        job = compile_job(cluster, name)
        meta = job.pop('Meta', None) or {}
        jobs[name] = {'meta': meta, 'sha256': digest(job)}
    FIXTURE.write_text(json.dumps(jobs, indent=1, sort_keys=True) + '\n')


if __name__ == '__main__':
    if sys.argv[1:2] == ['--write']:
        write(sys.argv[2])
    else:
        unittest.main()
