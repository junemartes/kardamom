"""Run the real Ansible image playbook and Docker module with isolated tool stubs."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest

ANSIBLE = Path(__file__).resolve().parents[1]
SERVICES = ['ingress', 'sequencer', 'executor', 'validator', 'da-watcher', 'batcher']


@unittest.skipUnless(shutil.which('ansible-playbook'), 'ansible-playbook required')
class ImageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='kardamom-image-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.tools = self.root / 'tools'
        self.tools.mkdir()
        for name in ('docker', 'cosign'):
            shutil.copyfile(ANSIBLE / 'tests/fixtures/image_tools.py', self.tools / name)
            (self.tools / name).chmod(0o755)
        self.manifest = self.root / 'images.digests'
        self.bundle = self.root / 'images.digests.sigbundle'
        self.manifest.write_text('previous complete manifest\n')
        self.bundle.write_text('previous signature\n')
        self.jar = self.root / 'cluster.jar'
        self.jar.write_text('test jar')
        self.release = self.root / 'release'
        self.release.mkdir()
        for name in SERVICES:
            (self.release / ('kardamom-' + name)).write_text('test binary')
        self.libs = self.release / 'build/rusteron/out/build/lib'
        self.libs.mkdir(parents=True)
        for name in ('libaeron.so', 'libaeron_archive_c_client.so'):
            (self.libs / name).write_text('test library')

    def run_images(self, extra=None, fail='', check=False, success=True):
        env = {k: v for k, v in os.environ.items() if not k.startswith(('ANSIBLE_', 'DOCKER_', 'KARDAMOM_COSIGN_'))}
        env.update(PATH=str(self.tools) + os.pathsep + env['PATH'],
                   KARDAMOM_IMAGE_TEST_DIR=str(self.root), KARDAMOM_IMAGE_TEST_FAIL=fail,
                   ANSIBLE_NOCOLOR='1', ANSIBLE_STDOUT_CALLBACK='default',
                   OBJC_DISABLE_INITIALIZE_FORK_SAFETY='YES')
        settings = {'images_mode': 'prebuilt', 'images_manifest': str(self.manifest),
                    'images_cluster_jar': str(self.jar), 'images_release_dir': str(self.release),
                    'images_registry': 'registry.example:5000', 'images_tag': 'test',
                    'images_push_node': '', 'images_sign': False,
                    'images_docker_binary': str(self.tools / 'docker'),
                    'cosign_binary': str(self.tools / 'cosign')} | (extra or {})
        result = subprocess.run(['ansible-playbook', '-i', 'localhost,', str(ANSIBLE / 'images.yml'),
                                 '-e', json.dumps(settings)] + (['--check'] if check else []),
                                env=env, cwd=ANSIBLE.parent, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=180)
        self.assertEqual(result.returncode == 0, success, result.stdout)
        return result.stdout

    def calls(self):
        path = self.root / 'calls.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_prebuilt_direct_push_and_unsigned_manifest(self):
        self.run_images()
        records = self.manifest.read_text().splitlines()
        self.assertEqual([r.split()[0] for r in records], ['aeron'] + SERVICES + ['cluster'])
        self.assertTrue(all('@sha256:' in r for r in records))
        self.assertFalse(self.bundle.exists(), 'an unsigned release must remove an old signature')
        self.assertFalse(any(tool == 'cosign' for tool, _ in self.calls()))
        self.assertFalse((self.release / '_aeronlibs').exists(), 'staging must not modify build artifacts')
        # Each push should name exactly the tagged image represented in its manifest record.
        pushes = [args[-1] for tool, args in self.calls() if args[0] == 'push']
        self.assertEqual(pushes, [r.split()[1].split('@')[0] for r in records])

    def test_source_builds_and_node_push_with_signing(self):
        self.run_images({'images_mode': 'source', 'images_push_node': 'control-0', 'images_sign': True})
        calls = self.calls()
        self.assertEqual(self.bundle.read_text(), self.manifest.read_text())
        signatures = [args for tool, args in calls if tool == 'cosign']
        self.assertEqual(len(signatures), 9)
        self.assertTrue(all(args[0] == 'sign' for args in signatures[:-1]))
        self.assertEqual(signatures[-1][0], 'sign-blob')
        self.assertFalse(any(args[0] == 'push' for tool, args in calls))
        self.assertEqual(sum(args[0] == 'exec' and args[2:4] == ['docker', 'push'] for _, args in calls), 8)
        self.assertEqual(list(self.root.glob('kardamom-images-*.tar')), [], 'node archives must be cleaned')

    def test_failed_digest_preserves_previous_release(self):
        self.run_images(fail='digest', success=False)
        self.assertEqual(self.manifest.read_text(), 'previous complete manifest\n')
        self.assertEqual(self.bundle.read_text(), 'previous signature\n')

    def test_failed_manifest_signing_preserves_previous_release(self):
        self.run_images({'images_sign': True}, fail='sign-blob', success=False)
        self.assertEqual(self.manifest.read_text(), 'previous complete manifest\n')
        self.assertEqual(self.bundle.read_text(), 'previous signature\n')

    def test_node_load_failure_cleans_archive(self):
        self.run_images({'images_push_node': 'control-0'}, fail='load', success=False)
        self.assertEqual(list(self.root.glob('kardamom-images-*.tar')), [])
        self.assertEqual(self.manifest.read_text(), 'previous complete manifest\n')

    def test_check_mode_does_not_build_or_publish(self):
        self.run_images(check=True)
        self.assertEqual(self.calls(), [])
        self.assertEqual(self.manifest.read_text(), 'previous complete manifest\n')

    def test_relative_manifest_environment_matches_deployment(self):
        play = self.root / 'defaults.yml'
        play.write_text(json.dumps([{
            'name': 'Check shared manifest path defaults',
            'hosts': 'localhost', 'connection': 'local', 'gather_facts': False,
            'vars_files': [str(ANSIBLE / 'group_vars/all.yml'),
                           str(ANSIBLE / 'roles/images/defaults/main.yml'),
                           str(ANSIBLE / 'roles/workloads/defaults/main.yml')],
            'vars': {'images_cluster_dir': str(self.root), 'workloads_cluster_dir': str(self.root)},
            'tasks': [{'name': 'Require the same absolute manifest path', 'ansible.builtin.assert': {
                'that': ['images_manifest == workloads_manifest',
                         'images_manifest == images_cluster_dir + "/release.digests"']}}],
        }]))
        env = dict(os.environ, DIGEST_MANIFEST='release.digests', ANSIBLE_STDOUT_CALLBACK='default',
                   OBJC_DISABLE_INITIALIZE_FORK_SAFETY='YES')
        result = subprocess.run(['ansible-playbook', '-i', 'localhost,', str(play)],
                                env=env, text=True, capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_most_recent_cached_library_build_is_staged(self):
        # One cargo target holds one rusteron-archive build per feature set.
        # The newest build of each library is staged, and the client crate's
        # own tree is never preferred over the archive crate's.
        newer = self.release / 'build/rusteron-archive-bbbb/out/build/lib'
        older = self.release / 'build/rusteron-archive-aaaa/out/build/lib'
        client = self.release / 'build/rusteron-client-cccc/out/build/lib'
        for path in (newer, older, client):
            path.mkdir(parents=True)
            for name in ('libaeron.so', 'libaeron_archive_c_client.so'):
                (path / name).write_text(path.parts[-4])
        now = time.time()
        for path, age in ((older, 300), (newer, 100), (client, 0)):
            for lib in path.iterdir():
                os.utime(lib, (now - age, now - age))
        output = self.run_images()
        self.assertIn('rusteron-archive-bbbb/out/build/lib/libaeron.so', output)
        self.assertNotIn('rusteron-archive-aaaa/out/build/lib/libaeron.so', output.split('Stage Aeron runtime libraries')[1])
        self.assertNotIn('rusteron-client', output.split('Stage Aeron runtime libraries')[1])

    def test_missing_library_fails_before_building(self):
        (self.libs / 'libaeron.so').unlink()
        self.run_images(success=False)
        self.assertFalse(any('buildx' in args for _, args in self.calls()))
        self.assertEqual(self.manifest.read_text(), 'previous complete manifest\n')


if __name__ == '__main__':
    unittest.main()
