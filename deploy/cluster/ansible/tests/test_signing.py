"""Offline cosign bundle contract formerly exercised by signing-selftest.sh.

Runs when cosign is installed. Uses disposable keys; never uploads to Rekor.
The image role's keyless command wiring is tested in test_images.py.
"""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


@unittest.skipUnless(shutil.which('cosign'), 'cosign required for offline bundle test')
class SigningTest(unittest.TestCase):
    def test_bundle_roundtrip_tamper_and_missing_bundle(self):
        with tempfile.TemporaryDirectory(prefix='kardamom-signing-test-') as directory:
            root = Path(directory)
            env = dict(os.environ, COSIGN_PASSWORD='')
            def run(*args, success=True):
                result = subprocess.run(['cosign', *args], cwd=root, env=env,
                                        text=True, capture_output=True, timeout=60)
                self.assertEqual(result.returncode == 0, success, result.stderr)
            manifest = root / 'images.digests'
            manifest.write_text('aeron registry.example/aeron:dev@sha256:' + 'a' * 64 + '\n')
            run('generate-key-pair')
            run('sign-blob', '--yes', '--key', 'cosign.key', '--bundle', 'bundle',
                '--use-signing-config=false', '--tlog-upload=false', str(manifest))
            verify = ['verify-blob', '--key', 'cosign.pub', '--bundle', 'bundle',
                      '--insecure-ignore-tlog=true', str(manifest)]
            run(*verify)
            manifest.write_text(manifest.read_text() + '# tampered\n')
            run(*verify, success=False)
            (root / 'bundle').unlink()
            run(*verify, success=False)
