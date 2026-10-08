"""Exercise hook decisions without running a push or the Rust toolchain."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

HOOK = Path(__file__).resolve().parents[1] / 'pre-push-style.sh'


class PushHookTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root / 'repo'
        self.other = self.root / 'other'
        self.bin = self.root / 'bin'
        for path in (self.repo, self.other, self.bin):
            path.mkdir()
        for path, origin in ((self.repo, 'junemartes/kardamom'),
                             (self.other, 'junemartes/other')):
            subprocess.run(['git', 'init', '-q', str(path)], check=True)
            subprocess.run(['git', '-C', str(path), 'remote', 'add', 'origin',
                            f'https://github.com/{origin}.git'], check=True)
        fake = self.bin / 'just'
        fake.write_text('#!/bin/sh\nprintf "%s\\n" "$PWD" >> "$HOOK_CALLS"\nexit 1\n')
        fake.chmod(0o755)
        self.calls = self.root / 'calls'

    def check_command(self, command, denied):
        env = dict(os.environ, PATH=f'{self.bin}:{os.environ["PATH"]}',
                   HOOK_CALLS=str(self.calls), TMPDIR=str(self.root))
        event = {'cwd': str(self.other), 'tool_input': {'command': command}}
        result = subprocess.run(['bash', str(HOOK)], input=json.dumps(event),
                                text=True, capture_output=True, env=env, check=True)
        if denied:
            answer = json.loads(result.stdout)['hookSpecificOutput']
            self.assertEqual(answer['permissionDecision'], 'deny')
            self.assertEqual(self.calls.read_text().strip(), str(self.repo))
        else:
            self.assertEqual(result.stdout, '')
            self.assertFalse(self.calls.exists())

    def test_unrelated_push_skips_style(self):
        self.check_command('git push origin main', False)

    def test_later_push_to_this_repository_is_checked(self):
        self.check_command(f'git push; cd {self.repo} && git push', True)

    def test_newline_separates_cd_from_push(self):
        self.check_command(f'cd {self.repo}\ngit push', True)

    def test_git_directory_option(self):
        self.check_command(f'git -C {self.repo} push', True)

    def test_explicit_other_pr_with_equals_skips_style(self):
        self.check_command(f'cd {self.repo} && gh pr create --repo=junemartes/other', False)

    def test_explicit_other_pr_does_not_hide_later_push(self):
        self.check_command(f'gh pr create --repo=junemartes/other; git -C {self.repo} push', True)

    def test_read_only_command_skips_style(self):
        self.check_command(f'git -C {self.repo} status', False)


if __name__ == '__main__':
    unittest.main()
