"""Shared full-stack binaries cover every test exactly once and cannot skip."""

import json
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import semantics_tests as semantics


class SemanticsTests(unittest.TestCase):
    def test_every_test_has_one_shard_independent_of_listing_order(self):
        names = [f"case_{index}" for index in range(37)]
        shards = semantics.SemanticsBundle.partition(names)
        self.assertEqual(shards, semantics.SemanticsBundle.partition(list(reversed(names))))
        self.assertEqual(sorted(name for shard in shards for name in shard), sorted(names))
        self.assertEqual(sorted(map(len, shards)), [9, 9, 9, 10])

    def test_duplicate_tests_or_empty_shards_are_not_coverage(self):
        for names in ([], ["one"], ["same"] * semantics.SHARDS):
            with self.subTest(names=names), self.assertRaises(ValueError):
                semantics.SemanticsBundle.partition(names)

    def fixture(self, body, **budgets):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / semantics.TEST_BINARY
            executable.parent.mkdir(parents=True)
            names = [f"case{index}" for index in range(8)]
            listing = "\n".join(name + ": test" for name in names)
            executable.write_text(f"#!{sys.executable}\nimport sys\nif '--list' in sys.argv:\n print({listing!r})\n sys.exit(0)\n" + body)
            executable.chmod(0o755)
            bundle = semantics.SemanticsBundle(root)
            manifest = {"revision": None, "shards": semantics.SHARDS,
                        "tests": bundle.partition(names),
                        "binaries": {semantics.TEST_BINARY: bundle.digest(executable)}}
            (root / "manifest.json").write_text(json.dumps(manifest))
            with patch.dict(semantics.os.environ, {}, clear=True), patch.object(semantics.shutil, "which", return_value="/runtime"), patch.multiple(semantics, **dict(SHARD_SECONDS=semantics.SHARD_SECONDS) | budgets):
                success = bundle.run(0, root / "results")
            return success, json.loads((root / "results/results.json").read_text())

    def test_one_process_runs_the_exact_shard(self):
        success, result = self.fixture('assert "--exact" in sys.argv and "case0" in sys.argv and "case4" in sys.argv\nprint("test case0 ... ok\\ntest case4 ... ok\\ntest result: ok. 2 passed; 0 failed; 0 ignored;")\n')
        self.assertTrue(success)
        self.assertEqual(result["tests"], ["case0", "case4"])

    def test_missing_wrong_or_silently_skipped_tests_cannot_pass(self):
        for text in ["test result: ok. 0 passed; 0 failed; 0 ignored;",
                     "test case0 ... ok\ntest case1 ... ok\ntest result: ok. 2 passed; 0 failed; 0 ignored;",
                     "test case0 ... SKIP: anvil not available\nok\ntest case4 ... ok\ntest result: ok. 2 passed; 0 failed; 0 ignored;"]:
            with self.subTest(text=text):
                self.assertFalse(self.fixture(f"print({text!r})\n")[0])

    def test_shard_deadline_terminates_the_process(self):
        success, result = self.fixture("import time\ntime.sleep(20)\n", SHARD_SECONDS=0.05)
        self.assertFalse(success)
        self.assertTrue(result["timed_out"])
        self.assertLess(result["seconds"], 2)

    def test_workflow_runs_all_shards_after_one_build(self):
        workflow = (semantics.ROOT / ".github/workflows/chain-semantics-e2e.yml").read_text()
        runtime = workflow.split("  chain-semantics:\n", 1)[1]
        matrix = json.loads(re.search(r"shard: (\[[^\n]+\])", runtime).group(1))
        self.assertEqual(matrix, list(range(semantics.SHARDS)))
        self.assertIn("    needs: build\n", runtime)
        self.assertIn("    timeout-minutes: 15\n", runtime)
        self.assertNotIn("cargo ", runtime)
        self.assertEqual(workflow.count("semantics_tests.py prepare"), 1)

    def test_build_selects_every_runtime_binary_in_one_graph(self):
        command = semantics.SemanticsBundle.build_command()
        self.assertEqual(command.count("cargo"), 1)
        names = {command[index + 1] for index, arg in enumerate(command) if arg == "--bin"}
        self.assertEqual(names, {"kardamom-" + name for name in semantics.SERVICES})
        self.assertNotIn("--bins", command)
        self.assertIn("e2e/full-pipeline-e2e", command)


if __name__ == "__main__":
    unittest.main()
