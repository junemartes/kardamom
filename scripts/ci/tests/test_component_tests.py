"""Prebuilt recovery tests run exactly, in a fixed plan, within both deadlines."""

import json
import re
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import component_tests as component

PASS = 'print("test recovers ... ok\\ntest result: ok. 1 passed; 0 failed; 0 ignored;")\n'


class ComponentTests(unittest.TestCase):
    def test_missing_or_ambiguous_names_do_not_supply_coverage(self):
        self.assertEqual(component.Bundle.resolve("recovery", ["actor::recovery"]), "actor::recovery")
        for names in [[], ["not_recovery"], ["left::recovery", "right::recovery"]]:
            with self.subTest(names=names), self.assertRaises(ValueError):
                component.Bundle.resolve("recovery", names)

    def test_incomplete_or_failed_builds_cannot_supply_test_binaries(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "build.jsonl"
            for messages in [[], [{"reason": "build-finished", "success": False}],
                             [{"reason": "build-finished", "success": True}] * 2]:
                log.write_text("\n".join(json.dumps(item) for item in messages))
                with self.subTest(messages=messages), self.assertRaises(ValueError):
                    component.Bundle.build_messages(log)
            log.write_text(json.dumps({"reason": "build-finished", "success": True}))
            self.assertEqual(len(component.Bundle.build_messages(log)), 1)

    def test_every_catalogued_suite_has_a_ci_mode_and_one_cargo_build(self):
        catalog = json.loads(component.CATALOG.read_text())
        self.assertTrue(all("ci_mode" in suite for suite in catalog["local_suites"].values()))
        suites = component.Bundle.selection(catalog)
        command = component.Bundle.build_command(suites)
        # The selection is every suite the catalog marks with a Cargo mode, so a
        # new port changes no number here, and a lost suite still fails.
        cargo = sorted(name for name, suite in catalog["local_suites"].items()
                       if suite["ci_mode"] in component.CARGO_MODES)
        self.assertTrue(cargo)
        self.assertEqual(sorted(suites), cargo)
        self.assertEqual(command.count("cargo"), 1)
        self.assertIn("kardamom-state/docker-e2e", command[command.index("--features") + 1])
        self.assertEqual(command[command.index("--bin") + 1], "kardamom-state-mirror")
        gradle = [name for name, suite in catalog["local_suites"].items() if suite["ci_mode"] == "gradle"]
        self.assertEqual(sorted(set(catalog["local_suites"]) - set(suites)), sorted(gradle))

    def test_the_repository_plan_covers_every_test_once_in_fixed_shards(self):
        plan = component.Plan.load()
        shards = plan.shards()
        self.assertEqual(len(shards), component.SHARDS)
        planned = [(test["suite"], test["test"]) for shard in shards for test in shard]
        self.assertCountEqual(planned, [(test["suite"], test["test"]) for test in plan.tests()])
        self.assertEqual(len(planned), len(set(planned)))
        self.assertEqual(shards, component.Plan.load().shards())

    def test_one_test_at_its_limit_still_fits_every_shard(self):
        plan = component.Plan.load()
        self.assertTrue(all(plan.worst_case(shard) <= component.SHARD_SECONDS for shard in plan.shards()))

    def test_a_suite_limit_replaces_the_default_test_limit(self):
        catalog = {"local_suites": {"slow": {"ci_mode": "gradle", "tests": ["a"], "test_timeout_s": 120},
                                    "fast": {"ci_mode": "in-process", "tests": ["b"]}}}
        plan = component.Plan(catalog, {"slow::a": 300.0})
        self.assertEqual({test["test"]: test["limit"] for test in plan.tests()}, {"a": 120, "b": 60})
        self.assertEqual(plan.estimate(plan.tests()[0]), 120)
        def run(command, log, seconds, environment):
            log.write_text("")
            return 0, False, 1.0
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(component.Shard, "execute", side_effect=run) as execute, \
                patch.object(component.Bundle, "command", return_value=(["x"], {})):
            output = Path(directory) / "results"
            output.mkdir()
            shard = component.Shard(component.Bundle(Path(directory)), None, output)
            shard.run_test(0, dict(plan.tests()[0], mode="in-process"), 600)
        self.assertEqual(execute.call_args.args[2], 120)

    def test_the_plan_spreads_the_longest_tests_first(self):
        catalog = {"local_suites": {"slow": {"ci_mode": "docker", "tests": ["a", "b", "c", "d"]},
                                    "fast": {"ci_mode": "in-process", "tests": [f"t{i}" for i in range(8)]}}}
        timings = {"slow::a": 50.0, "slow::b": 40.0, "slow::c": 30.0, "slow::d": 20.0}
        shards = component.Plan(catalog, timings).shards()
        self.assertEqual([sum(test["suite"] == "slow" for test in shard) for shard in shards], [1, 1, 1, 1])
        loads = [sum(map(component.Plan(catalog, timings).estimate, shard)) for shard in shards]
        self.assertLessEqual(max(loads) - min(loads), 50.0)

    def fixture(self, code, **budgets):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "actor"
            executable.write_text(f"#!{sys.executable}\n" + code)
            executable.chmod(0o755)
            bundle = component.Bundle(root)
            manifest = {"revision": None, "libraries": {}, "binaries": {"actor": bundle.digest(executable)},
                        "tests": [{"suite": "actor", "test": "recovers", "binary": "actor", "name": "recovers"}]}
            (root / "manifest.json").write_text(json.dumps(manifest))
            catalog = root / "catalog.json"
            catalog.write_text(json.dumps({"local_suites": {
                "actor": {"ci_mode": "in-process", "tests": ["recovers"]}
            }}))
            limits = dict(CATALOG=catalog, TEST_SECONDS=component.TEST_SECONDS,
                          SHARD_SECONDS=component.SHARD_SECONDS) | budgets
            with patch.dict(component.os.environ, {}, clear=True), patch.multiple(component, **limits):
                shard = component.Shard(bundle, component.SealerRuntime(root / "sealer"), root / "results")
                success = shard.run(0, [{"suite": "actor", "test": "recovers", "mode": "in-process"}])
            return success, json.loads((root / "results/results.json").read_text())

    def test_an_exact_test_must_run_and_pass(self):
        success, report = self.fixture(PASS)
        self.assertTrue(success)
        self.assertEqual(len(report["tests"]), 1)

    def test_zero_skipped_wrong_and_failed_tests_cannot_pass(self):
        for text in ["test result: ok. 0 passed; 0 failed; 0 ignored;",
                     "test result: ok. 0 passed; 0 failed; 1 ignored;",
                     "test another ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;",
                     "SKIP: anvil unavailable\ntest result: ok. 1 passed; 0 failed; 0 ignored;",
                     "test result: FAILED. 0 passed; 1 failed; 0 ignored;"]:
            with self.subTest(text=text):
                self.assertFalse(self.fixture(f"print({text!r})\n")[0])

    def test_per_test_deadline_terminates_the_process(self):
        success, report = self.fixture("import time\ntime.sleep(20)\n", TEST_SECONDS=0.05)
        self.assertFalse(success)
        self.assertTrue(report["tests"][0]["timed_out"])
        self.assertLess(report["seconds"], 2)

    def test_a_late_success_cannot_pass_the_test_budget(self):
        def late_success(command, log, seconds, environment):
            log.write_text("test recovers ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n")
            return 0, False, component.TEST_SECONDS + 0.01
        with patch.object(component.Shard, "execute", side_effect=late_success):
            self.assertFalse(self.fixture("pass\n")[0])

    def test_shard_deadline_bounds_a_test_with_remaining_budget(self):
        success, report = self.fixture("import time\ntime.sleep(20)\n", SHARD_SECONDS=0.05)
        self.assertFalse(success)
        self.assertTrue(report["tests"][0]["timed_out"])
        self.assertLess(report["seconds"], 2)

    def test_a_junit_method_passes_only_as_the_one_clean_testcase(self):
        entry = {"test": "io.kardamom.Actor::recovers()"}
        cases = {
            '<testcase name="recovers()"/>': True,
            '<testcase name="recovers()"><failure/></testcase>': False,
            '<testcase name="recovers()"><skipped/></testcase>': False,
            '<testcase name="other()"/>': False,
            '<testcase name="recovers()"/><testcase name="recovers()"/>': False,
            '': False,
        }
        for body, expected in cases.items():
            with self.subTest(body=body), tempfile.TemporaryDirectory() as directory:
                (Path(directory) / "TEST-junit-jupiter.xml").write_text(f"<testsuite>{body}</testsuite>")
                self.assertEqual(component.SealerRuntime.verdict(entry, "", Path(directory)), expected)

    def test_a_junit_command_selects_one_method(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime = component.SealerRuntime(Path(directory))
            (Path(directory) / "lib").mkdir()
            (Path(directory) / "lib/aeron.jar").write_text("")
            (Path(directory) / "jvm-args.txt").write_text(
                "--add-opens\njava.base/sun.nio.ch=ALL-UNNAMED\n-Dkardamom.testClasspath=/build/runner/only\n")
            (Path(directory) / "revision.txt").write_text("a" * 40 + "\n")
            with patch.dict(component.os.environ, GITHUB_SHA="b" * 40), self.assertRaises(ValueError):
                runtime.load()
            with patch.dict(component.os.environ, GITHUB_SHA="a" * 40):
                command, _ = runtime.command({"test": "io.kardamom.Actor::recovers()"}, Path(directory) / "reports")
            self.assertEqual(command[command.index("--select-method") + 1], "io.kardamom.Actor#recovers")
            self.assertEqual(command[1:3], ["--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"])
            classpath = command[command.index("-cp") + 1]
            self.assertIn(f"{directory}/lib/aeron.jar", classpath.split(component.os.pathsep))
            self.assertEqual([argument for argument in command if "testClasspath" in argument],
                             [f"-Dkardamom.testClasspath={classpath}"])

    def test_a_stack_test_runs_beside_its_services_with_the_jars(self):
        bundle = component.Bundle(Path("/bundle"))
        self.assertEqual(bundle.placed({"ci_mode": "stack"}, Path("/t/deps/pipeline-1")), "stack/deps/pipeline-1")
        self.assertEqual(bundle.placed({"ci_mode": "docker"}, Path("/t/deps/mirror-1")), "mirror-1")
        environment = component.SealerRuntime(Path("/sealer")).stack_environment()
        self.assertEqual(environment["KARDAMOM_CLUSTER_JAR"], "/sealer/kardamom-cluster-node.jar")
        self.assertTrue(environment["KARDAMOM_AERON_ALL_JAR"].endswith("crates/log/docker/aeron/aeron-all.jar"))

    def test_service_binaries_come_from_the_non_test_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "services.jsonl"
            manifest = Path(directory) / "Cargo.toml"
            manifest.write_text('[package]\nname = "kardamom-ingress"\n')
            messages = [
                {"reason": "compiler-artifact", "executable": "/t/kardamom-ingress", "manifest_path": str(manifest),
                 "profile": {"test": False}, "target": {"name": "kardamom-ingress", "kind": ["bin"]}},
                {"reason": "compiler-artifact", "executable": "/t/deps/kardamom_ingress-1",
                 "manifest_path": str(manifest), "profile": {"test": True},
                 "target": {"name": "kardamom_ingress", "kind": ["lib"]}},
                {"reason": "build-finished", "success": True}]
            log.write_text("\n".join(json.dumps(message) for message in messages))
            artifacts, services, _ = component.Bundle(Path(directory)).artifacts([log])
        self.assertEqual(services, {"kardamom-ingress": Path("/t/kardamom-ingress")})
        self.assertEqual(artifacts, {("kardamom-ingress", "kardamom_ingress"): Path("/t/deps/kardamom_ingress-1")})

    def test_an_exited_test_leaves_no_process_in_its_group(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "orphan.pid"
            code = ("import subprocess, sys\n"
                    f"child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
                    f"open({str(marker)!r}, 'w').write(str(child.pid))\n")
            result = component.Shard.execute([sys.executable, "-c", code], Path(directory) / "log", 10)
            self.assertEqual(result[:2], (0, False))
            pid = int(marker.read_text())
            deadline = component.time.monotonic() + 5
            while component.time.monotonic() < deadline and Path(f"/proc/{pid}").exists() \
                    and "Z" not in Path(f"/proc/{pid}/stat").read_text().split()[2]:
                component.time.sleep(0.05)
            alive = Path(f"/proc/{pid}").exists() and "Z" not in Path(f"/proc/{pid}/stat").read_text().split()[2]
            self.assertFalse(alive)

    def test_a_crate_with_no_library_uses_its_one_bin_test_target(self):
        bundle = component.Bundle(Path("/bundle"))
        artifacts = {("mirror", "mirror-bin"): Path("/t/deps/mirror-1"),
                     ("mirror", component.BIN_TESTS): [Path("/t/deps/mirror-1")],
                     ("two", component.BIN_TESTS): [Path("/t/deps/a-1"), Path("/t/deps/b-1")]}
        suite = {"command": ["cargo", "test", "-p", "mirror", "recovery::"]}
        self.assertEqual(bundle.source(suite, artifacts), Path("/t/deps/mirror-1"))
        for command in [["cargo", "test", "-p", "two"], ["cargo", "test", "-p", "two", "--test", "gone"]]:
            with self.subTest(command=command), self.assertRaises(ValueError):
                bundle.source({"command": command}, artifacts)

    def test_a_docker_test_leaves_no_container(self):
        listings = iter([{"old"}, {"old", "new"}])
        with patch.object(component.Docker, "containers", side_effect=lambda: next(listings)), \
                patch.object(component.subprocess, "run") as run:
            before = component.Docker.containers()
            self.assertEqual(component.Docker.remove_new(before), 1)
        self.assertEqual(run.call_args.args[0], ["docker", "rm", "-f", "-v", "new"])

    def test_timings_keep_only_catalogued_tests(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            timings, results = root / "timings.json", root / "results.json"
            timings.write_text(json.dumps({"gone::test": 9.0}))
            plan = component.Plan.load()
            known = plan.tests()[0]
            results.write_text(json.dumps({"tests": [dict(known, seconds=3.5, exit_code=0),
                                                     dict(known, test="unknown", seconds=1.0, exit_code=0)]}))
            with patch.object(component, "TIMINGS", timings), patch("builtins.print"):
                component.Timings.update([results])
            self.assertEqual(json.loads(timings.read_text()), {f"{known['suite']}::{known['test']}": 3.5})

    def test_workflow_schedules_every_runner_shard(self):
        workflow = (component.ROOT / ".github/workflows/ci.yml").read_text()
        block = workflow.split("  component-recovery:\n", 1)[1].split("\n\n", 1)[0]
        matrix = json.loads(re.search(r"shard: (\[[^\n]+\])", block).group(1))
        self.assertEqual(matrix, list(range(component.SHARDS)))
        self.assertIn("    name: component recovery (${{ matrix.shard }})\n", block)
        self.assertIn("    needs: [build, sealer-runtime]\n", block)
        self.assertIn("    timeout-minutes: 10\n", block)

    def test_bundle_rejects_a_changed_binary_or_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "actor"
            executable.write_text("original")
            bundle = component.Bundle(root)
            manifest = {"revision": "a" * 40, "libraries": {},
                        "binaries": {"actor": bundle.digest(executable)}, "tests": []}
            (root / "manifest.json").write_text(json.dumps(manifest))
            with patch.dict(component.os.environ, GITHUB_SHA="b" * 40), self.assertRaises(ValueError):
                bundle.load()
            executable.write_text("changed")
            with patch.dict(component.os.environ, GITHUB_SHA="a" * 40), self.assertRaises(ValueError):
                bundle.load()


if __name__ == "__main__":
    unittest.main()
