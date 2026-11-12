import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("delay_fault", Path(__file__).with_name("fault.py"))
fault = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fault)
PEERS = ["192.0.2.2", "192.0.2.3"]


class Kernel:
    def __init__(self):
        self.rows = [{"kind": "noqueue", "handle": "0:", "root": True}]
        self.filters = []
        self.index = 7
        self.txqlen = 0
        self.fail_filter = None
        self.commands = []

    def command(self, *args):
        self.commands.append(args)
        if args[:4] == ("ip", "-j", "route", "get"):
            return json.dumps([{"dev": "eth0"}])
        if args[:5] == ("ip", "-j", "link", "show", "dev"):
            return json.dumps([{"ifindex": self.index, "txqlen": self.txqlen}])
        if args == ("ip", "link", "set", "dev", "eth0", "txqueuelen", "0"):
            self.txqlen = 0
            return ""
        if args[:5] == ("tc", "-s", "-j", "qdisc", "show"):
            return json.dumps(self.rows)
        if args[:4] == ("tc", "-j", "filter", "show"):
            return json.dumps(self.filters)
        if args[:3] == ("tc", "qdisc", "add"):
            return self.add_queue(args)
        if args[:3] == ("tc", "filter", "add"):
            return self.add_filter(args)
        if args == ("tc", "qdisc", "del", "dev", "eth0", "root", "handle", fault.ROOT):
            self.rows = [{"kind": "noqueue", "handle": "0:", "root": True}]
            self.filters = []
            return ""
        raise AssertionError(args)

    def add_queue(self, args):
        handle = args[args.index("handle") + 1]
        if "root" in args:
            if self.txqlen == 0:
                self.txqlen = 1000
            self.rows = [{"kind": "prio", "handle": handle, "root": True, "drops": 0,
                          "options": {"bands": int(args[args.index("bands") + 1]),
                                      "priomap": [int(v) for v in args[args.index("priomap") + 1:]]}}]
        else:
            delay = args[args.index("delay") + 1]
            assert delay.endswith("ms")
            self.rows.append({"kind": "netem", "handle": handle,
                              "parent": args[args.index("parent") + 1],
                              "packets": 0, "drops": 0,
                              "options": {"limit": int(args[args.index("limit") + 1]),
                                          "delay": {"delay": float(delay[:-2]) / 1000, "jitter": 0}}})
        return ""

    def add_filter(self, args):
        if len(self.filters) == self.fail_filter:
            raise RuntimeError("injected filter setup failure")
        value = lambda key: args[args.index(key) + 1]
        self.filters.append({"kind": "flower", "protocol": value("protocol"),
                             "options": {"classid": value("classid"), "keys": {
                                 "ip_proto": value("ip_proto"), "dst_ip": value("dst_ip"),
                                 "dst_port": int(value("dst_port"))}}})
        return ""

    def classify(self, peer, protocol, port):
        matches = [row["options"]["classid"] for row in self.filters
                   if row["options"]["keys"] == {
                       "ip_proto": protocol, "dst_ip": peer, "dst_port": port}]
        return matches[0] if matches else fault.ROOT + "1"


class FaultTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.kernel = Kernel()
        self.fault = fault.Fault("123", PEERS)
        self.owner = Path(self.directory.name) / "owner.json"
        self.patches = [patch.object(fault, "OWNER", self.owner),
                        patch.object(fault, "run", self.kernel.command),
                        patch.object(fault.subprocess, "Popen"), patch.object(fault.time, "sleep")]
        self.mocks = [p.start() for p in self.patches]
        for p in self.patches:
            self.addCleanup(p.stop)

    def test_only_peer_raft_udp_enters_the_delay_queues(self):
        self.fault.arm()
        self.assertEqual(len(self.kernel.filters), 8)
        for index, peer in enumerate(PEERS):
            for port in fault.PORTS:
                self.assertEqual(self.kernel.classify(peer, "udp", port), fault.ROOT + str(index + 2))
                self.assertEqual(self.kernel.classify(peer, "tcp", port), fault.ROOT + "1")
        for peer, port in [(PEERS[0], 6379), (PEERS[0], 9024), (PEERS[0], 40205), ("192.0.2.9", 40201)]:
            self.assertEqual(self.kernel.classify(peer, "udp", port), fault.ROOT + "1")
        self.assertEqual(self.mocks[2].call_args.args[0][2:], ["123", "expire", *PEERS])
        self.assertTrue(self.mocks[2].call_args.kwargs["start_new_session"])

    def test_partial_setup_is_removed_and_cleanup_is_idempotent(self):
        self.kernel.fail_filter = 5
        with self.assertRaisesRegex(RuntimeError, "setup failure"):
            self.fault.arm()
        self.assertTrue(self.owner.exists())
        self.assertEqual(len(self.kernel.filters), 5)
        self.assertTrue(self.fault.clear()["restored"])
        self.assertFalse(self.owner.exists())
        self.assertEqual(self.kernel.txqlen, 0)
        self.assertEqual(self.kernel.rows, [{"kind": "noqueue", "handle": "0:", "root": True}])
        self.assertTrue(self.fault.clear()["restored"])

    def test_existing_root_is_never_overwritten(self):
        self.kernel.rows[0] = {"kind": "fq_codel", "handle": "0:", "root": True}
        with self.assertRaisesRegex(ValueError, "refuses to replace"):
            self.fault.arm()
        self.assertFalse(self.owner.exists())
        self.assertFalse(any(c[:3] == ("tc", "qdisc", "add") for c in self.kernel.commands))

    def test_replaced_interface_or_root_is_never_removed(self):
        self.fault.arm()
        self.kernel.index += 1
        with self.assertRaisesRegex(ValueError, "identity changed"):
            self.fault.clear()
        self.kernel.index -= 1
        self.kernel.rows[0] = {"kind": "fq_codel", "handle": "9000:", "root": True}
        with self.assertRaisesRegex(ValueError, "refuses to replace"):
            self.fault.clear()
        self.assertTrue(self.owner.exists())
        self.assertFalse(any(c[:3] == ("tc", "qdisc", "del") for c in self.kernel.commands))

    def test_an_expired_old_lease_cannot_remove_a_new_fault(self):
        self.fault.arm()
        self.fault.clear()
        new = fault.Fault("456", PEERS)
        new.arm()
        self.fault.expire()
        self.assertEqual(json.loads(self.owner.read_text())["token"], "456")
        self.assertEqual(self.kernel.rows[0]["kind"], "prio")
        new.expire()
        self.assertFalse(self.owner.exists())
        self.assertEqual(self.kernel.rows[0]["kind"], "noqueue")
        self.mocks[3].assert_called_with(fault.LEASE_SECONDS)

    def test_proof_rejects_wrong_delay_loss_missing_queues_and_broad_selectors(self):
        self.fault.arm()
        rows, filters = copy.deepcopy(self.kernel.rows), copy.deepcopy(self.kernel.filters)
        mutations = [
            lambda: self.kernel.rows[1]["options"]["delay"].update(delay=0),
            lambda: self.kernel.rows[1].update(drops=1),
            lambda: self.kernel.rows[1]["options"].update(reorder={"probability": 0.1}),
            lambda: self.kernel.filters[0]["options"].update(actions=[{"kind": "mirred"}]),
            lambda: self.kernel.rows.pop(),
            lambda: self.kernel.filters[0]["options"]["keys"].update(dst_ip="192.0.2.0/24"),
            lambda: self.kernel.filters[0]["options"]["keys"].update(ip_proto="tcp"),
            lambda: self.kernel.filters.pop(),
            lambda: self.kernel.filters.append({"kind": "u32", "protocol": "ip", "options": {}}),
            lambda: self.kernel.filters[0]["options"]["keys"].update(dst_port_mask="0xff00"),
            lambda: self.kernel.filters[0]["options"]["keys"].update(src_ip="192.0.2.1"),
            lambda: self.kernel.rows[0]["options"].update(priomap=[1] * 16),
            lambda: self.kernel.rows[0].update(drops=1),
        ]
        for mutate in mutations:
            self.kernel.rows, self.kernel.filters = copy.deepcopy(rows), copy.deepcopy(filters)
            mutate()
            with self.assertRaises((ValueError, KeyError)):
                self.fault.inspect()

    def test_priority_headers_are_allowed_but_unknown_filters_are_not(self):
        self.fault.arm()
        self.kernel.filters.insert(0, {"kind": "flower", "protocol": "ip", "pref": 10})
        self.fault.inspect()
        self.kernel.filters.append({"kind": "flower", "protocol": "ip", "options": {}})
        with self.assertRaises(KeyError):
            self.fault.inspect()

    def test_unvalidated_token_or_duplicate_peers_never_reach_tc(self):
        for token, peers in [("123;false", PEERS), ("1", [PEERS[0]] * 2),
                             ("1", [*PEERS, PEERS[0]]), ("1", ["invalid", PEERS[1]])]:
            with self.assertRaises(ValueError):
                fault.Fault(token, peers)
        self.assertEqual(self.kernel.commands, [])

    def test_queue_length_is_restored_only_when_the_kernel_change_matches(self):
        self.kernel.txqlen = 2000
        self.fault.arm()
        self.fault.clear()
        self.assertEqual(self.kernel.txqlen, 2000)
        self.fault.arm()
        self.kernel.txqlen = 4000
        with self.assertRaisesRegex(ValueError, "queue length changed"):
            self.fault.clear()
        self.assertEqual(self.kernel.txqlen, 4000)
        self.assertTrue(self.owner.exists())


if __name__ == "__main__":
    unittest.main()
