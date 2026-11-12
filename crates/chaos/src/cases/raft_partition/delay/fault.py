"""Delay only peer-bound Raft UDP; preserve an unmodified interface on cleanup."""

import ipaddress
import json
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = "7c00:"
HANDLES = ("7c10:", "7c20:")
PORTS = (40201, 40202, 40203, 40204)
LEASE_SECONDS = 300
OWNER = Path("/tmp/kardamom-raft-delay-owner.json")
DEADLINE = time.monotonic() + 15


def run(*args):
    remaining = DEADLINE - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("delay controller exceeds fifteen seconds")
    return subprocess.check_output(args, text=True, timeout=remaining)


class Fault:
    def __init__(self, token, peers):
        if not re.fullmatch(r"[0-9]+", token):
            raise ValueError("invalid fault token")
        self.token = token
        self.peers = [str(ipaddress.IPv4Address(peer)) for peer in peers]
        if len(self.peers) != 2 or len(set(self.peers)) != 2:
            raise ValueError("fault requires two distinct IPv4 peers")

    def device(self):
        routes = [json.loads(run("ip", "-j", "route", "get", peer))[0]
                  for peer in self.peers]
        devices = {route["dev"] for route in routes}
        if len(devices) != 1:
            raise ValueError("both peers must use the same interface")
        dev = devices.pop()
        if not re.fullmatch(r"[a-zA-Z0-9_.-]+", dev):
            raise ValueError("invalid interface name")
        return dev

    @staticmethod
    def link(dev):
        return json.loads(run("ip", "-j", "link", "show", "dev", dev))[0]

    @staticmethod
    def qdiscs(dev):
        return json.loads(run("tc", "-s", "-j", "qdisc", "show", "dev", dev))

    @staticmethod
    def root(rows):
        roots = [row for row in rows if row.get("root")]
        if len(roots) != 1:
            raise ValueError("interface requires exactly one root qdisc")
        return roots[0]

    @staticmethod
    def clean_root(root):
        if (root["kind"], root["handle"]) != ("noqueue", "0:"):
            raise ValueError("fault refuses to replace an existing queue discipline")

    def owned(self):
        state = json.loads(OWNER.read_text())
        if state["token"] != self.token:
            raise ValueError("another lease owns the interface")
        if self.link(state["device"])["ifindex"] != state["ifindex"]:
            raise ValueError("interface identity changed")
        return state

    def arm(self):
        dev = self.device()
        self.clean_root(self.root(self.qdiscs(dev)))
        link = self.link(dev)
        state = {"token": self.token, "device": dev, "ifindex": link["ifindex"],
                 "txqlen": link.get("txqlen", 0), "peers": self.peers}
        with OWNER.open("x") as output:
            json.dump(state, output)
        self.lease()
        run("tc", "qdisc", "add", "dev", dev, "root", "handle", ROOT,
            "prio", "bands", "3", "priomap", *(["0"] * 16))
        for index, peer in enumerate(self.peers):
            self.arm_peer(dev, index, peer)
        return self.inspect()

    @staticmethod
    def arm_peer(dev, index, peer):
        classid = ROOT + str(index + 2)
        run("tc", "qdisc", "add", "dev", dev, "parent", classid,
            "handle", HANDLES[index], "netem", "delay", "250ms", "limit", "10000")
        for port in PORTS:
            run("tc", "filter", "add", "dev", dev, "parent", ROOT,
                "protocol", "ip", "pref", str(10 + index * len(PORTS) + PORTS.index(port)),
                "flower", "skip_hw", "ip_proto", "udp", "dst_ip", peer,
                "dst_port", str(port), "classid", classid)

    def lease(self):
        subprocess.Popen([sys.executable, __file__, self.token, "expire", *self.peers],
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, start_new_session=True)

    def inspect(self):
        state = self.owned()
        rows = self.qdiscs(state["device"])
        root = self.root(rows)
        if (root["kind"], root["handle"]) != ("prio", ROOT):
            raise ValueError("fault root is missing or replaced")
        if root["options"]["bands"] != 3 or root["options"]["priomap"] != [0] * 16:
            raise ValueError("unmatched traffic must use the undelayed band")
        if root["drops"] != 0:
            raise ValueError("the delay root drops packets")
        queues = [self.queue(rows, index, peer) for index, peer in enumerate(self.peers)]
        filters = json.loads(run("tc", "-j", "filter", "show", "dev", state["device"],
                                 "parent", ROOT))
        self.check_filters(filters)
        return dict(state, queues=queues, qdiscs=rows, filters=filters)

    @staticmethod
    def queue(rows, index, peer):
        matches = [row for row in rows if row["handle"] == HANDLES[index]]
        if len(matches) != 1:
            raise ValueError("peer delay queue is missing")
        row = matches[0]
        options = row["options"]
        if row["kind"] != "netem" or row["parent"] != ROOT + str(index + 2):
            raise ValueError("peer delay queue has the wrong parent or kind")
        delay = options["delay"]
        if delay["delay"] != 0.25 or delay["jitter"] != 0 or options["limit"] != 10000:
            raise ValueError("peer delay configuration changed")
        if set(options) - {"delay", "limit", "ecn", "gap"} or options.get("ecn") or options.get("gap"):
            raise ValueError("peer queue has an additional impairment")
        if row["drops"] != 0:
            raise ValueError("the delay fault drops packets")
        return {"peer": peer, "packets": row["packets"], "drops": row["drops"]}

    def check_filters(self, filters):
        # tc emits a priority header and a concrete instance; only instances have keys.
        entries = [row for row in filters if "options" in row]
        if any(row.get("kind") != "flower" or row.get("protocol") != "ip" for row in filters):
            raise ValueError("unrecognized delay classifier")
        expected = {(peer, port, ROOT + str(index + 2))
                    for index, peer in enumerate(self.peers) for port in PORTS}
        observed = {self.selector(row) for row in entries}
        if len(entries) != len(expected) or observed != expected:
            raise ValueError("delay classifiers do not match exactly the peer Raft UDP flows")

    @staticmethod
    def selector(row):
        keys = row["options"]["keys"]
        if row["options"].get("actions"):
            raise ValueError("delay classifier has additional actions")
        if set(keys) - {"eth_type", "ip_proto", "dst_ip", "dst_port"}:
            raise ValueError("delay classifier has extra selectors")
        if row["kind"] != "flower" or row["protocol"] != "ip" or keys["ip_proto"] != "udp":
            raise ValueError("delay classifier is not IPv4 UDP")
        # An exact host is required, never an address range.
        peer = ipaddress.IPv4Interface(keys["dst_ip"])
        if peer.network.prefixlen != 32:
            raise ValueError("delay classifier covers more than one peer")
        return str(peer.ip), keys["dst_port"], row["options"]["classid"]

    def clear(self):
        if not OWNER.exists():
            self.clean_root(self.root(self.qdiscs(self.device())))
            return {"restored": True}
        state = self.owned()
        root = self.root(self.qdiscs(state["device"]))
        if (root["kind"], root["handle"]) == ("prio", ROOT):
            run("tc", "qdisc", "del", "dev", state["device"], "root", "handle", ROOT)
        self.clean_root(self.root(self.qdiscs(state["device"])))
        self.restore_length(state)
        OWNER.unlink()
        return {"restored": True, "device": state["device"], "ifindex": state["ifindex"]}

    def restore_length(self, state):
        dev, original = state["device"], state["txqlen"]
        current = self.link(dev).get("txqlen", 0)
        # Linux initializes a zero-length noqueue device to 1000 when a qdisc is attached.
        if original == 0 and current == 1000:
            run("ip", "link", "set", "dev", dev, "txqueuelen", "0")
        if self.link(dev).get("txqlen", 0) != original:
            raise ValueError("interface queue length changed outside the fault")

    def expire(self):
        global DEADLINE
        time.sleep(LEASE_SECONDS)
        DEADLINE = time.monotonic() + 15
        if OWNER.exists() and json.loads(OWNER.read_text())["token"] == self.token:
            self.clear()
        return {"lease_expired": self.token}


if __name__ == "__main__":
    fault = Fault(sys.argv[1], sys.argv[3:])
    operations = {"arm": fault.arm, "inspect": fault.inspect,
                  "clear": fault.clear, "expire": fault.expire}
    print(json.dumps(operations[sys.argv[2]]()))
