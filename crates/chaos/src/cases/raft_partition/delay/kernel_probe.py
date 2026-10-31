"""Exercise the exact delay controller on disposable veths in an isolated namespace."""

import json
import os
from pathlib import Path
import selectors
import socket
import subprocess
import sys
import tempfile
import time

import fault

PEERS = ["198.18.0.2", "198.18.0.3"]
OTHER = "198.18.0.4"


class Receiver:
    @staticmethod
    def serve():
        print("namespace-ready", flush=True)
        if sys.stdin.readline().strip() != "start":
            raise ValueError("receiver requires configured interfaces")
        selector = selectors.DefaultSelector()
        Receiver.bind(selector, socket.SOCK_DGRAM, 40201)
        Receiver.bind(selector, socket.SOCK_DGRAM, 40205)
        Receiver.bind(selector, socket.SOCK_STREAM, 40201)
        print("listening", flush=True)
        while True:
            Receiver.echo(selector)

    @staticmethod
    def bind(selector, kind, port):
        sock = socket.socket(socket.AF_INET, kind)
        sock.bind(("0.0.0.0", port))
        if kind == socket.SOCK_STREAM:
            sock.listen()
        selector.register(sock, selectors.EVENT_READ, kind)

    @staticmethod
    def echo(selector):
        for key, _ in selector.select(timeout=2):
            Receiver.reply(key)

    @staticmethod
    def reply(key):
        if key.data == socket.SOCK_DGRAM:
            data, address = key.fileobj.recvfrom(128)
            key.fileobj.sendto(data, address)
        else:
            conn, _ = key.fileobj.accept()
            with conn:
                conn.settimeout(2)
                with conn.makefile("rb") as stream:
                    conn.sendall(stream.read(len(b"delay-proof")))


class Probe:
    def __init__(self):
        self.receiver = None
        self.fault = fault.Fault(str(os.getpid()), PEERS)

    @staticmethod
    def command(*args):
        subprocess.run(args, check=True, timeout=5, stdout=subprocess.DEVNULL)

    def configure(self):
        self.command("ip", "link", "add", "kdm-send", "type", "veth", "peer", "name", "kdm-recv")
        self.receiver = subprocess.Popen(
            ["unshare", "--net", sys.executable, __file__, "receive"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        self.expect("namespace-ready")
        self.command("ip", "link", "set", "kdm-recv", "netns", str(self.receiver.pid))
        self.command("ip", "addr", "add", "198.18.0.1/24", "dev", "kdm-send")
        self.command("ip", "link", "set", "kdm-send", "up")
        self.command("ip", "link", "set", "lo", "up")
        for address in [*PEERS, OTHER]:
            self.inside("addr", "add", address + "/24", "dev", "kdm-recv")
        self.inside("link", "set", "kdm-recv", "up")
        self.inside("link", "set", "lo", "up")
        self.receiver.stdin.write("start\n")
        self.receiver.stdin.flush()
        self.expect("listening")

    def inside(self, *args):
        self.command("nsenter", "--net", "--target", str(self.receiver.pid), "ip", *args)

    def expect(self, message):
        with selectors.DefaultSelector() as ready:
            ready.register(self.receiver.stdout, selectors.EVENT_READ)
            if not ready.select(timeout=5):
                raise TimeoutError("receiver readiness timeout")
        actual = self.receiver.stdout.readline().strip()
        if actual != message:
            raise ValueError(f"receiver expected {message}, got {actual}")

    @staticmethod
    def sample(peer, port, kind):
        with socket.socket(socket.AF_INET, kind) as sock:
            sock.settimeout(2)
            started = time.monotonic()
            if kind == socket.SOCK_DGRAM:
                sock.sendto(b"delay-proof", (peer, port))
                echoed, _ = sock.recvfrom(128)
            else:
                sock.connect((peer, port))
                sock.sendall(b"delay-proof")
                with sock.makefile("rb") as stream:
                    echoed = stream.read(len(b"delay-proof"))
            elapsed = time.monotonic() - started
        if echoed != b"delay-proof":
            raise ValueError("packet probe did not receive its payload")
        return elapsed

    @staticmethod
    def measurements():
        flows = {
            "peer0_raft": (PEERS[0], 40201, socket.SOCK_DGRAM),
            "peer1_raft": (PEERS[1], 40201, socket.SOCK_DGRAM),
            "other_port": (PEERS[0], 40205, socket.SOCK_DGRAM),
            "other_peer": (OTHER, 40201, socket.SOCK_DGRAM),
            "tcp": (PEERS[0], 40201, socket.SOCK_STREAM),
        }
        return {name: [Probe.sample(*flow) for _ in range(5)] for name, flow in flows.items()}

    @staticmethod
    def require_timings(samples, delayed):
        for name, values in samples.items():
            Probe.require_flow(name, values, delayed and name in {"peer0_raft", "peer1_raft"})

    @staticmethod
    def require_flow(name, values, delayed):
        # Minimum RTT discards scheduler stalls; all delayed packets must wait at least 200 ms.
        shortest = min(values)
        if delayed and shortest < 0.2:
            raise ValueError(f"{name} bypasses the 250 ms queue: {values}")
        if not delayed and shortest >= 0.125:
            raise ValueError(f"{name} has no undelayed sample: {values}")

    def exercise(self):
        self.configure()
        baseline = self.measurements()
        self.require_timings(baseline, False)
        armed = self.control(self.fault.arm)
        delayed = self.measurements()
        self.require_timings(delayed, True)
        evidence = self.control(self.fault.inspect)
        if any(queue["packets"] < 5 or queue["drops"] != 0 for queue in evidence["queues"]):
            raise ValueError("both delay queues require delivered packets and zero drops")
        restored = self.control(self.fault.clear)
        after = self.measurements()
        self.require_timings(after, False)
        return {"baseline_seconds": baseline, "delayed_seconds": delayed,
                "restored_seconds": after, "armed": armed, "evidence": evidence,
                "cleanup": restored}

    @staticmethod
    def control(operation):
        fault.DEADLINE = time.monotonic() + 15
        return operation()

    def close(self):
        try:
            if fault.OWNER.exists():
                self.control(self.fault.clear)
        finally:
            if self.receiver is not None:
                self.receiver.kill()
                self.receiver.wait(timeout=5)

    def run(self):
        if os.readlink("/proc/self/ns/net") == os.readlink("/proc/1/ns/net"):
            raise ValueError("run the probe under unshare --net; never use the host network")
        with tempfile.TemporaryDirectory(prefix="kardamom-delay-probe-") as directory:
            fault.OWNER = Path(directory) / "owner.json"
            # The outer 60-second process timeout destroys this namespace. Lease logic has unit controls.
            self.fault.lease = lambda: None
            try:
                return self.exercise()
            finally:
                self.close()


if __name__ == "__main__":
    if sys.argv[1:] == ["receive"]:
        Receiver.serve()
    else:
        print(json.dumps(Probe().run(), indent=2))
