"""Exhaust only a dedicated, size-checked tmpfs inside the victim node."""

import errno
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

CAPACITY = 512 * 1024 * 1024
IMAGE_LIMIT = 64 * 1024 * 1024


class Volume:
    def __init__(self, token):
        if not re.fullmatch(r"[0-9]+", token):
            raise ValueError("invalid volume token")
        self.root = Path("/opt/kardamom/chaos-checkpoint-enospc-" + token)
        self.source = "kardamom-chaos-enospc-" + token
        self.filler = self.root / ".chaos-fill"

    def mount_rows(self):
        rows = [line.split() for line in Path("/proc/self/mountinfo").read_text().splitlines()]
        return [row for row in rows if row[4] == str(self.root)]

    def mount_row(self):
        matches = self.mount_rows()
        if len(matches) != 1:
            raise ValueError("fault requires exactly one dedicated mount")
        row = matches[0]
        tail = row[row.index("-") + 1:]
        if tail[:2] != ["tmpfs", self.source]:
            raise ValueError("fault refuses another filesystem or source")
        return row

    def checked(self):
        row = self.mount_row()
        stat = os.statvfs(self.root)
        if stat.f_blocks * stat.f_frsize != CAPACITY:
            raise ValueError("fault requires the bounded 512 MiB filesystem")
        return {"mount_id": row[0], "capacity": CAPACITY,
                "available": stat.f_bavail * stat.f_frsize}

    def prepare(self):
        self.root.mkdir()
        subprocess.run(["mount", "-t", "tmpfs", "-o", "size=512m,mode=0777",
                        self.source, str(self.root)], check=True)
        return self.checked()

    def inspect(self):
        report = self.checked()
        images = sorted((p for p in self.root.iterdir()
                         if re.fullmatch(r"checkpoint-[0-9]+", p.name)),
                        key=lambda p: int(p.name.split("-")[1]))
        if not images:
            return dict(report, checkpoint=None)
        latest = images[-1]
        size = (latest / "mdbx.dat").stat().st_size
        if size > IMAGE_LIMIT:
            raise ValueError("checkpoint exceeds the fault volume's 64 MiB image budget")
        return dict(report, checkpoint=latest.name, image_bytes=size)

    def lease(self):
        # The detached worker frees only this inode on this unique mount.
        # It never unmounts files that an executor may still use.
        stat = self.filler.stat()
        code = """import os, sys, time
time.sleep(600)
try:
    stat = os.stat(sys.argv[1])
    if (stat.st_dev, stat.st_ino) == (int(sys.argv[2]), int(sys.argv[3])):
        os.unlink(sys.argv[1])
except FileNotFoundError:
    pass
"""
        subprocess.Popen([sys.executable, "-c", code, str(self.filler),
                          str(stat.st_dev), str(stat.st_ino)], start_new_session=True,
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL)

    def arm(self):
        before = self.inspect()
        if before["checkpoint"] is None:
            raise ValueError("fault requires a completed baseline checkpoint")
        with self.filler.open("xb", buffering=0) as filler:
            self.lease()
            exhausted = self.fill(filler)
        # Age the newest completed base after the volume is full, so a base
        # cannot finish in the fill window and reset the six-hour age policy.
        baseline = self.inspect()["checkpoint"]
        old = time.time() - 86400
        os.utime(self.root / baseline, (old, old))
        return dict(self.checked(), errno=exhausted, baseline=baseline)

    @staticmethod
    def fill(filler):
        # The finite loop refuses an unexpectedly larger filesystem.
        page = bytes(4096)
        try:
            for _ in range(CAPACITY // len(page) + 1):
                if filler.write(page) != len(page):
                    raise ValueError("unexpected short filler write")
            os.fsync(filler.fileno())
        except OSError as error:
            if error.errno == errno.ENOSPC:
                return error.errno
            raise
        raise ValueError("filler did not reach ENOSPC inside its byte bound")

    def clear(self):
        if not self.mount_rows():
            if self.filler.exists():
                raise ValueError("refuse an unmounted filler")
            return {"available": None}
        self.checked()
        self.filler.unlink(missing_ok=True)
        return self.checked()

    def unused(self):
        ids = subprocess.check_output(["docker", "ps", "-q"], text=True, timeout=5).split()
        tasks = json.loads(subprocess.check_output(["docker", "inspect", *ids],
                           text=True, timeout=5)) if ids else []
        bound = [task["Id"] for task in tasks
                 if any(mount["Source"] == str(self.root) for mount in task["Mounts"])]
        return {"unused": not bound, "bound_containers": bound}

    def remove(self):
        if not self.root.exists():
            return {"removed": True}
        if not self.mount_rows():
            self.root.rmdir()
            return {"removed": True}
        if not self.unused()["unused"]:
            raise ValueError("an executor still binds the fault volume")
        self.clear()
        # A task bind must be gone before this operation. Never use lazy unmount.
        subprocess.run(["umount", str(self.root)], check=True)
        self.root.rmdir()
        return {"removed": True}


if __name__ == "__main__":
    volume = Volume(sys.argv[1])
    operation = {"prepare": volume.prepare, "inspect": volume.inspect,
                 "arm": volume.arm, "clear": volume.clear, "remove": volume.remove,
                 "unused": volume.unused}
    print(json.dumps(operation[sys.argv[2]]()))
