"""Exercise the shipped controller on the disposable container's bounded tmpfs."""

import hashlib
import json
import os
from pathlib import Path
import time

from volume import CAPACITY, Volume


class Probe:
    def __init__(self):
        if not Path("/.dockerenv").exists() or os.environ.get("KARDAMOM_VOLUME_PROBE") != "1":
            raise ValueError("probe requires its owned Docker container")
        self.volume = Volume("123")
        self.volume.root = Path("/dev/shm")
        self.volume.source = "shm"
        self.volume.filler = self.volume.root / ".chaos-fill"

    def image(self, block, mebibytes, byte):
        directory = self.volume.root / f"checkpoint-{block}"
        directory.mkdir()
        page = bytes([byte]) * (1024 * 1024)
        with (directory / "mdbx.dat").open("xb") as image:
            for _ in range(mebibytes):
                image.write(page)
            image.flush()
            os.fsync(image.fileno())
        return directory

    @staticmethod
    def digest(directory):
        with (directory / "mdbx.dat").open("rb") as image:
            return hashlib.file_digest(image, "sha256").hexdigest()

    def run(self):
        before = self.volume.checked()
        assert before["capacity"] == CAPACITY
        baseline = self.image(10, 80, 0x5A)
        expected = self.digest(baseline)
        image = self.volume.inspect()
        assert image["image_bytes"] == 80 * 1024 * 1024
        armed = self.volume.arm()
        assert armed["errno"] == 28 and armed["available"] == 0
        assert armed["baseline"] == baseline.name
        assert baseline.stat().st_mtime < time.time() - 21600
        assert self.digest(baseline) == expected
        held = self.volume.inspect()
        assert held["checkpoint"] == baseline.name and held["available"] == 0
        cleared = self.volume.clear()
        assert cleared["available"] >= 160 * 1024 * 1024
        replacement = self.image(20, 160, 0xA5)
        recovered = self.volume.inspect()
        assert recovered["checkpoint"] == replacement.name
        assert recovered["image_bytes"] == 160 * 1024 * 1024
        assert self.digest(baseline) == expected
        return {"baseline": image, "armed": armed, "cleared": cleared,
                "recovered": recovered, "baseline_preserved": True}


print(json.dumps(Probe().run()))
