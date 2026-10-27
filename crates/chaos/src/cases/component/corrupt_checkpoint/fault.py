"""Damage one stopped executor's local image, retaining exact fault evidence."""

import hashlib
import json
from pathlib import Path
import re
import shutil
import sys


class Fault:
    def __init__(self, root):
        self.root = root
        self.checkpoints = root / "checkpoints"
        self.proof = self.checkpoints / ".chaos-corrupt-proof.json"

    @staticmethod
    def digest(path):
        digest = hashlib.sha256()
        with path.open("rb") as image:
            for chunk in iter(lambda: image.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def images(self):
        return sorted(
            (p for p in self.checkpoints.iterdir()
             if re.fullmatch(r"checkpoint-[0-9]+", p.name)),
            key=lambda p: int(p.name.split("-")[1]),
        )

    def inspect(self):
        selected = self.images()[-1]
        image = selected / "mdbx.dat"
        return {
            "checkpoint": selected.name,
            "manifest_sha256": self.digest(selected / "MANIFEST"),
            "original_sha256": self.digest(image),
            "offset": image.stat().st_size // 2,
        }

    def arm(self):
        images = self.images()
        selected = images[-1]
        image = selected / "mdbx.dat"
        manifest = selected / "MANIFEST"
        evidence = self.inspect()
        if [evidence["original_sha256"], evidence["manifest_sha256"]] != sys.argv[3:]:
            raise ValueError("checkpoint differs from the verified baseline")
        with image.open("r+b") as data:
            data.seek(evidence["offset"])
            original = data.read(1)
            if len(original) != 1:
                raise ValueError("checkpoint image is empty")
            data.seek(evidence["offset"])
            data.write(bytes([original[0] ^ 0xFF]))
        evidence["corrupt_sha256"] = self.digest(image)
        if evidence["original_sha256"] == evidence["corrupt_sha256"]:
            raise ValueError("checkpoint image did not change")
        if self.digest(manifest) != evidence["manifest_sha256"]:
            raise ValueError("fault changed the checkpoint manifest")
        # With no other local base, success requires a verified peer image.
        for older in images[:-1]:
            shutil.rmtree(older)
        shutil.rmtree(self.root / "state")
        (self.root / "state").mkdir()
        self.proof.write_text(json.dumps(evidence))
        return evidence

    def verify(self):
        evidence = json.loads(self.proof.read_text())
        rejected = self.checkpoints / (".rejected-" + evidence["checkpoint"])
        if self.digest(rejected / "mdbx.dat") != evidence["corrupt_sha256"]:
            raise ValueError("quarantined image differs from the faulted image")
        if self.digest(rejected / "MANIFEST") != evidence["manifest_sha256"]:
            raise ValueError("quarantined manifest differs from the original")
        return evidence


fault = Fault(Path(sys.argv[1]))
operation = {"arm": fault.arm, "verify": fault.verify, "inspect": fault.inspect}[sys.argv[2]]
print(json.dumps(operation()))
