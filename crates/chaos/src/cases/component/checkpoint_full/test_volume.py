"""Controls execute the shipped fault code without mounting or filling host storage."""

import errno
import io
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

scope = {"__name__": "fault_control"}
exec(Path(sys.argv.pop(1)).read_text(), scope)
Volume = scope["Volume"]
CAPACITY = scope["CAPACITY"]


class Memory(io.BytesIO):
    def fileno(self):
        return 0


class Full(Memory):
    def write(self, data):
        if self.tell() >= 8192:
            raise OSError(errno.ENOSPC, "full")
        return super().write(data)


class FaultControls(unittest.TestCase):
    def test_actual_enospc_is_required(self):
        self.assertEqual(Volume.fill(Full()), errno.ENOSPC)
        with patch.dict(scope, CAPACITY=8192):
            with self.assertRaisesRegex(ValueError, "did not reach ENOSPC"):
                sink = Memory()
                with patch("os.fsync"):
                    Volume.fill(sink)

    def test_io_error_and_short_write_do_not_impersonate_disk_full(self):
        with patch.object(Full, "write", side_effect=OSError(errno.EIO, "io")):
            with self.assertRaises(OSError) as result:
                Volume.fill(Full())
            self.assertEqual(result.exception.errno, errno.EIO)
        with patch.object(Full, "write", return_value=1):
            with self.assertRaisesRegex(ValueError, "short"):
                Volume.fill(Full())

    def test_only_the_exact_bounded_mount_can_be_filled(self):
        volume = Volume("123")
        good = f"73 42 0:22 / {volume.root} rw - tmpfs {volume.source} rw\n"
        stat = SimpleNamespace(f_blocks=CAPACITY // 4096, f_frsize=4096, f_bavail=0)
        with patch("os.statvfs", return_value=stat), patch.object(Path, "read_text", return_value=good):
            self.assertEqual(volume.checked()["mount_id"], "73")
        for changed in ["", good + good, good.replace("tmpfs", "ext4"),
                        good.replace(volume.source, "another-source")]:
            with self.subTest(mount=changed), patch("os.statvfs", return_value=stat), \
                 patch.object(Path, "read_text", return_value=changed):
                with self.assertRaises(ValueError):
                    volume.checked()
        stat.f_blocks += 1
        with patch("os.statvfs", return_value=stat), patch.object(Path, "read_text", return_value=good):
            with self.assertRaisesRegex(ValueError, "bounded"):
                volume.checked()

    def test_arm_requires_a_baseline_and_ages_it_after_exhaustion(self):
        with tempfile.TemporaryDirectory() as directory:
            volume = Volume("123")
            volume.root = Path(directory)
            volume.filler = volume.root / ".chaos-fill"
            image = volume.root / "checkpoint-10"
            image.mkdir()
            before = image.stat().st_mtime
            events = []
            def fill(_):
                events.append("full")
                self.assertEqual(image.stat().st_mtime, before)
                return errno.ENOSPC
            with patch.object(Volume, "inspect", return_value={"checkpoint": None}):
                with self.assertRaisesRegex(ValueError, "baseline"):
                    volume.arm()
            self.assertFalse(volume.filler.exists())
            with patch.object(Volume, "inspect", return_value={"checkpoint": image.name}), \
                 patch.object(Volume, "lease"), patch.object(Volume, "checked", return_value={}), \
                 patch.object(Volume, "fill", side_effect=fill):
                self.assertEqual(volume.arm()["errno"], errno.ENOSPC)
            self.assertEqual(events, ["full"])
            self.assertLess(image.stat().st_mtime, before - 21600)
            with patch.object(Volume, "checked", return_value={"available": CAPACITY}), \
                 patch.object(Volume, "mount_rows", return_value=[["mounted"]]):
                volume.clear()
            self.assertFalse(volume.filler.exists())
            self.assertTrue(image.exists())

    def test_foreign_tokens_are_refused(self):
        for token in ["../state", "123;rm", "", "-1"]:
            with self.subTest(token=token), self.assertRaises(ValueError):
                Volume(token)

    def test_lease_cannot_remove_another_inode(self):
        volume = Volume("123")
        stat = SimpleNamespace(st_dev=11, st_ino=22)
        with patch.object(Path, "stat", return_value=stat), patch("subprocess.Popen") as spawn:
            volume.lease()
        args = spawn.call_args.args[0]
        self.assertTrue(spawn.call_args.kwargs["start_new_session"])
        for inode in [22, 23]:
            with patch("time.sleep") as sleep, patch("sys.argv", ["worker", *args[3:]]), \
                 patch("os.stat", return_value=SimpleNamespace(st_dev=11, st_ino=inode)), \
                 patch("os.unlink") as unlink:
                exec(args[2], {"__name__": "lease_control"})
                sleep.assert_called_once_with(600)
                self.assertEqual(unlink.call_count, int(inode == 22))

    def test_cleanup_refuses_a_live_bind_and_an_unmounted_filler(self):
        with tempfile.TemporaryDirectory() as directory:
            volume = Volume("123")
            volume.root = Path(directory)
            volume.filler = volume.root / ".chaos-fill"
            volume.filler.write_bytes(b"keep")
            with patch.object(Volume, "mount_rows", return_value=[]):
                with self.assertRaisesRegex(ValueError, "unmounted"):
                    volume.clear()
            with patch.object(Volume, "mount_rows", return_value=[["mounted"]]), \
                 patch.object(Volume, "unused", return_value={"unused": False}), \
                 patch("subprocess.run") as unmount:
                with self.assertRaisesRegex(ValueError, "still binds"):
                    volume.remove()
                unmount.assert_not_called()
            self.assertEqual(volume.filler.read_bytes(), b"keep")


unittest.main()
