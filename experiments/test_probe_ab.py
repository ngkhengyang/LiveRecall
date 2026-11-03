"""Socket-free checks for the ablation and deletion boundary."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments import probe_ab


class PatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "client.py"

    def test_current_client_changes_only_probe_start(self):
        source = (probe_ab.REPO / "submission_root/app/client.py").read_text(
            encoding="utf-8"
        )
        self.path.write_text(source, encoding="utf-8")
        probe_ab.patch_client(self.path)
        result = self.path.read_text(encoding="utf-8")
        self.assertEqual(result, source.replace(probe_ab.ANCHOR, probe_ab.REPLACEMENT))
        self.assertNotIn("asyncio.create_task(self._probe_loop())", result)
        compile(result, str(self.path), "exec")

    def test_missing_duplicate_and_changed_anchor_fail_without_writing(self):
        for source in (
            "# missing anchor\n",
            probe_ab.ANCHOR * 2,
            probe_ab.ANCHOR.replace("self._probe_loop()", "self._new_probe_loop()"),
        ):
            with self.subTest(source=source):
                self.path.write_text(source, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "exactly once"):
                    probe_ab.patch_client(self.path)
                self.assertEqual(self.path.read_text(encoding="utf-8"), source)

    def test_second_patch_fails(self):
        self.path.write_text(probe_ab.ANCHOR, encoding="utf-8")
        probe_ab.patch_client(self.path)
        with self.assertRaisesRegex(ValueError, "found 0"):
            probe_ab.patch_client(self.path)


class DeletionTests(unittest.TestCase):
    def test_resolved_boundaries_and_specific_variant(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            out = repo / "output/probe_ab"
            variant = out / "with_probe"
            variant.mkdir(parents=True)
            with patch.object(probe_ab, "REPO", repo):
                for invalid in (repo, repo / "output", repo / "elsewhere"):
                    with self.assertRaises(ValueError):
                        probe_ab.validate_out(invalid)
                for invalid in (out, repo, out / "unrelated"):
                    with self.assertRaises(ValueError):
                        probe_ab.remove_variant(invalid, out)
                escaped = repo / "escaped"
                escaped.mkdir()
                (out / "no_probe").symlink_to(escaped, target_is_directory=True)
                with self.assertRaises(ValueError):
                    probe_ab.remove_variant(out / "no_probe", out)
                self.assertTrue(escaped.is_dir())
                probe_ab.remove_variant(variant, out)
                self.assertFalse(variant.exists())


if __name__ == "__main__":
    unittest.main()
