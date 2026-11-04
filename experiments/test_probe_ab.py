"""Socket-free checks for the ablation and deletion boundary."""

from pathlib import Path
import tempfile
from types import SimpleNamespace
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


class NetworkTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.out = Path(self.temp.name)
        self.args = SimpleNamespace(require_t5=True)
        self.noqueue = [{"kind": "noqueue"}]
        self.netem = [{"kind": "netem"}]
        self.addCleanup(patch.stopall)
        patch.object(probe_ab.sys, "platform", "linux").start()
        patch.object(probe_ab.shutil, "which", return_value="/usr/sbin/tc").start()
        patch.object(probe_ab.sys.stdin, "isatty", return_value=False).start()

    def test_required_t5_fails_when_sudo_is_unavailable(self):
        with patch.object(probe_ab, "network_command", return_value=False):
            with self.assertRaisesRegex(RuntimeError, "sudo authentication failed"):
                probe_ab.run_t5(self.out, self.args, 0, 2)

    def test_failed_profile_still_resets_and_does_not_become_a_pass(self):
        with patch.object(
            probe_ab, "network_command", side_effect=[True, False, True]
        ) as network:
            with patch.object(probe_ab, "capture_qdisc", return_value=self.noqueue):
                with self.assertRaisesRegex(RuntimeError, "could not be applied"):
                    probe_ab.run_t5(self.out, self.args, 0, 2)
        self.assertIn("reset.sh", network.call_args.args[0][1])

    def test_runner_failure_still_restores_network(self):
        with patch.object(probe_ab, "network_command", return_value=True):
            with patch.object(
                probe_ab,
                "capture_qdisc",
                side_effect=[self.noqueue, self.netem, self.netem, self.noqueue],
            ):
                with patch.object(probe_ab, "run_arms", side_effect=KeyboardInterrupt):
                    with self.assertRaises(KeyboardInterrupt):
                        probe_ab.run_t5(self.out, self.args, 0, 2)
        self.assertIn(
            '"reset_ok": true', (self.out / "t5_network_status.json").read_text()
        )

    def test_leftover_netem_makes_successful_trials_fail(self):
        with patch.object(probe_ab, "network_command", return_value=True):
            with patch.object(
                probe_ab,
                "capture_qdisc",
                side_effect=[self.noqueue, self.netem, self.netem, self.netem],
            ):
                with patch.object(
                    probe_ab, "run_arms", return_value={"with_probe": 0, "no_probe": 0}
                ):
                    with self.assertRaisesRegex(RuntimeError, "network reset failed"):
                        probe_ab.run_t5(self.out, self.args, 0, 2)

    def test_existing_impairment_is_preserved(self):
        with patch.object(probe_ab, "network_command", return_value=True) as network:
            with patch.object(probe_ab, "capture_qdisc", return_value=self.netem):
                with self.assertRaisesRegex(RuntimeError, "already has a qdisc"):
                    probe_ab.run_t5(self.out, self.args, 0, 2)
        self.assertEqual(network.call_args_list[0].args[0], ["true"])
        self.assertEqual(network.call_count, 1)


if __name__ == "__main__":
    unittest.main()
