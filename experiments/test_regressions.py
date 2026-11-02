"""Regression checks for experiment switches, delivery bounds, and evidence reuse.

Run from the repository root: python -m unittest discover -s experiments -p 'test_regressions.py'
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "submission_root"))

from app import client as client_module
from app.client import Client
from app.outbound_buffer import OutboundBuffer
from app.protocol import CaptionUpdate, ClientHello
import run_matrix
from scenarios import DEFAULTS


def update(segment: int, final: bool = False) -> CaptionUpdate:
    return CaptionUpdate("L1", segment, 1, 0.0, final, "regression")


class AblationTests(unittest.IsolatedAsyncioTestCase):
    def test_environment_switches(self) -> None:
        for value, expected in (
            ("", []),
            ("probe", ["probe"]),
            ("grace", ["grace"]),
            ("resync", ["resync"]),
            (" probe, grace ", ["grace", "probe"]),
            ("all", ["all", "grace", "probe", "resync"]),
        ):
            with self.subTest(value=value):
                env = dict(os.environ, LIVERECALL_ABLATE=value)
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        "import sys,json; sys.path.insert(0,'submission_root'); "
                        "from app.server import Server; from app.client import ABLATIONS; "
                        "print(json.dumps(sorted(ABLATIONS)))",
                    ],
                    cwd=ROOT,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=True,
                )
                self.assertEqual(json.loads(result.stdout), expected)

    async def test_grace_switch_changes_pending_provisional_delivery(self) -> None:
        for ablations, held in ((set(), True), ({"grace"}, False)):
            with patch.object(client_module, "ABLATIONS", ablations):
                client = Client(Mock(), ClientHello(), Mock())
                client._catchup_until = asyncio.get_running_loop().time() + 10
                client.enqueue(update(1))
                self.assertEqual(client._next_delivery() is None, held)

    async def test_probe_switch_controls_probe_task(self) -> None:
        class Socket:
            send = AsyncMock()

            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

        for ablations, calls in ((set(), 1), ({"probe"}, 0)):
            with patch.object(client_module, "ABLATIONS", ablations):
                server = Mock()
                server.compute_history.return_value = []
                client = Client(Socket(), ClientHello(), server)
                client._sender_loop = AsyncMock()
                client._probe_loop = AsyncMock()
                with contextlib.redirect_stdout(io.StringIO()):
                    await client.run()
                self.assertEqual(client._probe_loop.call_count, calls)

    async def test_resync_switch_keeps_stalled_connection_open(self) -> None:
        for ablations, closes in ((set(), 1), ({"resync"}, 0)):
            with patch.object(client_module, "ABLATIONS", ablations):
                ws = Mock()
                ws.close = AsyncMock()
                ws.ping = AsyncMock(
                    return_value=asyncio.get_running_loop().create_future()
                )
                client = Client(ws, ClientHello(), Mock())
                client._behind_threshold = lambda: 0.01
                client._resync_threshold = lambda: 0.02
                with contextlib.redirect_stdout(io.StringIO()):
                    task = asyncio.create_task(client._probe_loop())
                    await asyncio.sleep(0.06)
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                self.assertEqual(ws.close.await_count, closes)


class BufferTests(unittest.TestCase):
    def test_soft_limit_preserves_active_and_final_state(self) -> None:
        buffer = OutboundBuffer()
        for segment in range(1, 301):
            buffer.offer(update(segment, final=segment % 2 == 0))
        self.assertTrue(buffer.pressured)
        self.assertFalse(buffer.should_close)
        self.assertEqual(buffer.pending, 300)
        finals = buffer.take_final_batch()
        self.assertEqual(len(finals), 64)
        self.assertTrue(all(item.final for item in finals))
        self.assertEqual(buffer.pending, 236)

    def test_hard_limit_refuses_new_state_without_evicting_pending_state(self) -> None:
        buffer = OutboundBuffer()
        buffer.MAX_PENDING_ACTIVE_SEGMENTS = 2
        buffer.offer(update(1))
        buffer.offer(update(2))
        buffer.offer(update(3))
        self.assertTrue(buffer.should_close)
        self.assertEqual(buffer.rejected, 1)
        self.assertEqual([buffer.next_update().segment for _ in range(2)], [1, 2])


class EvidenceTests(unittest.TestCase):
    def test_missing_log_counters_remain_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.log"
            path.write_text(
                "client disconnected: coalesced=2; rejected=0; batches=3; batched_finals=4\n"
            )
            result = run_matrix.log_metrics(path, False)
            self.assertEqual(result["coalesced"], 2)
            self.assertEqual(result["rejected"], 0)
            self.assertEqual(result["batches"], 3)
            self.assertIsNone(result["dropped"])
            self.assertIsNone(result["held"])
            self.assertIsNone(run_matrix.log_metrics(path, True)["coalesced"])

    def test_before_after_does_not_reuse_different_workload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / "T3").mkdir()
            (out / "T3" / "final_median.json").write_text('{"p95_ms": 123}')
            (out / "T3" / "final_configuration.json").write_text(
                '{"duration": 20, "quick": true}'
            )
            scenario = dict(DEFAULTS, id="BA_T3", suite="before_after", reuse="T3")
            reference = dict(DEFAULTS, id="T3", suite="public")
            with patch.object(
                run_matrix, "SCENARIOS", [reference, scenario]
            ), patch.object(
                sys,
                "argv",
                [
                    "run_matrix.py",
                    "--suite",
                    "before_after",
                    "--runs",
                    "1",
                    "--out",
                    directory,
                ],
            ), patch.object(
                run_matrix, "one_run", return_value={"p95_ms": 7}
            ) as run, contextlib.redirect_stdout(
                io.StringIO()
            ):
                self.assertEqual(run_matrix.main(), 0)
            self.assertEqual(run.call_count, 1)
            self.assertEqual(
                json.loads((out / "BA_T3" / "final_median.json").read_text())["p95_ms"],
                7,
            )


if __name__ == "__main__":
    unittest.main()
