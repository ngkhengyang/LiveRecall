"""Exercise wire invariants against a real server so routing is covered too."""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

import websockets
from websockets.exceptions import ConnectionClosed

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "test"))
from run_demo import terminate_process, wait_for_port


async def receive(ws: Any) -> dict:
    return json.loads(await asyncio.wait_for(ws.recv(), 0.8))


async def hello(ws: Any, segment: int = 0, revision: int = 0) -> dict:
    await ws.send(
        json.dumps(
            dict(
                type="CLIENT_HELLO",
                last_segment=segment,
                last_revision=revision,
                session="L1",
            )
        )
    )
    history = await receive(ws)
    assert history["type"] == "HISTORY", history
    return history


def update(segment: int, revision: int, final: bool = False) -> dict:
    return dict(
        type="CAPTION_UPDATE",
        session="L1",
        segment=segment,
        revision=revision,
        final=final,
        source_ts=time.time(),
        text="edge",
    )


async def checks_for(uri: str, checks: dict[str, bool]) -> None:
    async def check(name: str, action: Callable[[], Awaitable[None]]) -> None:
        try:
            await action()
        except Exception as exc:
            checks[name] = False
            print(f"FAIL {name}: {type(exc).__name__}: {exc}", flush=True)
        else:
            checks[name] = True
            print(f"PASS {name}", flush=True)

    async with websockets.connect(uri, close_timeout=0.2) as source:
        async with websockets.connect(uri, close_timeout=0.2) as client:
            await hello(client)
            sentinel_segment = 100

            async def send(payload: dict) -> None:
                await source.send(json.dumps(payload))

            async def delivered(payload: dict) -> None:
                await send(payload)
                frame = await receive(client)
                assert frame["type"] == "CAPTION_UPDATE", frame
                for key in ("segment", "revision", "final", "text"):
                    assert frame[key] == payload[key], frame

            async def silent(payloads: list[dict]) -> None:
                nonlocal sentinel_segment
                for payload in payloads:
                    await send(payload)
                # A fresh sentinel on the same source fences all preceding updates.
                sentinel_segment += 1
                sentinel = update(sentinel_segment, 1)
                await delivered(sentinel)
                try:
                    frame = await asyncio.wait_for(client.recv(), 0.15)
                except asyncio.TimeoutError:
                    return
                raise AssertionError(f"unexpected delivery: {frame}")

            async def extras() -> None:
                await send(
                    dict(
                        type="SOURCE_HELLO",
                        session="L1",
                        phase="warmup",
                        foo={"nested": [1]},
                    )
                )
                await delivered(dict(update(1, 1), phase="warmup", foo={"a": 1}))

            async def done() -> None:
                await send(dict(type="SOURCE_DONE", session="L1", segments=1))
                await delivered(update(2, 1))
                async with websockets.connect(uri, close_timeout=0.2) as fresh:
                    await hello(fresh)

            async def bad_hellos() -> None:
                failures = []
                # A CLIENT_HELLO with bad field types is not listed: the server
                # deliberately defaults them (see ClientHello.from_dict).
                for frame in (
                    "not json",
                    json.dumps(dict(type="UNKNOWN")),
                ):
                    async with websockets.connect(uri, close_timeout=0.2) as bad:
                        await bad.send(frame)
                        try:
                            response = await asyncio.wait_for(bad.recv(), 0.8)
                        except ConnectionClosed:
                            pass
                        except asyncio.TimeoutError:
                            failures.append(f"connection stayed open: {frame}")
                        else:
                            failures.append(f"accepted {frame}: {response}")
                    async with websockets.connect(uri, close_timeout=0.2) as fresh:
                        await hello(fresh)
                assert not failures, "; ".join(failures)

            async def duplicate() -> None:
                payload = update(3, 1)
                await delivered(payload)
                await silent([payload])

            async def stale_final() -> None:
                await delivered(update(4, 2))
                await silent([update(4, 1)])
                await delivered(update(4, 3, True))
                await silent([update(4, 4), update(4, 5, True)])

            async def resume() -> None:
                await delivered(update(10, 1))
                # Keep a partial revision marker to catch replay of segment K itself.
                async with websockets.connect(uri, close_timeout=0.2) as prior:
                    await hello(prior, 10, 1)
                await delivered(update(10, 2, True))
                await delivered(update(11, 1))
                await delivered(update(12, 2, True))
                await delivered(update(13, 3, True))
                await silent([update(12, 2, True)])
                async with websockets.connect(uri, close_timeout=0.2) as resumed:
                    # (10, 2) is segment 10's final, so 10 itself must not replay.
                    items = (await hello(resumed, 10, 2))["items"]
                    assert [i["segment"] for i in items] == [12, 13], items
                    assert all(i["final"] is True for i in items), items
                    assert len({i["segment"] for i in items}) == len(items), items

            for name, action in (
                ("unknown_fields", extras),
                ("source_done", done),
                ("bad_hellos", bad_hellos),
                ("duplicate", duplicate),
                ("stale_and_post_final", stale_final),
                ("resume", resume),
            ):
                await check(name, action)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()
    checks: dict[str, bool] = {}
    server = None
    alive = False
    try:
        server = subprocess.Popen(
            [
                sys.executable,
                "-u",
                str(ROOT / "submission_root/solution_server.py"),
                "--port",
                str(args.port),
            ],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        wait_for_port(args.port, server, timeout=3)
        asyncio.run(
            asyncio.wait_for(checks_for(f"ws://127.0.0.1:{args.port}", checks), 12)
        )
        alive = server.poll() is None
    except Exception as exc:
        checks["harness"] = False
        print(f"FAIL harness: {exc}", flush=True)
    finally:
        terminate_process(server, "solution_server.py")
    passed = int(len(checks) == 6 and all(checks.values()) and alive)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(
                dict(edge_pass=passed, checks=checks, server_alive=int(alive)), indent=2
            ),
            encoding="utf-8",
        )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
