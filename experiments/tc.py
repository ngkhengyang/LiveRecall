"""Keep privileged network changes scoped to a run, including failed runs."""

from __future__ import annotations

import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path

PROFILES = Path(__file__).resolve().parents[1] / "network_profiles"


def apply(profile_or_args: str, iface: str = "lo") -> None:
    profile = PROFILES / f"{profile_or_args}.sh"
    if profile.parent == PROFILES and profile.is_file():
        command = ["sudo", "bash", str(profile), iface]
    else:
        command = [
            "sudo",
            "tc",
            "qdisc",
            "replace",
            "dev",
            iface,
            "root",
            *shlex.split(profile_or_args),
        ]
    subprocess.run(command, check=True, timeout=15)


def reset(iface: str = "lo") -> None:
    subprocess.run(
        ["sudo", "bash", str(PROFILES / "reset.sh"), iface], check=True, timeout=15
    )


def available() -> bool:
    if not sys.platform.startswith("linux"):
        return False
    added = False
    try:
        subprocess.run(
            ["sudo", "-n", "tc", "qdisc", "show", "dev", "lo"],
            check=True,
            capture_output=True,
            timeout=10,
        )
        subprocess.run(
            [
                "sudo",
                "-n",
                "tc",
                "qdisc",
                "add",
                "dev",
                "lo",
                "root",
                "netem",
                "delay",
                "1ms",
            ],
            check=True,
            capture_output=True,
            timeout=10,
        )
        added = True
        return True
    except (OSError, subprocess.SubprocessError):
        return False
    finally:
        if added:
            try:
                subprocess.run(
                    ["sudo", "-n", "tc", "qdisc", "del", "dev", "lo", "root"],
                    check=True,
                    capture_output=True,
                    timeout=10,
                )
            except (OSError, subprocess.SubprocessError):
                return False


class Timeline:
    def __init__(self, events: list[tuple[float, str]], iface: str = "lo") -> None:
        self.events = sorted(events)
        self.iface = iface
        self.stop = threading.Event()
        self.error: Exception | None = None
        self.thread: threading.Thread | None = None

    def prepare(self) -> None:
        for offset, profile in self.events:
            if offset == 0:
                apply(profile, self.iface)

    def start(self, start_time: float) -> None:
        if not self.events:
            return

        def run() -> None:
            try:
                for offset, profile in self.events:
                    if offset == 0:
                        continue
                    if self.stop.wait(max(0, start_time + offset - time.monotonic())):
                        return
                    apply(profile, self.iface)
                self.stop.wait()
            except Exception as exc:
                self.error = exc
            finally:
                try:
                    reset(self.iface)
                except Exception as exc:
                    self.error = self.error or exc

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.stop.set()
        if self.thread is not None:
            self.thread.join()
        elif self.events:
            reset(self.iface)
