"""Keep the scraper running unattended: restart it after crashes, blocks and hangs.

Exit codes of `carscraper run`: 0 = nothing left to do, 2 = the site keeps blocking us,
anything else = crash. The supervisor gives up (exit 1) only after `max_crashes` crashes
within `crash_window` seconds, so a real bug surfaces instead of looping forever.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from collections import deque
from collections.abc import Callable
from pathlib import Path


def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _tail(path: Path, lines: int = 25) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - 16000))
            return "\n".join(f.read().decode("utf-8", "replace").splitlines()[-lines:])
    except OSError:
        return ""


def supervise(
    cmd: list[str],
    progress: Callable[[], object],
    log_path: str | Path,
    event: Callable[[str], None] = print,
    check_every: float = 30.0,
    stall_seconds: float = 45 * 60,
    term_grace: float = 90.0,
    crash_wait: float = 60.0,
    blocked_wait: float = 15 * 60,
    max_crashes: int = 5,
    crash_window: float = 3600.0,
    on_done: Callable[[], bool] | None = None,
    done_wait: float = 3600.0,
) -> int:
    """on_done() runs when the scraper finishes all work; returning True starts another round."""
    log_path = Path(log_path)
    crashes: deque[float] = deque()
    child: subprocess.Popen | None = None

    def forward(signum, _frame):
        if child and child.poll() is None:
            child.send_signal(signal.SIGINT)
            try:
                child.wait(term_grace)
            except subprocess.TimeoutExpired:
                child.kill()
        event(f"{_ts()} supraveghetor oprit (semnal {signum})")
        raise SystemExit(130)

    old_handlers = {s: signal.signal(s, forward) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        while True:
            event(f"{_ts()} pornesc: {' '.join(cmd)}")
            with open(log_path, "a") as logf:
                child = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT)
            last, last_change, stalled = progress(), time.monotonic(), False
            while True:
                try:
                    rc = child.wait(timeout=check_every)
                    break
                except subprocess.TimeoutExpired:
                    pass
                current = progress()
                if current != last:
                    last, last_change = current, time.monotonic()
                elif time.monotonic() - last_change > stall_seconds:
                    event(f"{_ts()} blocat fara progres de {stall_seconds / 60:.0f} min; repornesc")
                    stalled = True
                    child.send_signal(signal.SIGINT)  # graceful first
                    try:
                        rc = child.wait(term_grace)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        rc = child.wait()
                    break
            if stalled:
                continue
            if rc == 0:
                if on_done is not None and on_done():
                    event(f"{_ts()} runda terminata; peste {done_wait / 60:.0f} min caut anunturi noi")
                    time.sleep(done_wait)
                    continue
                event(f"{_ts()} terminat: nu mai e nimic de luat")
                return 0
            if rc == 2:
                event(f"{_ts()} blocat de site; astept {blocked_wait / 60:.0f} min si reiau")
                time.sleep(blocked_wait)
                continue
            now = time.monotonic()
            crashes.append(now)
            while crashes and now - crashes[0] > crash_window:
                crashes.popleft()
            event(f"{_ts()} EROARE: exit {rc} ({len(crashes)}/{max_crashes}); ultimele linii din log:\n"
                  f"{_tail(log_path)}")
            if len(crashes) >= max_crashes:
                event(f"{_ts()} OPRIT: prea multe erori la rand; trebuie reparat codul")
                return 1
            time.sleep(crash_wait)
    finally:
        for s, h in old_handlers.items():
            signal.signal(s, h)
