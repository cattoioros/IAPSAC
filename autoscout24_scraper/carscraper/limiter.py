"""Adaptive request limiter.

Two knobs, both adjusted from the site's answers:
- concurrency: requests in flight (grows slowly, halves on a 429/403);
- rate: optional cap in requests per second (grows very slowly, drops 30% on a 429/403).
AutoScout24 blocks an IP for ~2 minutes after ~1,000 fast requests, so a steady rate
gets more done than bursts followed by pauses.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable


class AdaptiveLimiter:
    def __init__(
        self,
        start: int = 10,
        minimum: int = 2,
        maximum: int = 25,
        increase_every: int = 50,
        rate: float | None = None,
        min_rate: float = 2.0,
        max_rate: float | None = None,
        rate_step: float = 0.2,
        rate_increase_every: int = 300,
        base_pause: float = 30.0,
        max_pause: float = 600.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.limit = start
        self.minimum = minimum
        self.maximum = maximum
        self.increase_every = increase_every
        self.rate = rate
        self.min_rate = min_rate
        self.max_rate = max_rate if max_rate is not None else rate
        self.rate_step = rate_step
        self.rate_increase_every = rate_increase_every
        self.base_pause = base_pause
        self.max_pause = max_pause
        self._clock = clock
        self._in_flight = 0
        self._streak = 0
        self._rate_streak = 0
        self._consecutive_throttles = 0
        self._pause_until = 0.0
        self._next_slot = 0.0
        self._cond: asyncio.Condition | None = None

    @property
    def in_flight(self) -> int:
        return self._in_flight

    def paused_for(self) -> float:
        return max(0.0, self._pause_until - self._clock())

    def success(self) -> None:
        self._consecutive_throttles = 0
        self._streak += 1
        if self._streak >= self.increase_every and self.limit < self.maximum:
            self.limit += 1  # waiters see it on the next release
            self._streak = 0
        if self.rate is not None:
            self._rate_streak += 1
            if self._rate_streak >= self.rate_increase_every:
                self.rate = min(self.max_rate, self.rate + self.rate_step)
                self._rate_streak = 0

    def throttled(self, retry_after: float | None = None) -> float:
        """Register a 429/403; returns how long requests are paused (seconds)."""
        now = self._clock()
        if now < self._pause_until:
            return self._pause_until - now  # already backing off for this burst
        self._streak = self._rate_streak = 0
        self._consecutive_throttles += 1
        self.limit = max(self.minimum, self.limit // 2)
        if self.rate is not None:
            self.rate = max(self.min_rate, round(self.rate * 0.7, 3))
        if retry_after is not None:
            pause = retry_after
        else:
            pause = min(self.max_pause, self.base_pause * 2 ** (self._consecutive_throttles - 1))
        self._pause_until = now + pause
        return pause

    async def __aenter__(self):
        if self._cond is None:
            self._cond = asyncio.Condition()
        async with self._cond:
            while True:
                wait = self.paused_for()
                if wait > 0:
                    self._cond.release()
                    try:
                        await asyncio.sleep(wait)
                    finally:
                        await self._cond.acquire()
                    continue
                if self._in_flight < self.limit:
                    self._in_flight += 1
                    break
                await self._cond.wait()
            delay = 0.0
            if self.rate:
                now = self._clock()
                slot = max(now, self._next_slot)
                self._next_slot = slot + 1.0 / self.rate
                delay = slot - now
        if delay > 0:
            await asyncio.sleep(delay)
        return self

    async def __aexit__(self, *exc):
        async with self._cond:
            self._in_flight -= 1
            self._cond.notify_all()
        return False
