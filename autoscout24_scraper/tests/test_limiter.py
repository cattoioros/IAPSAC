import asyncio
import time

from carscraper.limiter import AdaptiveLimiter


def test_limit_grows_after_successes_up_to_max():
    lim = AdaptiveLimiter(start=10, maximum=11, increase_every=3)
    for _ in range(3):
        lim.success()
    assert lim.limit == 11
    for _ in range(10):
        lim.success()
    assert lim.limit == 11


def test_throttle_halves_limit_but_not_below_min():
    lim = AdaptiveLimiter(start=10, minimum=4, base_pause=0)
    lim.throttled()
    assert lim.limit == 5
    lim.throttled()
    assert lim.limit == 4


def test_pause_doubles_on_consecutive_throttles_and_honors_retry_after():
    now = [100.0]
    lim = AdaptiveLimiter(base_pause=30, max_pause=100, clock=lambda: now[0])
    assert lim.throttled() == 30
    now[0] += 31
    assert lim.throttled() == 60
    now[0] += 61
    assert lim.throttled() == 100  # capped
    now[0] += 101
    assert lim.throttled(retry_after=7) == 7


def test_throttles_during_a_pause_do_not_escalate():
    now = [0.0]
    lim = AdaptiveLimiter(start=16, base_pause=30, clock=lambda: now[0])
    lim.throttled()
    now[0] += 5
    assert lim.throttled() == 25  # remaining pause only
    assert lim.limit == 8  # halved once, not twice


def test_success_resets_pause_escalation():
    now = [0.0]
    lim = AdaptiveLimiter(base_pause=30, clock=lambda: now[0])
    lim.throttled()
    now[0] += 31
    lim.success()
    assert lim.throttled() == 30


async def test_concurrency_never_exceeds_limit():
    lim = AdaptiveLimiter(start=2, minimum=1)
    active = peak = 0

    async def job():
        nonlocal active, peak
        async with lim:
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1

    await asyncio.gather(*(job() for _ in range(8)))
    assert peak == 2


async def test_rate_cap_spaces_out_requests():
    lim = AdaptiveLimiter(start=10, rate=40, max_rate=40)
    t0 = time.monotonic()
    for _ in range(9):
        async with lim:
            pass
    assert time.monotonic() - t0 >= 8 / 40 * 0.9


def test_throttle_lowers_rate_and_successes_raise_it_slowly():
    now = [0.0]
    lim = AdaptiveLimiter(rate=10, max_rate=11, min_rate=1, rate_step=0.5, rate_increase_every=4,
                          base_pause=1, clock=lambda: now[0])
    lim.throttled()
    assert lim.rate == 7
    for _ in range(4):
        lim.success()
    assert lim.rate == 7.5
    for _ in range(40):
        lim.success()
    assert lim.rate == 11


def test_no_rate_cap_by_default():
    assert AdaptiveLimiter().rate is None


async def test_acquire_waits_out_the_pause():
    lim = AdaptiveLimiter(base_pause=0.05)
    lim.throttled()
    t0 = time.monotonic()
    async with lim:
        pass
    assert time.monotonic() - t0 >= 0.045
