"""The scrape loop.

Modes:
- attrs:    like catalog, but every search is narrowed by body type, colour and door count, so those
            three fields come for free (19 of 20 fields at 20 listings per request);
- catalog:  walk every search page and save the 12 fields shown there (20 listings per request);
- details:  open the listing page of every catalogued listing to fill in the rest (1 request each);
- auto:     attrs, then catalog (listings without those filters set), then details;
- pipeline: search page -> its 20 listing pages -> next search page.

Countries take turns everywhere, so stopping at any point leaves a mix of all of them.
Progress lives in SQLite, so a stopped run resumes where it left off.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from .client import BlockedError
from .mapper import COUNTRY_NAMES, basic_record, sanitize_details, to_record
from .planner import (BODY_LABELS, COLOR_LABELS, COUNTRIES, MAX_PAGES, MAX_RESULTS, Scheduler, Slice,
                      needs_split, split)
from .store import Store

CY_TO_ISO = {"D": "DE", "I": "IT", "NL": "NL", "B": "BE", "F": "FR", "A": "AT", "E": "ES", "L": "LU"}
MODES = ("auto", "attrs", "catalog", "details", "pipeline")
MAX_PAGE_ATTEMPTS = 3


def country_name(cy: str) -> str:
    iso = CY_TO_ISO.get(cy, cy)
    return COUNTRY_NAMES.get(iso, iso)


@dataclass
class RunStats:
    saved: int = 0
    gone: int = 0
    pages: int = 0
    failed_pages: int = 0
    errors: int = 0
    blocked: bool = False
    per_country: Counter = field(default_factory=Counter)


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


class DetailFeeder:
    """Round-robin over countries through basic listings still waiting for their listing page."""

    def __init__(self, store: Store, country_names: list[str], batch: int = 500):
        self.store = store
        self.order = list(country_names)
        self.batch = batch
        self.cursor = {c: 0 for c in self.order}
        self.buffers = {c: deque() for c in self.order}
        self.exhausted: set[str] = set()
        self._i = 0

    def next(self):
        n = len(self.order)
        for k in range(n):
            c = self.order[(self._i + k) % n]
            if c in self.exhausted:
                continue
            if not self.buffers[c]:
                rows = self.store.basic_after(c, self.cursor[c], self.batch)
                if not rows:
                    self.exhausted.add(c)
                    continue
                self.cursor[c] = rows[-1][0]
                self.buffers[c].extend(rows)
            self._i = (self._i + k + 1) % n
            return self.buffers[c].popleft()
        return None


class Runner:
    def __init__(
        self,
        store: Store,
        client,
        countries: list[str] = COUNTRIES,
        mode: str = "pipeline",
        page_workers: int = 6,
        detail_workers: int = 16,
        max_listings: int | None = None,
        log: Callable[[str], None] = print,
        report_every: float = 15.0,
    ):
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        self.store = store
        self.client = client
        self.countries = list(countries)
        self.mode = mode
        self.page_workers = page_workers
        self.detail_workers = detail_workers
        self.max_listings = max_listings
        self.log = log
        self.report_every = report_every
        self.stats = RunStats()
        self.phase = mode
        self._stopping = False
        self._busy = 0
        self._reserved = 0
        self._attempts: Counter = Counter()
        self._cond: asyncio.Condition | None = None
        self._sched: Scheduler | None = None
        self._makes: list[int] = []
        self._t0 = time.monotonic()
        self._last = (self._t0, 0)

    @property
    def stopping(self) -> bool:
        return self._stopping

    def stop(self) -> None:
        self._stopping = True
        if self._cond is not None:
            asyncio.get_running_loop().create_task(self._wake_all())

    async def _wake_all(self) -> None:
        async with self._cond:
            self._cond.notify_all()

    def _limit_reached(self) -> bool:
        return self.max_listings is not None and self.stats.saved >= self.max_listings

    async def run(self) -> RunStats:
        self._cond = asyncio.Condition()
        fresh_makes = await self.client.bootstrap()  # also loads the current buildId
        self._makes = self.store.get_meta("make_ids") or fresh_makes
        self.store.set_meta("make_ids", self._makes)

        reporter = asyncio.create_task(self._report_loop())
        try:
            if self.mode in ("auto", "attrs"):
                self.phase = "atribute"
                self.store.add_slices([Slice(c, attrs=(None, None, None)) for c in self.countries])
                await self._crawl(attrs=True)
            if self.mode in ("auto", "catalog", "pipeline") and not self._stopping:
                self.phase = "pipeline" if self.mode == "pipeline" else "catalog"
                self.store.add_slices([Slice(c) for c in self.countries])  # no-op if already started
                await self._crawl(attrs=False)
            if self.mode in ("auto", "details") and not self._stopping:
                self.phase = "details"
                await self._details()
        finally:
            reporter.cancel()
        self._report(final=True)
        return self.stats

    # ---------- search pages ----------
    async def _crawl(self, attrs: bool) -> None:
        self._sched = Scheduler(self.countries)
        for s, pages in self.store.load_work():
            if s.country in self.countries and (s.attrs is not None) == attrs:
                self._sched.add(s, pages)
        await asyncio.gather(*(self._page_worker() for _ in range(self.page_workers)))

    async def _page_worker(self) -> None:
        while True:
            async with self._cond:
                while True:
                    item = None if self._stopping else self._sched.next()
                    if item is not None or self._busy == 0 or self._stopping:
                        break
                    await self._cond.wait()  # other pages may still add work
                if item is None:
                    return
                self._busy += 1
            s, page = item
            try:
                await self._process(s, page)
            except BlockedError as e:
                self._blocked(e)
            except Exception as e:  # noqa: BLE001 - keep the run going, retry the page later
                self.stats.errors += 1
                self._attempts[(s.key, page)] += 1
                if self._attempts[(s.key, page)] < MAX_PAGE_ATTEMPTS:
                    self._sched.add(s, [page])
                else:
                    self.stats.failed_pages += 1
                    self.log(f"Pagina {s.key} p{page} a esuat de {MAX_PAGE_ATTEMPTS} ori ({e!r}); "
                             "o reiau la urmatoarea rulare.")
            finally:
                async with self._cond:
                    self._busy -= 1
                    self._cond.notify_all()

    async def _process(self, s: Slice, page: int) -> None:
        data = await self.client.search(s.params(page))
        if data is None:
            raise RuntimeError("search page not found")
        if page == 1:
            self._plan(s, data)
        listings = [l for l in data.get("listings") or [] if l.get("id") and l.get("url")]

        if self.mode != "pipeline":
            seen = _now()
            rows = [(l["id"], l["url"].rsplit("/", 1)[-1], basic_record(l, seen)) for l in listings]
            if s.attrs_complete:
                body, color, doors = s.attrs
                for _, _, rec in rows:
                    rec.update(body_type=BODY_LABELS[body], color=COLOR_LABELS[color], doors=doors)
                added = self.store.save_attrs(rows)
            else:
                added = self.store.save_basic(rows)
            self.stats.saved += added
            self.stats.per_country[country_name(s.country)] += added
            self.store.mark_page_done(s, page)
            self.stats.pages += 1
            if self._limit_reached():
                self.stop()
            return

        slugs = {l["id"]: l["url"].rsplit("/", 1)[-1] for l in listings}
        missing = self.store.missing(list(slugs))
        new_ids = [i for i in slugs if i in missing]
        complete = True
        if self.max_listings is not None:
            room = max(0, self.max_listings - self.stats.saved - self._reserved)
            if len(new_ids) > room:
                new_ids, complete = new_ids[:room], False
        self._reserved += len(new_ids)  # pages running in parallel must not overshoot max_listings
        try:
            results = await asyncio.gather(*(self._fetch(i, slugs[i], None) for i in new_ids),
                                           return_exceptions=True)
        finally:
            self._reserved -= len(new_ids)
        rows = [r for r in results if isinstance(r, tuple)]
        self.store.save_listings(rows)
        self._count_saved(rows)

        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            raise next((e for e in errors if isinstance(e, BlockedError)), errors[0])
        if complete:
            self.store.mark_page_done(s, page)
            self.stats.pages += 1
        if self._limit_reached():
            self.stop()

    def _plan(self, s: Slice, data: dict) -> None:
        total = data.get("numberOfResults") or 0
        pages = min(data.get("numberOfPages") or 0, MAX_PAGES)
        children = split(s, self._makes) if needs_split(s, total) else []
        if children:
            self.store.add_slices(children)
            self.store.set_slice_result(s, total, pages, "split")
            for c in children:
                self._sched.add(c, [1])
            return
        if total > MAX_RESULTS:
            self.log(f"Atentie: {s.key} are {total} rezultate si nu se mai poate imparti; "
                     f"iau primele {MAX_RESULTS}.")
        self.store.set_slice_result(s, total, pages, "paging")
        self._sched.add(s, range(2, pages + 1))

    # ---------- listing pages ----------
    async def _details(self) -> None:
        feeder = DetailFeeder(self.store, [country_name(c) for c in self.countries])
        await asyncio.gather(*(self._detail_worker(feeder) for _ in range(self.detail_workers)))

    async def _detail_worker(self, feeder: DetailFeeder) -> None:
        while not self._stopping:
            if self.max_listings is not None and self.stats.saved + self._reserved >= self.max_listings:
                self.stop()
                return
            item = feeder.next()
            if item is None:
                return
            _, lid, slug, basic = item
            self._reserved += 1
            try:
                row = await self._fetch(lid, slug, basic.get("first_seen_at"))
            except BlockedError as e:
                self._blocked(e)
                return
            except Exception as e:  # noqa: BLE001 - listing stays 'basic'; retried next run
                self.stats.errors += 1
                if self.stats.errors <= 20 or self.stats.errors % 100 == 0:
                    self.log(f"Eroare la anuntul {slug}: {e!r} (total erori: {self.stats.errors})")
                continue
            finally:
                self._reserved -= 1
            if row is None:
                self.store.mark_gone([lid])
                continue
            self.store.save_listings([row])
            self._count_saved([row])
        if self._limit_reached():
            self.stop()

    async def _fetch(self, listing_id: str, slug: str, first_seen: str | None):
        details = await self.client.details(slug)
        if details is None:
            self.stats.gone += 1
            return None
        return listing_id, to_record(details, slug, first_seen or _now()), sanitize_details(details)

    def _count_saved(self, rows) -> None:
        self.stats.saved += len(rows)
        for _, rec, _ in rows:
            self.stats.per_country[rec["country"]] += 1

    def _blocked(self, e: Exception) -> None:
        self.stats.blocked = True
        self.log(f"Site-ul ne blocheaza ({e}). Ma opresc; reia mai tarziu cu aceeasi comanda.")
        self.stop()

    # ---------- progress ----------
    async def _report_loop(self) -> None:
        while True:
            await asyncio.sleep(self.report_every)
            self._report()

    def _report(self, final: bool = False) -> None:
        now = time.monotonic()
        last_t, last_saved = self._last
        rate = (self.stats.saved - last_saved) / max(now - last_t, 1e-9)
        self._last = (now, self.stats.saved)
        lim = getattr(self.client, "limiter", None)
        if lim is not None and lim.rate and lim.rate != self.store.get_meta("rate"):
            self.store.set_meta("rate", lim.rate)  # the next run starts at the speed that worked
        cstats = getattr(self.client, "stats", {})
        levels = self.store.count_by_level()
        countries = " ".join(
            f"{CY_TO_ISO.get(c, c)}:{self.stats.per_country.get(country_name(c), 0)}" for c in self.countries
        )
        queue = (f" | pagini in coada {len(self._sched)}" if self.phase != "details" and self._sched
                 else f" | asteapta detalii {levels.get('basic', 0) + levels.get('attrs', 0)}")
        line = (
            f"[{'GATA' if final else time.strftime('%H:%M:%S')}] {self.phase}: +{self.stats.saved}"
            + ("" if final else f" ({rate:.1f}/s)")
            + f" | DB complete {levels.get('full', 0)} 19/20 {levels.get('attrs', 0)} baza {levels.get('basic', 0)}"
            + f" | {countries}{queue}"
            + (f" | {lim.rate:.1f} req/s" if lim and getattr(lim, "rate", None) else "")
            + (f" | PAUZA {lim.paused_for():.0f}s" if lim and lim.paused_for() > 0 else "")
            + f" | refuzuri {cstats.get('throttled', 0)} | erori {self.stats.errors}"
            + f" | disparute {self.stats.gone}"
            + (f" | {(now - self._t0) / 60:.1f} min" if final else "")
        )
        self.log(line)
