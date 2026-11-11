"""Split AutoScout24 searches into slices under the 4,000-result cap and schedule their pages.

A search shows at most 200 pages x 20 listings. Any slice with more results is split:
country -> make -> price buckets -> price bisection -> first-registration-year bisection.
"""

from __future__ import annotations

import random
from collections.abc import Iterable
from dataclasses import dataclass

# AutoScout24 country codes (cy=...) in round-robin order.
COUNTRIES = ["D", "I", "NL", "B", "F", "A", "E", "L"]

MAX_RESULTS = 4000
MAX_PAGES = 200
MAX_PRICE = 99_999_999
YEAR_RANGE = (1900, 2030)

# First price split for a make; denser where most cars are.
PRICE_EDGES = [
    0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12500,
    14000, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 35000, 40000, 45000,
    50000, 60000, 75000, 100000, 150000, 250000, 500000, MAX_PRICE + 1,
]


# Search filters whose value we learn for free: every listing found in a slice with
# body=6 & bcol=2 & doors=5 is a blue 5-door "Berlina" (ids from the site's search form).
BODY_IDS = [1, 2, 3, 4, 5, 6, 12, 13, 7]
COLOR_IDS = [1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16]
DOOR_VALUES = [2, 3, 4, 5, 6, 7]
BODY_LABELS = {1: "Autoturism mic", 2: "Cabriolet / Roadster", 3: "Masina sport / Coupe",
               4: "Off-road/4x4/Pick-up", 5: "Combi", 6: "Berlina", 12: "Van", 13: "Autoutilitare",
               7: "Altele"}
COLOR_LABELS = {1: "Bej", 2: "Albastru", 3: "Maro", 4: "Bronz", 5: "Galben", 6: "Gri", 7: "Verde",
                10: "Rosu", 11: "Negru", 12: "Argintiu", 13: "Violet", 14: "Alb", 15: "Portocaliu",
                16: "Auriu"}

Attrs = tuple  # (body_id, color_id, doors), each None until fixed


@dataclass(frozen=True)
class Slice:
    country: str
    make_id: int | None = None
    price: tuple[int, int] | None = None  # inclusive bounds
    year: tuple[int, int] | None = None  # inclusive bounds
    attrs: Attrs | None = None  # None = plain catalog slice; tuple = body/color/doors slice

    @property
    def attrs_complete(self) -> bool:
        return self.attrs is not None and None not in self.attrs

    @property
    def key(self) -> str:
        def rng(r):
            return f"{r[0]}-{r[1]}" if r else "-"
        make = "-" if self.make_id is None else str(self.make_id)
        key = f"{self.country}|{make}|{rng(self.price)}|{rng(self.year)}"
        if self.attrs is not None:
            key += "|" + ".".join("-" if a is None else str(a) for a in self.attrs)
        return key

    @classmethod
    def from_key(cls, key: str) -> Slice:
        parts = key.split("|")
        country, make, price, year = parts[:4]

        def rng(s):
            return None if s == "-" else tuple(int(x) for x in s.split("-"))
        attrs = None
        if len(parts) == 5:
            attrs = tuple(None if a == "-" else int(a) for a in parts[4].split("."))
        return cls(country, None if make == "-" else int(make), rng(price), rng(year), attrs)

    def params(self, page: int) -> dict:
        p = {"atype": "C", "ustate": "N,U", "cy": self.country, "sort": "age", "desc": "0", "page": page}
        if self.make_id is not None:
            p["mmmv"] = f"{self.make_id}|||"
        if self.price:
            p["pricefrom"], p["priceto"] = self.price
        if self.year:
            p["fregfrom"], p["fregto"] = self.year
        if self.attrs is not None:
            body, color, doors = self.attrs
            if body is not None:
                p["body"] = body
            if color is not None:
                p["bcol"] = color
            if doors is not None:
                p["doorfrom"] = p["doorto"] = doors
        return p


def _bisect(lo: int, hi: int) -> list[tuple[int, int]]:
    mid = (lo + hi) // 2
    return [(lo, mid), (mid + 1, hi)]


def needs_split(s: Slice, total: int) -> bool:
    if s.attrs is not None and not s.attrs_complete:
        return total > 0  # keep narrowing until body, color and doors are all fixed
    return total > MAX_RESULTS


def split(s: Slice, make_ids: Iterable[int]) -> list[Slice]:
    """Children covering the same listings in smaller pieces; [] if it cannot be split further."""
    if s.attrs is not None and not s.attrs_complete:
        body, color, doors = s.attrs
        if body is None:
            return [Slice(s.country, attrs=(b, None, None)) for b in BODY_IDS]
        if color is None:
            return [Slice(s.country, attrs=(body, c, None)) for c in COLOR_IDS]
        return [Slice(s.country, attrs=(body, color, d)) for d in DOOR_VALUES]
    a = s.attrs
    if a is not None:  # attr slices: price first (~30 searches) rather than ~300 mostly empty makes
        if s.price is None:
            return [Slice(s.country, s.make_id, (lo, hi - 1), attrs=a) for lo, hi in zip(PRICE_EDGES, PRICE_EDGES[1:])]
        if s.price[0] < s.price[1]:
            return [Slice(s.country, s.make_id, r, attrs=a) for r in _bisect(*s.price)]
        if s.make_id is None:
            return [Slice(s.country, m, s.price, attrs=a) for m in make_ids]
        year = s.year or YEAR_RANGE
        if year[0] < year[1]:
            return [Slice(s.country, s.make_id, s.price, r, a) for r in _bisect(*year)]
        return []
    if s.make_id is None:
        return [Slice(s.country, m, attrs=a) for m in make_ids]
    if s.price is None:
        return [Slice(s.country, s.make_id, (lo, hi - 1), attrs=a) for lo, hi in zip(PRICE_EDGES, PRICE_EDGES[1:])]
    if s.price[0] < s.price[1]:
        return [Slice(s.country, s.make_id, r, attrs=a) for r in _bisect(*s.price)]
    year = s.year or YEAR_RANGE
    if year[0] < year[1]:
        return [Slice(s.country, s.make_id, s.price, r, a) for r in _bisect(*year)]
    return []


class Scheduler:
    """Hands out (slice, page) work: round-robin over countries, random order within a country."""

    def __init__(self, countries: list[str], rng: random.Random | None = None):
        self._countries = list(countries)
        self._pools: dict[str, list[tuple[Slice, int]]] = {c: [] for c in countries}
        self._next = 0
        self._rng = rng or random.Random()

    def add(self, s: Slice, pages: Iterable[int]) -> None:
        self._pools[s.country].extend((s, p) for p in pages)

    def next(self) -> tuple[Slice, int] | None:
        n = len(self._countries)
        for i in range(n):
            country = self._countries[(self._next + i) % n]
            pool = self._pools[country]
            if pool:
                self._next = (self._next + i + 1) % n
                j = self._rng.randrange(len(pool))
                pool[j], pool[-1] = pool[-1], pool[j]
                return pool.pop()
        return None

    def __len__(self) -> int:
        return sum(len(p) for p in self._pools.values())
