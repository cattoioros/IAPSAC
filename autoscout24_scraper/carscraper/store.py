"""SQLite storage: scraped listings plus search progress, so a run can be stopped and resumed."""

from __future__ import annotations

import sqlite3
import zlib
from collections.abc import Iterator
from pathlib import Path

import orjson

from .planner import Slice

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value BLOB);
CREATE TABLE IF NOT EXISTS slices (
    key TEXT PRIMARY KEY,
    country TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',  -- pending | paging | split
    total INTEGER,
    pages INTEGER
);
CREATE TABLE IF NOT EXISTS pages_done (
    key TEXT NOT NULL, page INTEGER NOT NULL, PRIMARY KEY (key, page)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY,
    listing_id TEXT NOT NULL UNIQUE,
    country TEXT,
    record BLOB NOT NULL,     -- JSON in Autovit format
    raw BLOB,                 -- zlib-compressed listing JSON without seller data
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    slug TEXT,
    level TEXT NOT NULL DEFAULT 'full'  -- basic (search page) | attrs (+ body/color/doors) | full | gone
);
"""
# Columns added after the first version; older databases get them on open.
MIGRATIONS = {
    "slug": "ALTER TABLE listings ADD COLUMN slug TEXT",
    "level": "ALTER TABLE listings ADD COLUMN level TEXT NOT NULL DEFAULT 'full'",
}
INDEXES = "CREATE INDEX IF NOT EXISTS listings_level_country ON listings (level, country, id);"


class Store:
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        cols = {row[1] for row in self.db.execute("PRAGMA table_info(listings)")}
        for col, sql in MIGRATIONS.items():
            if col not in cols:
                self.db.execute(sql)
        self.db.executescript(INDEXES)
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    # --- meta ---
    def get_meta(self, key: str):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return orjson.loads(row[0]) if row else None

    def set_meta(self, key: str, value) -> None:
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO meta VALUES (?, ?)", (key, orjson.dumps(value)))

    # --- search progress ---
    def has_slices(self) -> bool:
        return self.db.execute("SELECT 1 FROM slices LIMIT 1").fetchone() is not None

    def add_slices(self, slices: list[Slice]) -> None:
        with self.db:
            self.db.executemany(
                "INSERT OR IGNORE INTO slices (key, country) VALUES (?, ?)",
                [(s.key, s.country) for s in slices],
            )

    def set_slice_result(self, s: Slice, total: int, pages: int, state: str) -> None:
        with self.db:
            self.db.execute(
                "UPDATE slices SET total=?, pages=?, state=? WHERE key=?", (total, pages, state, s.key)
            )

    def mark_page_done(self, s: Slice, page: int) -> None:
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO pages_done VALUES (?, ?)", (s.key, page))

    def reset_crawl(self) -> None:
        """Forget search progress so the next catalog pass walks every search page again
        (to pick up listings posted since). Saved listings are kept."""
        with self.db:
            self.db.execute("DELETE FROM slices")
            self.db.execute("DELETE FROM pages_done")

    def load_work(self) -> list[tuple[Slice, list[int]]]:
        """Pages still to fetch for every unfinished slice."""
        done: dict[str, set[int]] = {}
        for key, page in self.db.execute("SELECT key, page FROM pages_done"):
            done.setdefault(key, set()).add(page)
        work = []
        rows = self.db.execute("SELECT key, state, pages FROM slices WHERE state != 'split' ORDER BY rowid")
        for key, state, pages in rows:
            if state == "pending":
                remaining = [1]
            else:
                remaining = [p for p in range(1, (pages or 0) + 1) if p not in done.get(key, ())]
            if remaining:
                work.append((Slice.from_key(key), remaining))
        return work

    # --- listings ---
    def missing(self, listing_ids: list[str]) -> set[str]:
        """Ids that still need their listing page (not saved in full and not gone)."""
        if not listing_ids:
            return set()
        marks = ",".join("?" * len(listing_ids))
        found = self.db.execute(
            f"SELECT listing_id FROM listings WHERE level IN ('full', 'gone') AND listing_id IN ({marks})",
            listing_ids,
        )
        return set(listing_ids) - {r[0] for r in found}

    def save_basic(self, rows: list[tuple[str, str, dict]]) -> int:
        """Save search-page records; never overwrites anything already saved. Returns how many were new."""
        before = self.db.total_changes
        with self.db:
            self.db.executemany(
                "INSERT OR IGNORE INTO listings (listing_id, country, slug, record, level) "
                "VALUES (?, ?, ?, ?, 'basic')",
                [(lid, rec.get("country"), slug, orjson.dumps(rec)) for lid, slug, rec in rows],
            )
        return self.db.total_changes - before

    def save_listings(self, rows: list[tuple[str, dict, dict | None]]) -> None:
        """Save full records (from the listing page); upgrades basic ones."""
        with self.db:
            self.db.executemany(
                "INSERT INTO listings (listing_id, country, record, raw, level) VALUES (?, ?, ?, ?, 'full') "
                "ON CONFLICT (listing_id) DO UPDATE SET country=excluded.country, record=excluded.record, "
                "raw=excluded.raw, level='full', fetched_at=datetime('now') WHERE listings.level != 'full'",
                [
                    (lid, rec.get("country"), orjson.dumps(rec),
                     zlib.compress(orjson.dumps(raw)) if raw is not None else None)
                    for lid, rec, raw in rows
                ],
            )

    def save_attrs(self, rows: list[tuple[str, str, dict]]) -> int:
        """Save search-page records that also carry body/color/doors (from the slice filters).
        Upgrades basic records (keeping first_seen_at), never touches full ones. Returns how many were new."""
        if not rows:
            return 0
        ids = [lid for lid, _, _ in rows]
        marks = ",".join("?" * len(ids))
        existing = {lid: (level, rec) for lid, level, rec in self.db.execute(
            f"SELECT listing_id, level, record FROM listings WHERE listing_id IN ({marks})", ids)}
        inserts, updates = [], []
        for lid, slug, rec in rows:
            if lid not in existing:
                inserts.append((lid, rec.get("country"), slug, orjson.dumps(rec)))
            elif existing[lid][0] in ("basic", "attrs"):
                old = orjson.loads(existing[lid][1])
                rec = {**rec, "first_seen_at": old.get("first_seen_at", rec.get("first_seen_at"))}
                updates.append((orjson.dumps(rec), slug, lid))
        with self.db:
            self.db.executemany(
                "INSERT OR IGNORE INTO listings (listing_id, country, slug, record, level) "
                "VALUES (?, ?, ?, ?, 'attrs')", inserts)
            self.db.executemany(
                "UPDATE listings SET record=?, slug=COALESCE(slug, ?), level='attrs' "
                "WHERE listing_id=? AND level IN ('basic', 'attrs')", updates)
        return len(inserts)

    def mark_gone(self, listing_ids: list[str]) -> None:
        with self.db:
            self.db.executemany(
                "UPDATE listings SET level='gone' WHERE listing_id=? AND level IN ('basic', 'attrs')",
                [(i,) for i in listing_ids],
            )

    def basic_after(self, country: str, after_id: int, limit: int) -> list[tuple[int, str, str, dict]]:
        """Next basic listings of a country still waiting for their listing page: (id, listing_id, slug, record)."""
        rows = []
        for level in ("basic", "attrs"):  # one indexed range scan per level, then merge by id
            rows += self.db.execute(
                "SELECT id, listing_id, slug, record FROM listings "
                "WHERE level=? AND country=? AND id>? ORDER BY id LIMIT ?",
                (level, country, after_id, limit),
            ).fetchall()
        rows.sort(key=lambda r: r[0])
        return [(i, lid, slug, orjson.loads(rec)) for i, lid, slug, rec in rows[:limit]]

    def get_raw(self, listing_id: str):
        row = self.db.execute("SELECT raw FROM listings WHERE listing_id=?", (listing_id,)).fetchone()
        return orjson.loads(zlib.decompress(row[0])) if row and row[0] else None

    def iter_records(self, only_full: bool = False, min_19: bool = False) -> Iterator[dict]:
        levels = "('full')" if only_full else "('attrs', 'full')" if min_19 else "('basic', 'attrs', 'full')"
        for (rec,) in self.db.execute(f"SELECT record FROM listings WHERE level IN {levels} ORDER BY id"):
            yield orjson.loads(rec)

    def count(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM listings WHERE level != 'gone'").fetchone()[0]

    def count_by_level(self) -> dict[str, int]:
        return dict(self.db.execute("SELECT level, COUNT(*) FROM listings GROUP BY level ORDER BY level"))

    def count_by_country(self) -> dict[str, int]:
        return dict(self.db.execute(
            "SELECT country, COUNT(*) FROM listings WHERE level != 'gone' GROUP BY country ORDER BY 2 DESC"))
