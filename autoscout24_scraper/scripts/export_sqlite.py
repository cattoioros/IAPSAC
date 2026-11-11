"""Write the listings with at least 19 of 20 fields into a standalone SQLite database (table `cars`).

uv run python scripts/export_sqlite.py [source.db] [out.db]
"""

import json
import sqlite3
import sys
from pathlib import Path

from carscraper.store import Store

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/autoscout24.db"
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else "export/autoscout24_19din20.db")

COLUMNS = [
    ("listing_id", "TEXT PRIMARY KEY"), ("make", "TEXT"), ("model", "TEXT"), ("version", "TEXT"),
    ("year", "INTEGER"), ("generation", "TEXT"), ("first_registration_date", "TEXT"),
    ("mileage_km", "INTEGER"), ("fuel_type", "TEXT"), ("transmission", "TEXT"), ("body_type", "TEXT"),
    ("engine_displacement_cm3", "INTEGER"), ("power_hp", "INTEGER"), ("price_euro", "INTEGER"),
    ("color", "TEXT"), ("doors", "INTEGER"), ("country", "TEXT"), ("first_seen_at", "TEXT"),
    ("source", "TEXT"), ("features", "TEXT"), ("url", "TEXT"),
]
NAMES = [c for c, _ in COLUMNS]

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.unlink(missing_ok=True)
out = sqlite3.connect(OUT)
out.execute(f"CREATE TABLE cars ({', '.join(f'{c} {t}' for c, t in COLUMNS)})")


def row(rec: dict) -> tuple:
    feats = rec.get("features")
    rec = {**rec, "features": json.dumps(feats, ensure_ascii=False) if feats is not None else None}
    return tuple(rec.get(c) for c in NAMES)


store = Store(SRC)
batch, n = [], 0
insert = f"INSERT INTO cars ({', '.join(NAMES)}) VALUES ({', '.join('?' * len(NAMES))})"
for rec in store.iter_records(min_19=True):
    batch.append(row(rec))
    if len(batch) == 10000:
        out.executemany(insert, batch)
        n += len(batch)
        batch.clear()
out.executemany(insert, batch)
n += len(batch)
for col in ("country", "make", "price_euro", "year"):
    out.execute(f"CREATE INDEX cars_{col} ON cars ({col})")
out.commit()
out.close()
print(f"{OUT}: {n} rows")
