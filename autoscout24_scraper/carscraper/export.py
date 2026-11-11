"""Stream saved listings to a JSON file in the cars.json format (or JSON Lines)."""

from __future__ import annotations

import json
import os
from pathlib import Path

from .store import Store


def export(store: Store, path: str | Path, fmt: str = "json", only_full: bool = False,
           min_19: bool = False) -> int:
    """Write all records without loading them into memory; returns how many were written."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    n = 0
    with open(tmp, "w", encoding="utf-8") as f:
        if fmt == "jsonl":
            for rec in store.iter_records(only_full=only_full, min_19=min_19):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n += 1
        else:
            for rec in store.iter_records(only_full=only_full, min_19=min_19):
                f.write("[\n" if n == 0 else ",\n")
                body = json.dumps(rec, ensure_ascii=False, indent=4)
                # split("\n"), not splitlines(): values may contain  , \x85 etc. that must stay inside strings
                f.write("\n".join("    " + line for line in body.split("\n")))
                n += 1
            f.write("\n]" if n else "[]")
    os.replace(tmp, path)
    return n
