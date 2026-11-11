"""Command line: carscraper run | supervise | export | stats."""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path

from .client import AS24Client
from .export import export
from .limiter import AdaptiveLimiter
from .planner import COUNTRIES
from .runner import CY_TO_ISO, MODES, Runner
from .store import Store
from .supervise import supervise

DEFAULT_DB = "data/autoscout24.db"
DEFAULT_RATE = 5.0
ISO_TO_CY = {iso: cy for cy, iso in CY_TO_ISO.items()}


def _countries(value: str) -> list[str]:
    out = []
    for code in (c.strip().upper() for c in value.split(",") if c.strip()):
        cy = ISO_TO_CY.get(code, code)
        if cy not in COUNTRIES:
            raise argparse.ArgumentTypeError(f"tara necunoscuta: {code} (posibile: {', '.join(ISO_TO_CY)})")
        out.append(cy)
    return out


def _log(line: str) -> None:
    print(line, flush=True)


def _keep_awake() -> subprocess.Popen | None:
    """On macOS, keep the Mac from sleeping (idle, and system sleep on power) while this process runs."""
    if sys.platform == "darwin" and shutil.which("caffeinate"):
        return subprocess.Popen(["caffeinate", "-ims", "-w", str(os.getpid())])
    return None


async def _run(args) -> int:
    store = Store(args.db)
    rate = args.rate if args.rate is not None else (store.get_meta("rate") or DEFAULT_RATE)
    rate = max(2.0, min(rate, args.max_rate))
    limiter = AdaptiveLimiter(start=args.start_concurrency, maximum=args.max_concurrency,
                              rate=rate, max_rate=max(rate, args.max_rate))
    client = AS24Client(limiter)
    runner = Runner(store, client, countries=args.countries, mode=args.mode,
                    max_listings=args.max_listings, log=_log)
    main = asyncio.current_task()
    _log(f"Pornesc: mod {args.mode}, {rate:.1f} cereri/s, tari {','.join(CY_TO_ISO[c] for c in args.countries)}")

    signalled = False

    def on_stop_signal():
        nonlocal signalled
        signalled = True
        if runner.stopping:
            main.cancel()
            return
        _log("\nMa opresc dupa paginile in curs (Ctrl+C inca o data = oprire imediata)...")
        runner.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, on_stop_signal)
    try:
        stats = await runner.run()
        _log(f"In baza de date: {store.count_by_level()}. Export: uv run carscraper export")
        if stats.blocked:
            return 2
        return 130 if signalled else 0  # 0 only when the work is really finished (supervisor relies on it)
    except asyncio.CancelledError:
        _log("Oprit imediat. Tot ce s-a salvat ramane; la urmatoarea rulare continua de unde a ramas.")
        return 130
    finally:
        await client.aclose()
        store.close()


def cmd_run(args) -> int:
    awake = None if args.no_caffeinate else _keep_awake()
    try:
        return asyncio.run(_run(args))
    finally:
        if awake:
            awake.terminate()


def cmd_supervise(args) -> int:
    Store(args.db).close()  # create / migrate before the first run
    progress_db = sqlite3.connect(args.db)

    def progress():
        return progress_db.execute("PRAGMA data_version").fetchone()[0]  # changes on every write

    events = Path(args.db).with_name("supervisor.log")

    def event(line: str) -> None:
        _log(line)
        with open(events, "a") as f:
            f.write(line + "\n")

    def on_done() -> bool:
        if not args.forever:
            return False
        store = Store(args.db)
        store.reset_crawl()  # next round walks the search pages again to find new listings
        store.close()
        return True

    run_args = [a for a in args.run_args if a != "--"]
    cmd = [sys.executable, "-m", "carscraper", "--db", args.db, "run", "--no-caffeinate", *run_args]
    awake = _keep_awake()
    try:
        return supervise(cmd, progress, Path(args.db).with_name("run.log"), event=event, on_done=on_done)
    finally:
        if awake:
            awake.terminate()


def cmd_refresh(args) -> int:
    store = Store(args.db)
    store.reset_crawl()
    _log("Cautarea o ia de la capat la urmatoarea rulare; anunturile salvate raman.")
    return 0


def cmd_export(args) -> int:
    store = Store(args.db)
    n = export(store, args.output, fmt=args.format, only_full=args.only_full, min_19=args.min_19)
    size = Path(args.output).stat().st_size / 1e6
    _log(f"Am scris {n} anunturi in {args.output} ({size:.1f} MB)")
    return 0


def cmd_stats(args) -> int:
    store = Store(args.db)
    levels = store.count_by_level()
    _log(f"Anunturi: {store.count()} (complete {levels.get('full', 0)}, 19/20 campuri {levels.get('attrs', 0)}, "
         f"doar date de baza {levels.get('basic', 0)}, disparute {levels.get('gone', 0)})")
    for country, n in store.count_by_country().items():
        _log(f"  {country}: {n}")
    rows = store.db.execute("SELECT state, COUNT(*) FROM slices GROUP BY state").fetchall()
    done = store.db.execute("SELECT COUNT(*) FROM pages_done").fetchone()[0]
    _log("Cautari: " + ", ".join(f"{state}={n}" for state, n in rows) + f" | pagini terminate: {done}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="carscraper", description="Scraper AutoScout24 in formatul Autovit")
    p.add_argument("--db", default=DEFAULT_DB, help=f"baza de date SQLite (implicit {DEFAULT_DB})")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="porneste sau continua scrapingul")
    r.add_argument("--mode", choices=MODES, default="auto",
                   help="auto = intai catalogul (date de baza, rapid), apoi detaliile fiecarui anunt")
    r.add_argument("--countries", type=_countries, default=list(COUNTRIES),
                   help="ex. DE,IT,NL (implicit: toate 8 tarile)")
    r.add_argument("--max-listings", type=int, default=None, help="opreste dupa atatea anunturi noi")
    r.add_argument("--rate", type=float, default=None,
                   help=f"cereri pe secunda la pornire (implicit: ultima viteza care a mers, sau {DEFAULT_RATE})")
    r.add_argument("--max-rate", type=float, default=8.0, help="pana unde poate creste singura viteza")
    r.add_argument("--start-concurrency", type=int, default=10)
    r.add_argument("--max-concurrency", type=int, default=25)
    r.add_argument("--no-caffeinate", action="store_true", help="nu impiedica Mac-ul sa intre in sleep")
    r.set_defaults(func=cmd_run)

    sv = sub.add_parser("supervise", help="ruleaza 'run' nesupravegheat, cu repornire automata")
    sv.add_argument("--forever", action="store_true",
                    help="dupa ce termina tot, reia cautarea ca sa prinda anunturile noi")
    sv.add_argument("run_args", nargs=argparse.REMAINDER, help="optiuni pentru run, dupa --")
    sv.set_defaults(func=cmd_supervise)

    rf = sub.add_parser("refresh", help="reia cautarea de la zero (pastreaza anunturile salvate)")
    rf.set_defaults(func=cmd_refresh)

    e = sub.add_parser("export", help="scrie anunturile salvate in format cars.json")
    e.add_argument("-o", "--output", default="data/autoscout24.json")
    e.add_argument("--format", choices=["json", "jsonl"], default="json")
    e.add_argument("--only-full", action="store_true", help="doar anunturile cu toate cele 20 de campuri")
    e.add_argument("--min-19", action="store_true", help="doar anunturile cu cel putin 19 din 20 de campuri")
    e.set_defaults(func=cmd_export)

    s = sub.add_parser("stats", help="cate anunturi sunt salvate")
    s.set_defaults(func=cmd_stats)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
