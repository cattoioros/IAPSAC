"""Render a shareable PNG status card from data/autoscout24.db.

uv run --with matplotlib python scripts/stats_card.py [db] [out.png]
"""

import sqlite3
import sys
import time

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

DB = sys.argv[1] if len(sys.argv) > 1 else "data/autoscout24.db"
OUT = sys.argv[2] if len(sys.argv) > 2 else "data/stats_card.png"
CY_NAME = {"D": "Germania", "I": "Italia", "NL": "Olanda", "B": "Belgia", "F": "Franta",
           "A": "Austria", "E": "Spania", "L": "Luxemburg"}
LABEL = {"Franta": "Franța"}
SHORT = {"Volkswagen": "VW", "Mercedes-Benz": "Mercedes"}
FIELDS = [("make", "marcă"), ("price_euro", "preț"), ("country", "țară"), ("fuel_type", "combustibil"),
          ("transmission", "cutie"), ("mileage_km", "km"), ("power_hp", "putere"), ("model", "model"),
          ("year", "an"), ("engine_displacement_cm3", "capacitate")]


def fmt(n):
    return f"{n:,}".replace(",", ".")


def pct(x):
    return (f"{x:.2f}" if 0 < x < 1 else f"{x:.1f}").replace(".", ",") + "%"


db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
q = lambda sql: db.execute(sql).fetchall()  # noqa: E731
LIVE = "level != 'gone'"
total, full, attrs = q(f"SELECT COUNT(*), SUM(level='full'), SUM(level='attrs') FROM listings WHERE {LIVE}")[0]
site = {CY_NAME[k.split("|")[0]]: t for k, t in q("SELECT key, total FROM slices WHERE key LIKE '%|-|-|-'")}
have = dict(q(f"SELECT country, COUNT(*) FROM listings WHERE {LIVE} GROUP BY country"))
site_total = sum(site.values())
nulls = q("SELECT " + ", ".join(f"SUM(json_extract(record,'$.{f}') IS NULL)" for f, _ in FIELDS)
          + f" FROM listings WHERE {LIVE}")[0]
fill = [(lbl, 100 * (1 - (n or 0) / total)) for (_, lbl), n in zip(FIELDS, nulls)]
dups = q("SELECT COUNT(*) - COUNT(DISTINCT listing_id) FROM listings")[0][0]
odd = q("SELECT SUM(json_extract(record,'$.price_euro') < 100 OR json_extract(record,'$.price_euro') > 2000000 "
        f"OR json_extract(record,'$.mileage_km') > 1500000) FROM listings WHERE {LIVE}")[0][0] or 0
median = q(f"SELECT json_extract(record,'$.price_euro') FROM listings WHERE {LIVE} "
           f"ORDER BY json_extract(record,'$.price_euro') LIMIT 1 OFFSET {total // 2}")[0][0]
top_make, top_n = q(f"SELECT json_extract(record,'$.make'), COUNT(*) FROM listings WHERE {LIVE} "
                    "GROUP BY 1 ORDER BY 2 DESC LIMIT 1")[0]

INK, SUB, MUTED, LINE, CARD = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#f5f4f0"
BLUE, OK_BG, OK_FG, WN_BG, WN_FG = "#2a78d6", "#e3f2e1", "#1e6b1e", "#fdf0d5", "#8a5a00"
plt.rcParams["font.family"] = ["Arial", "Helvetica", "DejaVu Sans"]
W, H = 1080, 1350
fig = plt.figure(figsize=(W / 100, H / 100), dpi=100)
ax = fig.add_axes([0, 0, 1, 1])
ax.set_xlim(0, W)
ax.set_ylim(H, 0)
ax.axis("off")
fig.patch.set_facecolor("white")


def box(x, y, w, h, color, r=14):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad=0,rounding_size={r}",
                                fc=color, ec="none"))


def text(x, y, s, size, color=INK, weight="normal", ha="left", va="baseline"):
    return ax.text(x, y, s, fontsize=size, color=color, fontweight=weight, ha=ha, va=va, zorder=3)


def width(s, size):
    t = ax.text(0, 0, s, fontsize=size)
    w = t.get_window_extent(fig.canvas.get_renderer()).width
    t.remove()
    return w


L, R = 70, W - 70
text(L, 95, f"AutoScout24  ·  8 țări  ·  {time.strftime('%d.%m.%Y, %H:%M')}", 17, SUB)
text(L, 205, fmt(total), 82, INK, "bold")
text(L, 255, f"anunțuri strânse din {fmt(site_total)} pe site  ·  {pct(100 * total / site_total)}", 19, SUB)

kpis = [("Duplicate", fmt(dups)), ("Preț median", f"{fmt(median)} €"),
        ("19/20 câmpuri", fmt((attrs or 0) + (full or 0))), ("Complete 20/20", fmt(full or 0))]
kw = (R - L - 3 * 16) / 4
for i, (k, v) in enumerate(kpis):
    x = L + i * (kw + 16)
    box(x, 295, kw, 105, CARD)
    text(x + 20, 333, k, 14, SUB)
    text(x + 20, 378, v, 25, INK, "bold")

text(L, 470, "Acoperire pe țări (strânse / total pe site)", 18, SUB, "bold")
rows = sorted(site, key=lambda c: -have.get(c, 0) / site[c])
y = 515
for c in rows:
    p = min(100.0, 100 * have.get(c, 0) / site[c])
    text(L, y + 6, LABEL.get(c, c), 18, INK, va="center")
    bx, bw = L + 165, 520
    box(bx, y - 7, bw, 16, CARD, r=8)
    box(bx, y - 7, max(16, bw * p / 100), 16, BLUE, r=8)
    text(R, y + 6, f"{fmt(have.get(c, 0))}  ·  {pct(p)}", 17, INK, ha="right", va="center")
    y += 50

y += 30
text(L, y, "Câmpuri completate (nu sunt null)", 18, SUB, "bold")
x, y = L, y + 28
for lbl, p in fill:
    s = f"{lbl} {pct(p) if p < 99.95 else '100%'}"
    w = width(s, 16) + 28
    if x + w > R:
        x, y = L, y + 52
    good = p >= 90
    box(x, y, w, 40, OK_BG if good else WN_BG, r=10)
    text(x + w / 2, y + 21, s, 16, OK_FG if good else WN_FG, ha="center", va="center")
    x += w + 10

y += 95
ax.plot([L, R], [y - 32, y - 32], color=LINE, lw=1)
text(L, y, f"Valori suspecte: {fmt(odd)} ({pct(100 * odd / total)}) — preț sub 100 € sau peste 2 mil. €, "
           "km peste 1,5 mil.", 15, MUTED)
text(L, y + 32, "Capacitate lipsă = mai ales electrice · An lipsă = mașini noi neînmatriculate.", 15, MUTED)
text(L, y + 64, "19/20 = + caroserie, culoare, uși (din filtre) · Dotări: în etapa de detalii.", 15, MUTED)
from matplotlib.transforms import Bbox  # noqa: E402

bottom = y + 64 + 70
fig.savefig(OUT, dpi=100, facecolor="white", bbox_inches=Bbox([[0, (H - bottom) / 100], [W / 100, H / 100]]))
print(OUT, total, full)
