"""PNG status card for the 19/20 export (table `cars`).

uv run --no-project --with matplotlib python scripts/stats_card_19.py [export.db] [out.png]
"""

import sqlite3
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402
from matplotlib.transforms import Bbox  # noqa: E402

DB = sys.argv[1] if len(sys.argv) > 1 else "export/autoscout24_19din20.db"
OUT = sys.argv[2] if len(sys.argv) > 2 else "export/stats_19din20.png"
LABEL = {"Franta": "Franța"}
FIELDS = [("make", "marcă"), ("price_euro", "preț"), ("country", "țară"), ("body_type", "caroserie"),
          ("doors", "uși"), ("color", "culoare"), ("fuel_type", "combustibil"), ("version", "versiune"),
          ("transmission", "cutie"), ("mileage_km", "km"), ("power_hp", "putere"), ("model", "model"),
          ("year", "an"), ("engine_displacement_cm3", "capacitate"), ("features", "dotări"),
          ("generation", "generație")]


def fmt(n):
    return f"{n:,}".replace(",", ".")


def pct(x):
    return (f"{x:.2f}" if 0 < x < 1 else f"{x:.1f}").replace(".", ",") + "%"


db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
q = lambda sql: db.execute(sql).fetchall()  # noqa: E731
total = q("SELECT COUNT(*) FROM cars")[0][0]
by_country = q("SELECT country, COUNT(*) FROM cars GROUP BY country ORDER BY 2 DESC")
nulls = q("SELECT " + ", ".join(f"SUM({f} IS NULL OR {f} = '')" for f, _ in FIELDS) + " FROM cars")[0]
fill = [(lbl, 100 * (1 - n / total)) for (_, lbl), n in zip(FIELDS, nulls)]
median = q(f"SELECT price_euro FROM cars ORDER BY price_euro LIMIT 1 OFFSET {total // 2}")[0][0]
top = q("SELECT make, COUNT(*) FROM cars GROUP BY make ORDER BY 2 DESC LIMIT 3")
odd = q("SELECT COUNT(*) FROM cars WHERE price_euro < 100 OR price_euro > 2000000 OR mileage_km > 1500000")[0][0]
with_features = q("SELECT COUNT(*) FROM cars WHERE features IS NOT NULL")[0][0]

INK, SUB, MUTED, LINE, CARD = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#f5f4f0"
BLUE, OK_BG, OK_FG, WN_BG, WN_FG, NO_BG, NO_FG = ("#2a78d6", "#e3f2e1", "#1e6b1e", "#fdf0d5", "#8a5a00",
                                                   "#fbe4e4", "#9b2c2c")
plt.rcParams["font.family"] = ["Arial", "Helvetica", "DejaVu Sans"]
W, H = 1080, 1500
fig = plt.figure(figsize=(W / 100, H / 100), dpi=100)
ax = fig.add_axes([0, 0, 1, 1])
ax.set_xlim(0, W)
ax.set_ylim(H, 0)
ax.axis("off")
fig.patch.set_facecolor("white")


def box(x, y, w, h, color, r=14):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad=0,rounding_size={r}", fc=color, ec="none"))


def text(x, y, s, size, color=INK, weight="normal", ha="left", va="baseline"):
    return ax.text(x, y, s, fontsize=size, color=color, fontweight=weight, ha=ha, va=va, zorder=3)


def width(s, size):
    t = ax.text(0, 0, s, fontsize=size)
    w = t.get_window_extent(fig.canvas.get_renderer()).width
    t.remove()
    return w


L, R = 70, W - 70
text(L, 95, "AutoScout24  ·  setul 19/20 câmpuri  ·  8 țări", 17, SUB)
text(L, 205, fmt(total), 82, INK, "bold")
text(L, 255, "anunțuri cu 19 din 20 de câmpuri, format Autovit", 19, SUB)

kpis = [("Duplicate", "0"), ("Preț median", f"{fmt(median)} €"),
        ("Top marcă", f"{'VW' if top[0][0] == 'Volkswagen' else top[0][0]} · {round(top[0][1] / 1000)}k"),
        ("Cu dotări (20/20)", fmt(with_features))]
kw = (R - L - 3 * 16) / 4
for i, (k, v) in enumerate(kpis):
    x = L + i * (kw + 16)
    box(x, 295, kw, 105, CARD)
    text(x + 20, 333, k, 14, SUB)
    text(x + 20, 378, v, 25, INK, "bold")

text(L, 470, "Pe țări", 18, SUB, "bold")
y = 515
biggest = by_country[0][1]
for c, n in by_country:
    text(L, y + 6, LABEL.get(c, c), 18, INK, va="center")
    bx, bw = L + 165, 560
    box(bx, y - 7, bw * n / biggest, 16, BLUE, r=8)
    text(R, y + 6, f"{fmt(n)}  ·  {pct(100 * n / total)}", 17, INK, ha="right", va="center")
    y += 50

y += 30
text(L, y, "Câmpuri completate (nu sunt null)", 18, SUB, "bold")
x, y = L, y + 28
for lbl, p in fill:
    s = f"{lbl} {pct(p) if p < 99.95 else '100%'}"
    w = width(s, 16) + 28
    if x + w > R:
        x, y = L, y + 52
    bg, fg = (OK_BG, OK_FG) if p >= 90 else (WN_BG, WN_FG) if p >= 50 else (NO_BG, NO_FG)
    box(x, y, w, 40, bg, r=10)
    text(x + w / 2, y + 21, s, 16, fg, ha="center", va="center")
    x += w + 10

y += 95
ax.plot([L, R], [y - 32, y - 32], color=LINE, lw=1)
text(L, y, "Lipsesc: dotări (doar pe pagina fiecărui anunț) · generație (AutoScout24 nu o are).", 15, MUTED)
text(L, y + 32, f"Valori suspecte: {fmt(odd)} ({pct(100 * odd / total)}) — preț sub 100 € / peste 2 mil. €, "
                "km peste 1,5 mil.", 15, MUTED)
text(L, y + 64, "Top mărci: " + " · ".join(f"{m} {fmt(n)}" for m, n in top), 15, MUTED)
bottom = y + 64 + 70
fig.savefig(OUT, dpi=100, facecolor="white", bbox_inches=Bbox([[0, (H - bottom) / 100], [W / 100, H / 100]]))
print(OUT, total)
