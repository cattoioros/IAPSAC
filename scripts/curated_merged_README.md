# Merged curated car listings

The curated Autovit.ro (Romania) and AutoScout24 (8 EU countries) datasets in one, produced by
`scripts/merge_curated.py`. Same columns as `data/curated/` and `data/curated_autoscout/`; the
`source` column says which site each row comes from. What every column and flag means is in
`scripts/curated_data_README.md`; AutoScout specifics are in `scripts/curated_autoscout_README.md`.
Everything in this folder is regenerated (overwritten) on each run, and `data/` is git-ignored.

## How to run

Same setup as the other scripts (Java 17+, `pip install pyspark==4.1.1`). From the `IAPSAC`
folder, curate both sites first, then merge:

```
source .venv/bin/activate
python scripts/curate_cars.py --input ../cars.json
python scripts/curate_autoscout.py --input ../autoscout24_19din20.json
python scripts/merge_curated.py
```

If one of the curated folders is missing, the merge script says which script to run.
`--out` changes the output folder (default: `data/curated_merged`).

## Files

| File | Contents |
|---|---|
| `listings_clean.*` | All cars from both sites that have an equipment list |
| `train.*` | The subset for the price model (same rules as before) |
| `feature_counts.csv` | For each equipment option: how many cars from each site have it, and whether it appears on both sites |

`.json` and `.csv` for reading, Parquet folders for code, as in the other folders.

## What the merge changes

- **Only cars with an equipment list.** Autovit listings without one are dropped (the AutoScout data already has only cars with one).
- **Body types.** AutoScout does not separate saloons, hatchbacks and small cars consistently: the same model appears under both of its categories. So `sedan`, `compact`, `city_car` and `small_car` all become `sedan_hatchback`. The other types (`suv`, `estate`, `mpv`, `coupe`, `convertible`, `commercial`, `other`) are unchanged.
- **Equipment names.** AutoScout's option names are translated to Autovit's using `scripts/feature_name_map.csv`, e.g. `Tempomat` → `Pilot automat` and `Jante aluminiu (18")` → `Jante aliaj 18`. About two thirds of AutoScout's options have an Autovit equivalent; the rest keep their AutoScout names. Rows marked "approximate" in the CSV are close but not exact equivalents. Edit the CSV to add or change translations.

## Things to keep in mind when training

- **Options listed by only one site.** Autovit lists some options that AutoScout has no box for (e.g. `Geamuri electrice spate`), so they are always missing on AutoScout cars, and the reverse. Such options say more about the site than about the car. Prefer options with `on_both_sites = yes` in `feature_counts.csv`, or give the model the `source` column.
- **`year`** is the production year on Autovit and the registration year on AutoScout.
- **Registration dates** from AutoScout are always the 1st of the month.
- **Prices differ by country.** Use `country` as an input.
- **The AutoScout part is not a random sample** of the market (see its README).
- **Plug-in hybrids:** a few AutoScout plug-ins are labelled `hybrid` because their name gives no hint.
