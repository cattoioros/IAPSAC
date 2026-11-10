# Curated AutoScout24 listings

Cleaned version of the AutoScout24 scrape (`autoscout24_*.json`), produced by
`scripts/curate_autoscout.py`. It has the **same columns, categories, flags and training rules**
as the Autovit data in `data/curated/`. `scripts/curated_data_README.md` explains every
column and flag; this file only covers what is different. Everything in this folder is
regenerated (overwritten) on each run, and `data/` is git-ignored.

**Only listings with an equipment list (`features`) are kept**, as for Autovit (the switch is
`KEEP_ONLY_WITH_FEATURES` in `scripts/curate_cars.py`). In the first scrape that was
about 0.3% of the listings, almost all from the start of the scrape, so they are not a random
sample of the market (many classics, microcars, vans and Porsches; few BMWs and Toyotas).

## How to run

Needs the same setup as `curate_cars.py`: Java 17+ and `pip install pyspark==4.1.1`.
From the `IAPSAC` folder:

```
source .venv/bin/activate
python scripts/curate_autoscout.py --input ../autoscout24_19din20.json
```

- `--input` (required): path to the AutoScout24 scrape, relative to where you run the command
- `--out`: output folder (default: `data/curated_autoscout` in the repo)

The run ends with the same report as `curate_cars.py`. For errors, see "Common problems" in
`scripts/curated_data_README.md`.

## Files

Same as `data/curated/`: `listings_clean.*` (every kept listing) and `train.*` (the subset for
the price model), each as `.json`, `.csv` and a Parquet folder. The `.json`/`.csv` copies are
skipped if a table has more than 200,000 rows, because they would be about 1 GB each.

## How AutoScout24 differs, and what the script does about it

| | AutoScout24 scrape | Curated data |
|---|---|---|
| Fuel | `Motorina`, `Electric/Benzina`, `GPL`, ... | `diesel`, `hybrid` or `plugin_hybrid`, `petrol_lpg`, ... Plug-ins are recognised from the model and version text (`PHEV`, `eHybrid`, `330e`, `NX 450h+`, `P400e`), so a few with no hint in the name (e.g. Citroën `Hybrid 225`) stay `hybrid`. Rare extras: `ethanol`, `hydrogen`, `other`. |
| Gearbox | `Semi-automata` | `automatic` |
| Body type | Coarser than Autovit: `Berlina` covers saloons and hatchbacks, `Autoturism mic` city and small cars | `sedan`, `small_car`, ...; vans and trucks become `commercial`, `Altele` becomes `other` |
| Colour | `Argintiu`, `Violet`, `Bronz` | `silver`, `other` |
| Registration date | Month and year only (`februarie 2023`) | The 1st of that month |
| `year` | Registration year (Autovit's is the production year) | Unchanged |
| Names | `MINI`, `SEAT`; BMW `320`; Porsche `992`; `Altele` for unknown | `Mini`, `Seat`; `Seria 3` (with `320` kept in `version`); `911` with `generation_code` `992`; null |
| Country | 8 countries | Unchanged (`Germania`, `Olanda`, ...) |
| `features` | AutoScout's own 154 names | Kept as scraped (different names from Autovit's; `scripts/merge_curated.py` translates them). `wheel_size_in` comes from `Jante aluminiu (18")`. |
| `ev_range_km` | Not on the site | Empty |
| `url` | Present | Not included, so the columns match the Autovit data |

The Autovit fixes for values in the wrong column do not apply: this scraper does not have
that problem.

## Extra `dq_flags`

| Flag | Meaning |
|---|---|
| `unknown_make_or_model_dropped` | The site's `Altele` ("other") or `Oldtimer` placeholder, set to null |
| `model_renamed_to_autovit_style` | BMW `320` → `Seria 3`, Porsche `992` → `911` |
| `plugin_hybrid_from_version` | Hybrid recognised as a plug-in from the model/version text |
| `price_out_of_range`, `power_out_of_range`, `mileage_out_of_range`, `displacement_out_of_range` | Placeholder or typo (€1, 999,999 km, 1 hp, 0 cm³...), set to null |
| `ev_displacement_dropped` | Electric car with an engine displacement, set to null |

`reg_date_is_jan1_placeholder` and `reg_date_two_digit_year` never appear here.

## Who is left out of `train`

The same rules as for Autovit, plus `commercial_vehicle` (vans and trucks). Values that were
out of range are null, so those rows show up as `missing_<column>`.
