# Curated car listings

Cleaned version of the Autovit.ro scrape (`cars.json`), produced by `scripts/curate_cars.py`.
Everything in this folder is regenerated (and overwritten) each time the script runs, and
`data/` is git-ignored, so none of it is committed.

## How to run the script

### What you need

- **Java 17 or newer.** Check with `java -version`. On macOS: `brew install openjdk@21`.
- **Python 3.10+ with PySpark:** `pip install pyspark==4.1.1` (4.1.1 is required on Python 3.14).
- **The full scrape `cars.json`.** It is not in git. The `cars.json` inside the repo is only a
  2-listing example.
- Optional, to open the Parquet folders from Python: `pip install pandas pyarrow`.

### One-time setup (in the `IAPSAC` folder)

```
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark==4.1.1
```

### Run

```
cd IAPSAC
source .venv/bin/activate
python scripts/curate_cars.py --input ../cars.json
```

- `--input`: path to the scrape, relative to where you run the command (default: `cars.json` in the repo root)
- `--out`: output folder (default: `data/curated` in the repo)

It takes about 20 seconds. Spark prints some `WARN` lines, which are normal. At the end
it prints a report with row counts and how many rows each fix touched.

### Common problems

| Error | Fix |
|---|---|
| `can't open file '.../scripts/curate_cars.py'` | You are not in the `IAPSAC` folder. `cd IAPSAC` first, or from the folder above run `python IAPSAC/scripts/curate_cars.py --input cars.json`. |
| `Unable to locate a Java Runtime` or `Java gateway process exited` | Java is missing or too old. After `brew install openjdk@21`, run `export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"`. |
| `ModuleNotFoundError: No module named 'pyspark'` | The virtual environment is not active: `source .venv/bin/activate`. |
| `Path does not exist: .../cars.json` | `--input` points to the wrong place. Give the path to the full scrape. |

## Files

| File | Contents |
|---|---|
| `listings_clean.*` | **The curated dataset**: every listing from `cars.json` that has an equipment list, fixed, minus duplicates |
| `train.*` | The subset used to train the price model, with model-ready columns |

Each table comes in three formats:

- `.json`: same layout as `cars.json`; open in VS Code or any editor
- `.csv`: open in Excel / Numbers; lists (e.g. `features`) are joined with `"; "`.
  If Excel puts everything in one column, use *Data → From Text/CSV* and pick comma.
- folder (Parquet): for code, e.g. `pd.read_parquet("data/curated/train")`

## What was done to the data

- **Dropped:** failed scrapes (no data besides the id), listings without an equipment list, and duplicate listings of the same car (the most recently seen one is kept). Equipment affects the price, and a missing list can't be told apart from a car with no equipment. To keep those listings, set `KEEP_ONLY_WITH_FEATURES = False` at the top of `scripts/curate_cars.py`; this also applies to the AutoScout24 script.
- **Fixed:** values the scraper put in the wrong column. It reads the page's top details by position, so a missing detail shifts the rest. For electric cars, horsepower and range are moved to the right columns.
- **Standardised:** categories translated to English (`Benzina` → `petrol`, `Automata` → `automatic`), dates as `YYYY-MM-DD`, `SsangYong` → `KGM`.
- **Recorded:** every change is listed in the row's `dq_flags`.

## Columns worth knowing

| Column | Meaning |
|---|---|
| `dq_flags` | Data-quality flags: what was fixed or looks suspicious in this row. Empty = untouched. |
| `train_exclude_reasons` | Why the row is not in `train`. Empty = it is in `train`. |
| `duplicate_listings` | How many other listings of this same car were removed |
| `model_family` | Model grouped across variants (`A4 Avant` → `A4`, Mercedes `A 180` → `A`) |
| `age_years` | Age from `year` (mid-year build assumed). Not from the registration date, which for imports is often years later. |
| `km_per_year`, `is_new` | Mileage per year of age; `is_new` = 100 km or less (dealer stock) |
| `ev_range_km` | Electric range; only for electric cars |
| `features_known` | `false` = the listing had no equipment list (unknown, not "no equipment"). Always `true` unless `KEEP_ONLY_WITH_FEATURES` is turned off. |
| `log_price` | Natural log of `price_euro`, so a model weighs errors in % rather than euros. Euros = `exp(log_price)`. |

## What `dq_flags` can contain

| Flag | Meaning |
|---|---|
| `ev_power_and_range_unswapped` | Electric car: hp and range moved to the correct columns |
| `shifted_columns_realigned` | Values had slid one column over; moved back |
| `power_taken_from_displacement` | Horsepower was in the engine-size field |
| `fuel_inferred_from_version` | Fuel missing; worked out from the version text |
| `ev_transmission_assumed_automatic` | Gearbox missing on an electric car |
| `make_alias_merged`, `model_spelling_unified` | Name standardised |
| `generation_contradicts_year` | Generation didn't match the year, so it was removed |
| `duplicates_collapsed` | Another copy of this car was removed |
| `reg_date_two_digit_year` | Year like `14` read as 2014 |
| `reg_date_is_jan1_placeholder` | Registration date is 1 January: probably only the year was known |
| `implausible_km_per_year` | Mileage too high for the car's age |
| `features_missing` | No equipment list (only appears if `KEEP_ONLY_WITH_FEATURES` is turned off) |
| `unrecognised_*`, `reg_date_unparseable_or_future`, `year_out_of_range` | Safety nets for new scrapes; if they appear, the site changed and the script needs a new rule |

## Who is left out of `train`

Rows stay in `listings_clean` but are excluded from `train` when they are: missing a required
field (price, make, model, year, mileage, power, fuel, gearbox, body), exotic or over €250,000,
classic (30+ years), microcars (under 50 hp), or have implausible mileage (over 80,000 km/year).
The thresholds are constants at the top of `scripts/curate_cars.py`.
