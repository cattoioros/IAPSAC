"""
Curate the AutoScout24 scrape (autoscout24_*.json) into the same layout as the Autovit data
from curate_cars.py: same columns, same English categories, same flags and training rules.
Only listings that come with an equipment list (`features`) are kept.

Writes to data/curated_autoscout/ (git-ignored):
  listings_clean   every kept listing, cleaned; dq_flags / train_exclude_reasons as in curate_cars.py
  train            rows with no exclusion reason, model-ready columns
each as Parquet (folder) + .json + .csv, plus README.md.

The shared steps (duplicates, derived columns, flags, training rules, output layout) are imported
from curate_cars.py, so both datasets are treated the same way. This file only holds what is
specific to AutoScout24: its vocabulary, date format, naming and value checks.

Usage (from the repo root):
  python scripts/curate_autoscout.py --input ../autoscout24_19din20.json

Needs Java 17+ and pyspark >= 3.5 (pyspark 4.1.1+ on Python 3.14), like curate_cars.py.
"""

import argparse
import json
import re
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

import curate_cars as base
from curate_cars import as_map, is_true, to_int, with_columns

# ---------------------------------------------------------------------------
# Settings (tune here)
# ---------------------------------------------------------------------------
# Whether listings without an equipment list are dropped is set by KEEP_ONLY_WITH_FEATURES in
# curate_cars.py, so both sites follow the same rule. In this scrape only the first ~18 minutes
# opened each car's page; after that the equipment list is empty for every listing.
READABLE_COPIES_MAX_ROWS = 200_000   # above this the .json/.csv copies would be ~1 GB, so they are skipped

# Values outside these ranges are placeholders or typos in this scrape: set to null and flagged.
PRICE_RANGE_EUR = (100, 5_000_000)       # listings at EUR 1 and EUR 28 million exist
POWER_RANGE_HP = (2, 1_600)              # "1 hp" placeholders; a 1,899 hp microcar
MILEAGE_MAX_KM = 999_998                 # 999,999 km is used for "unknown"
DISPLACEMENT_RANGE_CM3 = (50, 10_000)    # 0 and 99,900 cm3 exist

# ---------------------------------------------------------------------------
# AutoScout24 vocabulary -> the English values used for the Autovit data
# ---------------------------------------------------------------------------
FUEL = {"Benzina": "petrol", "Motorina": "diesel", "Electric": "electric",
        "Electric/Benzina": "hybrid", "Electric/Motorina": "hybrid",   # plug-ins are detected below
        "GPL": "petrol_lpg", "Gaz petrolier compresat": "petrol_cng",
        "Etanol": "ethanol", "Hidrogen": "hydrogen", "Diverse": "other"}
TRANSMISSION = {"Automata": "automatic", "Manuala": "manual", "Semi-automata": "automatic"}
# AutoScout's body types are coarser than Autovit's: 'Berlina' covers saloons and hatchbacks,
# 'Autoturism mic' covers city cars and small cars.
BODY = {"Off-road/4x4/Pick-up": "suv", "Berlina": "sedan", "Combi": "estate", "Autoturism mic": "small_car",
        "Van": "mpv", "Cabriolet / Roadster": "convertible", "Masina sport / Coupe": "coupe",
        "Altele": "other", "Autoutilitare": "commercial", "Duba": "commercial",
        "Duba cu acoperis inalt": "commercial"}
COMMERCIAL_PATTERN = r"(?i)camion|duba|sasiu|bena|basculare|autoutilitar"   # trucks, chassis cabs...
COLOR = {"Negru": "black", "Alb": "white", "Gri": "grey", "Albastru": "blue", "Argintiu": "silver",
         "Rosu": "red", "Verde": "green", "Galben": "yellow_gold", "Auriu": "yellow_gold", "Maro": "brown",
         "Bej": "beige", "Portocaliu": "orange", "Violet": "other", "Bronz": "other"}

# AutoScout has one hybrid category; Autovit separates plug-ins. Detected from model + version text:
# 'PHEV', 'Plug-in', 'eHybrid', '18,4 kWh', 'NX 450h+', '330e', 'E 300 e', 'P400e', 'R-EV', 'EM-i'...
# Text-based, so it misses plug-ins whose name gives no hint (e.g. Citroen 'Hybrid 225').
PLUGIN_PATTERN = (r"(?i)phev|plug-?in|e-?hybrid|kwh|recharge|em-i|dm-i|range extender|r-ev|reev"
                  r"|\b\d{2,3}h\+|\b\d{3} ?x?e\b|\bp\d{3}e\b")

MAKE_SPELLING = {"BYD": "Byd", "CUPRA": "Cupra", "Lynk & Co": "Lynk&Co", "MINI": "Mini",
                 "SEAT": "Seat", "XEV": "Xev", "smart": "Smart"}   # Autovit's spelling
NOT_A_MAKE = ["Altele", "Oldtimer"]     # the site's "other" / "classic car" placeholders
UNKNOWN_MODEL = "Altele"
BMW_NUMBERED = r"^([1-8])\d{2}[a-zA-Z]*$"   # '320' -> 'Seria 3', as Autovit names it
PORSCHE_911_CODES = ["930", "964", "993", "996", "997", "991", "992"]   # 911 generations used as model

REG_DATE_PATTERN = r"^(\p{L}+) (\d{4})$"   # 'februarie 2023' (no day)

# Same fields as the Autovit scrape plus `url`; `year` is sometimes a string ("2026").
RAW_SCHEMA = T.StructType(
    [T.StructField("year", T.StringType()) if f.name == "year" else f for f in base.RAW_SCHEMA.fields]
    + [T.StructField("url", T.StringType())])


# ---------------------------------------------------------------------------
# AutoScout-specific steps (each DataFrame -> DataFrame)
# ---------------------------------------------------------------------------
def json_array_to_lines(src: str, dst: str, keep=None) -> int:
    """Stream a JSON array file into JSON Lines (one record per line) without loading it all.

    Spark cannot split a file that is one big JSON array: it parses it in a single task, which
    takes over 10 minutes for the 900 MB scrape. JSON Lines is read in parallel in seconds.
    `keep(record)` can skip records on the way. Returns how many records the array had."""
    decoder, skip = json.JSONDecoder(), re.compile(r"[\s,]*")
    n = 0
    with open(src, encoding="utf-8") as fin, open(dst, "w", encoding="utf-8") as fout:
        buf, pos, eof = fin.read(1 << 20).lstrip(), 1, False
        if not buf.startswith("["):
            raise ValueError(f"{src} is not a JSON array")
        while True:
            pos = skip.match(buf, pos).end()
            if pos < len(buf) and buf[pos] == "]":
                return n
            try:
                record, end = decoder.raw_decode(buf, pos)
            except json.JSONDecodeError:
                if eof:
                    raise
                more = fin.read(1 << 20)          # record continues past the buffer: read more
                eof = not more
                buf, pos = buf[pos:] + more, 0
                continue
            n += 1
            if keep is None or keep(record):
                fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            pos = end


def has_feature_list(record: dict) -> bool:
    """Cheap pre-filter while converting; junk-only lists are removed by keep_listings_with_features."""
    return bool(record.get("features"))


def read_raw(spark: SparkSession, json_lines_path: str) -> DataFrame:
    return spark.read.schema(RAW_SCHEMA).option("mode", "FAILFAST").json(json_lines_path)


def standardise_names(df: DataFrame) -> DataFrame:
    """Make spelling as on Autovit; the site's 'Altele' (other) placeholders become null;
    BMW '320' -> model 'Seria 3' (the number moves into the version if it is not there yet);
    Porsche '992' -> model '911' with generation 992."""
    make, model, version = F.col("make"), F.col("model"), F.col("version")
    unknown_make = is_true(make.isin(*NOT_A_MAKE))
    unknown_model = is_true(model == UNKNOWN_MODEL)

    bmw_series = F.regexp_extract(model, BMW_NUMBERED, 1)
    bmw_numbered = is_true((make == "BMW") & (bmw_series != ""))
    bmw_number = F.regexp_extract(model, r"^(\d{3})", 1)
    porsche_code = is_true((make == "Porsche") & model.isin(*PORSCHE_911_CODES))

    return with_columns(df, {
        "make": F.when(~unknown_make, F.coalesce(as_map(MAKE_SPELLING)[make], make)),
        "model": (F.when(unknown_model, F.lit(None).cast("string"))
                   .when(bmw_numbered, F.concat(F.lit("Seria "), bmw_series))
                   .when(porsche_code, F.lit("911"))
                   .otherwise(model)),
        "version": F.when(bmw_numbered & ~is_true(version.contains(bmw_number)), F.concat_ws(" ", model, version))
                    .otherwise(version),
        "_porsche_generation": F.when(porsche_code, model),
        "_f_unknown_make_or_model_dropped": unknown_make | unknown_model,
        "_f_model_renamed_to_autovit_style": bmw_numbered | porsche_code,
    })


def translate_categories(df: DataFrame) -> DataFrame:
    fuel_raw, trans_raw, color_raw = F.col("fuel_type"), F.col("transmission"), F.col("color")
    body_raw = F.trim(F.regexp_replace(F.col("body_type"), r"[\s ]+", " "))   # some have a no-break space

    fuel = as_map(FUEL)[fuel_raw]
    plugin = is_true((fuel == "hybrid") & F.concat_ws(" ", F.col("model"), F.col("version")).rlike(PLUGIN_PATTERN))
    trans = as_map(TRANSMISSION)[trans_raw]
    body = F.coalesce(as_map(BODY)[body_raw], F.when(body_raw.rlike(COMMERCIAL_PATTERN), F.lit("commercial")))
    color = as_map(COLOR)[color_raw]

    unrecognised = (is_true(fuel_raw.isNotNull() & fuel.isNull()) | is_true(trans_raw.isNotNull() & trans.isNull())
                    | is_true(body_raw.isNotNull() & body.isNull()))
    return with_columns(df, {
        "fuel_type": F.when(plugin, F.lit("plugin_hybrid")).otherwise(fuel),
        "transmission": trans,
        "body_type": body,
        "color": color,
        "_f_plugin_hybrid_from_version": plugin,
        "_f_unrecognised_category_dropped": unrecognised,
        "_f_unrecognised_color_dropped": is_true(color_raw.isNotNull() & color.isNull()),
    })


def parse_dates(df: DataFrame) -> DataFrame:
    """'februarie 2023' -> 2023-02-01 (the site gives no day). `year` is the registration year
    here (Autovit's is the production year); some years are strings ('2026') or typos (3026)."""
    seen = F.to_timestamp("first_seen_at")
    reg = F.trim(F.col("first_registration_date"))
    month = as_map(base.MONTHS)[F.lower(F.regexp_extract(reg, REG_DATE_PATTERN, 1))]
    yr = to_int(F.regexp_extract(reg, REG_DATE_PATTERN, 2))
    parsed = F.try_to_timestamp(F.format_string("%04d-%02d-01", yr, month), F.lit("yyyy-MM-dd")).cast("date")
    reg_date = F.when(parsed <= F.to_date(seen), parsed)

    year = to_int(F.trim(F.col("year")))
    year_ok = is_true(year.between(1900, F.year(seen) + 1))
    return with_columns(df, {
        "first_seen_at": seen,
        "first_registration_date": reg_date,
        "year": F.when(year_ok, year),
        "_f_reg_date_unparseable_or_future": is_true(reg.isNotNull() & reg_date.isNull()),
        "_f_year_out_of_range": F.col("year").isNotNull() & ~year_ok,
    })


def check_value_ranges(df: DataFrame) -> DataFrame:
    """Placeholders and typos (EUR 1, 999,999 km, 1 hp, 0 cm3) -> null, flagged. Electric cars have
    no engine displacement, as in the Autovit data."""
    price, power = F.col("price_euro"), F.col("power_hp")
    mileage, disp = F.col("mileage_km"), F.col("engine_displacement_cm3")
    is_ev = is_true(F.col("fuel_type") == "electric")

    price_ok = price.between(*PRICE_RANGE_EUR)
    power_ok = power.between(*POWER_RANGE_HP)
    mileage_ok = mileage.between(0, MILEAGE_MAX_KM)
    disp_ok = disp.between(*DISPLACEMENT_RANGE_CM3)
    return with_columns(df, {
        "price_euro": F.when(price_ok, price),
        "power_hp": F.when(power_ok, power),
        "mileage_km": F.when(mileage_ok, mileage),
        "engine_displacement_cm3": F.when(~is_ev & is_true(disp_ok), disp),
        "ev_range_km": F.lit(None).cast("int"),   # not published by AutoScout24
        "_f_price_out_of_range": is_true(~price_ok),
        "_f_power_out_of_range": is_true(~power_ok),
        "_f_mileage_out_of_range": is_true(~mileage_ok),
        "_f_displacement_out_of_range": ~is_ev & is_true(~disp_ok),
        "_f_ev_displacement_dropped": is_ev & disp.isNotNull(),
    })


def add_porsche_generation(df: DataFrame) -> DataFrame:
    return with_columns(df, {
        "generation_code": F.coalesce("_porsche_generation", "generation_code"),
    }).drop("_porsche_generation")


def exclude_commercial_vehicles(df: DataFrame) -> DataFrame:
    """Vans and trucks are a different market from cars: kept in listings_clean, not in train."""
    extra = F.when(F.col("body_type") == "commercial", F.array(F.lit("commercial_vehicle"))) \
             .otherwise(F.array().cast("array<string>"))
    return with_columns(df, {"train_exclude_reasons": F.concat("train_exclude_reasons", extra)})


# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", required=True, help="the AutoScout24 scrape, e.g. ../autoscout24_19din20.json")
    parser.add_argument("--out", default=str(base.REPO_ROOT / "data" / "curated_autoscout"))
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("curate-autoscout")
             .master("local[*]")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    tmp = tempfile.TemporaryDirectory()
    lines_path = str(Path(tmp.name) / "listings.jsonl")
    print("converting the JSON array to JSON Lines...")
    only_with_features = base.KEEP_ONLY_WITH_FEATURES
    raw_n = json_array_to_lines(args.input, lines_path, keep=has_feature_list if only_with_features else None)

    raw = read_raw(spark, lines_path)
    listings = (base.keep_listings_with_features(raw) if only_with_features else raw).cache()

    cleaned = (listings
               .transform(standardise_names)
               .transform(translate_categories)
               .transform(parse_dates)
               .transform(check_value_ranges)
               .transform(base.fill_from_context)
               .transform(base.parse_generation)
               .transform(add_porsche_generation)
               .transform(base.clean_features)
               .transform(base.normalise_make_model)
               .transform(base.add_derived_columns)
               .cache())

    clean = (cleaned
             .transform(base.drop_duplicate_listings)
             .transform(base.mark_training_rows)
             .transform(exclude_commercial_vehicles)
             .transform(base.collect_dq_flags)
             .select(*base.CLEAN_COLUMNS)
             .cache())
    train = base.build_training_set(clean)

    readable = clean.count() <= READABLE_COPIES_MAX_ROWS
    for name, table in [("listings_clean", clean), ("train", train)]:
        table.coalesce(1).write.mode("overwrite").parquet(f"{args.out}/{name}")
        if readable:
            base.write_readable_copies(table, f"{args.out}/{name}")
    shutil.copyfile(Path(__file__).with_name("curated_autoscout_README.md"), Path(args.out) / "README.md")

    dropped = {"without features dropped": raw_n - listings.count()} if only_with_features else {}
    base.print_report(raw_n, dropped, cleaned.count(), clean, train)
    print(f"written to {args.out}/:")
    print("  listings_clean/  train/              Parquet, for pandas / Spark")
    if readable:
        print("  listings_clean.json  train.json      open in an editor")
        print("  listings_clean.csv   train.csv       open in Excel / Numbers")
    else:
        print(f"  (no .json/.csv copies: more than {READABLE_COPIES_MAX_ROWS:,} rows)")
    print("  README.md                            what the files and columns mean")
    spark.stop()
    tmp.cleanup()


if __name__ == "__main__":
    main()
