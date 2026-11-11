"""
Curate the Autovit.ro scrape (cars.json) for a used-car price model.

Writes to data/curated/ (git-ignored), each table as Parquet + .json + .csv:
  listings_clean   cleaned, de-duplicated listings with an equipment list
                   (dq_flags: corrections made; train_exclude_reasons: why a row is not in train)
  train            rows with no exclusion reason, model-ready columns

Usage (from the repo root):
  python scripts/curate_cars.py --input ../cars.json

Needs Java 17+ and pyspark >= 3.5 (4.1.1+ on Python 3.14).
"""

import argparse
import csv
import json
import shutil
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------
# Thresholds (tune here)
# ---------------------------------------------------------------------------
KEEP_ONLY_WITH_FEATURES = True # drop listings without an equipment list
NEW_CAR_MAX_KM = 100           # up to this mileage a car counts as new
MAX_KM_PER_YEAR = 80_000       # more is likely a typo in year or mileage
CLASSIC_MIN_AGE_YEARS = 30     # collectibles, priced differently
MICROCAR_MAX_HP = 50           # licence-free quadricycles (Aixam, Ligier...)
PRICE_CAP_EUR = 250_000        # supercars: too few to learn from
EXOTIC_MAKES = ["Ferrari", "Lamborghini", "Rolls-Royce", "Bentley", "McLaren", "Aston Martin", "Lotus"]
REQUIRED_FOR_TRAINING = ["price_euro", "make", "model", "year", "mileage_km", "power_hp",
                         "fuel_type", "transmission", "body_type"]

# ---------------------------------------------------------------------------
# Site vocabulary (Romanian) -> English
# ---------------------------------------------------------------------------
FUEL = {"Diesel": "diesel", "Benzina": "petrol", "Hibrid": "hybrid", "Hibrid Plug-In": "plugin_hybrid",
        "Electric": "electric", "Benzina + GPL": "petrol_lpg", "Benzina + CNG": "petrol_cng"}
TRANSMISSION = {"Automata": "automatic", "Manuala": "manual"}
BODY = {"SUV": "suv", "Sedan": "sedan", "Combi": "estate", "Compacta": "compact", "Monovolum": "mpv",
        "Masina de oras": "city_car", "Masina mica": "small_car", "Coupe": "coupe", "Cabrio": "convertible"}
COLOR = {"Negru": "black", "Gri": "grey", "Alb": "white", "Albastru": "blue", "Argint": "silver",
         "Rosu": "red", "Maro": "brown", "Verde": "green", "Bej": "beige", "Galben/Auriu": "yellow_gold",
         "Portocaliu": "orange", "Alta culoare": "other"}
MONTHS = {"ianuarie": 1, "februarie": 2, "martie": 3, "aprilie": 4, "mai": 5, "iunie": 6, "iulie": 7,
          "august": 8, "septembrie": 9, "octombrie": 10, "noiembrie": 11, "decembrie": 12}

MAKE_ALIASES = {"SsangYong": "KGM"}  # rebranded in 2023
MERCEDES_CHASSIS = {"W124": "E", "W126": "S", "W140": "S", "W201": "190", "123 C": "W123"}
BODY_SUFFIX = r"(?i)\s+(4-door Coupe|Limuzina|Avant|Variant|Allroad|Allstreet|Sportback|Cabrio|Coup[eé]|3-portiere|Crossback)$"

# Fuel guessed from the version text when missing; first match wins.
FUEL_FROM_VERSION = [
    (r"(?i)kwh|\bbev\b", "electric"),
    (r"(?i)phev|dm-i|plug-in", "plugin_hybrid"),
    (r"(?i)\b(hev|hybrid)\b", "hybrid"),
    (r"(?i)\b(tdi|cdi|dci|crdi|tdci|hdi|bluehdi|d-4d|multijet)\b", "diesel"),
    (r"(?i)\b(tsi|tfsi|tce|puretech|ecoboost|t-gdi|mpi)\b", "petrol"),
]
AWD_PATTERN = r"(?i)4matic|xdrive|quattro|4motion|\bawd\b|4x4|all4|e-four|4wd|\bq4\b|allgrip"
GENERATION_PATTERN = r"^(.*?)\s*[\[(]\s*(\d{4})\s*-\s*(\d{4}|[Pp]rezent|Present)?"
REG_DATE_PATTERN = r"^(\d{1,2}) (\p{L}+) (\d{2}|\d{4})$"

RAW_SCHEMA = T.StructType([
    T.StructField("listing_id", T.StringType()),
    T.StructField("make", T.StringType()),
    T.StructField("model", T.StringType()),
    T.StructField("version", T.StringType()),
    T.StructField("year", T.IntegerType()),
    T.StructField("generation", T.StringType()),
    T.StructField("first_registration_date", T.StringType()),
    T.StructField("mileage_km", T.IntegerType()),
    T.StructField("fuel_type", T.StringType()),
    T.StructField("transmission", T.StringType()),
    T.StructField("body_type", T.StringType()),
    T.StructField("engine_displacement_cm3", T.IntegerType()),
    T.StructField("power_hp", T.IntegerType()),
    T.StructField("price_euro", T.IntegerType()),
    T.StructField("color", T.StringType()),
    T.StructField("doors", T.IntegerType()),
    T.StructField("country", T.StringType()),
    T.StructField("first_seen_at", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("features", T.ArrayType(T.StringType())),
])

# Columns that identify the same car (for duplicates).
SPEC_COLUMNS = ["make", "model", "version", "year", "first_registration_date", "mileage_km", "fuel_type",
                "transmission", "body_type", "engine_displacement_cm3", "power_hp", "color", "doors"]

CLEAN_COLUMNS = [
    "listing_id", "price_euro", "make", "model", "model_family", "version",
    "year", "age_years", "first_registration_date", "reg_gap_years",
    "mileage_km", "km_per_year", "is_new",
    "fuel_type", "transmission", "body_type", "engine_displacement_cm3", "power_hp", "ev_range_km",
    "awd_in_version", "color", "doors",
    "generation_code", "generation_start_year", "generation_end_year",
    "features", "feature_count", "wheel_size_in",
    "first_seen_at", "source", "country", "duplicate_listings",
    "dq_flags", "train_exclude_reasons",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def as_map(mapping: dict) -> Column:
    return F.create_map(*[F.lit(x) for pair in mapping.items() for x in pair])


def to_int(c: Column) -> Column:
    """Digits-only string -> int, otherwise null."""
    return F.when(c.rlike(r"^\d+$"), c.cast("int"))


def is_true(c: Column) -> Column:
    """Null condition -> False."""
    return F.coalesce(c, F.lit(False))


def with_columns(df: DataFrame, columns: dict) -> DataFrame:
    """withColumns where every expression reads the input columns. Works around a Spark 4.0 bug:
    a map lookup can read a new column with the same name instead of the original one."""
    tmp = {f"__new__{name}": expr for name, expr in columns.items()}
    df = df.withColumns(tmp)
    kept = [F.col(c) for c in df.columns if c not in columns and c not in tmp]
    return df.select(*kept, *[F.col(t).alias(name) for t, name in zip(tmp, columns)])


# ---------------------------------------------------------------------------
# Pipeline steps. Boolean columns _f_<name> end up as "<name>" in dq_flags.
# ---------------------------------------------------------------------------
def read_raw(spark: SparkSession, path: str) -> DataFrame:
    # FAILFAST: stop if a future scrape changes a field's type
    return spark.read.schema(RAW_SCHEMA).option("multiLine", True).option("mode", "FAILFAST").json(path)


def drop_empty_listings(df: DataFrame) -> DataFrame:
    """Failed scrapes (only the id survived)."""
    return df.filter(F.col("make").isNotNull() | F.col("price_euro").isNotNull())


def keep_listings_with_features(df: DataFrame) -> DataFrame:
    """Keep listings with at least one real equipment entry."""
    real = F.filter(F.coalesce("features", F.array().cast("array<string>")),
                    lambda x: (F.trim(x) != "") & ~F.lower(x).isin("true", "false"))
    return df.filter(F.size(real) > 0)


def realign_shifted_columns(df: DataFrame) -> DataFrame:
    """The scraper reads the details by position, so a missing one shifts the rest by one column
    (fuel_type='Automata', transmission='SUV'). Values are put back by what they are."""
    fuel, trans, body = F.col("fuel_type"), F.col("transmission"), F.col("body_type")
    disp, power = F.col("engine_displacement_cm3"), F.col("power_hp")

    shifted = is_true(fuel.isin(*TRANSMISSION)) | is_true(trans.isin(*BODY))
    body_cm3 = to_int(F.regexp_replace(F.regexp_extract(body, r"^([\d ]+) ?cm3$", 1), " ", ""))
    body_hp = to_int(F.regexp_replace(F.regexp_extract(body, r"^([\d ]+) ?CP$", 1), " ", ""))

    unrecognised = (
        is_true(fuel.isNotNull() & ~fuel.isin(*FUEL, *TRANSMISSION))
        | is_true(trans.isNotNull() & ~trans.isin(*TRANSMISSION, *BODY))
        | is_true(body.isNotNull() & ~body.isin(*BODY) & body_cm3.isNull() & body_hp.isNull())
    )
    return with_columns(df, {
        "fuel_type": F.when(fuel.isin(*FUEL), fuel),
        "transmission": F.coalesce(F.when(fuel.isin(*TRANSMISSION), fuel),
                                   F.when(trans.isin(*TRANSMISSION), trans)),
        "body_type": F.coalesce(F.when(trans.isin(*BODY), trans), F.when(body.isin(*BODY), body)),
        "engine_displacement_cm3": F.when(shifted, body_cm3).otherwise(disp),
        "power_hp": F.when(shifted, F.coalesce(body_hp, disp)).otherwise(power),
        "_f_shifted_columns_realigned": shifted,
        "_f_unrecognised_category_dropped": unrecognised,
    })


def translate_categories(df: DataFrame) -> DataFrame:
    color = F.col("color")
    english_color = as_map(COLOR)[color]
    return with_columns(df, {
        "fuel_type": as_map(FUEL)[F.col("fuel_type")],
        "transmission": as_map(TRANSMISSION)[F.col("transmission")],
        "body_type": as_map(BODY)[F.col("body_type")],
        "color": english_color,
        "_f_unrecognised_color_dropped": is_true(color.isNotNull() & english_color.isNull()),
    })


def fix_engine_columns(df: DataFrame) -> DataFrame:
    """Same cause: electric cars have hp in the displacement field and range in power_hp.
    Cars with no displacement on the page also have hp there (under 600, power missing)."""
    disp, power = F.col("engine_displacement_cm3"), F.col("power_hp")
    not_shifted = ~F.col("_f_shifted_columns_realigned")
    is_ev = is_true(F.col("fuel_type") == "electric")

    ev_swap = is_ev & not_shifted
    hp_in_disp = ~is_ev & not_shifted & is_true(power.isNull() & (disp < 600))
    moved = ev_swap | hp_in_disp
    return with_columns(df, {
        "ev_range_km": F.when(ev_swap, power),
        "power_hp": F.when(moved, disp).otherwise(power),
        "engine_displacement_cm3": F.when(moved, F.lit(None).cast("int")).otherwise(disp),
        "_f_ev_power_and_range_unswapped": ev_swap,
        "_f_power_taken_from_displacement": hp_in_disp,
    })


def fill_from_context(df: DataFrame) -> DataFrame:
    """Missing fuel: guessed from the version ('2.0 TSI' -> petrol). Missing gearbox on an EV: automatic."""
    fuel, version, trans = F.col("fuel_type"), F.col("version"), F.col("transmission")
    inferred = None
    for pattern, value in FUEL_FROM_VERSION:
        cond = version.rlike(pattern)
        inferred = F.when(cond, value) if inferred is None else inferred.when(cond, value)
    fuel_filled = F.coalesce(fuel, inferred)
    trans_filled = F.coalesce(trans, F.when(fuel_filled == "electric", "automatic"))
    return with_columns(df, {
        "fuel_type": fuel_filled,
        "transmission": trans_filled,
        "_f_fuel_inferred_from_version": is_true(fuel.isNull() & fuel_filled.isNotNull()),
        "_f_ev_transmission_assumed_automatic": is_true(trans.isNull() & trans_filled.isNotNull()),
    })


def parse_dates(df: DataFrame) -> DataFrame:
    """'9 februarie 2023' -> date; 2-digit years handled, future dates dropped. Also checks `year`."""
    seen = F.to_timestamp("first_seen_at")
    reg = F.trim(F.col("first_registration_date"))

    day = to_int(F.regexp_extract(reg, REG_DATE_PATTERN, 1))
    month = as_map(MONTHS)[F.lower(F.regexp_extract(reg, REG_DATE_PATTERN, 2))]
    year_text = F.regexp_extract(reg, REG_DATE_PATTERN, 3)
    yr = to_int(year_text)
    two_digit = is_true(F.length(year_text) == 2)
    yr4 = F.when(two_digit, F.when(yr + 2000 <= F.year(seen), yr + 2000).otherwise(yr + 1900)).otherwise(yr)
    parsed = F.try_to_timestamp(F.format_string("%04d-%02d-%02d", yr4, month, day), F.lit("yyyy-MM-dd")).cast("date")
    reg_date = F.when(parsed <= F.to_date(seen), parsed)

    year = F.col("year")
    year_ok = year.between(1900, F.year(seen) + 1)
    return with_columns(df, {
        "first_seen_at": seen,
        "first_registration_date": reg_date,
        "year": F.when(year_ok, year),
        "_f_reg_date_two_digit_year": two_digit & reg_date.isNotNull(),
        "_f_reg_date_unparseable_or_future": is_true(reg.isNotNull() & reg_date.isNull()),
        "_f_reg_date_is_jan1_placeholder": is_true((F.month(reg_date) == 1) & (F.dayofmonth(reg_date) == 1)),
        "_f_year_out_of_range": is_true(year.isNotNull() & ~year_ok),
    })


def parse_generation(df: DataFrame) -> DataFrame:
    """'X253 [2015-2022]' -> code, start and end year. Dropped if it contradicts `year`."""
    g, year = F.col("generation"), F.col("year")
    code = F.trim(F.regexp_extract(g, GENERATION_PATTERN, 1))
    start = to_int(F.regexp_extract(g, GENERATION_PATTERN, 2))
    end = to_int(F.regexp_extract(g, GENERATION_PATTERN, 3))
    contradicts = is_true((year < start - 1) | (year > end + 1))
    keep = g.isNotNull() & ~contradicts
    return with_columns(df, {
        "generation_code": F.when(keep & (code != ""), code),
        "generation_start_year": F.when(keep, start),
        "generation_end_year": F.when(keep, end),
        "_f_generation_contradicts_year": contradicts,
    })


WHEEL_FEATURE = r'^Jante (aliaj \d+|aluminiu \(\d+"\))$'   # 'Jante aliaj 18', 'Jante aluminiu (18")'


def clean_features(df: DataFrame) -> DataFrame:
    """Remove junk entries; an empty list becomes null (unknown, not "no equipment").
    Also counts the options and reads the wheel size."""
    kept = F.array_sort(F.array_distinct(F.filter(
        "features", lambda x: (F.trim(x) != "") & ~F.lower(x).isin("true", "false"))))
    features = F.when(F.size(kept) > 0, kept)
    wheel_sizes = F.transform(
        F.filter(features, lambda x: x.rlike(WHEEL_FEATURE)),
        lambda x: F.regexp_extract(x, r"(\d+)", 1).cast("int"))
    return with_columns(df, {
        "features": features,
        "feature_count": F.when(features.isNotNull(), F.size(features)),
        "wheel_size_in": F.array_max(wheel_sizes),
        "_f_features_missing": features.isNull(),
    })


def model_family(make: Column, model: Column) -> Column:
    """Groups variants: 'A4 Avant' -> 'A4', Mercedes 'A 180' -> 'A', Lexus 'RX 450h' -> 'RX'."""
    fam = F.regexp_replace(model, BODY_SUFFIX, "")
    lexus = F.regexp_extract(fam, r"^(?:Seria\s+)?([A-Z]{2,3})\b", 1)
    fam = F.when((make == "Lexus") & (lexus != ""), lexus).otherwise(fam)
    fam = F.when(make == "Opel", F.regexp_replace(fam, r"\s+X$", "")).otherwise(fam)
    fam = F.when(make == "Volkswagen", F.regexp_replace(fam, r"^T\d(\.\d)?\s+", "")).otherwise(fam)
    mb_class = F.regexp_extract(fam, r"^(AMG GT|[A-Z]{1,3})(?:\s|\+|$)", 1)
    mercedes = F.coalesce(as_map(MERCEDES_CHASSIS)[fam], F.when(mb_class != "", mb_class), fam)
    return F.when(make == "Mercedes-Benz", mercedes).otherwise(fam)


def normalise_make_model(df: DataFrame) -> DataFrame:
    """Rebranded makes merged, model spellings unified ('cla' -> 'CLA'), model_family added."""
    df = with_columns(df, {
        "make": F.coalesce(as_map(MAKE_ALIASES)[F.col("make")], F.col("make")),
        "_f_make_alias_merged": is_true(F.col("make").isin(*MAKE_ALIASES)),
    })
    df = df.withColumn("_n", F.count(F.lit(1)).over(Window.partitionBy("make", "model")))
    by_spelling = Window.partitionBy("make", F.lower("model")).orderBy(F.desc("_n"), F.asc("model"))
    canonical = F.first("model").over(by_spelling)
    df = with_columns(df, {
        "model": canonical,
        "_f_model_spelling_unified": is_true(F.col("model") != canonical),
    }).drop("_n")
    return df.withColumn("model_family", model_family(F.col("make"), F.col("model")))


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Age, km per year, new car, AWD. Age uses `year`: for imports the registration date
    can be years later than the build."""
    seen = F.to_date("first_seen_at")
    age = F.greatest(F.datediff(seen, F.make_date(F.col("year"), F.lit(7), F.lit(1))) / 365.25, F.lit(0.0))
    km_per_year = F.col("mileage_km") / F.greatest(age, F.lit(1.0))
    is_new = F.col("mileage_km") <= NEW_CAR_MAX_KM
    return with_columns(df, {
        "age_years": F.round(age, 2),
        "km_per_year": F.round(km_per_year).cast("int"),
        "is_new": is_new,
        "awd_in_version": is_true(F.col("version").rlike(AWD_PATTERN)),
        "reg_gap_years": F.year("first_registration_date") - F.col("year"),
        "_f_implausible_km_per_year": is_true(~is_new & (km_per_year > MAX_KM_PER_YEAR)),
    })


def drop_duplicate_listings(df: DataFrame) -> DataFrame:
    """Same car posted more than once (new cars must also match price and equipment).
    Keeps the latest listing; duplicate_listings counts the others."""
    used = is_true(F.col("mileage_km") > NEW_CAR_MAX_KM)
    key = F.when(used, F.to_json(F.struct(*SPEC_COLUMNS))) \
           .otherwise(F.to_json(F.struct(*SPEC_COLUMNS, "price_euro", "features")))
    group = Window.partitionBy("_dup_key")
    return (df.withColumn("_dup_key", key)
              .withColumn("_rank", F.row_number().over(group.orderBy(F.desc("first_seen_at"), "listing_id")))
              .withColumn("duplicate_listings", F.count(F.lit(1)).over(group) - 1)
              .withColumn("_f_duplicates_collapsed", F.col("duplicate_listings") > 0)
              .filter("_rank = 1")
              .drop("_dup_key", "_rank"))


def mark_training_rows(df: DataFrame) -> DataFrame:
    """Why a row is left out of train (it stays in listings_clean)."""
    checks = {f"missing_{c}": F.col(c).isNull() for c in REQUIRED_FOR_TRAINING}
    checks.update({
        "exotic_or_above_price_cap": F.col("make").isin(*EXOTIC_MAKES) | (F.col("price_euro") > PRICE_CAP_EUR),
        "classic": F.col("age_years") >= CLASSIC_MIN_AGE_YEARS,
        "microcar": F.col("power_hp") < MICROCAR_MAX_HP,
        "implausible_mileage": F.col("_f_implausible_km_per_year"),
    })
    reasons = F.array(*[F.when(is_true(cond), F.lit(name)) for name, cond in checks.items()])
    return df.withColumn("train_exclude_reasons", F.filter(reasons, lambda x: x.isNotNull()))


def collect_dq_flags(df: DataFrame) -> DataFrame:
    flag_cols = [c for c in df.columns if c.startswith("_f_")]
    flags = F.array(*[F.when(F.col(c), F.lit(c[3:])) for c in flag_cols])
    return df.withColumn("dq_flags", F.filter(flags, lambda x: x.isNotNull())).drop(*flag_cols)


def build_training_set(clean: DataFrame) -> DataFrame:
    return (clean
            .filter(F.size("train_exclude_reasons") == 0)
            .select(
                "listing_id",
                "price_euro",
                F.log("price_euro").alias("log_price"),
                "make", "model", "model_family", "version",
                "fuel_type", "transmission", "body_type",
                F.coalesce("color", F.lit("unknown")).alias("color"),
                "doors", "year", "age_years", "mileage_km", "km_per_year", "is_new",
                "power_hp",
                # electric cars have no engine: 0 instead of null
                F.when(F.col("fuel_type") == "electric", F.lit(0))
                 .otherwise(F.col("engine_displacement_cm3")).alias("engine_displacement_cm3"),
                "ev_range_km", "awd_in_version", "reg_gap_years",
                F.col("features").isNotNull().alias("features_known"),
                "feature_count", "wheel_size_in", "features",
            ))


def write_readable_copies(df: DataFrame, path_stem: str) -> None:
    """Also save the table as .json and .csv (loads it into memory: fine for small tables)."""
    rows = [r.asDict(recursive=True) for r in df.collect()]
    with open(f"{path_stem}.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=4, ensure_ascii=False, default=str)
    # utf-8-sig so Excel shows diacritics correctly
    with open(f"{path_stem}.csv", "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        for r in rows:
            writer.writerow(["; ".join(v) if isinstance(v, list) else ("" if v is None else v)
                             for v in r.values()])


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def print_report(raw_n: int, dropped: dict, deduped_input_n: int, clean: DataFrame, train: DataFrame) -> None:
    """`dropped`: rows removed before cleaning, as {label: count}."""
    clean_n, train_n = clean.count(), train.count()

    def counts(column: str) -> list:
        return (clean.select(F.explode(column).alias("v")).groupBy("v").count()
                .orderBy(F.desc("count")).collect())

    print("\n================ CURATION REPORT ================")
    print(f"raw rows                     {raw_n:>6}")
    for label, n in dropped.items():
        print(f"{label:<29}{n:>6}")
    print(f"duplicate listings dropped   {deduped_input_n - clean_n:>6}")
    print(f"listings_clean rows          {clean_n:>6}")
    print(f"train rows                   {train_n:>6}")
    print("\nautomatic corrections / flags (rows):")
    for r in counts("dq_flags"):
        print(f"  {r['v']:<38}{r['count']:>6}")
    print("\nexcluded from training (rows; a row can have several reasons):")
    for r in counts("train_exclude_reasons"):
        print(f"  {r['v']:<38}{r['count']:>6}")
    nulls = train.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in train.columns]).first()
    print("\nnulls left in train (optional columns):")
    for c in train.columns:
        if nulls[c]:
            print(f"  {c:<38}{nulls[c]:>6}")
    fam = clean.agg(F.countDistinct("model").alias("m"), F.countDistinct("model_family").alias("f")).first()
    print(f"\ndistinct models {fam['m']} -> model families {fam['f']}")
    print("=================================================\n")


# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default=str(REPO_ROOT / "cars.json"))
    parser.add_argument("--out", default=str(REPO_ROOT / "data" / "curated"))
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("curate-cars")
             .master("local[*]")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = read_raw(spark, args.input).cache()
    scraped = drop_empty_listings(raw).cache()
    listings = keep_listings_with_features(scraped) if KEEP_ONLY_WITH_FEATURES else scraped

    cleaned = (listings
               .transform(realign_shifted_columns)
               .transform(translate_categories)
               .transform(fix_engine_columns)
               .transform(fill_from_context)
               .transform(parse_dates)
               .transform(parse_generation)
               .transform(clean_features)
               .transform(normalise_make_model)
               .transform(add_derived_columns)
               .cache())

    clean = (cleaned
             .transform(drop_duplicate_listings)
             .transform(mark_training_rows)
             .transform(collect_dq_flags)
             .select(*CLEAN_COLUMNS)
             .cache())
    train = build_training_set(clean)

    for name, table in [("listings_clean", clean), ("train", train)]:
        table.coalesce(1).write.mode("overwrite").parquet(f"{args.out}/{name}")
        write_readable_copies(table, f"{args.out}/{name}")
    # data/ is git-ignored, so the README template lives in scripts/
    shutil.copyfile(Path(__file__).with_name("curated_data_README.md"), Path(args.out) / "README.md")

    raw_n, scraped_n = raw.count(), scraped.count()
    dropped = {"empty scrapes dropped": raw_n - scraped_n}
    if KEEP_ONLY_WITH_FEATURES:
        dropped["without features dropped"] = scraped_n - listings.count()
    print_report(raw_n, dropped, cleaned.count(), clean, train)
    print(f"written to {args.out}/:")
    print("  listings_clean/  train/              Parquet, for pandas / Spark")
    print("  listings_clean.json  train.json      open in an editor")
    print("  listings_clean.csv   train.csv       open in Excel / Numbers")
    print("  README.md                            what the files and columns mean")
    spark.stop()


if __name__ == "__main__":
    main()
