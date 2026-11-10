"""
Unite the curated Autovit data (data/curated) and AutoScout24 data (data/curated_autoscout) into
one dataset, data/curated_merged/, in the same layout. Run curate_cars.py and curate_autoscout.py
first. The `source` column tells the rows apart.

Both inputs already contain only cars with an equipment list (KEEP_ONLY_WITH_FEATURES in
curate_cars.py), so no car is dropped here. On top of a plain union it:
  - reduces body types to categories both sites use the same way
  - renames AutoScout's equipment options to Autovit's names, using scripts/feature_name_map.csv
  - writes feature_counts.csv: how many cars from each site have each option

Writes listings_clean and train (Parquet folder + .json + .csv), feature_counts.csv and README.md.

Usage (from the repo root):
  python scripts/merge_curated.py
"""

import argparse
import csv
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import curate_cars as base
from curate_cars import as_map, with_columns

AUTOSCOUT = "AutoScout24"            # value of `source` for AutoScout rows

# AutoScout uses 'Berlina' and 'Autoturism mic' interchangeably for the same cars (a Golf is listed
# 8,718 times as one and 2,866 times as the other), while Autovit splits them into four types.
# The only consistent common category is all of them together.
COMMON_BODY = {"sedan": "sedan_hatchback", "compact": "sedan_hatchback",
               "city_car": "sedan_hatchback", "small_car": "sedan_hatchback"}

FEATURE_MAP_FILE = Path(__file__).with_name("feature_name_map.csv")
WHEEL_RENAME = (r'^Jante aluminiu \((\d+)"\)$', "Jante aliaj $1")   # 'Jante aluminiu (18")' -> 'Jante aliaj 18'

INPUTS = [("data/curated", "curate_cars.py"), ("data/curated_autoscout", "curate_autoscout.py")]


def load_feature_map(path: Path) -> dict:
    with open(path, newline="", encoding="utf-8") as f:
        return {row["autoscout"]: row["autovit"] for row in csv.DictReader(f)}


def read_curated(spark: SparkSession, folder: Path, script: str) -> DataFrame:
    path = folder / "listings_clean"
    if not path.exists():
        raise SystemExit(f"{path} not found: run scripts/{script} first")
    df = spark.read.parquet(str(path))
    if df.columns != base.CLEAN_COLUMNS:
        raise SystemExit(f"{path} does not have the expected columns: re-run scripts/{script}")
    return df


def harmonise_body_types(df: DataFrame) -> DataFrame:
    body = F.col("body_type")
    return with_columns(df, {"body_type": F.coalesce(as_map(COMMON_BODY)[body], body)})


def rename_autoscout_features(df: DataFrame, mapping: dict) -> DataFrame:
    """AutoScout option names -> Autovit's. Names without an equivalent are kept as they are.
    clean_features then recomputes the sorted list, feature_count and wheel_size_in."""
    names = as_map(mapping)
    renamed = F.transform("features", lambda x: F.coalesce(names[x], F.regexp_replace(x, *WHEEL_RENAME)))
    df = with_columns(df, {
        "features": F.when(F.col("source") == AUTOSCOUT, renamed).otherwise(F.col("features")),
    })
    return base.clean_features(df).drop("_f_features_missing")


def feature_counts(df: DataFrame, sources: list) -> list:
    """Rows of (feature, cars per source..., on_both_sites), most common first."""
    counts = (df.select("source", F.explode("features").alias("feature"))
                .groupBy("feature").pivot("source", sources).count().fillna(0))
    rows = []
    for r in counts.collect():
        per_source = [r[s] for s in sources]
        rows.append([r["feature"], *per_source, "yes" if all(per_source) else "no"])
    return sorted(rows, key=lambda row: -sum(row[1:-1]))


def write_feature_counts(rows: list, sources: list, path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["feature", *[f"cars_{s}" for s in sources], "on_both_sites"])
        writer.writerows(rows)


def print_report(merged: DataFrame, sources: list, counts: list) -> None:
    def per_source(df: DataFrame) -> dict:
        return {r["source"]: r["count"] for r in df.groupBy("source").count().collect()}

    # train has no `source` column (same layout as the other train tables), so count it here
    listings, in_train = per_source(merged), per_source(merged.filter(F.size("train_exclude_reasons") == 0))
    print("\n================= MERGE REPORT =================")
    print(f"{'':<26}" + "".join(f"{s:>14}" for s in sources) + f"{'total':>10}")
    for label, values in [("listings", listings), ("train", in_train)]:
        print(f"{label:<26}" + "".join(f"{values.get(s, 0):>14}" for s in sources) + f"{sum(values.values()):>10}")
    both = sum(1 for row in counts if row[-1] == "yes")
    print(f"\noptions: {len(counts)} distinct, {both} appear on both sites (see feature_counts.csv)")
    bodies = {r["body_type"]: r["count"] for r in merged.groupBy("body_type").count().collect()}
    print("body types:", dict(sorted(bodies.items(), key=lambda kv: -kv[1])))
    print("=================================================\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default=str(base.REPO_ROOT / "data" / "curated_merged"))
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("merge-curated")
             .master("local[*]")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    parts = [read_curated(spark, base.REPO_ROOT / folder, script) for folder, script in INPUTS]
    merged = (parts[0].unionByName(parts[1])
              .transform(harmonise_body_types)
              .transform(lambda df: rename_autoscout_features(df, load_feature_map(FEATURE_MAP_FILE)))
              .select(*base.CLEAN_COLUMNS)
              .cache())
    sources = sorted(r["source"] for r in merged.select("source").distinct().collect())
    train = base.build_training_set(merged)

    out = Path(args.out)
    for name, table in [("listings_clean", merged), ("train", train)]:
        table.coalesce(1).write.mode("overwrite").parquet(str(out / name))
        base.write_readable_copies(table, str(out / name))
    counts = feature_counts(merged, sources)
    write_feature_counts(counts, sources, out / "feature_counts.csv")
    shutil.copyfile(Path(__file__).with_name("curated_merged_README.md"), out / "README.md")

    print_report(merged, sources, counts)
    print(f"written to {out}/:")
    print("  listings_clean/  train/              Parquet, for pandas / Spark")
    print("  listings_clean.json  train.json      open in an editor")
    print("  listings_clean.csv   train.csv       open in Excel / Numbers")
    print("  feature_counts.csv                   how many cars from each site have each option")
    print("  README.md                            what the files and columns mean")
    spark.stop()


if __name__ == "__main__":
    main()
