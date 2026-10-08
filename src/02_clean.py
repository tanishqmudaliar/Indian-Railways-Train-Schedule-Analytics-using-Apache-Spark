"""
02_clean.py
-----------
Cleans the raw schedule data and writes a unified stops table.

Steps:
  1. Load the main schedule/stops file from data/raw/.
  2. Select and rename columns to a standard schema.
  3. Convert time strings to proper times; treat "None" / "" as null.
  4. Compute stop duration in minutes (departure − arrival).
  5. Remove duplicate rows and rows missing train/station identifiers.
  6. Write cleaned data to data/processed/stops.parquet.
"""

import os
import sys
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.spark_helper import get_spark

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")


def find_main_file():
    """
    Find the main schedule file in data/raw/.
    Look for the largest JSON or CSV file, which is likely the schedules.
    """
    candidates = []
    for fname in os.listdir(RAW_DIR):
        fpath = os.path.join(RAW_DIR, fname)
        if os.path.isfile(fpath):
            size = os.path.getsize(fpath)
            candidates.append((fname, fpath, size))

    if not candidates:
        print("ERROR: No files in data/raw/. Run 00_download.py first.")
        sys.exit(1)

    # Sort by size descending — the biggest file is likely the schedules
    candidates.sort(key=lambda x: x[2], reverse=True)
    print("Files found (sorted by size):")
    for name, _, size in candidates:
        print(f"  {name} ({size / 1024 / 1024:.1f} MB)")

    return candidates


def clean_time_column(df, col_name):
    """
    Replace string values like 'None', 'nan', '' with null,
    and keep the rest as-is (HH:MM or HH:MM:SS strings).
    """
    return df.withColumn(
        col_name,
        F.when(
            (F.col(col_name).isNull()) |
            (F.trim(F.col(col_name)) == "") |
            (F.lower(F.trim(F.col(col_name))) == "none") |
            (F.lower(F.trim(F.col(col_name))) == "nan") |
            (F.lower(F.trim(F.col(col_name))) == "null"),
            F.lit(None)
        ).otherwise(F.trim(F.col(col_name)))
    )


def compute_duration(df):
    """
    Compute stop duration in minutes as (departure − arrival).
    Handles the midnight crossover case.
    Both arrival and departure are HH:MM strings.
    """
    # Extract hours and minutes from arrival and departure
    df = df.withColumn("arr_hour", F.split(F.col("arrival"), ":").getItem(0).cast(IntegerType()))
    df = df.withColumn("arr_min", F.split(F.col("arrival"), ":").getItem(1).cast(IntegerType()))
    df = df.withColumn("dep_hour", F.split(F.col("departure"), ":").getItem(0).cast(IntegerType()))
    df = df.withColumn("dep_min", F.split(F.col("departure"), ":").getItem(1).cast(IntegerType()))

    # Total minutes since midnight
    df = df.withColumn("arr_total", F.col("arr_hour") * 60 + F.col("arr_min"))
    df = df.withColumn("dep_total", F.col("dep_hour") * 60 + F.col("dep_min"))

    # Duration: if departure < arrival, the stop crosses midnight
    df = df.withColumn(
        "stop_duration_mins",
        F.when(
            F.col("arrival").isNull() | F.col("departure").isNull(),
            F.lit(None).cast(IntegerType())
        ).otherwise(
            F.when(
                F.col("dep_total") >= F.col("arr_total"),
                F.col("dep_total") - F.col("arr_total")
            ).otherwise(
                # Midnight crossover: add 24 hours to departure
                (F.col("dep_total") + 1440) - F.col("arr_total")
            )
        )
    )

    # Drop helper columns
    df = df.drop("arr_hour", "arr_min", "dep_hour", "dep_min", "arr_total", "dep_total")
    return df


def main():
    spark = get_spark("CleanData")

    files = find_main_file()
    main_file = files[0]  # biggest file
    print(f"\nUsing main file: {main_file[0]}")

    fname, fpath, _ = main_file
    ext = os.path.splitext(fname)[1].lower()

    # Load the file
    if ext == ".json":
        df = spark.read.json(fpath, multiLine=True)
        if df.count() == 0:
            df = spark.read.json(fpath, multiLine=False)
    elif ext == ".csv":
        df = spark.read.csv(fpath, header=True, inferSchema=True)
    else:
        print(f"Unsupported format: {ext}")
        sys.exit(1)

    raw_count = df.count()
    print(f"\nRaw rows loaded: {raw_count:,}")
    print(f"Columns: {df.columns}")
    df.printSchema()

    # ---------- Adapt column mapping to the actual data ----------
    # We'll inspect the columns and build a mapping dynamically.
    cols_lower = {c.lower(): c for c in df.columns}

    # Build a mapping: target_name -> source_column
    mapping = {}

    # Train number
    for candidate in ["train_number", "trainno", "train_no", "trainNumber", "number"]:
        if candidate.lower() in cols_lower:
            mapping["train_number"] = cols_lower[candidate.lower()]
            break

    # Train name
    for candidate in ["train_name", "trainname", "trainName", "name"]:
        if candidate.lower() in cols_lower:
            mapping["train_name"] = cols_lower[candidate.lower()]
            break

    # Station code
    for candidate in ["station_code", "stationcode", "stationCode", "station", "stn_code"]:
        if candidate.lower() in cols_lower:
            mapping["station_code"] = cols_lower[candidate.lower()]
            break

    # Station name
    for candidate in ["station_name", "stationname", "stationName", "stn_name"]:
        if candidate.lower() in cols_lower:
            mapping["station_name"] = cols_lower[candidate.lower()]
            break

    # Arrival time
    for candidate in ["arrival", "arrival_time", "arrivaltime", "arr"]:
        if candidate.lower() in cols_lower:
            mapping["arrival"] = cols_lower[candidate.lower()]
            break

    # Departure time
    for candidate in ["departure", "departure_time", "departuretime", "dep"]:
        if candidate.lower() in cols_lower:
            mapping["departure"] = cols_lower[candidate.lower()]
            break

    # Day number (optional)
    for candidate in ["day", "day_number", "dayno", "day_count"]:
        if candidate.lower() in cols_lower:
            mapping["day"] = cols_lower[candidate.lower()]
            break

    # Sequence / stop number (optional)
    for candidate in ["seq", "sequence", "stop_number", "stop_seq", "sno", "serial", "stop_no", "id"]:
        if candidate.lower() in cols_lower:
            mapping["stop_sequence"] = cols_lower[candidate.lower()]
            break

    print(f"\nColumn mapping:")
    for target, source in mapping.items():
        print(f"  {target} <- {source}")

    # Check required columns
    required = ["train_number", "station_code", "arrival", "departure"]
    missing = [r for r in required if r not in mapping]
    if missing:
        # If station_code is missing but station_name exists, use station_name
        if "station_code" in missing and "station_name" in mapping:
            print("  station_code not found, will use station_name only")
            missing.remove("station_code")
        if missing:
            print(f"\nERROR: Cannot find required columns: {missing}")
            print(f"Available columns: {df.columns}")
            sys.exit(1)

    # Select and rename columns
    select_exprs = []
    for target, source in mapping.items():
        select_exprs.append(F.col(source).alias(target))

    df = df.select(*select_exprs)

    # ---------- Clean time columns ----------
    df = clean_time_column(df, "arrival")
    df = clean_time_column(df, "departure")

    # Also clean "None" values in train_name and station_name if they exist
    for c in ["train_name", "station_name", "station_code"]:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(
                    (F.col(c).isNull()) |
                    (F.trim(F.col(c)) == "") |
                    (F.lower(F.trim(F.col(c))) == "none"),
                    F.lit(None)
                ).otherwise(F.trim(F.col(c)))
            )

    # Cast train_number to string (it may be integer)
    df = df.withColumn("train_number", F.col("train_number").cast("string"))

    # ---------- Remove rows missing train or station ----------
    before_filter = df.count()

    # Determine which station column to use for filtering
    station_col = "station_code" if "station_code" in df.columns else "station_name"
    df_clean = df.filter(
        F.col("train_number").isNotNull() & F.col(station_col).isNotNull()
    )

    after_filter = df_clean.count()
    removed_missing = before_filter - after_filter
    print(f"\nRemoved {removed_missing:,} rows with missing train/station.")

    # ---------- Remove exact duplicates ----------
    df_dedup = df_clean.dropDuplicates()
    after_dedup = df_dedup.count()
    removed_dupes = after_filter - after_dedup
    print(f"Removed {removed_dupes:,} exact duplicate rows.")

    # ---------- Compute stop duration ----------
    df_final = compute_duration(df_dedup)

    # Standardise arrival and departure strings to HH:MM format
    df_final = df_final.withColumn(
        "arrival",
        F.when(F.col("arrival").isNotNull(), F.substring(F.col("arrival"), 1, 5)).otherwise(None)
    ).withColumn(
        "departure",
        F.when(F.col("departure").isNotNull(), F.substring(F.col("departure"), 1, 5)).otherwise(None)
    )

    # ---------- Optional Enrichment from stations.json and trains.json ----------
    stations_path = os.path.join(RAW_DIR, "stations.json")
    if os.path.exists(stations_path):
        try:
            print("\nEnriching with station metadata (state, zone) from stations.json …")
            stn_raw = spark.read.json(stations_path, multiLine=True)
            stn_flat = (
                stn_raw.select(F.explode("features.properties").alias("p"))
                .select(
                    F.trim(F.col("p.code")).alias("stn_code_meta"),
                    F.trim(F.col("p.state")).alias("state"),
                    F.trim(F.col("p.zone")).alias("zone")
                )
                .filter(F.col("stn_code_meta").isNotNull() & (F.col("stn_code_meta") != ""))
                .dropDuplicates(["stn_code_meta"])
            )
            df_final = df_final.join(
                stn_flat,
                df_final["station_code"] == stn_flat["stn_code_meta"],
                how="left"
            ).drop("stn_code_meta")
            print("  Station metadata joined successfully.")
        except Exception as e:
            print(f"  Warning: Could not enrich from stations.json ({e})")

    trains_path = os.path.join(RAW_DIR, "trains.json")
    if os.path.exists(trains_path):
        try:
            print("\nEnriching with train metadata (train_type) from trains.json …")
            trn_raw = spark.read.json(trains_path, multiLine=True)
            trn_flat = (
                trn_raw.select(F.explode("features.properties").alias("p"))
                .select(
                    F.trim(F.col("p.number").cast("string")).alias("trn_num_meta"),
                    F.trim(F.col("p.type")).alias("train_type")
                )
                .filter(F.col("trn_num_meta").isNotNull() & (F.col("trn_num_meta") != ""))
                .dropDuplicates(["trn_num_meta"])
            )
            df_final = df_final.join(
                trn_flat,
                df_final["train_number"] == trn_flat["trn_num_meta"],
                how="left"
            ).drop("trn_num_meta")
            print("  Train metadata joined successfully.")
        except Exception as e:
            print(f"  Warning: Could not enrich from trains.json ({e})")

    final_count = df_final.count()
    print(f"\nFinal cleaned rows: {final_count:,}")
    print(f"  Removed total: {raw_count - final_count:,} rows")
    print(f"    - Missing train/station: {removed_missing:,}")
    print(f"    - Duplicates: {removed_dupes:,}")

    # Show sample rows
    print("\nSample cleaned rows:")
    df_final.show(10, truncate=False)

    # ---------- Write to Parquet ----------
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    output_path = os.path.join(PROCESSED_DIR, "stops.parquet")
    df_final.coalesce(1).write.mode("overwrite").parquet(output_path)
    print(f"\nWritten to {output_path}")

    # Save cleaning stats for the analysis step
    stats = {
        "raw_rows": raw_count,
        "cleaned_rows": final_count,
        "removed_missing": removed_missing,
        "removed_duplicates": removed_dupes
    }
    stats_path = os.path.join(PROCESSED_DIR, "clean_stats.json")
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    spark.stop()


if __name__ == "__main__":
    main()
