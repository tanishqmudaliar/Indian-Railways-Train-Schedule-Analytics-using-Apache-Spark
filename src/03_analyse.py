"""
03_analyse.py
-------------
Reads the cleaned stops.parquet and computes analytics:
  1. Busiest stations (by stop count and distinct trains)
  2. Stoppage time statistics per station
  3. Hourly traffic distribution
  4. Longest trains (most stops)
  5. Additional breakdowns if data supports it
  6. At least two analyses done via Spark SQL

Writes each result to data/results/ as Parquet.
Also writes a run_summary.json with key figures.
"""

import os
import sys
import json
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.spark_helper import get_spark

from pyspark.sql import functions as F

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")


def main():
    start_time = time.time()
    spark = get_spark("AnalyseData")

    # Load cleaned data
    stops_path = os.path.join(PROCESSED_DIR, "stops.parquet")
    if not os.path.exists(stops_path):
        print("ERROR: stops.parquet not found. Run 02_clean.py first.")
        sys.exit(1)

    df = spark.read.parquet(stops_path)
    total_rows = df.count()
    print(f"Loaded {total_rows:,} cleaned stop records.")
    print(f"Columns: {df.columns}")

    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Determine the station identifier column
    station_col = "station_code" if "station_code" in df.columns else "station_name"
    station_name_col = "station_name" if "station_name" in df.columns else station_col

    # ----------------------------------------------------------------
    # 1. BUSIEST STATIONS (DataFrame API)
    # ----------------------------------------------------------------
    print("\n--- Analysis 1: Busiest Stations ---")
    busiest = (
        df.groupBy(station_col, station_name_col)
        .agg(
            F.count("*").alias("total_stops"),
            F.countDistinct("train_number").alias("distinct_trains")
        )
        .orderBy(F.desc("total_stops"))
        .limit(20)
    )
    busiest.show(20, truncate=False)

    out_path = os.path.join(RESULTS_DIR, "busiest_stations.parquet")
    busiest.coalesce(1).write.mode("overwrite").parquet(out_path)
    print(f"  Saved to {out_path}")

    # ----------------------------------------------------------------
    # 2. STOPPAGE TIME STATISTICS (Spark SQL — one of two SQL queries)
    # ----------------------------------------------------------------
    print("\n--- Analysis 2: Stoppage Time Statistics (Spark SQL) ---")

    # Register as temp view for SQL
    df.createOrReplaceTempView("stops")

    stoppage_sql = f"""
    SELECT
        {station_col},
        {station_name_col},
        COUNT(*) AS num_stops,
        ROUND(AVG(stop_duration_mins), 1) AS avg_duration_mins,
        MAX(stop_duration_mins) AS max_duration_mins,
        ROUND(PERCENTILE_APPROX(stop_duration_mins, 0.9), 1) AS p90_duration_mins
    FROM stops
    WHERE stop_duration_mins IS NOT NULL
    GROUP BY {station_col}, {station_name_col}
    HAVING COUNT(*) >= 20
    ORDER BY avg_duration_mins DESC
    """
    stoppage = spark.sql(stoppage_sql)
    stoppage.show(20, truncate=False)

    out_path = os.path.join(RESULTS_DIR, "stoppage_stats.parquet")
    stoppage.coalesce(1).write.mode("overwrite").parquet(out_path)
    print(f"  Saved to {out_path}")

    # ----------------------------------------------------------------
    # 3. HOURLY TRAFFIC (DataFrame API)
    # ----------------------------------------------------------------
    print("\n--- Analysis 3: Hourly Traffic ---")

    # Extract the hour from arrival time (HH:MM format)
    hourly = (
        df.filter(F.col("arrival").isNotNull())
        .withColumn("hour", F.split(F.col("arrival"), ":").getItem(0).cast("int"))
        .groupBy("hour")
        .agg(F.count("*").alias("arrivals"))
        .orderBy("hour")
    )
    hourly.show(24, truncate=False)

    out_path = os.path.join(RESULTS_DIR, "hourly_traffic.parquet")
    hourly.coalesce(1).write.mode("overwrite").parquet(out_path)
    print(f"  Saved to {out_path}")

    # ----------------------------------------------------------------
    # 4. LONGEST TRAINS (Spark SQL — second SQL query)
    # ----------------------------------------------------------------
    print("\n--- Analysis 4: Longest Trains (Spark SQL) ---")

    # Use train_name if available
    train_name_col = "train_name" if "train_name" in df.columns else "train_number"

    longest_sql = f"""
    SELECT
        train_number,
        {train_name_col},
        COUNT(*) AS num_stops
    FROM stops
    GROUP BY train_number, {train_name_col}
    ORDER BY num_stops DESC
    LIMIT 20
    """
    longest = spark.sql(longest_sql)
    longest.show(20, truncate=False)

    out_path = os.path.join(RESULTS_DIR, "longest_trains.parquet")
    longest.coalesce(1).write.mode("overwrite").parquet(out_path)
    print(f"  Saved to {out_path}")

    # ----------------------------------------------------------------
    # 5. ADDITIONAL BREAKDOWNS (if data supports it)
    # ----------------------------------------------------------------
    # Check for zone, state, train_type columns
    extra_cols = [c for c in df.columns if c.lower() in
                  ("zone", "state", "train_type", "type", "category")]
    if extra_cols:
        for ecol in extra_cols:
            print(f"\n--- Analysis 5: Breakdown by {ecol} ---")
            breakdown = (
                df.groupBy(ecol)
                .agg(
                    F.count("*").alias("total_stops"),
                    F.countDistinct("train_number").alias("distinct_trains")
                )
                .orderBy(F.desc("total_stops"))
            )
            breakdown.show(30, truncate=False)
            out_path = os.path.join(RESULTS_DIR, f"breakdown_{ecol}.parquet")
            breakdown.coalesce(1).write.mode("overwrite").parquet(out_path)
            print(f"  Saved to {out_path}")
    else:
        print("\n--- Analysis 5: No zone/state/type columns found. Skipping. ---")

    # ----------------------------------------------------------------
    # RUN SUMMARY
    # ----------------------------------------------------------------
    elapsed = round(time.time() - start_time, 1)
    distinct_trains = df.select("train_number").distinct().count()
    distinct_stations = df.select(station_col).distinct().count()

    # Load cleaning stats if available
    clean_stats_path = os.path.join(PROCESSED_DIR, "clean_stats.json")
    raw_rows = total_rows  # fallback
    if os.path.exists(clean_stats_path):
        with open(clean_stats_path) as f:
            clean_stats = json.load(f)
            raw_rows = clean_stats.get("raw_rows", total_rows)

    summary = {
        "raw_rows": raw_rows,
        "cleaned_rows": total_rows,
        "distinct_trains": distinct_trains,
        "distinct_stations": distinct_stations,
        "pipeline_runtime_seconds": elapsed
    }

    summary_path = os.path.join(RESULTS_DIR, "run_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'='*50}")
    print("RUN SUMMARY")
    print(f"{'='*50}")
    for k, v in summary.items():
        print(f"  {k}: {v:,}" if isinstance(v, int) else f"  {k}: {v}")
    print(f"\nSummary saved to {summary_path}")

    spark.stop()


if __name__ == "__main__":
    main()
