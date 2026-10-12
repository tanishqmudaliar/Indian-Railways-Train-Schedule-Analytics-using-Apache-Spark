"""
test_app.py — Automated verification test runner for app data and pipeline outputs.
Executes pytest test suite and performs explicit assertions across data artifacts.
Exits with exit code 1 on failure.
"""

import os
import sys
import json
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")

print("==================================================")
print("1. Running pytest test suite...")
print("==================================================")

try:
    import pytest
    ret_code = pytest.main(["tests", "-v"])
    if ret_code != 0:
        print(f"\n[FAIL] pytest suite failed with exit code: {ret_code}")
        sys.exit(1)
    print("\n[PASS] pytest suite completed successfully.")
except ImportError:
    print("\n[SKIP] pytest is not installed in the current environment.")

print("\n==================================================")
print("2. Running explicit assertions on data & charts...")
print("==================================================")

try:
    # 1. Summary
    summary_file = os.path.join(RESULTS_DIR, "run_summary.json")
    assert os.path.exists(summary_file), f"Missing {summary_file}"
    with open(summary_file, "r", encoding="utf-8") as f:
        summary = json.load(f)
    assert summary["raw_rows"] > 0, "raw_rows must be > 0"
    assert summary["cleaned_rows"] > 0, "cleaned_rows must be > 0"
    assert summary["distinct_trains"] > 0, "distinct_trains must be > 0"
    assert summary["distinct_stations"] > 0, "distinct_stations must be > 0"
    assert summary["pipeline_runtime_seconds"] > 0, "pipeline_runtime_seconds must be > 0"
    print(f"  [✓] Summary verified: {summary['cleaned_rows']:,} rows, runtime {summary['pipeline_runtime_seconds']}s")

    # 2. Busiest Stations
    busiest_file = os.path.join(RESULTS_DIR, "busiest_stations.parquet")
    assert os.path.exists(busiest_file), f"Missing {busiest_file}"
    busiest = pd.read_parquet(busiest_file)
    assert len(busiest) > 0, "busiest_stations table is empty"
    required_cols = {"station_code", "station_name", "total_stops", "distinct_trains"}
    assert required_cols.issubset(set(busiest.columns)), f"Missing columns in busiest_stations: {required_cols - set(busiest.columns)}"
    assert (busiest["total_stops"] > 0).all(), "total_stops must be positive"
    print(f"  [✓] Busiest stations verified: {len(busiest)} rows, top is {busiest['station_name'].iloc[0]}")

    # 3. Longest Trains
    longest_file = os.path.join(RESULTS_DIR, "longest_trains.parquet")
    assert os.path.exists(longest_file), f"Missing {longest_file}"
    longest = pd.read_parquet(longest_file)
    assert len(longest) > 0, "longest_trains table is empty"
    assert "num_stops" in longest.columns, "Missing num_stops column"
    assert (longest["num_stops"] > 0).all(), "num_stops must be positive"
    print(f"  [✓] Longest trains verified: {len(longest)} rows, top train has {longest['num_stops'].iloc[0]} stops")

    # 4. Stoppage Stats
    stoppage_file = os.path.join(RESULTS_DIR, "stoppage_stats.parquet")
    assert os.path.exists(stoppage_file), f"Missing {stoppage_file}"
    stoppage = pd.read_parquet(stoppage_file)
    assert len(stoppage) > 0, "stoppage_stats table is empty"
    assert "avg_duration_mins" in stoppage.columns, "Missing avg_duration_mins column"
    assert (stoppage["avg_duration_mins"].dropna() >= 0).all(), "Found negative avg duration"
    print(f"  [✓] Stoppage stats verified: {len(stoppage)} rows, all non-negative")

    # 5. Hourly Traffic
    hourly_file = os.path.join(RESULTS_DIR, "hourly_traffic.parquet")
    assert os.path.exists(hourly_file), f"Missing {hourly_file}"
    hourly = pd.read_parquet(hourly_file)
    assert len(hourly) == 24, f"hourly_traffic must have 24 rows, got {len(hourly)}"
    assert set(hourly["hour"]) == set(range(24)), "hourly_traffic must cover hours 0..23"
    assert (hourly["arrivals"] > 0).all(), "All hourly windows must have positive arrivals"
    print(f"  [✓] Hourly traffic verified: exactly 24 hours, total arrivals: {hourly['arrivals'].sum():,}")

    # 6. Stops Parquet
    stops_file = os.path.join(PROCESSED_DIR, "stops.parquet")
    assert os.path.exists(stops_file), f"Missing {stops_file}"
    stops = pd.read_parquet(stops_file, columns=["station_code", "station_name", "stop_duration_mins"])
    assert len(stops) > 0, "stops.parquet is empty"
    non_null_durations = stops["stop_duration_mins"].dropna()
    assert (non_null_durations >= 0).all(), "Found negative stop_duration_mins in stops.parquet"
    print(f"  [✓] Cleaned stops verified: {len(stops):,} rows, all stop durations non-negative")

    # 7. Station Lookup Filter Test
    test_station = busiest["station_name"].iloc[0]
    filtered = stops[stops["station_name"] == test_station]
    assert len(filtered) > 0, f"Expected matching stops for {test_station}, found 0"
    print(f"  [✓] Station lookup test for '{test_station}': {len(filtered)} matching stops")

    # 8. Train Lookup Filter Test
    trains_file = os.path.join(PROCESSED_DIR, "stops.parquet")
    all_stops = pd.read_parquet(trains_file)
    test_train_num = "15906"
    train_stops = all_stops[all_stops["train_number"] == test_train_num].sort_values("stop_sequence")
    assert len(train_stops) > 0, f"Expected matching stops for train {test_train_num}, found 0"
    assert "station_name" in train_stops.columns, "Missing station_name in train route"
    assert "stop_sequence" in train_stops.columns, "Missing stop_sequence in train route"
    print(f"  [✓] Train lookup test for Train #{test_train_num} ({train_stops['train_name'].iloc[0]}): {len(train_stops)} route halts")

    # 9. Global Clean Dataset CSV Generation Test
    from app import get_clean_dataset_csv_bytes
    clean_bytes = get_clean_dataset_csv_bytes()
    assert clean_bytes is not None, "get_clean_dataset_csv_bytes returned None"
    assert len(clean_bytes) > 10_000_000, f"Clean CSV bytes too small ({len(clean_bytes)} bytes)"
    print(f"  [✓] Global clean CSV export verified: {len(clean_bytes)/1024/1024:.2f} MB generated")

    # 10. Interactive Altair Chart Generation Test
    from src.chart_utils import (
        create_busiest_stations_chart,
        create_longest_trains_chart,
        create_stoppage_stats_chart,
        create_hourly_traffic_chart,
        create_train_dwell_profile_chart
    )
    ch1 = create_busiest_stations_chart(busiest)
    assert ch1 is not None, "busiest_stations Altair chart failed to compile"
    ch2 = create_longest_trains_chart(longest)
    assert ch2 is not None, "longest_trains Altair chart failed to compile"
    ch3 = create_stoppage_stats_chart(stoppage.head(20))
    assert ch3 is not None, "stoppage_stats Altair chart failed to compile"
    ch4 = create_hourly_traffic_chart(hourly)
    assert ch4 is not None, "hourly_traffic Altair chart failed to compile"
    ch5 = create_train_dwell_profile_chart(train_stops)
    assert ch5 is not None, "train_dwell_profile Altair chart failed to compile"
    print("  [✓] Interactive Altair charts with tooltips compiled successfully")

except AssertionError as e:
    print(f"\n[FAIL] Assertion error during verification: {e}")
    sys.exit(1)
except Exception as e:
    print(f"\n[FAIL] Unexpected error during verification: {e}")
    sys.exit(1)

print("\n==================================================")
print("ALL PIPELINE & APPLICATION ASSERTIONS PASSED! [OK]")
print("==================================================")
sys.exit(0)
