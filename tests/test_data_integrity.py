"""
Data integrity and schema tests for pipeline outputs:
- Validates all generated Parquet result tables and JSON summaries.
- Asserts row counts > 0 across result tables.
- Asserts required columns for busiest stations.
- Asserts hourly traffic has exactly 24 rows and hours 0..23.
- Asserts stoppage stats has no negative durations.
- Asserts stops parquet has positive stop durations for all non-null values.
"""

import os
import json
import pytest
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")


def test_run_summary_json():
    """Verify run_summary.json exists and contains valid metrics."""
    summary_path = os.path.join(RESULTS_DIR, "run_summary.json")
    assert os.path.exists(summary_path), f"Missing {summary_path}"

    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    assert "raw_rows" in summary
    assert "cleaned_rows" in summary
    assert "distinct_trains" in summary
    assert "distinct_stations" in summary
    assert "pipeline_runtime_seconds" in summary

    assert summary["raw_rows"] > 0
    assert summary["cleaned_rows"] > 0
    assert summary["distinct_trains"] > 0
    assert summary["distinct_stations"] > 0
    assert summary["pipeline_runtime_seconds"] > 0


def test_clean_stats_json():
    """Verify clean_stats.json exists and contains valid cleansing stats."""
    clean_stats_path = os.path.join(PROCESSED_DIR, "clean_stats.json")
    assert os.path.exists(clean_stats_path), f"Missing {clean_stats_path}"

    with open(clean_stats_path, "r", encoding="utf-8") as f:
        stats = json.load(f)

    assert "raw_rows" in stats
    assert "cleaned_rows" in stats
    assert "removed_missing" in stats
    assert "removed_duplicates" in stats

    assert stats["raw_rows"] > 0
    assert stats["cleaned_rows"] > 0
    assert stats["removed_missing"] >= 0
    assert stats["removed_duplicates"] >= 0


def test_busiest_stations_schema_and_counts():
    """Verify busiest_stations.parquet row count and required schema."""
    path = os.path.join(RESULTS_DIR, "busiest_stations.parquet")
    assert os.path.exists(path), f"Missing {path}"

    df = pd.read_parquet(path)
    assert len(df) > 0, "busiest_stations table is empty"

    required_cols = {"station_code", "station_name", "total_stops", "distinct_trains"}
    assert required_cols.issubset(set(df.columns)), f"Missing columns in busiest_stations: {required_cols - set(df.columns)}"

    assert (df["total_stops"] > 0).all(), "total_stops must be positive"
    assert (df["distinct_trains"] > 0).all(), "distinct_trains must be positive"


def test_hourly_traffic_24_hours():
    """Verify hourly_traffic has exactly 24 rows and all hours 0..23."""
    path = os.path.join(RESULTS_DIR, "hourly_traffic.parquet")
    assert os.path.exists(path), f"Missing {path}"

    df = pd.read_parquet(path)
    assert len(df) == 24, f"hourly_traffic must have exactly 24 rows, got {len(df)}"
    assert "hour" in df.columns
    assert "arrivals" in df.columns

    hours = set(df["hour"].tolist())
    expected_hours = set(range(24))
    assert hours == expected_hours, f"Missing or extra hours in hourly traffic: {expected_hours.symmetric_difference(hours)}"

    assert (df["arrivals"] > 0).all(), "Arrivals count should be positive for all hours"


def test_stoppage_stats_no_negative_durations():
    """Verify stoppage_stats has no negative durations."""
    path = os.path.join(RESULTS_DIR, "stoppage_stats.parquet")
    assert os.path.exists(path), f"Missing {path}"

    df = pd.read_parquet(path)
    assert len(df) > 0, "stoppage_stats table is empty"

    duration_cols = [c for c in ["avg_duration_mins", "max_duration_mins", "p90_duration_mins"] if c in df.columns]
    assert len(duration_cols) > 0, "No duration columns found in stoppage_stats"

    for col in duration_cols:
        non_null = df[col].dropna()
        assert (non_null >= 0).all(), f"Found negative values in stoppage_stats.{col}"


def test_longest_trains_parquet():
    """Verify longest_trains.parquet has valid records."""
    path = os.path.join(RESULTS_DIR, "longest_trains.parquet")
    assert os.path.exists(path), f"Missing {path}"

    df = pd.read_parquet(path)
    assert len(df) > 0, "longest_trains table is empty"
    assert "train_number" in df.columns
    assert "num_stops" in df.columns
    assert (df["num_stops"] > 0).all()


def test_breakdown_tables_non_empty():
    """Verify auxiliary breakdown tables are non-empty."""
    for table_name in ["breakdown_zone.parquet", "breakdown_train_type.parquet", "breakdown_state.parquet"]:
        path = os.path.join(RESULTS_DIR, table_name)
        if os.path.exists(path):
            df = pd.read_parquet(path)
            assert len(df) > 0, f"{table_name} is empty"


def test_stops_parquet_non_negative_durations():
    """Verify stops.parquet has rows and all non-null stop_duration_mins are positive (>= 0)."""
    path = os.path.join(PROCESSED_DIR, "stops.parquet")
    assert os.path.exists(path), f"Missing {path}"

    df = pd.read_parquet(path, columns=["station_code", "stop_duration_mins"])
    assert len(df) > 0, "stops.parquet is empty"

    non_null_durations = df["stop_duration_mins"].dropna()
    assert len(non_null_durations) > 0, "No non-null stop durations found"
    assert (non_null_durations >= 0).all(), "Found negative stop_duration_mins in stops.parquet"
