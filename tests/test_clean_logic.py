"""
Unit tests for data cleaning logic:
- Midnight-crossing duration calculation
- Time column cleaning (nulls, empty strings, nan, null)
"""

import os
import sys
import importlib
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def calculate_duration_mins(arrival: str, departure: str):
    """
    Python mirror of compute_duration logic from src/02_clean.py:
    duration = (dep - arr) if dep >= arr else (dep + 1440 - arr)
    """
    if arrival is None or departure is None:
        return None
    arr_clean = str(arrival).strip()
    dep_clean = str(departure).strip()
    if not arr_clean or not dep_clean or ":" not in arr_clean or ":" not in dep_clean:
        return None
    try:
        arr_h, arr_m = [int(p) for p in arr_clean.split(":")[:2]]
        dep_h, dep_m = [int(p) for p in dep_clean.split(":")[:2]]
    except (ValueError, IndexError):
        return None

    arr_total = arr_h * 60 + arr_m
    dep_total = dep_h * 60 + dep_m

    if dep_total >= arr_total:
        return dep_total - arr_total
    else:
        # Midnight crossover: add 24 hours (1440 mins) to departure
        return (dep_total + 1440) - arr_total


def clean_time_value(time_val):
    """
    Python mirror of clean_time_column logic from src/02_clean.py:
    Empty string, None, 'nan', 'none', 'null' become None.
    """
    if time_val is None:
        return None
    s = str(time_val).strip()
    if s == "" or s.lower() in ("none", "nan", "null"):
        return None
    return s


# ==============================================================================
# Pure Python Logic Verification Tests
# ==============================================================================

def test_compute_duration_normal():
    """Test normal duration within the same day."""
    # 08:00 to 08:20 -> 20 minutes
    assert calculate_duration_mins("08:00", "08:20") == 20
    # 10:15 to 11:30 -> 75 minutes
    assert calculate_duration_mins("10:15", "11:30") == 75
    # Same arrival and departure -> 0 minutes
    assert calculate_duration_mins("12:00", "12:00") == 0


def test_compute_duration_midnight_crossover():
    """Test duration when departure crosses midnight."""
    # 23:45 to 00:15 -> 30 minutes
    assert calculate_duration_mins("23:45", "00:15") == 30
    # 23:55 to 00:05 -> 10 minutes
    assert calculate_duration_mins("23:55", "00:05") == 10
    # 22:30 to 01:15 -> 165 minutes
    assert calculate_duration_mins("22:30", "01:15") == 165


def test_compute_duration_null_handling():
    """Test that null arrival or departure produces null duration."""
    assert calculate_duration_mins(None, "08:20") is None
    assert calculate_duration_mins("08:00", None) is None
    assert calculate_duration_mins(None, None) is None
    assert calculate_duration_mins("", "08:20") is None
    assert calculate_duration_mins("08:00", "") is None


def test_clean_time_column_nulls_and_placeholders():
    """Test that empty strings and placeholder literals become None."""
    assert clean_time_value("") is None
    assert clean_time_value("   ") is None
    assert clean_time_value("None") is None
    assert clean_time_value("none") is None
    assert clean_time_value("NONE") is None
    assert clean_time_value("nan") is None
    assert clean_time_value("NaN") is None
    assert clean_time_value("null") is None
    assert clean_time_value("NULL") is None
    assert clean_time_value(None) is None


def test_clean_time_column_valid_times():
    """Test that valid time formats are preserved and trimmed."""
    assert clean_time_value("08:00") == "08:00"
    assert clean_time_value(" 14:30 ") == "14:30"
    assert clean_time_value("00:15:00") == "00:15:00"
    assert clean_time_value("23:59") == "23:59"


# ==============================================================================
# PySpark DataFrame Integration Tests (when Spark JVM is available)
# ==============================================================================

@pytest.fixture(scope="module")
def spark_session():
    """Fixture to obtain or skip SparkSession for DataFrame unit tests."""
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .appName("TestCleanLogic")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.local.dir", "./.spark_tmp")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        yield spark
        spark.stop()
    except Exception:
        yield None


def test_spark_compute_duration(spark_session):
    """Test compute_duration function directly on PySpark DataFrame."""
    if spark_session is None:
        pytest.skip("SparkSession not available in this environment")

    clean_mod = importlib.import_module("src.02_clean")
    compute_duration = clean_mod.compute_duration

    data = [
        ("08:00", "08:20"),  # Normal: 20 mins
        ("23:45", "00:15"),  # Midnight crossover: 30 mins
        (None, "08:20"),     # Null arrival: None
        ("08:00", None),     # Null departure: None
        (None, None),        # Both null: None
    ]
    df = spark_session.createDataFrame(data, ["arrival", "departure"])
    result_df = compute_duration(df)
    rows = result_df.select("stop_duration_mins").collect()

    assert rows[0]["stop_duration_mins"] == 20
    assert rows[1]["stop_duration_mins"] == 30
    assert rows[2]["stop_duration_mins"] is None
    assert rows[3]["stop_duration_mins"] is None
    assert rows[4]["stop_duration_mins"] is None


def test_spark_clean_time_column(spark_session):
    """Test clean_time_column function directly on PySpark DataFrame."""
    if spark_session is None:
        pytest.skip("SparkSession not available in this environment")

    clean_mod = importlib.import_module("src.02_clean")
    clean_time_column = clean_mod.clean_time_column

    data = [
        ("", ),
        ("   ", ),
        ("None", ),
        ("nan", ),
        ("null", ),
        (None, ),
        ("08:00", ),
        (" 14:30 ", ),
    ]
    df = spark_session.createDataFrame(data, ["time_col"])
    result_df = clean_time_column(df, "time_col")
    rows = result_df.select("time_col").collect()

    assert rows[0]["time_col"] is None
    assert rows[1]["time_col"] is None
    assert rows[2]["time_col"] is None
    assert rows[3]["time_col"] is None
    assert rows[4]["time_col"] is None
    assert rows[5]["time_col"] is None
    assert rows[6]["time_col"] == "08:00"
    assert rows[7]["time_col"] == "14:30"
