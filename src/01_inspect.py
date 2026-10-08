"""
01_inspect.py
-------------
Loads every data file in data/raw/ with Spark and prints:
  - schema, row count, 10 sample rows, and null counts per column.
Writes an inspection_report.txt to data/results/.
"""

import os
import sys
import glob

# Add project root to path so we can import spark_helper
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.spark_helper import get_spark

from pyspark.sql.functions import col, count, when, isnull

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")


def inspect_file(spark, filepath, report_lines):
    """Load one file and print/record its schema, counts and sample rows."""
    fname = os.path.basename(filepath)
    ext = os.path.splitext(fname)[1].lower()

    print(f"\n{'='*60}")
    print(f"FILE: {fname}")
    print(f"{'='*60}")
    report_lines.append(f"\n{'='*60}")
    report_lines.append(f"FILE: {fname}")

    # Load based on extension
    if ext == ".json":
        # Try multiLine=True first (file may be a single JSON array)
        df = spark.read.json(filepath, multiLine=True)
        if len(df.columns) == 0 or df.count() == 0:
            # Fall back to line-delimited JSON
            df = spark.read.json(filepath, multiLine=False)
    elif ext == ".csv":
        df = spark.read.csv(filepath, header=True, inferSchema=True)
    elif ext in (".parquet", ".pq"):
        df = spark.read.parquet(filepath)
    else:
        print(f"  Skipping unsupported format: {ext}")
        report_lines.append(f"  Skipped (unsupported format: {ext})")
        return None

    # Schema
    print("\nSCHEMA:")
    df.printSchema()
    schema_str = df._jdf.schema().treeString()
    report_lines.append(f"\nSCHEMA:\n{schema_str}")

    # Row count
    row_count = df.count()
    col_count = len(df.columns)
    print(f"\nRows: {row_count:,}   Columns: {col_count}")
    report_lines.append(f"Rows: {row_count:,}   Columns: {col_count}")

    # Column names
    col_names = df.columns
    print(f"Columns: {col_names}")
    report_lines.append(f"Columns: {col_names}")

    # Null counts per column (computed in a single pass)
    print("\nNULL COUNTS:")
    null_exprs = [
        count(when(
            isnull(col(c)) | (col(c).cast("string") == "") | (col(c).cast("string") == "None"),
            c
        )).alias(c)
        for c in df.columns
    ]
    null_row = df.select(*null_exprs).first().asDict()
    null_counts = {}
    for c in df.columns:
        n = null_row.get(c, 0)
        null_counts[c] = n
        line = f"  {c}: {n:,} nulls ({100*n/max(row_count,1):.1f}%)"
        print(line)
        report_lines.append(line)

    # Sample rows (bypassing PySpark toPandas distutils requirement)
    import pandas as pd
    print("\nSAMPLE ROWS (first 10):")
    rows = [r.asDict() for r in df.limit(10).collect()]
    sample = pd.DataFrame(rows)
    print(sample.to_string(index=False))
    report_lines.append(f"\nSAMPLE ROWS (first 10):\n{sample.to_string(index=False)}")

    return {"file": fname, "rows": row_count, "cols": col_count, "columns": col_names}


def main():
    # Find all files in data/raw/
    all_files = sorted(glob.glob(os.path.join(RAW_DIR, "*")))
    if not all_files:
        print(f"ERROR: No files found in {RAW_DIR}")
        print("Run 00_download.py first to get the data.")
        sys.exit(1)

    print(f"Found {len(all_files)} file(s) in data/raw/:")
    for f in all_files:
        print(f"  {os.path.basename(f)}")

    spark = get_spark("InspectData")
    report_lines = ["INSPECTION REPORT", "=" * 40]
    summaries = []

    for fpath in all_files:
        # Skip directories
        if os.path.isdir(fpath):
            continue
        info = inspect_file(spark, fpath, report_lines)
        if info:
            summaries.append(info)

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    report_lines.append(f"\n{'='*60}")
    report_lines.append("SUMMARY")
    for s in summaries:
        line = f"  {s['file']}: {s['rows']:,} rows, {s['cols']} columns"
        print(line)
        report_lines.append(line)

    # Write report
    os.makedirs(RESULTS_DIR, exist_ok=True)
    report_path = os.path.join(RESULTS_DIR, "inspection_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    print(f"\nReport saved to {report_path}")

    spark.stop()


if __name__ == "__main__":
    main()
