# EXPLAINER — What Each Script Does and Why

This document explains the project in plain English, suitable for a viva examination.

---

## Overview

This project analyses the Indian Railways scheduled timetable data using a **big data pipeline**. The pipeline follows the classic **Extract → Transform → Load → Analyse → Visualise** pattern.

---

## Scripts

### `src/00_download.py` — Extract

Downloads the Indian Railways dataset from Kaggle into `data/raw/`. It tries the `kagglehub` Python library first, then falls back to the Kaggle CLI. The raw data is a large JSON file containing every scheduled train stop at every station.

**Why?** The first step in any data pipeline is acquiring the raw data from its source.

### `src/01_inspect.py` — Understand the Data

Loads every file in `data/raw/` using Apache Spark and prints the schema (column names and types), row count, sample rows, and null counts. This tells us what we are working with before we start cleaning.

**Why?** You should never assume what the data looks like. Inspection prevents bugs and wrong assumptions downstream.

### `src/02_clean.py` — Transform

This is the core ETL (Extract–Transform–Load) step:

1. **Loads** the main data file into a Spark DataFrame.
2. **Maps columns** to standard names (train_number, station_code, arrival, departure, etc.).
3. **Cleans** values: replaces "None", empty strings, and "nan" with actual nulls.
4. **Computes stop duration** in minutes (departure time minus arrival time), handling the midnight crossover case (e.g., arrives at 23:50, departs at 00:05 = 15 minutes).
5. **Removes** rows with missing train numbers or station identifiers.
6. **Removes** exact duplicate rows.
7. **Writes** the result to `data/processed/stops.parquet`.

**Why Parquet?** Parquet is a columnar storage format. Unlike CSV or JSON:
- It stores data by column, so reading just one column is very fast.
- It compresses much better (often 5–10× smaller than JSON).
- It preserves data types (no need to re-infer schemas).
- It is the standard output format for Spark and other big data tools.

### `src/03_analyse.py` — Analyse

Reads the cleaned Parquet data and computes five analyses:

1. **Busiest stations** — the top 20 stations by number of train stops and distinct trains.
2. **Stoppage time statistics** — average, maximum, and 90th percentile stop duration per station.
3. **Hourly traffic** — how many trains arrive in each hour of the day (0–23).
4. **Longest trains** — the 20 trains with the most stops along their route.
5. **Breakdowns** — by zone, state, or train type if those columns exist in the data.

Two of these analyses are written using **Spark SQL** (`spark.sql(...)` on a temporary view) to demonstrate both the DataFrame API and the SQL API.

Each result is saved as a small Parquet file in `data/results/`. A `run_summary.json` captures key numbers (row counts, distinct trains/stations, runtime).

### `app.py` — Visualise

A Streamlit web app that reads the pre-computed Parquet results with pandas (not Spark). It has five tabs:

- **Overview** — bar charts and tables of the busiest stations and longest trains.
- **Stoppage Times** — average stop duration chart with a slider to filter by minimum number of stops.
- **Hourly Traffic** — a bar chart of train arrivals per hour.
- **Station Lookup** — pick any station from a dropdown and see every train that stops there.
- **About** — explains the pipeline and states its limitations.

---

## Why Apache Spark?

Apache Spark is a distributed computing framework designed for large-scale data processing. Even though this dataset fits on a single machine, we use Spark because:

1. **Scalability** — the same code would work on a multi-node cluster with terabytes of data, just by changing the `master()` setting.
2. **The DataFrame API** — Spark DataFrames are like SQL tables in code. Operations like `groupBy`, `agg`, `filter`, and `join` are expressive and optimised by Spark's Catalyst query planner.
3. **Spark SQL** — you can write standard SQL queries on DataFrames, which is familiar and easy to explain.
4. **Parquet integration** — Spark reads and writes Parquet natively and efficiently.
5. **Industry standard** — Spark is widely used in industry for ETL pipelines, making this a realistic demonstration.

## Why Parquet over CSV/JSON?

| Feature | CSV/JSON | Parquet |
|---------|----------|---------|
| Storage size | Large | Compressed (5–10× smaller) |
| Read speed | Must scan entire file | Column pruning — reads only needed columns |
| Schema | Inferred (error-prone) | Embedded in the file |
| Data types | Everything is strings | Native types (int, float, timestamp) |
| Splittable | CSV yes, JSON usually no | Yes — supports parallel reads |

---

## Key Concepts for Viva

- **ETL** = Extract, Transform, Load — the standard pattern for data pipelines.
- **Local mode** = Spark runs on a single machine using all CPU cores (`local[*]`).
- **DataFrame API** = a programmatic way to express data transformations (like SQL in code).
- **Spark SQL** = write actual SQL queries on Spark DataFrames using temporary views.
- **Parquet** = columnar storage format — fast reads, good compression, embedded schema.
- **Batch processing** = process all data at once (as opposed to streaming / real-time).
