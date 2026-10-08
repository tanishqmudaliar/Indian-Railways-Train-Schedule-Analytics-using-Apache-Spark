# Indian Railways Train Schedule Analytics using Apache Spark

Distributed Big Data ETL and timetable analytics pipeline processing 417,000+ Indian Railways scheduled stops using Apache Spark, PySpark SQL, and Parquet, visualized with an interactive Streamlit dashboard.

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.9-E25A1C?logo=apachespark&logoColor=white)
![Java](https://img.shields.io/badge/OpenJDK-17_Temurin-5382A1?logo=openjdk&logoColor=white)
![Apache Parquet](https://img.shields.io/badge/Storage-Apache_Parquet-4B6A9B)
![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit_1.65-FF4B4B?logo=streamlit&logoColor=white)
![Platform](https://img.shields.io/badge/Platform-WSL2_Ubuntu_Linux-E95420?logo=ubuntu&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
  - [Distributed ETL Pipeline](#distributed-etl-pipeline)
  - [Big Data Analytics Engine](#big-data-analytics-engine)
  - [Interactive Streamlit Dashboard](#interactive-streamlit-dashboard)
- [Architecture](#architecture)
- [Data Flow & Processing Stages](#data-flow--processing-stages)
- [Analytics & Key Findings](#analytics--key-findings)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Quick Setup](#quick-setup)
  - [Step-by-Step Execution](#step-by-step-execution)
- [Project Structure](#project-structure)
- [Comprehensive File Breakdown](#comprehensive-file-breakdown)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Known Limitations](#known-limitations)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

Indian Railways is the fourth-largest national railway network in the world, managing thousands of passenger services and millions of passenger journeys daily across more than 8,500 stations. Operating at this magnitude introduces complex scheduling bottlenecks, long-distance dwell-time variations, and high transit density corridors that cannot be analyzed effectively with traditional single-threaded spreadsheets.

This project implements an end-to-end Big Data Analytics (BDA) pipeline using **Apache Spark 3.5 (PySpark)** and **Apache Parquet**. The pipeline ingests over **417,000 scheduled stop records** from Indian Railways timetable data, performs schema validation, cleanses missing and midnight-crossing schedule timings, enriches records with railway zones and state coordinates, and computes five core analytical aggregations using the Spark DataFrame API and Spark SQL.

All processed datasets and analytical summaries are persisted in columnar Snappy-compressed Parquet format and presented through an interactive, multi-tab **Streamlit** dashboard featuring customizable stoppage threshold sliders, station route lookups, and visual distribution charts.

---

## Features

### Distributed ETL Pipeline
- **Automated Kaggle Extraction:** Seamlessly fetches `schedules.json`, `stations.json`, and `trains.json` using `kagglehub`.
- **Spark Schema & Quality Inspection:** Scans 417,080 rows and 8 schema columns, cataloging null counts, distinct distributions, and structural data anomalies.
- **Robust Timetable Cleaning:** Standardizes time formats (`HH:MM`), corrects arrival/departure flips, and computes true dwell duration across midnight-crossing boundaries `(departure_minutes + 1440 - arrival_minutes)`.
- **Relational Metadata Enrichment:** Joins nested GeoJSON records to associate each stop with official Railway Zones (e.g., NR, WR, CR, SR), Indian States, and standardized Train Types (Rajdhani, Superfast, Mail/Express, Passenger).
- **Columnar Parquet Storage:** Stores processed stop events using Apache Parquet with Snappy compression, enabling high compression ratios and sub-second selective column scans.

### Big Data Analytics Engine
- **Network Transit Hub Identification:** Aggregates total scheduled stops per station to identify the nation's critical railway junction bottlenecks.
- **Stoppage Duration Outlier Analysis:** Computes average dwell time and the 90th percentile stoppage duration using Spark SQL's `PERCENTILE_APPROX(duration_minutes, 0.9)`.
- **Hourly Temporal Flow Profiling:** Extracts arrival hours to map peak morning and evening commuter surges across the entire subcontinental grid.
- **Route Length Ranking:** Analyzes multi-day passenger routes using Spark SQL to rank trains by total scheduled stops across their end-to-end journey.
- **Multi-Dimensional Partitioning:** Aggregates traffic patterns across railway zones, states, and service categories.

### Interactive Streamlit Dashboard
- **Executive Overview Tab:** Live KPI metric cards displaying total stops, trains, stations, and Spark execution runtimes.
- **Dynamic Stoppage Slider:** Interactive UI control allowing users to dynamically filter stations by minimum stop count (10 to 100+ stops) and visualize top dwell times.
- **Hourly Traffic Bar Chart:** Visualizes bimodal 24-hour network traffic patterns.
- **Instant Station Lookup:** Interactive search bar to query any of the 8,539 stations, revealing total stopping trains, average stoppage time, and a searchable schedule table.
- **Pipeline Architecture & Diagnostics:** On-screen documentation detailing schema definitions, processing stages, and execution parameters.

---

## Architecture

The project follows a decoupled Big Data batch architecture. Apache Spark is dedicated strictly to heavy extraction, distributed cleaning, and aggregation, writing columnar Parquet files to disk. The frontend (Streamlit) acts as a lean presentation layer that reads only the lightweight pre-computed Parquet tables using Pandas and PyArrow, ensuring immediate dashboard responsiveness without keeping a heavy Spark JVM process active.

```
+---------------------------------------------------------------------------------------------------+
|                                     DATA INGESTION & STORAGE                                      |
|                                                                                                   |
|  Kaggle Indian Railways Dataset                                                                   |
|  ├── schedules.json (78.4 MB, 417k stops) ──┐                                                     |
|  ├── stations.json  (1.8 MB GeoJSON)      ──┼──> [ data/raw/ ]                                    |
|  └── trains.json    (14.1 MB GeoJSON)     ──┘                                                     |
+---------------------------------------------------------------------------------------------------+
                                                  │
                                                  ▼
+---------------------------------------------------------------------------------------------------+
|                                  APACHE SPARK 3.5 BATCH PIPELINE                                  |
|                                                                                                   |
|  [01_inspect.py]  Schema validation, null counts, column distribution -> inspection_report.txt    |
|         │                                                                                         |
|         ▼                                                                                         |
|  [02_clean.py]    Time format standardization, midnight crossing duration calculation,           |
|                   GeoJSON enrichment (zone, state, train_type)                                    |
|                   └── Output: data/processed/stops.parquet (417,080 rows)                         |
|         │                                                                                         |
|         ▼                                                                                         |
|  [03_analyse.py]  Distributed Aggregations via Spark DataFrame API & Spark SQL:                   |
|                   ├── busiest_stations.parquet   (Top 20 transit junctions)                       |
|                   ├── stoppage_stats.parquet     (Avg & P90 stoppage with PERCENTILE_APPROX)      |
|                   ├── hourly_traffic.parquet     (24-hour network arrival distribution)           |
|                   ├── longest_trains.parquet     (Top 20 train routes by stop count)              |
|                   ├── breakdown_*.parquet        (State, Zone, Train Type aggregations)           |
|                   └── run_summary.json           (Pipeline verification metrics)                  |
+---------------------------------------------------------------------------------------------------+
                                                  │
                                                  ▼
+---------------------------------------------------------------------------------------------------+
|                                   STREAMLIT PRESENTATION LAYER                                    |
|                                                                                                   |
|  [app.py]  Reads Parquet via PyArrow / Pandas (No active Spark context needed)                    |
|            ├── Tab 1: Overview KPIs & Executive Metrics                                           |
|            ├── Tab 2: Stoppage Times Analysis (Dynamic Min-Stops Slider)                          |
|            ├── Tab 3: Hourly Network Traffic Chart                                                |
|            ├── Tab 4: Interactive Station Lookup & Timetable Search                               |
|            └── Tab 5: Architecture & Pipeline Diagnostics                                         |
+---------------------------------------------------------------------------------------------------+
```

---

## Data Flow & Processing Stages

| Stage | Script | Input | Transformation / Spark Operation | Output |
| :--- | :--- | :--- | :--- | :--- |
| **0. Extract** | `src/00_download.py` | Kaggle API / HTTP | Ingests raw JSON archives directly into project root storage | `data/raw/*.json` |
| **1. Inspect** | `src/01_inspect.py` | `data/raw/` | `spark.read.json()`, column null counting, distinct type checks | `data/results/inspection_report.txt` |
| **2. Clean** | `src/02_clean.py` | `data/raw/` | Handles midnight crossing durations, regex time cleansing, joins station/train GeoJSON metadata | `data/processed/stops.parquet` |
| **3. Analyse** | `src/03_analyse.py` | `stops.parquet` | Spark SQL `GROUP BY`, `COUNT`, `AVG`, `PERCENTILE_APPROX(0.9)`, sorting | `data/results/*.parquet` & `run_summary.json` |
| **4. Visualise** | `app.py` | `data/results/` | PyArrow table loading, Pandas reshaping, Streamlit UI rendering | Interactive Web UI (`http://localhost:8501`) |

---

## Analytics & Key Findings

All figures below are computed from the real 417,080 record dataset during actual pipeline execution.

### Overall Pipeline Summary
- **Raw Timetable Records Ingested:** `417,080`
- **Cleaned & Processed Records:** `417,080`
- **Unique Trains Analyzed:** `5,208`
- **Unique Railway Stations Mapped:** `8,539`
- **End-to-End Spark Pipeline Runtime:** `140.1 seconds` (executed on local 8-core CPU)

### 1. Kanpur Central & Vijayawada are India's Primary Transit Bottlenecks
Kanpur Central is the busiest railway junction in the entire timetable network, serving as the central funnel between Delhi and eastern India:

| Rank | Station Code | Station Name | Total Scheduled Stops | Unique Trains Serviced |
| :---: | :--- | :--- | :---: | :---: |
| 1 | **CNB** | KANPUR CENTRAL | **312** | 300 |
| 2 | **BZA** | VIJAYAWADA JN | **297** | 291 |
| 3 | **NDLS** | NEW DELHI | **269** | 249 |
| 4 | **BSB** | VARANASI JN | **238** | 225 |
| 5 | **HWH** | HOWRAH JN | **238** | 227 |
| 6 | **AGC** | AGRA CANTT | **237** | 226 |
| 7 | **BPL** | BHOPAL JN | **236** | 227 |
| 8 | **ALD** | ALLAHABAD JN (PRAYAGRAJ) | **236** | 230 |
| 9 | **ET** | ITARSI JN | **234** | 224 |
| 10 | **GZB** | GHAZIABAD | **231** | 221 |

### 2. The 698-Stop Vivek Express & Trans-Subcontinental Corridors
The longest scheduled routes in the country span over 4,000 km and stop nearly 700 times across 8 states:
1. **Vivek Express (#15906 / #15905)** — *Dibrugarh ↔ Kanniyakumari*: **698 scheduled stops**
2. **Guwahati – Trivandrum SF Express (#12507 / #12508)**: **637 scheduled stops**
3. **Guwahati – Trivandrum Central Express (#12516 / #12515)**: **633 scheduled stops**
4. **Bengaluru – New Tinsukia Weekly Express (#15901 / #15902)**: **627 scheduled stops**
5. **Chennai Egmore – Dibrugarh Weekly Express (#15929 / #15930)**: **554 scheduled stops**

### 3. Bimodal Hourly Commuter Surges & Loco-Reversal Stoppages
- **Network Traffic Distribution:** Scheduled train arrivals exhibit a distinct bimodal peak across the country. Morning traffic peaks at **08:00 (17,930 arrivals)**, while evening traffic peaks at **18:00 (17,422 arrivals)**. Midday exhibits the lowest traffic at **13:00 (13,662 arrivals)**.
- **Stoppage Duration Outliers:** While suburban halts average 1–2 minutes, major locomotive reversal junctions demonstrate average dwell times near 20 minutes with 90th percentiles up to 30–35 minutes:
  - **Sambalpur (`SBP`):** Average **18.9 mins** | 90th Percentile: **25.0 mins** | Max: **135 mins**
  - **Anuppur Junction (`APR`):** Average **18.7 mins** | 90th Percentile: **30.0 mins** | Max: **320 mins**
  - **Mailani (`MLN`):** Average **18.6 mins** | 90th Percentile: **30.0 mins** | Max: **60 mins**
  - **Saharanpur (`SRE`):** Average **18.1 mins** | 90th Percentile: **35.0 mins** | Max: **100 mins**
  - **Gorakhpur Junction (`GKP`):** Average **18.0 mins** | 90th Percentile: **25.0 mins** | Max: **40 mins**

---

## Tech Stack

| Layer | Technologies |
| :--- | :--- |
| **Distributed Engine** | Apache Spark 3.5.9 (PySpark DataFrame API, Spark SQL) |
| **Java Virtual Machine** | Eclipse Temurin OpenJDK 17.0.20.1+1 (isolated in project folder) |
| **Language & Environment** | Python 3.12 (WSL2 Ubuntu Linux, virtual environment `.venv/`) |
| **Storage Formats** | Apache Parquet (Snappy compression), JSON / GeoJSON |
| **Data Processing & Analysis** | Pandas, PyArrow, NumPy |
| **Frontend Dashboard** | Streamlit 1.65.0, Matplotlib |
| **Dataset Ingestion** | KaggleHub API / Direct Kaggle download |
| **Shell & Orchestration** | Bash (`run_all.sh`, `run_python.sh`) |

---

## Getting Started

### Prerequisites
- **Operating System:** Windows 10/11 with **WSL2** (Ubuntu 22.04 or later) installed, or native Linux / macOS.
- **Python:** Python 3.10+ (Python 3.12 verified).
- **Disk Space:** At least 1.5 GB of free space for datasets, virtualenv, and Parquet storage.
- **Internet Access:** For first-time dataset download via KaggleHub.

### Quick Setup

All installations, Java runtimes, virtual environments, and caches reside strictly inside the project root directory.

1. **Open your project directory in WSL2:**
   ```bash
   cd "/mnt/c/Users/tanis/Documents/VSC Projects/Python Projects/Indian Railways Train Schedule Analytics using Apache Spark"
   ```

2. **Verify Python 3 virtual environment and packages:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip setuptools
   pip install pyspark==3.5.9 pandas pyarrow streamlit matplotlib kagglehub
   ```

3. **Verify Local JDK 17:**
   The project bundles a self-contained Eclipse Temurin OpenJDK 17 inside `.java_local/current/` to eliminate modern Linux cgroup v2 container bugs.

### Step-by-Step Execution

#### Option 1: One-Click Execution (Recommended)
Run the master bash script to execute all pipeline stages sequentially:
```bash
bash run_all.sh
```

#### Option 2: Running Pipeline Stages Individually
```bash
# Set environment variables using the helper wrapper
./run_python.sh src/00_download.py   # Extract raw dataset
./run_python.sh src/01_inspect.py    # Schema inspection
./run_python.sh src/02_clean.py      # Cleaning & Parquet generation
./run_python.sh src/03_analyse.py    # Spark SQL analytics jobs
./run_python.sh test_app.py          # Automated verification test
```

#### Option 3: Launching the Web Dashboard
```bash
source .venv/bin/activate
streamlit run app.py
```
Open **`http://localhost:8501`** in your browser.

---

## Project Structure

```
Indian Railways Train Schedule Analytics using Apache Spark/
├── .java_local/                       # Isolated Temurin OpenJDK 17 installation
│   └── current/                       # JAVA_HOME target
├── .venv/                             # Python 3.12 virtual environment
├── data/
│   ├── raw/                           # Raw datasets from Kaggle
│   │   ├── schedules.json             # 417,080 timetable stop records (78.4 MB)
│   │   ├── stations.json              # GeoJSON station metadata (1.8 MB)
│   │   └── trains.json                # GeoJSON train metadata (14.1 MB)
│   ├── processed/
│   │   ├── stops.parquet/             # Cleaned & enriched stops dataset
│   │   └── clean_stats.json           # Validation metrics
│   └── results/                       # Spark aggregation outputs
│       ├── busiest_stations.parquet/  # Top 20 transit stations
│       ├── stoppage_stats.parquet/    # Avg and 90th percentile dwell times
│       ├── hourly_traffic.parquet/    # 24-hour arrival counts
│       ├── longest_trains.parquet/    # Top 20 trains ranked by stop count
│       ├── breakdown_state.parquet/   # State-wise distribution
│       ├── breakdown_zone.parquet/    # Railway zone distribution
│       ├── breakdown_train_type.parquet # Train category distribution
│       ├── inspection_report.txt      # Schema inspection and null distribution
│       └── run_summary.json           # Execution metrics & runtime
├── src/
│   ├── 00_download.py                 # Kaggle extraction script
│   ├── 01_inspect.py                  # PySpark schema inspection job
│   ├── 02_clean.py                    # PySpark cleaning & transformation job
│   └── 03_analyse.py                  # PySpark DataFrame & Spark SQL analytics
├── app.py                             # Interactive Streamlit dashboard
├── test_app.py                        # Automated dashboard data verification test
├── run_all.sh                         # Master execution script
├── run_python.sh                      # Environment-safe Python runner
├── README.md                          # Project documentation manual
├── EXPLAINER.md                       # Architecture deep-dive & viva defense guide
└── PLAN.md                            # Project requirements & engineering spec
```

---

## Comprehensive File Breakdown

- **`run_all.sh`**: The end-to-end master automation script. Verifies `JAVA_HOME` in `.java_local/current/`, activates `.venv`, validates script permissions, and executes Phases 0 through 4 with status checks and color-coded logging.
- **`run_python.sh`**: A wrapper script that dynamically configures `JAVA_HOME`, `PATH`, and relative Spark parameters (`-Dderby.system.home=./metastore_db`), safely handling paths containing spaces.
- **`src/00_download.py`**: Interacts with `kagglehub` to download `sripaadsrinivasan/indian-railways-dataset`. Copies `schedules.json`, `stations.json`, and `trains.json` directly into `data/raw/`.
- **`src/01_inspect.py`**: Initializes a PySpark session, infers schemas, validates row counts, counts nulls per column, and generates `data/results/inspection_report.txt`.
- **`src/02_clean.py`**: Core Spark cleaning job. Standardizes timestamps, calculates duration across midnight crossings, extracts coordinates/zones from GeoJSON, and writes Snappy-compressed `data/processed/stops.parquet`.
- **`src/03_analyse.py`**: Core analytics job. Executes 5 distinct Spark SQL and DataFrame aggregations, including `PERCENTILE_APPROX(0.9)` for dwell time distribution, exporting 7 Parquet result directories and `run_summary.json`.
- **`app.py`**: The Streamlit user interface. Built with 5 tabs, KPI metric cards, dynamic Matplotlib visualizations, interactive threshold filtering, and station lookup.
- **`test_app.py`**: Headless test suite that loads all generated Parquet files, verifies row integrity, tests chart generation, and validates station lookup matching without opening a browser.
- **`EXPLAINER.md`**: Complete viva defense guide covering Apache Spark architecture, shuffle partition optimization, Parquet columnar internals, design trade-offs, and typical examiner questions.

---

## Configuration

| Setting / Variable | Default Value | Description |
| :--- | :--- | :--- |
| `JAVA_HOME` | `.java_local/current` | Path to the local Eclipse Temurin OpenJDK 17 installation |
| `spark.master` | `local[*]` | Runs Spark in local mode utilizing all available CPU threads |
| `spark.sql.shuffle.partitions` | `8` | Optimized partition count matching local machine core capacity |
| `spark.driver.memory` | `4g` | JVM heap allocation for the PySpark driver |
| `spark.local.dir` | `./.spark_tmp` | Temporary scratch space for Spark shuffle and spill data |
| `derby.system.home` | `./metastore_db` | Embedded Apache Derby metastore directory (uses relative path) |
| `spark.sql.warehouse.dir` | `./spark-warehouse` | Local Spark SQL table warehouse directory |

---

## Troubleshooting

### 1. Spaces in Workspace Path Crashing JVM Classpath
- **Error:** `Error: Could not find or load main class Projects.Python`
- **Cause:** When absolute paths containing spaces (e.g., `.../VSC Projects/Python Projects/...`) are passed into JVM options like `-Dderby.system.home`, Java breaks the arguments on the space character.
- **Solution:** Configured all JVM system properties with strict relative paths (`-Dderby.system.home=./metastore_db`) within `run_python.sh` and Python scripts.

### 2. Linux Kernel cgroup v2 JMX Crash in Early Java 17
- **Error:** `NullPointerException: Cannot invoke jdk.internal.platform.CgroupInfo.getMountPoint() because anyController is null` followed by `InstanceNotFoundException` in Spark's `DirectPoolMemory`.
- **Cause:** Standard OpenJDK 17.0.2 has a known JVM bug on modern Linux kernels (Linux 6.x / modern WSL2) where unmounted cgroup controllers trigger a NullPointerException during JMX MBean registration.
- **Solution:** Bundled Eclipse Temurin OpenJDK **17.0.20.1+1** in `.java_local/current/`, which includes the upstream patch for cgroup v2 hierarchy handling.

### 3. Removal of `distutils` in Python 3.12
- **Error:** `ModuleNotFoundError: No module named 'distutils'` when calling PySpark's `.toPandas()`
- **Cause:** Python 3.12 officially deprecated and removed the standard library `distutils` package, which legacy PySpark routines rely on for version comparisons.
- **Solution:** Installed `setuptools` (which supplies the `distutils` compatibility layer) and refactored sample row formatting to collect Python dictionaries directly.

---

## Known Limitations

1. **Scheduled Timetable Data Only:** The dataset reflects planned Indian Railways timetables. It does not record real-time GPS tracking, actual arrival delays, or operational cancellations. No conclusions about punctuality or operational delay causes can be drawn.
2. **Historical Dataset Snapshot:** The data represents a snapshot of the Indian Railways network and does not include recent route modifications or newly inaugurated services (such as Vande Bharat express routes).
3. **Single-Node Simulation:** Spark executes in local standalone mode (`local[*]`). While the PySpark DataFrame code is 100% cluster-ready, distributed cluster deployment on Hadoop YARN or Kubernetes was not simulated.
4. **Batch Processing Scope:** The pipeline is designed around batch ETL and static analytics. It does not implement streaming architectures (e.g., Apache Kafka or Spark Structured Streaming).

---

## Contributing

Contributions, feedback, and issue submissions are welcome:

1. **Fork the repository**
2. **Create a feature branch:**
   ```bash
   git checkout -b feature/new-analytics-metric
   ```
3. **Commit your modifications:**
   ```bash
   git commit -m "Add regional passenger density aggregation"
   ```
4. **Push to your branch:**
   ```bash
   git push origin feature/new-analytics-metric
   ```
5. **Open a Pull Request**

---

## License

This project is licensed under the **MIT License**. See the `LICENSE` file for details.

---

Made with ❤️ by [Tanishq Mudaliar](https://github.com/tanishqmudaliar)

Stop scrolling schedules. Start processing big data. Uncover India's railway backbone with Apache Spark! 🚆⚡
