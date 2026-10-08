"""
app.py — Indian Railways Train Schedule Analytics
--------------------------------------------------
A simple Streamlit dashboard that reads pre-computed results from
data/results/ and data/processed/ (Parquet files).
No Spark is needed to run this app — it uses pandas only.
"""

import os
import json
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")  # non-interactive backend for Streamlit

# ---------- paths (relative to this file) ----------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")


# ---------- helper functions ----------

def load_parquet(name, directory=RESULTS_DIR):
    """
    Load a Parquet directory (Spark writes a folder, not a single file).
    Returns a pandas DataFrame, or None if the path does not exist.
    """
    path = os.path.join(directory, name)
    if not os.path.exists(path):
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        st.error(f"Error reading {name}: {e}")
        return None


def load_json(name, directory=RESULTS_DIR):
    """Load a JSON file and return a dict, or None."""
    path = os.path.join(directory, name)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def missing_data_warning(file_name):
    """Show a friendly message when data files are missing."""
    st.warning(
        f"**{file_name}** not found. "
        f"Please run the pipeline first:\n\n"
        f"```bash\nbash run_all.sh\n```"
    )


# ---------- page config ----------
st.set_page_config(
    page_title="Indian Railways Analytics",
    page_icon="🚂",
    layout="wide"
)

st.title("🚂 Indian Railways Train Schedule Analytics")
st.caption("Built with Apache Spark • Data from Kaggle")

# ---------- header metrics ----------
summary = load_json("run_summary.json")
if summary:
    cols = st.columns(5)
    cols[0].metric("Raw Records", f"{summary['raw_rows']:,}")
    cols[1].metric("Cleaned Records", f"{summary['cleaned_rows']:,}")
    cols[2].metric("Distinct Trains", f"{summary['distinct_trains']:,}")
    cols[3].metric("Distinct Stations", f"{summary['distinct_stations']:,}")
    cols[4].metric("Pipeline Time", f"{summary['pipeline_runtime_seconds']}s")
    st.divider()
else:
    missing_data_warning("run_summary.json")

# ---------- tabs ----------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview",
    "⏱ Stoppage Times",
    "🕐 Hourly Traffic",
    "🔍 Station Lookup",
    "ℹ️ About the Pipeline"
])

# ====== TAB 1: Overview ======
with tab1:
    st.header("Busiest Stations")
    busiest = load_parquet("busiest_stations.parquet")
    if busiest is not None:
        # Determine station columns dynamically
        name_col = "station_name" if "station_name" in busiest.columns else (
            "station_code" if "station_code" in busiest.columns else busiest.columns[0])
        code_col = "station_code" if "station_code" in busiest.columns else name_col

        # Bar chart of top 20 by total stops
        fig, ax = plt.subplots(figsize=(12, 6))
        display_col = name_col if name_col != code_col else code_col
        ax.barh(
            busiest[display_col].astype(str),
            busiest["total_stops"],
            color="#2196F3"
        )
        ax.set_xlabel("Total Train Stops")
        ax.set_title("Top 20 Busiest Stations by Number of Train Stops")
        ax.invert_yaxis()  # highest at top
        plt.tight_layout()
        st.pyplot(fig)

        # Table
        st.subheader("Data Table")
        st.dataframe(busiest, use_container_width=True, hide_index=True)

        # Longest trains
        longest = load_parquet("longest_trains.parquet")
        if longest is not None:
            st.subheader("Top 20 Trains with Most Stops")
            train_display = "train_name" if "train_name" in longest.columns else "train_number"
            fig2, ax2 = plt.subplots(figsize=(12, 6))
            ax2.barh(
                longest[train_display].astype(str),
                longest["num_stops"],
                color="#FF9800"
            )
            ax2.set_xlabel("Number of Stops")
            ax2.set_title("Top 20 Longest Trains (Most Stops)")
            ax2.invert_yaxis()
            plt.tight_layout()
            st.pyplot(fig2)
        # Breakdowns by Zone and Train Type
        zone_df = load_parquet("breakdown_zone.parquet")
        if zone_df is not None:
            st.subheader("Traffic by Railway Zone")
            valid_zone = zone_df[zone_df["zone"].notnull() & (zone_df["zone"] != "") & (zone_df["zone"] != "?")].head(15)
            fig3, ax3 = plt.subplots(figsize=(12, 5))
            ax3.bar(valid_zone["zone"], valid_zone["total_stops"], color="#009688")
            ax3.set_ylabel("Total Train Stops")
            ax3.set_title("Train Stops by Railway Zone (Top 15)")
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig3)

        type_df = load_parquet("breakdown_train_type.parquet")
        if type_df is not None:
            st.subheader("Stops by Train Category")
            valid_type = type_df[type_df["train_type"].notnull() & (type_df["train_type"] != "")].head(10)
            fig4, ax4 = plt.subplots(figsize=(10, 5))
            ax4.bar(valid_type["train_type"], valid_type["total_stops"], color="#E91E63")
            ax4.set_ylabel("Total Train Stops")
            ax4.set_title("Train Stops by Category (Express, SF, Passenger, etc.)")
            plt.tight_layout()
            st.pyplot(fig4)
    else:
        missing_data_warning("busiest_stations.parquet")

# ====== TAB 2: Stoppage Times ======
with tab2:
    st.header("Station Stoppage Times")
    stoppage = load_parquet("stoppage_stats.parquet")
    if stoppage is not None:
        # Slider for minimum number of stops
        min_stops = st.slider(
            "Minimum number of stops at a station",
            min_value=20,
            max_value=int(stoppage["num_stops"].max()) if len(stoppage) > 0 else 100,
            value=50,
            step=10
        )

        filtered = stoppage[stoppage["num_stops"] >= min_stops].sort_values(
            "avg_duration_mins", ascending=False
        ).head(30)

        name_col = "station_name" if "station_name" in filtered.columns else (
            "station_code" if "station_code" in filtered.columns else filtered.columns[0])

        if len(filtered) > 0:
            fig, ax = plt.subplots(figsize=(12, 8))
            y_pos = range(len(filtered))
            ax.barh(
                [str(x) for x in filtered[name_col]],
                filtered["avg_duration_mins"],
                color="#4CAF50",
                label="Avg Duration"
            )
            ax.set_xlabel("Minutes")
            ax.set_title(f"Average Stop Duration (stations with ≥{min_stops} stops)")
            ax.invert_yaxis()
            plt.tight_layout()
            st.pyplot(fig)
        else:
            st.info("No stations match the selected filter.")

        st.subheader("Detailed Statistics")
        st.dataframe(
            stoppage.sort_values("avg_duration_mins", ascending=False),
            use_container_width=True,
            hide_index=True
        )
    else:
        missing_data_warning("stoppage_stats.parquet")

# ====== TAB 3: Hourly Traffic ======
with tab3:
    st.header("Train Arrivals by Hour of Day")
    hourly = load_parquet("hourly_traffic.parquet")
    if hourly is not None:
        hourly = hourly.sort_values("hour")

        fig, ax = plt.subplots(figsize=(14, 5))
        bars = ax.bar(
            hourly["hour"],
            hourly["arrivals"],
            color="#9C27B0",
            edgecolor="white"
        )
        ax.set_xlabel("Hour of Day (0–23)")
        ax.set_ylabel("Number of Train Arrivals")
        ax.set_title("Hourly Distribution of Train Arrivals")
        ax.set_xticks(range(0, 24))
        ax.set_xticklabels([f"{h:02d}:00" for h in range(24)], rotation=45)
        plt.tight_layout()
        st.pyplot(fig)

        st.subheader("Arrivals per Hour")
        st.dataframe(hourly, use_container_width=True, hide_index=True)
    else:
        missing_data_warning("hourly_traffic.parquet")

# ====== TAB 4: Station Lookup ======
with tab4:
    st.header("Station Lookup")
    stops = load_parquet("stops.parquet", directory=PROCESSED_DIR)
    if stops is not None:
        # Determine station column names
        name_col = "station_name" if "station_name" in stops.columns else (
            "station_code" if "station_code" in stops.columns else stops.columns[0])

        # Build list of unique stations for the dropdown
        stations = sorted(stops[name_col].dropna().unique().tolist())

        selected = st.selectbox(
            "Select a station:",
            stations,
            index=0 if stations else None
        )

        if selected:
            station_stops = stops[stops[name_col] == selected].copy()

            # Sort by train number if possible
            if "train_number" in station_stops.columns:
                station_stops = station_stops.sort_values("train_number")

            st.write(f"**{len(station_stops)}** trains stop at **{selected}**")

            # Select display columns
            display_cols = [c for c in [
                "train_number", "train_name", "station_code", "station_name",
                "arrival", "departure", "stop_duration_mins", "day", "stop_sequence"
            ] if c in station_stops.columns]

            st.dataframe(
                station_stops[display_cols],
                use_container_width=True,
                hide_index=True
            )
    else:
        missing_data_warning("stops.parquet")

# ====== TAB 5: About the Pipeline ======
with tab5:
    st.header("About the Pipeline")

    st.markdown("""
    ### How the Pipeline Works

    This project follows a classic **big data ETL pipeline**:

    1. **Extract**: The raw Indian Railways schedule data is downloaded from Kaggle.
       It contains the scheduled timetable of trains — every stop at every station.

    2. **Transform**: Apache Spark loads the raw JSON/CSV data, cleans it
       (removes nulls, duplicates, invalid entries), standardises time formats,
       and computes derived fields like stop duration in minutes.

    3. **Load**: The cleaned data is written as Apache Parquet — a columnar
       storage format designed for fast analytical queries and efficient compression.

    4. **Analyse**: Spark computes aggregations using both the DataFrame API and
       Spark SQL: busiest stations, stoppage time statistics, hourly traffic
       patterns, and longest-route trains.

    5. **Visualise**: This Streamlit web app reads the pre-computed Parquet results
       with pandas and shows interactive charts and tables. No Spark runs here —
       the heavy lifting was already done in the pipeline.

    ### Tools Used

    | Tool | Purpose |
    |------|---------|
    | **Apache Spark (PySpark)** | Distributed data processing engine (run in local mode) |
    | **Apache Parquet** | Columnar file format for efficient storage and querying |
    | **Python / pandas** | Data manipulation for the web app |
    | **Streamlit** | Lightweight web dashboard framework |
    | **matplotlib** | Charts and visualisations |
    | **OpenJDK 17** | JVM runtime required by Spark |

    ### Limitations

    > ⚠️ **Please read these carefully — they are important for understanding
    > what this project can and cannot show.**

    - This is **scheduled timetable data**, **not** actual running data.
      There is **no delay or real-time information**. Everything shown is what
      the timetable says, not what actually happened.
    - The dataset is **several years old** and may not match the current
      Indian Railways timetable.
    - This is a **batch pipeline** — there is no streaming or real-time
      processing.
    - The data is large enough to justify Spark (hundreds of thousands of
      records), but it is not terabytes. The pipeline is built with big data
      tools and **would scale to much larger data** on a multi-node cluster.
    - All analysis is based on the columns available in the source dataset.
      Some analyses may be limited by what fields the data provides.
    """)
