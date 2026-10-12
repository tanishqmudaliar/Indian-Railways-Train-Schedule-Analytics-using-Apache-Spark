"""
app.py — Indian Railways Train Schedule Analytics
--------------------------------------------------
A modern, executive transportation intelligence dashboard that reads pre-computed
results from data/results/ and data/processed/ (Parquet files).
No active Spark cluster required at runtime — uses Pandas and Altair.
"""

import os
import sys
import json
import html
import streamlit as st
import pandas as pd
import altair as alt
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")  # non-interactive backend for Streamlit fallback

# Add project root to sys.path so we can import src.chart_utils
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.chart_utils import (
    create_busiest_stations_chart,
    create_longest_trains_chart,
    create_stoppage_stats_chart,
    create_hourly_traffic_chart,
    create_breakdown_chart,
    create_train_dwell_profile_chart,
    COLOR_STEEL_BLUE,
    COLOR_AMBER,
    COLOR_EMERALD,
    COLOR_INDIGO,
    COLOR_TEAL,
    COLOR_ROSE,
)

RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")


# ---------- caching & data loaders ----------

@st.cache_data
def load_css(css_file="assets/style.css"):
    """Load custom CSS stylesheet from assets directory."""
    css_path = os.path.join(PROJECT_ROOT, css_file)
    if os.path.exists(css_path):
        with open(css_path, "r", encoding="utf-8") as f:
            return f.read()
    return ""


@st.cache_data
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


@st.cache_data
def load_json(name, directory=RESULTS_DIR):
    """Load a JSON file and return a dict, or None."""
    path = os.path.join(directory, name)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_data
def get_clean_dataset_csv_bytes():
    """
    Produce CSV bytes of the authoritative final cleaned dataset (stops.parquet).
    Excludes internal indices and formats columns cleanly.
    """
    stops_df = load_parquet("stops.parquet", directory=PROCESSED_DIR)
    if stops_df is None or len(stops_df) == 0:
        return None
    export_cols = [
        "train_number", "train_name", "train_type",
        "station_code", "station_name",
        "arrival", "departure", "stop_sequence", "day",
        "stop_duration_mins", "zone", "state"
    ]
    cols_to_use = [c for c in export_cols if c in stops_df.columns]
    return stops_df[cols_to_use].to_csv(index=False).encode("utf-8")


def missing_data_warning(file_name):
    """Show a friendly message when data files are missing."""
    st.warning(
        f"**{file_name}** not found. "
        f"Please run the pipeline first:\n\n"
        f"```bash\nbash run_all.sh\n```"
    )


def make_columns(spec, vertical_alignment="center"):
    """Create Streamlit columns with vertical alignment fallback for backwards compatibility."""
    try:
        return st.columns(spec, vertical_alignment=vertical_alignment)
    except TypeError:
        return st.columns(spec)


def render_csv_download(
    df: pd.DataFrame,
    filename: str,
    label: str = "📥 Download CSV",
    key: str = None,
    caption: str = None,
    compact: bool = False
):
    """
    Standardized, strictly right-aligned CSV export action bar.
    Displays a subtle record counter on the left and the download button pinned to the right.
    """
    if df is None or len(df) == 0:
        st.caption("No data available to export.")
        return
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    button_label = f"{label} ({len(df):,} rows)"

    if compact:
        col_info, col_btn = make_columns([1, 1.8], vertical_alignment="center")
    else:
        col_info, col_btn = make_columns([3, 1.2], vertical_alignment="center")

    with col_info:
        if caption:
            st.markdown(
                f"<div style='font-size: 0.76rem; color: #94A3B8; padding-top: 2px;'>{caption}</div>",
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                f"<div style='font-size: 0.76rem; color: #64748B; padding-top: 2px;'>📊 <strong>{len(df):,}</strong> records available for export</div>",
                unsafe_allow_html=True
            )
    with col_btn:
        st.download_button(
            label=button_label,
            data=csv_bytes,
            file_name=filename,
            mime="text/csv",
            key=key,
            help=f"Download {filename} representing the current filtered data",
            use_container_width=True
        )


# ---------- page config & styling ----------

st.set_page_config(
    page_title="Indian Railways Analytics",
    page_icon="🚂",
    layout="wide",
    initial_sidebar_state="collapsed"
)

css_styles = load_css()
if css_styles:
    st.markdown(f"<style>{css_styles}</style>", unsafe_allow_html=True)


# ---------- hero header ----------

st.markdown("""
<div class="hero-box">
  <div class="hero-eyebrow">
    <span class="pulse-dot"></span>
    <span class="eyebrow-text">INDIAN RAILWAYS • DATA INTELLIGENCE</span>
    <span class="eyebrow-badge">APACHE SPARK PIPELINE</span>
  </div>
  <div class="hero-title">Train Schedule Analytics</div>
  <div class="hero-subtitle">
    Explore network activity, station utilization, stoppage patterns, and end-to-end train routes from processed Indian Railways timetable data.
  </div>
  <div class="hero-tags">
    <span class="hero-tag-item">Engine: <strong>PySpark Local</strong></span>
    <span>•</span>
    <span class="hero-tag-item">Storage: <strong>Apache Parquet</strong></span>
    <span>•</span>
    <span class="hero-tag-item">Source: <strong>Kaggle Dataset</strong></span>
  </div>
</div>
""", unsafe_allow_html=True)


# ---------- summary metrics & global clean export ----------

summary = load_json("run_summary.json")
if summary:
    st.markdown(f"""
    <div class="kpi-container">
      <div class="kpi-item">
        <div class="kpi-header"><span class="kpi-label">Raw Records</span></div>
        <div class="kpi-num">{summary['raw_rows']:,}</div>
        <div class="kpi-subtext">Source timetable rows</div>
      </div>
      <div class="kpi-item">
        <div class="kpi-header"><span class="kpi-label">Cleaned Records</span></div>
        <div class="kpi-num">{summary['cleaned_rows']:,}</div>
        <div class="kpi-subtext">Valid schedule stops</div>
      </div>
      <div class="kpi-item">
        <div class="kpi-header"><span class="kpi-label">Distinct Trains</span></div>
        <div class="kpi-num">{summary['distinct_trains']:,}</div>
        <div class="kpi-subtext">Active train numbers</div>
      </div>
      <div class="kpi-item">
        <div class="kpi-header"><span class="kpi-label">Distinct Stations</span></div>
        <div class="kpi-num">{summary['distinct_stations']:,}</div>
        <div class="kpi-subtext">Network station nodes</div>
      </div>
      <div class="kpi-item">
        <div class="kpi-header"><span class="kpi-label">Pipeline Time</span></div>
        <div class="kpi-num">{summary['pipeline_runtime_seconds']}s</div>
        <div class="kpi-subtext">Spark batch runtime</div>
      </div>
    </div>
    """, unsafe_allow_html=True)
else:
    missing_data_warning("run_summary.json")

# Global Clean Dataset Download Action Bar
clean_csv_bytes = get_clean_dataset_csv_bytes()
if clean_csv_bytes:
    col_dl_info, col_dl_btn = make_columns([3, 1.2], vertical_alignment="center")
    with col_dl_info:
        st.markdown("""
        <div style="font-size: 0.84rem; color: #94A3B8; padding-top: 2px;">
          📦 <strong>Complete Clean Dataset Available:</strong> 417,080 standardized, validated stop records enriched with Railway Zones and State metadata.
        </div>
        """, unsafe_allow_html=True)
    with col_dl_btn:
        st.download_button(
            label="📥 Download Clean Dataset (CSV)",
            data=clean_csv_bytes,
            file_name="cleaned-train-schedule.csv",
            mime="text/csv",
            key="global_clean_dataset_download",
            help="Download the complete, authoritative cleaned dataset (417,080 rows) as CSV",
            use_container_width=True
        )
    st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)


# ---------- navigation tabs ----------

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Overview",
    "⏱ Stoppage Times",
    "🕐 Hourly Traffic",
    "🔍 Station Lookup",
    "🚆 Train Lookup",
    "ℹ️ About the Pipeline"
])


# ==============================================================================
# ====== TAB 1: Overview ======
# ==============================================================================
with tab1:
    busiest_raw = load_parquet("busiest_stations.parquet")
    longest_raw = load_parquet("longest_trains.parquet")
    zone_raw = load_parquet("breakdown_zone.parquet")
    type_raw = load_parquet("breakdown_train_type.parquet")

    if busiest_raw is not None:
        name_col = "station_name" if "station_name" in busiest_raw.columns else "station_code"

        st.markdown("""
        <div class="section-card">
          <div class="section-head">
            <div class="section-title-wrap">
              <span class="section-pill">PRIMARY ANALYTICS • NETWORK TRAFFIC</span>
              <h2 class="section-h2">Network Overview & Busiest Junctions</h2>
              <p class="section-desc">Analyze top railway transit bottlenecks with interactive slicers and detailed tooltip inspection.</p>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Slicers Panel for Tab 1
        with st.expander("⚙️ Interactive Slicers & Display Filters", expanded=True):
            s1_col1, s1_col2, s1_col3, s1_col4 = st.columns([1.5, 2, 1.5, 1])

            with s1_col1:
                top_n_stations = st.slider(
                    "Top Stations Count",
                    min_value=5,
                    max_value=len(busiest_raw),
                    value=min(20, len(busiest_raw)),
                    step=5,
                    key="slicer_top_n_stations"
                )

            with s1_col2:
                # Station search / filter
                station_search = st.text_input(
                    "Search Station Name/Code",
                    value="",
                    placeholder="e.g. KANPUR, NDLS, HWH",
                    key="slicer_station_search"
                )

            with s1_col3:
                top_n_trains = st.slider(
                    "Top Longest Trains Count",
                    min_value=5,
                    max_value=len(longest_raw) if longest_raw is not None else 20,
                    value=min(20, len(longest_raw)) if longest_raw is not None else 20,
                    step=5,
                    key="slicer_top_n_trains"
                )

            with s1_col4:
                st.write("")
                st.write("")
                reset_t1 = st.button("↺ Reset Slicers", key="reset_t1_filters")
                if reset_t1:
                    st.rerun()

        # Apply Slicers to Busiest Stations
        busiest_filtered = busiest_raw.copy()
        if station_search.strip():
            term = station_search.strip().lower()
            busiest_filtered = busiest_filtered[
                busiest_filtered["station_name"].str.lower().str.contains(term, na=False) |
                busiest_filtered["station_code"].str.lower().str.contains(term, na=False)
            ]
        busiest_filtered = busiest_filtered.head(top_n_stations)

        # 1. Busiest Stations Visualization
        if len(busiest_filtered) > 0:
            chart1 = create_busiest_stations_chart(busiest_filtered, display_col=name_col)
            st.altair_chart(chart1, use_container_width=True)
            render_csv_download(
                busiest_filtered,
                filename="busiest_stations_chart.csv",
                label="📥 Export Chart Data",
                key="dl_busiest_chart",
                caption="Visual chart snapshot"
            )
        else:
            st.info("No stations match the search filter.")

        # Data Table
        st.markdown("""
        <div style="margin-top: 18px; margin-bottom: 8px;">
          <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #94A3B8; text-transform: uppercase;">DATA TABLE • BUSIEST STATIONS</span>
        </div>
        """, unsafe_allow_html=True)
        st.dataframe(busiest_filtered, use_container_width=True, hide_index=True)
        render_csv_download(
            busiest_filtered,
            filename="busiest_stations_table.csv",
            label="📥 Export Busiest Stations Table",
            key="dl_busiest_table",
            caption="Complete junction table data"
        )

        # 2. Longest Trains Section
        if longest_raw is not None:
            st.markdown("""
            <div class="section-card" style="margin-top: 36px;">
              <div class="section-head">
                <div class="section-title-wrap">
                  <span class="section-pill">ROUTE INTELLIGENCE • TRANSIT STOPS</span>
                  <h2 class="section-h2">Trains with Most Scheduled Stops</h2>
                  <p class="section-desc">Long-distance trans-subcontinental services operating the highest number of intermediate halts.</p>
                </div>
              </div>
            </div>
            """, unsafe_allow_html=True)

            longest_filtered = longest_raw.head(top_n_trains)
            train_display = "train_name" if "train_name" in longest_filtered.columns else "train_number"

            chart2 = create_longest_trains_chart(longest_filtered, display_col=train_display)
            st.altair_chart(chart2, use_container_width=True)
            render_csv_download(
                longest_filtered,
                filename="longest_trains_chart.csv",
                label="📥 Export Chart Data",
                key="dl_longest_chart",
                caption="Longest route visual data"
            )

            st.dataframe(longest_filtered, use_container_width=True, hide_index=True)
            render_csv_download(
                longest_filtered,
                filename="longest_trains_table.csv",
                label="📥 Export Longest Trains Table",
                key="dl_longest_table",
                caption="Complete longest train rankings"
            )

        # 3. Network Composition Section
        if zone_raw is not None or type_raw is not None:
            st.markdown("""
            <div class="section-card" style="margin-top: 36px;">
              <div class="section-head">
                <div class="section-title-wrap">
                  <span class="section-pill">NETWORK COMPOSITION • STRUCTURAL DISTRIBUTION</span>
                  <h2 class="section-h2">Network Composition Breakdown</h2>
                  <p class="section-desc">Distribution of scheduled train operations partitioned by administrative Railway Zone and Category.</p>
                </div>
              </div>
            </div>
            """, unsafe_allow_html=True)

            comp_col1, comp_col2 = st.columns(2)

            with comp_col1:
                if zone_raw is not None:
                    st.markdown("""
                    <div style="margin-bottom: 8px;">
                      <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #14B8A6; text-transform: uppercase;">TRAFFIC BY RAILWAY ZONE</span>
                    </div>
                    """, unsafe_allow_html=True)
                    valid_zone = zone_raw[zone_raw["zone"].notnull() & (zone_raw["zone"] != "") & (zone_raw["zone"] != "?")].head(15)
                    chart_z = create_breakdown_chart(valid_zone, category_col="zone", title="Railway Zone", color=COLOR_TEAL)
                    if chart_z:
                        st.altair_chart(chart_z, use_container_width=True)
                    render_csv_download(
                        valid_zone,
                        filename="railway_zone_traffic.csv",
                        label="📥 Export Zone Traffic",
                        key="dl_zone_csv",
                        compact=True
                    )

            with comp_col2:
                if type_raw is not None:
                    st.markdown("""
                    <div style="margin-bottom: 8px;">
                      <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #F43F5E; text-transform: uppercase;">STOPS BY TRAIN CATEGORY</span>
                    </div>
                    """, unsafe_allow_html=True)
                    valid_type = type_raw[type_raw["train_type"].notnull() & (type_raw["train_type"] != "")].head(10)
                    chart_t = create_breakdown_chart(valid_type, category_col="train_type", title="Train Category", color=COLOR_ROSE)
                    if chart_t:
                        st.altair_chart(chart_t, use_container_width=True)
                    render_csv_download(
                        valid_type,
                        filename="train_category_stops.csv",
                        label="📥 Export Category Stops",
                        key="dl_type_csv",
                        compact=True
                    )
    else:
        missing_data_warning("busiest_stations.parquet")


# ==============================================================================
# ====== TAB 2: Stoppage Times ======
# ==============================================================================
with tab2:
    stoppage_raw = load_parquet("stoppage_stats.parquet")
    if stoppage_raw is not None:
        st.markdown("""
        <div class="section-card">
          <div class="section-head">
            <div class="section-title-wrap">
              <span class="section-pill">EFFICIENCY & DWELL TIMES • INVESTIGATION WORKSPACE</span>
              <h2 class="section-h2">Station Stoppage Times & Dwell Outliers</h2>
              <p class="section-desc">Analyse average and 90th percentile dwell durations with interactive slicers to isolate locomotive reversal hubs.</p>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        name_col = "station_name" if "station_name" in stoppage_raw.columns else "station_code"

        # Stoppage Slicers Panel
        with st.expander("⚙️ Interactive Slicers & Filters", expanded=True):
            st_col1, st_col2, st_col3, st_col4 = st.columns([1.5, 1.5, 1.5, 1])

            with st_col1:
                min_stops = st.slider(
                    "Min Scheduled Stops",
                    min_value=20,
                    max_value=int(stoppage_raw["num_stops"].max()) if len(stoppage_raw) > 0 else 100,
                    value=50,
                    step=10,
                    key="slicer_min_stops"
                )

            with st_col2:
                max_avg_dwell = float(stoppage_raw["avg_duration_mins"].max())
                dwell_threshold = st.slider(
                    "Min Avg Dwell Time (min)",
                    min_value=1.0,
                    max_value=float(max(25.0, float(round(max_avg_dwell)))),
                    value=10.0,
                    step=1.0,
                    key="slicer_dwell_threshold"
                )

            with st_col3:
                stoppage_search = st.text_input(
                    "Filter Station Name",
                    value="",
                    placeholder="e.g. Sambalpur, Mailani",
                    key="slicer_stoppage_search"
                )

            with st_col4:
                top_stoppage_n = st.selectbox(
                    "Display Limit",
                    options=[15, 25, 50, 100],
                    index=1,
                    key="slicer_top_stoppage_n"
                )

            reset_t2 = st.button("↺ Reset Slicers", key="reset_t2_filters")
            if reset_t2:
                st.rerun()

        # Apply Filters
        filtered_stoppage = stoppage_raw[
            (stoppage_raw["num_stops"] >= min_stops) &
            (stoppage_raw["avg_duration_mins"] >= dwell_threshold)
        ].copy()

        if stoppage_search.strip():
            term = stoppage_search.strip().lower()
            filtered_stoppage = filtered_stoppage[
                filtered_stoppage["station_name"].str.lower().str.contains(term, na=False) |
                filtered_stoppage["station_code"].str.lower().str.contains(term, na=False)
            ]

        filtered_stoppage = filtered_stoppage.sort_values("avg_duration_mins", ascending=False)
        display_stoppage = filtered_stoppage.head(top_stoppage_n)

        # Dynamic Metrics
        st.markdown(f"""
        <div style="margin-top: 14px; margin-bottom: 14px; display: flex; justify-content: space-between; align-items: baseline;">
          <div>
            <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #10B981; text-transform: uppercase;">ANALYTICAL RESULT • DWELL DURATION</span>
            <div style="font-size: 1.15rem; font-weight: 700; color: #F8FAFC;">
              Stations with ≥{min_stops} stops and ≥{dwell_threshold}m avg dwell
            </div>
          </div>
          <div style="font-size: 0.8rem; color: #94A3B8;">
            Showing <strong>{len(display_stoppage)}</strong> of <strong>{len(filtered_stoppage)}</strong> matching stations
          </div>
        </div>
        """, unsafe_allow_html=True)

        if len(display_stoppage) > 0:
            chart_stoppage = create_stoppage_stats_chart(display_stoppage, name_col=name_col)
            st.altair_chart(chart_stoppage, use_container_width=True)
            render_csv_download(
                display_stoppage,
                filename="stoppage_duration_chart.csv",
                label="📥 Export Chart Data",
                key="dl_stoppage_chart",
                caption="Top dwell bottleneck snapshot"
            )
        else:
            st.info("No stations match the selected filter combination. Try adjusting the thresholds.")

        st.markdown("""
        <div style="margin-top: 28px; margin-bottom: 8px;">
          <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #94A3B8; text-transform: uppercase;">DETAILED STATISTICS TABLE</span>
        </div>
        """, unsafe_allow_html=True)
        st.dataframe(filtered_stoppage, use_container_width=True, hide_index=True)
        render_csv_download(
            filtered_stoppage,
            filename="stoppage_stats_table.csv",
            label="📥 Export Filtered Stoppage Table",
            key="dl_stoppage_table",
            caption="All matching station dwell statistics"
        )
    else:
        missing_data_warning("stoppage_stats.parquet")


# ==============================================================================
# ====== TAB 3: Hourly Traffic ======
# ==============================================================================
with tab3:
    hourly_raw = load_parquet("hourly_traffic.parquet")
    if hourly_raw is not None:
        hourly_raw = hourly_raw.sort_values("hour")

        st.markdown("""
        <div class="section-card">
          <div class="section-head">
            <div class="section-title-wrap">
              <span class="section-pill">TEMPORAL NETWORK ACTIVITY • 24-HOUR PROFILE</span>
              <h2 class="section-h2">Train Arrivals by Hour of Day</h2>
              <p class="section-desc">Diurnal distribution of scheduled arrivals across India, featuring time window slicing and traffic proportion metrics.</p>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Slicers Panel for Tab 3
        with st.expander("⚙️ Interactive Time Window Slicers", expanded=True):
            h_col1, h_col2, h_col3 = st.columns([2, 2, 1])

            with h_col1:
                hour_range = st.slider(
                    "Hourly Window (Start Hour – End Hour)",
                    min_value=0,
                    max_value=23,
                    value=(0, 23),
                    step=1,
                    key="slicer_hour_range"
                )

            with h_col2:
                time_preset = st.selectbox(
                    "Quick Transit Window Preset",
                    options=[
                        "Custom Range",
                        "All 24 Hours (00:00 – 23:00)",
                        "Morning Surge (06:00 – 11:00)",
                        "Midday Lull (12:00 – 15:00)",
                        "Evening Peak (16:00 – 21:00)",
                        "Overnight Corridors (22:00 – 05:00)"
                    ],
                    index=1,
                    key="slicer_time_preset"
                )

            with h_col3:
                st.write("")
                st.write("")
                reset_t3 = st.button("↺ Reset Slicers", key="reset_t3_filters")
                if reset_t3:
                    st.rerun()

        # Apply Preset or Custom Range
        start_h, end_h = hour_range
        if time_preset == "All 24 Hours (00:00 – 23:00)":
            start_h, end_h = 0, 23
        elif time_preset == "Morning Surge (06:00 – 11:00)":
            start_h, end_h = 6, 11
        elif time_preset == "Midday Lull (12:00 – 15:00)":
            start_h, end_h = 12, 15
        elif time_preset == "Evening Peak (16:00 – 21:00)":
            start_h, end_h = 16, 21
        elif time_preset == "Overnight Corridors (22:00 – 05:00)":
            # Handled with wrapping
            pass

        if time_preset == "Overnight Corridors (22:00 – 05:00)":
            hourly_filtered = hourly_raw[(hourly_raw["hour"] >= 22) | (hourly_raw["hour"] <= 5)].copy()
        else:
            hourly_filtered = hourly_raw[(hourly_raw["hour"] >= start_h) & (hourly_raw["hour"] <= end_h)].copy()

        # Dynamic KPIs for Window
        total_24h = hourly_raw["arrivals"].sum()
        window_total = hourly_filtered["arrivals"].sum()
        window_pct = (window_total / max(total_24h, 1)) * 100.0
        peak_row = hourly_filtered.loc[hourly_filtered["arrivals"].idxmax()] if len(hourly_filtered) > 0 else None

        st.markdown(f"""
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin: 12px 0 20px 0;">
          <div class="kpi-item" style="padding: 12px 16px;">
            <div class="kpi-label">Window Arrivals</div>
            <div class="kpi-num" style="font-size: 1.4rem;">{window_total:,}</div>
            <div class="kpi-subtext">{window_pct:.1f}% of nationwide daily traffic</div>
          </div>
          <div class="kpi-item" style="padding: 12px 16px;">
            <div class="kpi-label">Peak Traffic Window</div>
            <div class="kpi-num" style="font-size: 1.4rem; color: #818CF8;">
              {int(peak_row['hour']):02d}:00
            </div>
            <div class="kpi-subtext">{int(peak_row['arrivals']):,} arrivals during peak</div>
          </div>
          <div class="kpi-item" style="padding: 12px 16px;">
            <div class="kpi-label">Active Hour Bins</div>
            <div class="kpi-num" style="font-size: 1.4rem;">{len(hourly_filtered)} / 24</div>
            <div class="kpi-subtext">Hourly intervals analyzed</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        if len(hourly_filtered) > 0:
            chart_hourly = create_hourly_traffic_chart(hourly_filtered)
            st.altair_chart(chart_hourly, use_container_width=True)
            render_csv_download(
                hourly_filtered,
                filename="hourly_traffic_chart.csv",
                label="📥 Export Chart Data",
                key="dl_hourly_chart",
                caption="Window traffic snapshot"
            )

        st.markdown("""
        <div style="margin-top: 24px; margin-bottom: 8px;">
          <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #94A3B8; text-transform: uppercase;">DATA TABLE • ARRIVALS PER HOUR</span>
        </div>
        """, unsafe_allow_html=True)
        st.dataframe(hourly_filtered, use_container_width=True, hide_index=True)
        render_csv_download(
            hourly_filtered,
            filename="hourly_traffic_table.csv",
            label="📥 Export Hourly Table CSV",
            key="dl_hourly_table",
            caption="Diurnal hourly arrival data"
        )
    else:
        missing_data_warning("hourly_traffic.parquet")


# ==============================================================================
# ====== TAB 4: Station Lookup ======
# ==============================================================================
with tab4:
    stops = load_parquet("stops.parquet", directory=PROCESSED_DIR)
    if stops is not None:
        st.markdown("""
        <div class="section-card">
          <div class="section-head">
            <div class="section-title-wrap">
              <span class="section-pill">NODE INTELLIGENCE • TIMETABLE QUERY</span>
              <h2 class="section-h2">Station Schedule Lookup</h2>
              <p class="section-desc">Query station-specific train arrivals, departures, halt durations, and transit sequences with multi-attribute filtering.</p>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        name_col = "station_name" if "station_name" in stops.columns else "station_code"

        @st.cache_data
        def get_unique_stations(df_names):
            return sorted(df_names.dropna().unique().tolist())

        stations = get_unique_stations(stops[name_col])

        # Station Selector
        selected_station = st.selectbox(
            "Select or Search a Railway Station:",
            stations,
            index=stations.index("KANPUR CENTRAL") if "KANPUR CENTRAL" in stations else 0,
            key="selectbox_station_lookup"
        )

        if selected_station:
            raw_station_stops = stops[stops[name_col] == selected_station].copy()

            # Slicers within Station Lookup
            with st.expander("⚙️ Filter Station Timetable (Category, Time, Duration)", expanded=False):
                stn_c1, stn_c2, stn_c3, stn_c4 = st.columns([1.5, 1.5, 1.5, 1])

                with stn_c1:
                    available_types = ["All"] + sorted([t for t in raw_station_stops["train_type"].dropna().unique().tolist() if t])
                    type_filter = st.selectbox(
                        "Train Category",
                        options=available_types,
                        index=0,
                        key="stn_slicer_train_type"
                    )

                with stn_c2:
                    min_halt_slider = st.slider(
                        "Min Halt Duration (mins)",
                        min_value=0,
                        max_value=30,
                        value=0,
                        step=2,
                        key="stn_slicer_min_halt"
                    )

                with stn_c3:
                    time_window = st.selectbox(
                        "Arrival Time Period",
                        options=["All Day (00:00 – 23:59)", "Morning (06:00–12:00)", "Afternoon (12:00–18:00)", "Evening (18:00–24:00)", "Night (00:00–06:00)"],
                        index=0,
                        key="stn_slicer_time_period"
                    )

                with stn_c4:
                    st.write("")
                    st.write("")
                    reset_stn = st.button("↺ Reset Filters", key="reset_stn_filters")
                    if reset_stn:
                        st.rerun()

            # Apply Slicers
            station_stops = raw_station_stops.copy()
            if type_filter != "All":
                station_stops = station_stops[station_stops["train_type"] == type_filter]

            if min_halt_slider > 0:
                station_stops = station_stops[station_stops["stop_duration_mins"] >= min_halt_slider]

            if time_window != "All Day (00:00 – 23:59)":
                def in_window(arr_str):
                    if not arr_str or ":" not in str(arr_str):
                        return False
                    try:
                        h = int(arr_str.split(":")[0])
                        if time_window.startswith("Morning") and 6 <= h < 12:
                            return True
                        if time_window.startswith("Afternoon") and 12 <= h < 18:
                            return True
                        if time_window.startswith("Evening") and 18 <= h <= 23:
                            return True
                        if time_window.startswith("Night") and 0 <= h < 6:
                            return True
                    except:
                        return False
                    return False

                station_stops = station_stops[station_stops["arrival"].apply(in_window)]

            # Sort by train number or arrival
            if "arrival" in station_stops.columns:
                station_stops = station_stops.sort_values("arrival", na_position="last")

            station_code_val = raw_station_stops['station_code'].iloc[0] if ('station_code' in raw_station_stops.columns and len(raw_station_stops) > 0) else ""
            avg_halt = raw_station_stops["stop_duration_mins"].mean()

            escaped_code = html.escape(str(station_code_val))
            escaped_name = html.escape(str(selected_station))

            st.markdown(f"""
            <div class="station-stat-card">
              <div class="station-stat-header">
                <span class="station-stat-badge">SELECTED STATION NODE</span>
                <span class="station-code-badge">{escaped_code}</span>
              </div>
              <div class="station-stat-name">{escaped_name}</div>
              <div class="station-stat-count">
                <span class="stat-highlight">{len(raw_station_stops):,}</span> total scheduled train services stop at this station
                • Average Halt Duration: <strong style="color:#10B981;">{avg_halt:.1f} mins</strong>
                • Showing <strong>{len(station_stops):,}</strong> with active slicers
              </div>
            </div>
            """, unsafe_allow_html=True)

            display_cols = [c for c in [
                "train_number", "train_name", "train_type", "station_code", "station_name",
                "arrival", "departure", "stop_duration_mins", "day", "stop_sequence"
            ] if c in station_stops.columns]

            st.markdown("""
            <div style="margin-top: 16px; margin-bottom: 8px;">
              <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #94A3B8; text-transform: uppercase;">STATION TIMETABLE SCHEDULE</span>
            </div>
            """, unsafe_allow_html=True)
            st.dataframe(
                station_stops[display_cols],
                use_container_width=True,
                hide_index=True
            )
            render_csv_download(
                station_stops[display_cols],
                filename=f"station_{station_code_val}_schedule.csv",
                label="📥 Export Station Timetable",
                key="dl_stn_schedule",
                caption=f"Complete schedule timetable for {selected_station}"
            )
    else:
        missing_data_warning("stops.parquet")


# ==============================================================================
# ====== TAB 5: Train Lookup (NEW FEATURE) ======
# ==============================================================================
with tab5:
    stops_train = load_parquet("stops.parquet", directory=PROCESSED_DIR)
    if stops_train is not None:
        st.markdown("""
        <div class="section-card">
          <div class="section-head">
            <div class="section-title-wrap">
              <span class="section-pill">TRANSIT ROUTE INTELLIGENCE • ITINERARY EXPLORER</span>
              <h2 class="section-h2">Train Route & Timetable Lookup</h2>
              <p class="section-desc">Search any train by number or service name to inspect its end-to-end station itinerary, halt profiles, and transited states.</p>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        @st.cache_data
        def build_train_catalog(df):
            """Pre-compute unique trains list with metadata for instant dropdown population."""
            trains_meta = (
                df[["train_number", "train_name", "train_type"]]
                .drop_duplicates(subset=["train_number"])
                .sort_values("train_number")
            )
            trains_meta["label"] = trains_meta.apply(
                lambda r: f"{r['train_number']} — {r['train_name']} ({r['train_type'] if pd.notnull(r['train_type']) and r['train_type'] != '' else 'Rail'})",
                axis=1
            )
            return trains_meta

        train_catalog = build_train_catalog(stops_train)

        # Train Search & Filter Slicers
        with st.expander("🔍 Search Filters & Route Slicers", expanded=True):
            tr_s1, tr_s2, tr_s3 = st.columns([2, 1.5, 1])

            with tr_s1:
                search_query = st.text_input(
                    "Search Train by Number or Name",
                    value="",
                    placeholder="e.g. 15906, Rajdhani, Vivek, Howrah",
                    key="trn_text_search"
                )

            with tr_s2:
                cat_options = ["All Categories"] + sorted([t for t in train_catalog["train_type"].dropna().unique().tolist() if t])
                selected_cat = st.selectbox(
                    "Filter Category",
                    options=cat_options,
                    index=0,
                    key="trn_category_filter"
                )

            with tr_s3:
                st.write("")
                st.write("")
                reset_trn_btn = st.button("↺ Reset Search", key="reset_trn_filters")
                if reset_trn_btn:
                    st.rerun()

        # Filter the catalog
        matching_trains = train_catalog.copy()
        if selected_cat != "All Categories":
            matching_trains = matching_trains[matching_trains["train_type"] == selected_cat]

        if search_query.strip():
            sq = search_query.strip().lower()
            matching_trains = matching_trains[
                matching_trains["train_number"].str.lower().str.contains(sq, na=False) |
                matching_trains["train_name"].str.lower().str.contains(sq, na=False)
            ]

        if len(matching_trains) == 0:
            st.warning(f"No trains found matching '{search_query}'. Showing all services.")
            matching_trains = train_catalog

        # Dropdown selection
        selected_train_label = st.selectbox(
            "Select Train Service:",
            options=matching_trains["label"].tolist(),
            index=0,
            key="selectbox_train_picker"
        )

        selected_train_num = selected_train_label.split(" — ")[0].strip()

        # Fetch all stops for the selected train
        train_route = stops_train[stops_train["train_number"] == selected_train_num].copy()
        if "stop_sequence" in train_route.columns:
            train_route = train_route.sort_values("stop_sequence")

        # Determine Origin, Destination & Metrics
        first_stop = train_route.iloc[0]
        last_stop = train_route.iloc[-1]
        origin_stn = f"{first_stop['station_name']} ({first_stop['station_code']})"
        dest_stn = f"{last_stop['station_name']} ({last_stop['station_code']})"
        total_halts = len(train_route)
        train_name_val = first_stop.get("train_name", selected_train_num)
        train_type_val = first_stop.get("train_type", "Standard")
        states_list = sorted(train_route["state"].dropna().unique().tolist()) if "state" in train_route.columns else []
        zones_list = sorted(train_route["zone"].dropna().unique().tolist()) if "zone" in train_route.columns else []
        max_day = int(train_route["day"].max()) if ("day" in train_route.columns and train_route["day"].notnull().any()) else 1

        escaped_train_num = html.escape(str(selected_train_num))
        escaped_train_name = html.escape(str(train_name_val))
        escaped_train_type = html.escape(str(train_type_val))
        escaped_origin = html.escape(str(origin_stn))
        escaped_dest = html.escape(str(dest_stn))

        # Train Information Card
        st.markdown(f"""
        <div class="train-stat-card">
          <div class="train-stat-header">
            <span class="train-type-badge">{escaped_train_type}</span>
            <span class="train-stat-number">#{escaped_train_num}</span>
          </div>
          <div class="train-stat-name">{escaped_train_name}</div>
          <div class="train-route-line">
            <span>🏁 <strong>{escaped_origin}</strong></span>
            <span>➔</span>
            <span>🎯 <strong>{escaped_dest}</strong></span>
          </div>
          <div class="train-metrics-row">
            <div class="train-sub-metric">
              <div class="train-sub-label">Total Halts</div>
              <div class="train-sub-val">{total_halts:,} stations</div>
            </div>
            <div class="train-sub-metric">
              <div class="train-sub-label">Journey Days</div>
              <div class="train-sub-val">{max_day} {'day' if max_day == 1 else 'days'}</div>
            </div>
            <div class="train-sub-metric">
              <div class="train-sub-label">States Crossed</div>
              <div class="train-sub-val">{len(states_list)} states</div>
            </div>
            <div class="train-sub-metric">
              <div class="train-sub-label">Railway Zones</div>
              <div class="train-sub-val">{len(zones_list)} zones</div>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Route-level Slicers
        with st.expander("⚙️ Filter Route Itinerary (By State, Zone, or Halt Duration)", expanded=False):
            rf_col1, rf_col2, rf_col3 = st.columns([1.5, 1.5, 1.5])
            with rf_col1:
                selected_states = st.multiselect(
                    "Filter Stops by State",
                    options=states_list,
                    default=[],
                    key="route_filter_states"
                )
            with rf_col2:
                selected_zones = st.multiselect(
                    "Filter Stops by Zone",
                    options=zones_list,
                    default=[],
                    key="route_filter_zones"
                )
            with rf_col3:
                min_route_halt = st.slider(
                    "Min Halt Duration (mins)",
                    min_value=0,
                    max_value=20,
                    value=0,
                    step=1,
                    key="route_filter_halt"
                )

        # Apply route slicers
        filtered_route = train_route.copy()
        if selected_states:
            filtered_route = filtered_route[filtered_route["state"].isin(selected_states)]
        if selected_zones:
            filtered_route = filtered_route[filtered_route["zone"].isin(selected_zones)]
        if min_route_halt > 0:
            filtered_route = filtered_route[filtered_route["stop_duration_mins"] >= min_route_halt]

        # 1. Visual Route Halt Profile Chart
        st.markdown("""
        <div style="margin-top: 20px; margin-bottom: 8px;">
          <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #38BDF8; text-transform: uppercase;">VISUAL ROUTE PROFILE • HALT DURATION PER STATION</span>
        </div>
        """, unsafe_allow_html=True)

        if len(filtered_route[filtered_route["stop_duration_mins"].notnull()]) > 0:
            dwell_chart = create_train_dwell_profile_chart(filtered_route)
            if dwell_chart:
                st.altair_chart(dwell_chart, use_container_width=True)
            render_csv_download(
                filtered_route[["stop_sequence", "station_code", "station_name", "arrival", "departure", "stop_duration_mins", "day", "state"]],
                filename=f"train_{selected_train_num}_halts_chart.csv",
                label="📥 Export Halt Profile Data",
                key="dl_train_halts_chart",
                caption=f"Route dwell profile for Train #{selected_train_num}"
            )
        else:
            st.info("No intermediate halt durations available for the selected filter.")

        # 2. Detailed Route Timetable Table
        st.markdown("""
        <div style="margin-top: 24px; margin-bottom: 8px;">
          <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #94A3B8; text-transform: uppercase;">COMPLETE TIMETABLE ITINERARY</span>
        </div>
        """, unsafe_allow_html=True)

        route_display_cols = [c for c in [
            "stop_sequence", "station_code", "station_name", "arrival", "departure",
            "stop_duration_mins", "day", "state", "zone"
        ] if c in filtered_route.columns]

        st.dataframe(filtered_route[route_display_cols], use_container_width=True, hide_index=True)

        render_csv_download(
            filtered_route[route_display_cols],
            filename=f"train_{selected_train_num}_route_schedule.csv",
            label="📥 Export Route Schedule",
            key="dl_train_route_table",
            caption=f"Complete schedule itinerary for Train #{selected_train_num}"
        )
    else:
        missing_data_warning("stops.parquet")


# ==============================================================================
# ====== TAB 6: About the Pipeline ======
# ==============================================================================
with tab6:
    st.markdown("""
    <div class="section-card">
      <div class="section-head">
        <div class="section-title-wrap">
          <span class="section-pill">SYSTEM ARCHITECTURE • BIG DATA ENGINEERING</span>
          <h2 class="section-h2">About the Pipeline & Global Data Export</h2>
          <p class="section-desc">Technical specification of the distributed Apache Spark ETL lifecycle, analytical data model, and system boundaries.</p>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="margin-bottom: 8px;">
      <span style="font-size: 0.75rem; font-weight: 700; letter-spacing: 0.08em; color: #38BDF8; text-transform: uppercase;">HOW THE PIPELINE WORKS</span>
      <p style="font-size: 0.88rem; color: #CBD5E1; margin: 4px 0 12px 0;">This project follows a classic <strong>big data ETL pipeline</strong> architecture:</p>
    </div>

    <div class="pipeline-flow">
      <div class="flow-step">
        <div class="flow-num">Stage 01</div>
        <div class="flow-title">Extract</div>
        <div class="flow-desc">
          Raw Indian Railways schedule data downloaded from Kaggle containing the scheduled timetable of trains — every stop at every station.
        </div>
      </div>
      <div class="flow-step">
        <div class="flow-num">Stage 02</div>
        <div class="flow-title">Transform</div>
        <div class="flow-desc">
          Apache Spark loads the raw JSON/CSV data, cleans it (removes nulls, duplicates, invalid entries), standardises time formats, and computes derived fields like stop duration in minutes.
        </div>
      </div>
      <div class="flow-step">
        <div class="flow-num">Stage 03</div>
        <div class="flow-title">Load</div>
        <div class="flow-desc">
          The cleaned data is written as Apache Parquet — a columnar storage format designed for fast analytical queries and efficient compression.
        </div>
      </div>
      <div class="flow-step">
        <div class="flow-num">Stage 04</div>
        <div class="flow-title">Analyse</div>
        <div class="flow-desc">
          Spark computes aggregations using both the DataFrame API and Spark SQL: busiest stations, stoppage time statistics, hourly traffic patterns, and longest-route trains.
        </div>
      </div>
      <div class="flow-step">
        <div class="flow-num">Stage 05</div>
        <div class="flow-title">Visualise</div>
        <div class="flow-desc">
          This Streamlit web app reads the pre-computed Parquet results with pandas and shows interactive charts and tables. No Spark runs here — the heavy lifting was already done in the pipeline.
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Stage 03 Clean Dataset Export Section
    st.markdown("""
    <div class="section-card" style="margin-top: 24px;">
      <div class="section-head">
        <div class="section-title-wrap">
          <span class="section-pill">AUTHORITATIVE CLEAN DATASET EXPORT</span>
          <h2 class="section-h2">Download Clean Processed Timetable (CSV)</h2>
          <p class="section-desc">Directly download the final validated dataset (417,080 rows) outputted by Stage 02/03 of the Apache Spark ETL pipeline.</p>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    clean_bytes_t6 = get_clean_dataset_csv_bytes()
    if clean_bytes_t6:
        col_c_info, col_c_btn = make_columns([3, 1.2], vertical_alignment="center")
        with col_c_info:
            st.markdown("""
            <div style="font-size: 0.78rem; color: #94A3B8; padding-top: 2px;">
              📊 <strong>417,080</strong> cleaned schedule stops with normalized timestamps and metadata.
            </div>
            """, unsafe_allow_html=True)
        with col_c_btn:
            st.download_button(
                label="📥 Download Clean Dataset (CSV)",
                data=clean_bytes_t6,
                file_name="cleaned-train-schedule.csv",
                mime="text/csv",
                key="dl_clean_dataset_tab6",
                help="Download the complete, authoritative cleaned dataset (417,080 rows) as CSV",
                use_container_width=True
            )
    else:
        st.warning("Cleaned dataset (stops.parquet) is not available.")

    st.markdown("""
    <div style="margin-top: 28px; margin-bottom: 12px;">
      <span style="font-size: 0.75rem; font-weight: 700; letter-spacing: 0.08em; color: #38BDF8; text-transform: uppercase;">TOOLS & TECHNOLOGY STACK</span>
    </div>
    """, unsafe_allow_html=True)

    tools_data = [
        {"Tool": "Apache Spark (PySpark 3.5)", "Purpose": "Distributed batch data processing engine (run in local mode)"},
        {"Tool": "Apache Parquet", "Purpose": "Columnar Snappy-compressed storage for high throughput analytical reads"},
        {"Tool": "Python / pandas / pyarrow", "Purpose": "High-efficiency data manipulation and Parquet table loading"},
        {"Tool": "Streamlit & Altair", "Purpose": "Interactive reactive web dashboard with fluid hover tooltips and dynamic slicers"},
        {"Tool": "Eclipse Temurin OpenJDK 17", "Purpose": "JVM runtime required by Apache Spark"},
    ]
    st.dataframe(pd.DataFrame(tools_data), use_container_width=True, hide_index=True)

    st.markdown("""
    <div class="limitation-box">
      <div class="limitation-box-title">
        <span>⚠️</span>
        <span>Operational Limitations & Data Boundaries</span>
      </div>
      <div class="limitation-box-sub">
        Please read these carefully — they are important for understanding what this project can and cannot show.
      </div>
      <ul>
        <li>
          This is <strong>scheduled timetable data</strong>, <strong>not</strong> actual running data.
          There is <strong>no delay or real-time GPS tracking</strong>. Everything shown reflects scheduled timetables, not operational delays.
        </li>
        <li>
          The dataset is a <strong>historical snapshot</strong> and does not reflect newly inaugurated routes (e.g. Vande Bharat Express).
        </li>
        <li>
          This is an <strong>offline batch pipeline</strong> — there is no real-time stream ingestion.
        </li>
        <li>
          The pipeline is built with big data tools and <strong>would scale to terabytes of data</strong> across an Apache Spark cluster.
        </li>
      </ul>
    </div>
    """, unsafe_allow_html=True)
