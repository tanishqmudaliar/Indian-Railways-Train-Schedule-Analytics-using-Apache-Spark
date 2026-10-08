"""
app.py — Indian Railways Train Schedule Analytics
--------------------------------------------------
A modern, executive data dashboard that reads pre-computed results from
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


# ---------- chart styling engine ----------

CHART_THEME = {
    "bg": "#111827",
    "surface": "#162032",
    "grid": "#1E293B",
    "border": "#27354A",
    "text": "#F8FAFC",
    "text_muted": "#94A3B8",
    "steel_blue": "#38BDF8",
    "railway_red": "#EF4444",
    "amber": "#F59E0B",
    "teal": "#14B8A6",
    "rose": "#F43F5E",
    "emerald": "#10B981",
    "indigo": "#818CF8",
}

def apply_chart_theme(fig, ax, is_horizontal=False, show_grid=True):
    """Apply consistent, professional enterprise styling to a Matplotlib figure and axis."""
    fig.patch.set_facecolor(CHART_THEME["bg"])
    ax.set_facecolor(CHART_THEME["bg"])
    
    # Clean spines
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(CHART_THEME["border"])
    ax.spines["bottom"].set_color(CHART_THEME["border"])
    ax.spines["left"].set_linewidth(1.0)
    ax.spines["bottom"].set_linewidth(1.0)
    
    # Tick params
    ax.tick_params(colors=CHART_THEME["text_muted"], labelsize=9.5, width=1, length=4)
    
    # Labels & Title
    ax.xaxis.label.set_color(CHART_THEME["text_muted"])
    ax.xaxis.label.set_fontsize(10)
    ax.xaxis.label.set_fontweight("500")
    ax.yaxis.label.set_color(CHART_THEME["text_muted"])
    ax.yaxis.label.set_fontsize(10)
    ax.yaxis.label.set_fontweight("500")
    ax.title.set_color(CHART_THEME["text"])
    ax.title.set_fontsize(12)
    ax.title.set_fontweight("600")
    
    # Grid
    if show_grid:
        if is_horizontal:
            ax.grid(axis="x", linestyle="--", alpha=0.4, color=CHART_THEME["grid"], linewidth=0.75)
            ax.grid(axis="y", visible=False)
        else:
            ax.grid(axis="y", linestyle="--", alpha=0.4, color=CHART_THEME["grid"], linewidth=0.75)
            ax.grid(axis="x", visible=False)
    else:
        ax.grid(False)
    ax.set_axisbelow(True)


# ---------- page config ----------

st.set_page_config(
    page_title="Indian Railways Analytics",
    page_icon="🚂",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------- custom CSS styling system ----------

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

:root {
  --bg-primary: #0A0E17;
  --bg-card: #111827;
  --bg-card-elevated: #162032;
  --bg-card-hover: #1A263D;
  --border-subtle: #1E293B;
  --border-card: #243247;
  --border-highlight: #334155;
  --text-main: #F8FAFC;
  --text-secondary: #94A3B8;
  --text-dim: #64748B;
  --railway-red: #EF4444;
  --railway-red-glow: rgba(239, 68, 68, 0.15);
  --steel-blue: #38BDF8;
  --network-teal: #14B8A6;
  --transit-amber: #F59E0B;
  --dwell-emerald: #10B981;
  --traffic-indigo: #818CF8;
}

/* Global Reset & Base */
html, body, [data-testid="stAppViewContainer"] {
  background-color: var(--bg-primary) !important;
  color: var(--text-main) !important;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif !important;
  -webkit-font-smoothing: antialiased;
}

[data-testid="stHeader"] {
  background: transparent !important;
  border-bottom: none !important;
}

.block-container {
  padding-top: 1.5rem !important;
  padding-bottom: 4rem !important;
  max-width: 1400px !important;
}

/* Scrollbars */
::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}
::-webkit-scrollbar-track {
  background: var(--bg-primary);
}
::-webkit-scrollbar-thumb {
  background: var(--border-highlight);
  border-radius: 3px;
}
::-webkit-scrollbar-thumb:hover {
  background: var(--text-dim);
}

/* Hero Header */
.hero-box {
  background: linear-gradient(180deg, #131C2E 0%, #0F172A 100%);
  border: 1px solid var(--border-card);
  border-radius: 12px;
  padding: 24px 28px;
  margin-bottom: 24px;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.35);
  position: relative;
  overflow: hidden;
}

.hero-box::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 2px;
  background: linear-gradient(90deg, #EF4444 0%, #38BDF8 50%, #10B981 100%);
}

.hero-eyebrow {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 8px;
}

.pulse-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: #EF4444;
  box-shadow: 0 0 8px #EF4444;
  display: inline-block;
}

.eyebrow-text {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.12em;
  color: var(--text-secondary);
  text-transform: uppercase;
}

.eyebrow-badge {
  font-size: 0.68rem;
  font-weight: 600;
  letter-spacing: 0.06em;
  background: rgba(239, 68, 68, 0.12);
  color: #F87171;
  border: 1px solid rgba(239, 68, 68, 0.25);
  padding: 2px 8px;
  border-radius: 9999px;
  text-transform: uppercase;
}

.hero-title {
  font-size: 2.1rem;
  font-weight: 800;
  letter-spacing: -0.03em;
  color: #FFFFFF;
  margin: 4px 0 8px 0;
  line-height: 1.2;
}

.hero-subtitle {
  font-size: 0.95rem;
  color: var(--text-secondary);
  max-width: 850px;
  margin: 0 0 16px 0;
  line-height: 1.5;
}

.hero-tags {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px;
  font-size: 0.78rem;
  color: var(--text-dim);
}

.hero-tag-item {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  background: rgba(15, 23, 42, 0.6);
  padding: 4px 10px;
  border-radius: 6px;
  border: 1px solid var(--border-subtle);
  color: var(--text-secondary);
}

.hero-tag-item strong {
  color: #E2E8F0;
  font-weight: 600;
}

/* KPI Card Grid */
.kpi-container {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 14px;
  margin-bottom: 28px;
}

.kpi-item {
  background: var(--bg-card);
  border: 1px solid var(--border-card);
  border-radius: 10px;
  padding: 16px 18px;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  position: relative;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}

.kpi-item:hover {
  transform: translateY(-2px);
  border-color: var(--border-highlight);
  box-shadow: 0 6px 16px rgba(0, 0, 0, 0.3);
  background: var(--bg-card-elevated);
}

.kpi-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}

.kpi-label {
  font-size: 0.72rem;
  font-weight: 600;
  letter-spacing: 0.08em;
  color: var(--text-dim);
  text-transform: uppercase;
}

.kpi-num {
  font-family: 'JetBrains Mono', monospace;
  font-size: 1.75rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  color: #F8FAFC;
  margin-bottom: 4px;
  line-height: 1.1;
}

.kpi-subtext {
  font-size: 0.72rem;
  color: var(--text-secondary);
}

/* Tabs Styling */
.stTabs [data-baseweb="tab-list"] {
  background: #0E1524 !important;
  border: 1px solid var(--border-card) !important;
  border-radius: 10px !important;
  padding: 6px !important;
  gap: 6px !important;
  margin-bottom: 28px !important;
}

.stTabs [data-baseweb="tab"] {
  background: transparent !important;
  color: var(--text-secondary) !important;
  font-size: 0.88rem !important;
  font-weight: 500 !important;
  padding: 8px 18px !important;
  border-radius: 6px !important;
  border: 1px solid transparent !important;
  transition: all 0.15s ease !important;
}

.stTabs [data-baseweb="tab"]:hover {
  color: var(--text-main) !important;
  background: rgba(255, 255, 255, 0.04) !important;
}

.stTabs [data-baseweb="tab"][aria-selected="true"] {
  background: #1A2438 !important;
  color: #FFFFFF !important;
  font-weight: 600 !important;
  border: 1px solid var(--border-highlight) !important;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.25) !important;
}

.stTabs [data-baseweb="tab-highlight"],
.stTabs [data-baseweb="tab-border"] {
  display: none !important;
}

/* Section Framing */
.section-card {
  background: var(--bg-card);
  border: 1px solid var(--border-card);
  border-radius: 12px;
  padding: 20px 22px;
  margin-bottom: 24px;
}

.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
  padding-bottom: 12px;
  border-bottom: 1px solid var(--border-subtle);
}

.section-title-wrap {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.section-pill {
  font-size: 0.65rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--steel-blue);
}

.section-h2 {
  font-size: 1.25rem;
  font-weight: 700;
  color: #F8FAFC;
  margin: 0;
}

.section-desc {
  font-size: 0.82rem;
  color: var(--text-secondary);
  margin: 0;
}

/* Filter Control Panel */
.filter-card {
  background: #111A29;
  border: 1px solid #1E2D42;
  border-left: 3px solid #38BDF8;
  border-radius: 8px;
  padding: 16px 20px;
  margin-bottom: 20px;
}

.filter-header-title {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  color: #38BDF8;
  text-transform: uppercase;
  margin-bottom: 4px;
}

.filter-header-desc {
  font-size: 0.82rem;
  color: var(--text-secondary);
  margin-bottom: 12px;
}

/* Station Lookup Highlights */
.station-stat-card {
  background: linear-gradient(180deg, #152238 0%, #111A2B 100%);
  border: 1px solid #23354E;
  border-radius: 10px;
  padding: 18px 22px;
  margin: 16px 0 20px 0;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
}

.station-stat-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 6px;
}

.station-stat-badge {
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  color: #38BDF8;
  text-transform: uppercase;
}

.station-code-badge {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  font-weight: 600;
  background: rgba(56, 189, 248, 0.12);
  color: #38BDF8;
  border: 1px solid rgba(56, 189, 248, 0.25);
  padding: 2px 8px;
  border-radius: 4px;
}

.station-stat-name {
  font-size: 1.5rem;
  font-weight: 800;
  color: #FFFFFF;
  letter-spacing: -0.02em;
  margin-bottom: 4px;
}

.station-stat-count {
  font-size: 0.95rem;
  color: var(--text-secondary);
}

.stat-highlight {
  font-family: 'JetBrains Mono', monospace;
  font-size: 1.15rem;
  font-weight: 700;
  color: #38BDF8;
}

/* Architecture ETL Steps */
.pipeline-flow {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 12px;
  margin: 16px 0 24px 0;
}

.flow-step {
  background: var(--bg-card);
  border: 1px solid var(--border-card);
  border-radius: 8px;
  padding: 14px;
  position: relative;
  transition: all 0.2s ease;
}

.flow-step:hover {
  border-color: var(--border-highlight);
  transform: translateY(-2px);
  background: var(--bg-card-elevated);
}

.flow-num {
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  color: #38BDF8;
  text-transform: uppercase;
  margin-bottom: 4px;
}

.flow-title {
  font-size: 0.92rem;
  font-weight: 700;
  color: #FFFFFF;
  margin-bottom: 6px;
}

.flow-desc {
  font-size: 0.76rem;
  color: var(--text-secondary);
  line-height: 1.4;
}

/* Limitations Box */
.limitation-box {
  background: #171B24;
  border: 1px solid #332A1C;
  border-left: 4px solid #F59E0B;
  border-radius: 8px;
  padding: 20px;
  margin-top: 24px;
}

.limitation-box-title {
  font-size: 0.85rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  color: #F59E0B;
  text-transform: uppercase;
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}

.limitation-box-sub {
  font-size: 0.82rem;
  color: #CBD5E1;
  font-weight: 500;
  margin-bottom: 12px;
}

.limitation-box ul {
  margin: 0;
  padding-left: 20px;
  color: var(--text-secondary);
  font-size: 0.84rem;
  line-height: 1.6;
}

.limitation-box li {
  margin-bottom: 8px;
}

.limitation-box li strong {
  color: #E2E8F0;
}

/* Streamlit Native Widgets Polish */
[data-testid="stSelectbox"] label,
[data-testid="stSlider"] label {
  font-size: 0.82rem !important;
  font-weight: 600 !important;
  color: #CBD5E1 !important;
  letter-spacing: 0.02em !important;
}

[data-testid="stDataFrame"] {
  border: 1px solid var(--border-card) !important;
  border-radius: 8px !important;
  overflow: hidden !important;
}

[data-testid="stAlert"] {
  background-color: #162032 !important;
  border: 1px solid var(--border-card) !important;
  border-radius: 8px !important;
  color: var(--text-main) !important;
}

/* Responsive Breakpoints */
@media (max-width: 1100px) {
  .kpi-container {
    grid-template-columns: repeat(3, 1fr);
  }
  .pipeline-flow {
    grid-template-columns: repeat(3, 1fr);
  }
}

@media (max-width: 700px) {
  .kpi-container {
    grid-template-columns: 1fr;
  }
  .pipeline-flow {
    grid-template-columns: 1fr;
  }
  .hero-title {
    font-size: 1.6rem;
  }
  .hero-box {
    padding: 18px 20px;
  }
}
</style>
""", unsafe_allow_html=True)


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
    Explore network activity, station utilization, stoppage patterns and hourly traffic from processed Indian Railways timetable data.
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


# ---------- summary metrics ----------

summary = load_json("run_summary.json")
if summary:
    st.markdown(f"""
    <div class="kpi-container">
      <div class="kpi-item">
        <div class="kpi-header">
          <span class="kpi-label">Raw Records</span>
        </div>
        <div class="kpi-num">{summary['raw_rows']:,}</div>
        <div class="kpi-subtext">Source timetable rows</div>
      </div>
      <div class="kpi-item">
        <div class="kpi-header">
          <span class="kpi-label">Cleaned Records</span>
        </div>
        <div class="kpi-num">{summary['cleaned_rows']:,}</div>
        <div class="kpi-subtext">Valid schedule stops</div>
      </div>
      <div class="kpi-item">
        <div class="kpi-header">
          <span class="kpi-label">Distinct Trains</span>
        </div>
        <div class="kpi-num">{summary['distinct_trains']:,}</div>
        <div class="kpi-subtext">Active train numbers</div>
      </div>
      <div class="kpi-item">
        <div class="kpi-header">
          <span class="kpi-label">Distinct Stations</span>
        </div>
        <div class="kpi-num">{summary['distinct_stations']:,}</div>
        <div class="kpi-subtext">Network station nodes</div>
      </div>
      <div class="kpi-item">
        <div class="kpi-header">
          <span class="kpi-label">Pipeline Time</span>
        </div>
        <div class="kpi-num">{summary['pipeline_runtime_seconds']}s</div>
        <div class="kpi-subtext">Spark batch runtime</div>
      </div>
    </div>
    """, unsafe_allow_html=True)
else:
    missing_data_warning("run_summary.json")


# ---------- navigation tabs ----------

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview",
    "⏱ Stoppage Times",
    "🕐 Hourly Traffic",
    "🔍 Station Lookup",
    "ℹ️ About the Pipeline"
])


# ====== TAB 1: Overview ======
with tab1:
    busiest = load_parquet("busiest_stations.parquet")
    if busiest is not None:
        # Determine station columns dynamically
        name_col = "station_name" if "station_name" in busiest.columns else (
            "station_code" if "station_code" in busiest.columns else busiest.columns[0])
        code_col = "station_code" if "station_code" in busiest.columns else name_col

        # Section 1: Network Overview (Busiest Stations)
        st.markdown("""
        <div class="section-card">
          <div class="section-head">
            <div class="section-title-wrap">
              <span class="section-pill">PRIMARY ANALYTICS • NETWORK TRAFFIC</span>
              <h2 class="section-h2">Busiest Stations</h2>
              <p class="section-desc">Top 20 stations across the Indian Railways network ranked by aggregate scheduled train stops.</p>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Bar chart of top 20 by total stops
        fig, ax = plt.subplots(figsize=(12, 6.2))
        display_col = name_col if name_col != code_col else code_col
        bars = ax.barh(
            busiest[display_col].astype(str),
            busiest["total_stops"],
            color="#38BDF8",
            edgecolor="none",
            height=0.68
        )
        ax.set_xlabel("Total Train Stops")
        ax.set_title("Top 20 Busiest Stations by Number of Train Stops")
        ax.invert_yaxis()  # highest at top
        apply_chart_theme(fig, ax, is_horizontal=True)
        ax.bar_label(bars, fmt="{:,.0f}", padding=5, color="#94A3B8", fontsize=8.5)
        plt.tight_layout()
        st.pyplot(fig, clear_figure=True)

        # Table
        st.markdown("""
        <div style="margin-top: 24px; margin-bottom: 8px;">
          <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #94A3B8; text-transform: uppercase;">DATA TABLE • BUSIEST STATIONS</span>
        </div>
        """, unsafe_allow_html=True)
        st.dataframe(busiest, use_container_width=True, hide_index=True)

        # Section 2: Route Intelligence (Longest trains)
        longest = load_parquet("longest_trains.parquet")
        if longest is not None:
            st.markdown("""
            <div class="section-card" style="margin-top: 36px;">
              <div class="section-head">
                <div class="section-title-wrap">
                  <span class="section-pill">ROUTE INTELLIGENCE • TRANSIT STOPS</span>
                  <h2 class="section-h2">Top 20 Trains with Most Stops</h2>
                  <p class="section-desc">Scheduled passenger services operating the highest number of intermediate station halts.</p>
                </div>
              </div>
            </div>
            """, unsafe_allow_html=True)

            train_display = "train_name" if "train_name" in longest.columns else "train_number"
            fig2, ax2 = plt.subplots(figsize=(12, 6.2))
            bars2 = ax2.barh(
                longest[train_display].astype(str),
                longest["num_stops"],
                color="#F59E0B",
                edgecolor="none",
                height=0.68
            )
            ax2.set_xlabel("Number of Stops")
            ax2.set_title("Top 20 Longest Trains (Most Stops)")
            ax2.invert_yaxis()
            apply_chart_theme(fig2, ax2, is_horizontal=True)
            ax2.bar_label(bars2, fmt="{:,.0f}", padding=5, color="#94A3B8", fontsize=8.5)
            plt.tight_layout()
            st.pyplot(fig2, clear_figure=True)

        # Section 3: Network Composition (Breakdowns by Zone and Train Type)
        zone_df = load_parquet("breakdown_zone.parquet")
        type_df = load_parquet("breakdown_train_type.parquet")

        if zone_df is not None or type_df is not None:
            st.markdown("""
            <div class="section-card" style="margin-top: 36px;">
              <div class="section-head">
                <div class="section-title-wrap">
                  <span class="section-pill">NETWORK COMPOSITION • STRUCTURAL DISTRIBUTION</span>
                  <h2 class="section-h2">Network Composition Breakdown</h2>
                  <p class="section-desc">Distribution of scheduled train operations partitioned by administrative Railway Zone and operational Category.</p>
                </div>
              </div>
            </div>
            """, unsafe_allow_html=True)

            comp_col1, comp_col2 = st.columns(2)

            with comp_col1:
                if zone_df is not None:
                    st.markdown("""
                    <div style="margin-bottom: 8px;">
                      <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #14B8A6; text-transform: uppercase;">TRAFFIC BY RAILWAY ZONE</span>
                    </div>
                    """, unsafe_allow_html=True)
                    valid_zone = zone_df[zone_df["zone"].notnull() & (zone_df["zone"] != "") & (zone_df["zone"] != "?")].head(15)
                    fig3, ax3 = plt.subplots(figsize=(6.5, 4.8))
                    bars3 = ax3.bar(valid_zone["zone"], valid_zone["total_stops"], color="#14B8A6", edgecolor="none", width=0.65)
                    ax3.set_ylabel("Total Train Stops")
                    ax3.set_title("Train Stops by Railway Zone (Top 15)")
                    apply_chart_theme(fig3, ax3, is_horizontal=False)
                    plt.xticks(rotation=45, ha="right")
                    plt.tight_layout()
                    st.pyplot(fig3, clear_figure=True)

            with comp_col2:
                if type_df is not None:
                    st.markdown("""
                    <div style="margin-bottom: 8px;">
                      <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #F43F5E; text-transform: uppercase;">STOPS BY TRAIN CATEGORY</span>
                    </div>
                    """, unsafe_allow_html=True)
                    valid_type = type_df[type_df["train_type"].notnull() & (type_df["train_type"] != "")].head(10)
                    fig4, ax4 = plt.subplots(figsize=(6.5, 4.8))
                    bars4 = ax4.bar(valid_type["train_type"], valid_type["total_stops"], color="#F43F5E", edgecolor="none", width=0.65)
                    ax4.set_ylabel("Total Train Stops")
                    ax4.set_title("Train Stops by Category (Express, SF, Passenger, etc.)")
                    apply_chart_theme(fig4, ax4, is_horizontal=False)
                    plt.xticks(rotation=45, ha="right")
                    plt.tight_layout()
                    st.pyplot(fig4, clear_figure=True)
    else:
        missing_data_warning("busiest_stations.parquet")


# ====== TAB 2: Stoppage Times ======
with tab2:
    stoppage = load_parquet("stoppage_stats.parquet")
    if stoppage is not None:
        st.markdown("""
        <div class="section-card">
          <div class="section-head">
            <div class="section-title-wrap">
              <span class="section-pill">EFFICIENCY & DWELL TIMES • INVESTIGATION WORKSPACE</span>
              <h2 class="section-h2">Station Stoppage Times</h2>
              <p class="section-desc">Analyse average dwell duration across stations with dynamic frequency thresholds to isolate high-traffic transit nodes.</p>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Filter Panel Card
        st.markdown("""
        <div class="filter-card">
          <div class="filter-header-title">INVESTIGATION FILTER</div>
          <div class="filter-header-desc">Set minimum station frequency to exclude low-volume stops from duration calculations</div>
        </div>
        """, unsafe_allow_html=True)

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

        st.markdown(f"""
        <div style="margin-top: 20px; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: baseline;">
          <div>
            <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #10B981; text-transform: uppercase;">ANALYTICAL RESULT • DWELL DURATION</span>
            <div style="font-size: 1.15rem; font-weight: 700; color: #F8FAFC;">Average Stop Duration (stations with ≥{min_stops} stops)</div>
          </div>
          <div style="font-size: 0.78rem; color: #94A3B8;">Showing Top <strong>{len(filtered)}</strong> matching stations</div>
        </div>
        """, unsafe_allow_html=True)

        if len(filtered) > 0:
            fig, ax = plt.subplots(figsize=(12, 7.8))
            bars = ax.barh(
                [str(x) for x in filtered[name_col]],
                filtered["avg_duration_mins"],
                color="#10B981",
                edgecolor="none",
                height=0.68,
                label="Avg Duration"
            )
            ax.set_xlabel("Minutes")
            ax.set_title(f"Average Stop Duration (stations with ≥{min_stops} stops)")
            ax.invert_yaxis()
            apply_chart_theme(fig, ax, is_horizontal=True)
            ax.bar_label(bars, fmt="%.1f m", padding=5, color="#94A3B8", fontsize=8.5)
            plt.tight_layout()
            st.pyplot(fig, clear_figure=True)
        else:
            st.info("No stations match the selected filter.")

        st.markdown("""
        <div style="margin-top: 32px; margin-bottom: 8px;">
          <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #94A3B8; text-transform: uppercase;">DETAILED STATISTICS TABLE</span>
        </div>
        """, unsafe_allow_html=True)
        st.dataframe(
            stoppage.sort_values("avg_duration_mins", ascending=False),
            use_container_width=True,
            hide_index=True
        )
    else:
        missing_data_warning("stoppage_stats.parquet")


# ====== TAB 3: Hourly Traffic ======
with tab3:
    hourly = load_parquet("hourly_traffic.parquet")
    if hourly is not None:
        hourly = hourly.sort_values("hour")

        st.markdown("""
        <div class="section-card">
          <div class="section-head">
            <div class="section-title-wrap">
              <span class="section-pill">TEMPORAL NETWORK ACTIVITY • 24-HOUR PROFILE</span>
              <h2 class="section-h2">Train Arrivals by Hour of Day</h2>
              <p class="section-desc">Diurnal distribution of scheduled train arrivals across the nationwide Indian Railways network.</p>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        fig, ax = plt.subplots(figsize=(14, 5.2))
        bars = ax.bar(
            hourly["hour"],
            hourly["arrivals"],
            color="#818CF8",
            edgecolor="none",
            width=0.72
        )
        ax.set_xlabel("Hour of Day (0–23)")
        ax.set_ylabel("Number of Train Arrivals")
        ax.set_title("Hourly Distribution of Train Arrivals")
        ax.set_xticks(range(0, 24))
        ax.set_xticklabels([f"{h:02d}:00" for h in range(24)], rotation=45, ha="right")
        apply_chart_theme(fig, ax, is_horizontal=False)
        ax.bar_label(bars, fmt="{:,.0f}", padding=4, color="#94A3B8", fontsize=8)
        plt.tight_layout()
        st.pyplot(fig, clear_figure=True)

        st.markdown("""
        <div style="margin-top: 28px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center;">
          <span style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em; color: #94A3B8; text-transform: uppercase;">DATA TABLE • ARRIVALS PER HOUR</span>
          <span style="font-size: 0.75rem; color: #64748B;">24 Hourly Windows (00:00 – 23:00)</span>
        </div>
        """, unsafe_allow_html=True)
        st.dataframe(hourly, use_container_width=True, hide_index=True)
    else:
        missing_data_warning("hourly_traffic.parquet")


# ====== TAB 4: Station Lookup ======
with tab4:
    stops = load_parquet("stops.parquet", directory=PROCESSED_DIR)
    if stops is not None:
        st.markdown("""
        <div class="section-card">
          <div class="section-head">
            <div class="section-title-wrap">
              <span class="section-pill">NODE INTELLIGENCE • TIMETABLE QUERY</span>
              <h2 class="section-h2">Station Schedule Lookup</h2>
              <p class="section-desc">Query station-specific train arrivals, departures, halt durations, and transit sequences from processed timetable data.</p>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

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

            station_code_val = station_stops['station_code'].iloc[0] if ('station_code' in station_stops.columns and len(station_stops) > 0) else ""

            st.markdown(f"""
            <div class="station-stat-card">
              <div class="station-stat-header">
                <span class="station-stat-badge">SELECTED STATION NODE</span>
                <span class="station-code-badge">{station_code_val}</span>
              </div>
              <div class="station-stat-name">{selected}</div>
              <div class="station-stat-count">
                <span class="stat-highlight">{len(station_stops):,}</span> scheduled train services stop at this station
              </div>
            </div>
            """, unsafe_allow_html=True)

            # Select display columns
            display_cols = [c for c in [
                "train_number", "train_name", "station_code", "station_name",
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
    else:
        missing_data_warning("stops.parquet")


# ====== TAB 5: About the Pipeline ======
with tab5:
    st.markdown("""
    <div class="section-card">
      <div class="section-head">
        <div class="section-title-wrap">
          <span class="section-pill">SYSTEM ARCHITECTURE • BIG DATA ENGINEERING</span>
          <h2 class="section-h2">About the Pipeline</h2>
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

    st.markdown("""
    <div style="margin-top: 28px; margin-bottom: 12px;">
      <span style="font-size: 0.75rem; font-weight: 700; letter-spacing: 0.08em; color: #38BDF8; text-transform: uppercase;">TOOLS & TECHNOLOGY STACK</span>
    </div>
    """, unsafe_allow_html=True)

    tools_data = [
        {"Tool": "Apache Spark (PySpark)", "Purpose": "Distributed data processing engine (run in local mode)"},
        {"Tool": "Apache Parquet", "Purpose": "Columnar file format for efficient storage and querying"},
        {"Tool": "Python / pandas", "Purpose": "Data manipulation for the web app"},
        {"Tool": "Streamlit", "Purpose": "Lightweight web dashboard framework"},
        {"Tool": "matplotlib", "Purpose": "Charts and visualisations"},
        {"Tool": "OpenJDK 17", "Purpose": "JVM runtime required by Spark"},
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
          There is <strong>no delay or real-time information</strong>. Everything shown is what the timetable says, not what actually happened.
        </li>
        <li>
          The dataset is <strong>several years old</strong> and may not match the current Indian Railways timetable.
        </li>
        <li>
          This is a <strong>batch pipeline</strong> — there is no streaming or real-time processing.
        </li>
        <li>
          The data is large enough to justify Spark (hundreds of thousands of records), but it is not terabytes.
          The pipeline is built with big data tools and <strong>would scale to much larger data</strong> on a multi-node cluster.
        </li>
        <li>
          All analysis is based on the columns available in the source dataset. Some analyses may be limited by what fields the data provides.
        </li>
      </ul>
    </div>
    """, unsafe_allow_html=True)
