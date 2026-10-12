"""
src/chart_utils.py
------------------
Interactive Altair chart generator for Indian Railways Analytics dashboard.
Provides responsive, dark-themed charts with informative hover tooltips.
"""

import altair as alt
import pandas as pd


# Enterprise Dark Theme Palette
COLOR_STEEL_BLUE = "#38BDF8"
COLOR_AMBER = "#F59E0B"
COLOR_EMERALD = "#10B981"
COLOR_INDIGO = "#818CF8"
COLOR_TEAL = "#14B8A6"
COLOR_ROSE = "#F43F5E"
COLOR_GRID = "#1E293B"
COLOR_TEXT_MUTED = "#94A3B8"
COLOR_TEXT_MAIN = "#F8FAFC"


def create_busiest_stations_chart(df: pd.DataFrame, display_col: str = "station_name") -> alt.Chart:
    """Create interactive horizontal bar chart for busiest stations with rich tooltips."""
    if df is None or len(df) == 0:
        return None

    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color=COLOR_STEEL_BLUE)
        .encode(
            y=alt.Y(
                f"{display_col}:N",
                sort="-x",
                title="Station",
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    labelLimit=250,
                    labelFontSize=10,
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            x=alt.X(
                "total_stops:Q",
                title="Total Scheduled Train Stops",
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    gridColor=COLOR_GRID,
                    gridDash=[3, 3],
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            tooltip=[
                alt.Tooltip(f"{display_col}:N", title="Station Name"),
                alt.Tooltip("station_code:N", title="Station Code"),
                alt.Tooltip("total_stops:Q", title="Scheduled Stops", format=","),
                alt.Tooltip("distinct_trains:Q", title="Unique Trains Serviced", format=",")
            ]
        )
        .properties(height=max(320, len(df) * 22), background="transparent")
        .configure_view(strokeWidth=0)
    )
    return chart


def create_longest_trains_chart(df: pd.DataFrame, display_col: str = "train_name") -> alt.Chart:
    """Create interactive horizontal bar chart for longest train routes by stop count."""
    if df is None or len(df) == 0:
        return None

    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color=COLOR_AMBER)
        .encode(
            y=alt.Y(
                f"{display_col}:N",
                sort="-x",
                title="Train Name / Route",
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    labelLimit=280,
                    labelFontSize=10,
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            x=alt.X(
                "num_stops:Q",
                title="Scheduled Intermediate Halts",
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    gridColor=COLOR_GRID,
                    gridDash=[3, 3],
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            tooltip=[
                alt.Tooltip(f"{display_col}:N", title="Train Name"),
                alt.Tooltip("train_number:N", title="Train Number"),
                alt.Tooltip("num_stops:Q", title="Total Stops", format=",")
            ]
        )
        .properties(height=max(320, len(df) * 22), background="transparent")
        .configure_view(strokeWidth=0)
    )
    return chart


def create_stoppage_stats_chart(df: pd.DataFrame, name_col: str = "station_name") -> alt.Chart:
    """Create interactive horizontal bar chart for average stoppage dwell times."""
    if df is None or len(df) == 0:
        return None

    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color=COLOR_EMERALD)
        .encode(
            y=alt.Y(
                f"{name_col}:N",
                sort="-x",
                title="Station Name",
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    labelLimit=250,
                    labelFontSize=10,
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            x=alt.X(
                "avg_duration_mins:Q",
                title="Average Stop Duration (Minutes)",
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    gridColor=COLOR_GRID,
                    gridDash=[3, 3],
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            tooltip=[
                alt.Tooltip(f"{name_col}:N", title="Station"),
                alt.Tooltip("station_code:N", title="Station Code"),
                alt.Tooltip("avg_duration_mins:Q", title="Avg Dwell Time (min)", format=".1f"),
                alt.Tooltip("p90_duration_mins:Q", title="90th Percentile P90 (min)", format=".1f"),
                alt.Tooltip("max_duration_mins:Q", title="Max Stoppage (min)", format=".0f"),
                alt.Tooltip("num_stops:Q", title="Stopping Services", format=",")
            ]
        )
        .properties(height=max(320, len(df) * 22), background="transparent")
        .configure_view(strokeWidth=0)
    )
    return chart


def create_hourly_traffic_chart(df: pd.DataFrame) -> alt.Chart:
    """Create interactive vertical bar chart for 24-hour diurnal arrival traffic distribution."""
    if df is None or len(df) == 0:
        return None

    df_copy = df.copy()
    if "hour_label" not in df_copy.columns:
        df_copy["hour_label"] = df_copy["hour"].apply(lambda h: f"{int(h):02d}:00")

    total_arrivals = df_copy["arrivals"].sum()
    df_copy["pct_daily"] = (df_copy["arrivals"] / max(total_arrivals, 1)) * 100.0

    chart = (
        alt.Chart(df_copy)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color=COLOR_INDIGO)
        .encode(
            x=alt.X(
                "hour_label:N",
                title="Hour of Day (24-Hour Clock)",
                sort=None,
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    labelAngle=-45,
                    labelFontSize=9.5,
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            y=alt.Y(
                "arrivals:Q",
                title="Scheduled Train Arrivals",
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    gridColor=COLOR_GRID,
                    gridDash=[3, 3],
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            tooltip=[
                alt.Tooltip("hour_label:N", title="Time Window"),
                alt.Tooltip("arrivals:Q", title="Scheduled Arrivals", format=","),
                alt.Tooltip("pct_daily:Q", title="Share of Daily Traffic", format=".1f")
            ]
        )
        .properties(height=340, background="transparent")
        .configure_view(strokeWidth=0)
    )
    return chart


def create_breakdown_chart(df: pd.DataFrame, category_col: str, title: str, color: str = COLOR_TEAL) -> alt.Chart:
    """Create interactive vertical bar chart for zone or train category breakdowns."""
    if df is None or len(df) == 0:
        return None

    df_copy = df[df[category_col].notnull() & (df[category_col] != "") & (df[category_col] != "?")].copy()
    if len(df_copy) == 0:
        return None

    chart = (
        alt.Chart(df_copy)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color=color)
        .encode(
            x=alt.X(
                f"{category_col}:N",
                sort="-y",
                title=title,
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    labelAngle=-45,
                    labelFontSize=9.5,
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            y=alt.Y(
                "total_stops:Q",
                title="Total Train Stops",
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    gridColor=COLOR_GRID,
                    gridDash=[3, 3],
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            tooltip=[
                alt.Tooltip(f"{category_col}:N", title=title),
                alt.Tooltip("total_stops:Q", title="Scheduled Stops", format=","),
                alt.Tooltip("distinct_trains:Q", title="Distinct Trains", format=",") if "distinct_trains" in df_copy.columns else alt.Tooltip("total_stops:Q", title="Stops", format=",")
            ]
        )
        .properties(height=320, background="transparent")
        .configure_view(strokeWidth=0)
    )
    return chart


def create_train_dwell_profile_chart(df: pd.DataFrame) -> alt.Chart:
    """Create interactive halt duration profile chart along a train's journey sequence."""
    if df is None or len(df) == 0:
        return None

    df_copy = df[df["stop_duration_mins"].notnull()].copy()
    if len(df_copy) == 0:
        return None

    # Limit to stops with actual halts for clean visualization
    chart = (
        alt.Chart(df_copy)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3, color=COLOR_STEEL_BLUE)
        .encode(
            x=alt.X(
                "station_name:N",
                sort=alt.EncodingSortField(field="stop_sequence", order="ascending"),
                title="Station Halts Along Route (in Journey Order)",
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    labelAngle=-60,
                    labelLimit=120,
                    labelFontSize=9,
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            y=alt.Y(
                "stop_duration_mins:Q",
                title="Halt Duration (Minutes)",
                axis=alt.Axis(
                    labelColor=COLOR_TEXT_MUTED,
                    titleColor=COLOR_TEXT_MUTED,
                    gridColor=COLOR_GRID,
                    gridDash=[3, 3],
                    tickColor=COLOR_GRID,
                    domainColor=COLOR_GRID
                )
            ),
            tooltip=[
                alt.Tooltip("stop_sequence:Q", title="Stop #"),
                alt.Tooltip("station_name:N", title="Station"),
                alt.Tooltip("station_code:N", title="Code"),
                alt.Tooltip("arrival:N", title="Arrival"),
                alt.Tooltip("departure:N", title="Departure"),
                alt.Tooltip("stop_duration_mins:Q", title="Halt (min)", format=".0f"),
                alt.Tooltip("day:Q", title="Day", format=".0f"),
                alt.Tooltip("state:N", title="State")
            ]
        )
        .properties(height=300, background="transparent")
        .configure_view(strokeWidth=0)
    )
    return chart
