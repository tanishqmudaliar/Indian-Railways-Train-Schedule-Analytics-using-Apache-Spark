import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
from src.chart_utils import (
    create_busiest_stations_chart,
    create_longest_trains_chart,
    create_stoppage_stats_chart,
    create_hourly_traffic_chart,
    create_breakdown_chart,
    create_train_dwell_profile_chart
)

print("Testing chart_utils...")

def test_chart_generation():
    """Verify that all Altair chart generation functions compile with real parquet tables."""
    busiest = pd.read_parquet("data/results/busiest_stations.parquet")
    c1 = create_busiest_stations_chart(busiest)
    assert c1 is not None, "Failed to create busiest stations chart"

    longest = pd.read_parquet("data/results/longest_trains.parquet")
    c2 = create_longest_trains_chart(longest)
    assert c2 is not None, "Failed to create longest trains chart"

    stoppage = pd.read_parquet("data/results/stoppage_stats.parquet").head(20)
    c3 = create_stoppage_stats_chart(stoppage)
    assert c3 is not None, "Failed to create stoppage stats chart"

    hourly = pd.read_parquet("data/results/hourly_traffic.parquet")
    c4 = create_hourly_traffic_chart(hourly)
    assert c4 is not None, "Failed to create hourly traffic chart"

    zone = pd.read_parquet("data/results/breakdown_zone.parquet")
    c5 = create_breakdown_chart(zone, "zone", "Railway Zone")
    assert c5 is not None, "Failed to create breakdown chart"

    stops = pd.read_parquet("data/processed/stops.parquet")
    sample_train = stops[stops["train_number"] == "15906"].sort_values("stop_sequence")
    c6 = create_train_dwell_profile_chart(sample_train)
    assert c6 is not None, "Failed to create train dwell profile chart"


if __name__ == "__main__":
    test_chart_generation()
    print("ALL CHART UTILS TESTS PASSED!")
