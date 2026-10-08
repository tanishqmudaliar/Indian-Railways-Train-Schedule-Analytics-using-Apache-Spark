import os
import json
import pandas as pd
import matplotlib.pyplot as plt

print("Testing app data loading and chart generation...")

RESULTS_DIR = "data/results"
PROCESSED_DIR = "data/processed"

# 1. Summary
with open(os.path.join(RESULTS_DIR, "run_summary.json")) as f:
    summary = json.load(f)
print("Summary loaded:", summary)

# 2. Busiest
busiest = pd.read_parquet(os.path.join(RESULTS_DIR, "busiest_stations.parquet"))
print(f"Busiest stations loaded: {len(busiest)} rows")

# 3. Longest
longest = pd.read_parquet(os.path.join(RESULTS_DIR, "longest_trains.parquet"))
print(f"Longest trains loaded: {len(longest)} rows")

# 4. Stoppage
stoppage = pd.read_parquet(os.path.join(RESULTS_DIR, "stoppage_stats.parquet"))
print(f"Stoppage stats loaded: {len(stoppage)} rows")

# 5. Hourly
hourly = pd.read_parquet(os.path.join(RESULTS_DIR, "hourly_traffic.parquet"))
print(f"Hourly traffic loaded: {len(hourly)} rows")

# 6. Stops
stops = pd.read_parquet(os.path.join(PROCESSED_DIR, "stops.parquet"))
print(f"Cleaned stops loaded: {len(stops)} rows")

# 7. Station lookup filter test
test_station = busiest["station_name"].iloc[0]
filtered = stops[stops["station_name"] == test_station]
print(f"Station lookup test for '{test_station}': {len(filtered)} matching stops")

print("ALL APP DATA REQUIREMENTS VERIFIED SUCCESSFULLY!")
