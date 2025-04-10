from streamlit_autorefresh import st_autorefresh
import streamlit as st
import pandas as pd
from datetime import datetime
from google.transit import gtfs_realtime_pb2

st.set_page_config(page_title="SydLink Dashboard", layout="wide")
st.title("🚆 Sydney Trains Realtime Dashboard")

# -------------------------
# Load local data files
# -------------------------
with open("trains.pb", "rb") as f:
    pb_bytes = f.read()

stops_df = pd.read_csv("mock_stops.csv")

# -------------------------
# Helper function
# -------------------------
def safe_convert_time(unix_time):
    if unix_time:
        try:
            return datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return "N/A"
    return "N/A"

def parse_trip_updates(pb_bytes):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(pb_bytes)
    records = []
    for entity in feed.entity:
        if not entity.HasField('trip_update'):
            continue
        tu = entity.trip_update
        trip_id = tu.trip.trip_id
        route_id = tu.trip.route_id
        for su in tu.stop_time_update:
            stop_id = su.stop_id
            arr = su.arrival.time if su.HasField('arrival') and su.arrival.HasField('time') else None
            dep = su.departure.time if su.HasField('departure') and su.departure.HasField('time') else None
            records.append({
                "trip_id": trip_id,
                "route_id": route_id,
                "stop_id": stop_id,
                "ETA": safe_convert_time(arr),
                "ETD": safe_convert_time(dep)
            })
    return pd.DataFrame(records)

# -------------------------
# Parse and display data
# -------------------------
trip_updates_df = parse_trip_updates(pb_bytes)

trip_updates_df["stop_id"] = trip_updates_df["stop_id"].astype(str)
stops_df["stop_id"] = stops_df["stop_id"].astype(str)

combined_df = pd.merge(trip_updates_df, stops_df, on="stop_id", how="left")

if combined_df.empty:
    st.warning("No trip updates found in the feed.")
else:
    st.success(f"✅ Feed loaded successfully! {len(trip_updates_df)} stop updates found.")
    st.dataframe(combined_df)

    # 🗺️ Show map
    st.subheader("🗺️ Stop Locations")
    map_data = combined_df.dropna(subset=["stop_lat", "stop_lon"])[["stop_lat", "stop_lon"]].drop_duplicates()
    map_data = map_data.rename(columns={"stop_lat": "lat", "stop_lon": "lon"})
    st.map(map_data)

# 🔁 Auto-refresh every 30 seconds
st_autorefresh(interval=30 * 1000, key="refresh")
