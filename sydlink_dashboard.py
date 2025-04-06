from streamlit_autorefresh import st_autorefresh
import streamlit as st
import pandas as pd
from datetime import datetime

# If not already installed, you need: pip install gtfs-realtime-bindings streamlit-autorefresh
from google.transit import gtfs_realtime_pb2
from streamlit.runtime.scriptrunner import add_script_run_ctx  # Optional, advanced

# or if you wanted auto-refresh specifically
import streamlit as st  # ✅ and use st.experimental_rerun() or manual refresh logic


st.title("Sydney Trains Realtime Dashboard")

# File upload inputs
uploaded_pb = st.file_uploader("Upload GTFS-Realtime feed (.pb)", type=["pb"])
uploaded_stops = st.file_uploader("Upload Stops CSV", type=["csv"])

# Define helper functions
def safe_convert_time(unix_time):
    """Convert Unix timestamp to readable format, or 'N/A' if None/zero."""
    if unix_time:
        try:
            return datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return "N/A"
    return "N/A"

def parse_trip_updates(pb_bytes):
    """Parse GTFS-Realtime protobuf bytes and return a DataFrame of stop updates."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(pb_bytes)  # parse the binary feed&#8203;:contentReference[oaicite:4]{index=4}
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
    df = pd.DataFrame(records)
    return df

# Only proceed if both files are provided
if not uploaded_pb or not uploaded_stops:
    st.info("Please upload a .pb feed and a stops.csv to view data.")
    st.stop()

# Read stops data
try:
    stops_df = pd.read_csv(uploaded_stops)
except Exception as e:
    st.error(f"Could not read stops CSV: {e}")
    st.stop()

# Validate stops columns
required_cols = {"stop_id", "stop_name", "stop_lat", "stop_lon"}
if not required_cols.issubset(stops_df.columns):
    st.error("Stops CSV must contain stop_id, stop_name, stop_lat, stop_lon columns.")
    st.stop()

# Parse the GTFS realtime feed
try:
    feed_bytes = uploaded_pb.read()
    trip_updates_df = parse_trip_updates(feed_bytes)
except Exception as e:
    st.error(f"Failed to parse GTFS feed: {e}")
    st.stop()

trip_updates_df["stop_id"] = trip_updates_df["stop_id"].astype(str)
stops_df["stop_id"] = stops_df["stop_id"].astype(str)

# Merge with stop info
combined_df = pd.merge(trip_updates_df, stops_df, on="stop_id", how="left")

# Display results or handle no data
if combined_df.empty:
    st.warning("No trip updates found in the feed.")
else:
    st.success(f"Feed loaded successfully! {len(trip_updates_df)} stop updates found.")
    st.dataframe(combined_df)

    # Prepare and display map
    map_data = combined_df.dropna(subset=["stop_lat", "stop_lon"])[["stop_lat", "stop_lon"]].drop_duplicates()
    map_data = map_data.rename(columns={"stop_lat": "lat", "stop_lon": "lon"})
    st.map(map_data)

# Auto-refresh every 30 seconds to update with new data if available
st_autorefresh(interval=30 * 1000, key="refresh")
