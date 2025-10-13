import os
import requests
import pandas as pd
import numpy as np
from google.transit import gtfs_realtime_pb2
from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, atan2

# -------------------------
# CONFIG
# -------------------------
GTFS_ZIP = "https://transitfeeds.com/p/helsinki-regional-transport/735/latest/download"
STOP_TIMES_PARQUET = "stop_times.parquet"
desktop = os.path.join(os.path.expanduser("~"), "Desktop")
CSV_OUTPUT = os.path.join(desktop, "hsl_vehicle_delays.csv")

# -------------------------
# GEO UTILS
# -------------------------
def haversine_np(lat1, lon1, lat2, lon2):
    R = 6371000
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))

# -------------------------
# LOAD GTFS
# -------------------------
def load_gtfs():
    import zipfile
    import io

    if os.path.exists(STOP_TIMES_PARQUET):
        print("Using cached stop_times.parquet")
        stop_times = pd.read_parquet(STOP_TIMES_PARQUET)
        # Always reload routes from GTFS zip
        r = requests.get(GTFS_ZIP)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        with z.open("routes.txt") as f:
            routes = pd.read_csv(f)
        return stop_times, routes

    # Otherwise, build from scratch
    r = requests.get(GTFS_ZIP)
    z = zipfile.ZipFile(io.BytesIO(r.content))

    with z.open("stop_times.txt") as f:
        stop_times = pd.read_csv(f)
    with z.open("stops.txt") as f:
        stops = pd.read_csv(f)
    with z.open("trips.txt") as f:
        trips = pd.read_csv(f)
    with z.open("routes.txt") as f:
        routes = pd.read_csv(f)

    # Merge stop coordinates and route info
    stop_times = stop_times.merge(stops[["stop_id", "stop_lat", "stop_lon"]], on="stop_id", how="left")
    stop_times = stop_times.merge(trips[["trip_id", "route_id"]], on="trip_id", how="left")

    stop_times.to_parquet(STOP_TIMES_PARQUET)
    print("Saved stop_times.parquet")

    return stop_times, routes


# -------------------------
# CACHE BUILDERS
# -------------------------
def build_route_cache(stop_times):
    print("Building route -> stops cache")
    return {rid: df for rid, df in stop_times.groupby("route_id")}

def build_trip_cache(stop_times):
    print("Building trip -> stops cache")
    return {tid: df for tid, df in stop_times.groupby("trip_id")}

# -------------------------
# REALTIME VEHICLES
# -------------------------
# -------------------------
# REALTIME VEHICLES
# -------------------------
def get_vehicle_positions(routes):
    print("Fetching realtime vehicles…")

    # Map route_id -> route_short_name (e.g., "M2", "118", etc.)
    route_map = dict(zip(routes["route_id"].astype(str), routes["route_short_name"]))

    # HSL GTFS-RT vehicle positions
    url = "https://realtime.hsl.fi/realtime/vehicle-positions/v2/hsl"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"Realtime request failed: {e}")
        return pd.DataFrame([])

    # Parse protobuf feed
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(resp.content)
    except Exception as e:
        print(f"Failed to parse GTFS-RT protobuf: {e}")
        return pd.DataFrame([])

    # Use Helsinki local time
    hki_now = datetime.now(TZ)

    rows = []
    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue
        v = e.vehicle

        # Guard against missing nested fields
        trip_id = v.trip.trip_id if v.trip.HasField("trip_id") else None
        route_id = v.trip.route_id if v.trip.HasField("route_id") else None
        line_number = route_map.get(str(route_id), "Unknown")

        # Skip vehicles we can't map to a line
        if line_number == "Unknown":
            continue

        lat = v.position.latitude if v.HasField("position") else None
        lon = v.position.longitude if v.HasField("position") else None

        # Prefer vehicle's own timestamp if present
        ts_dt = hki_now
        if v.HasField("timestamp"):
            try:
                # v.timestamp is seconds since epoch (UTC)
                ts_dt = datetime.fromtimestamp(v.timestamp, pytz.utc).astimezone(TZ)
            except Exception:
                pass

        rows.append({
            "vehicle_id": v.vehicle.id if v.HasField("vehicle") else None,
            "trip_id": trip_id,
            "route_id": route_id,
            "line_number": line_number,
            "latitude": lat,
            "longitude": lon,
            "timestamp_dt": ts_dt,
        })

    df = pd.DataFrame(rows)

    # Drop obvious invalid rows
    df = df.dropna(subset=["latitude", "longitude"])
    return df



# -------------------------
# NEAREST STOP
# -------------------------
from datetime import datetime, timedelta, timezone
import pytz  # pip install pytz

TZ = pytz.timezone("Europe/Helsinki")

def parse_gtfs_time_to_seconds(arrival_time_str):
    # Handles HH:MM:SS with HH possibly >= 24
    h, m, s = [int(x) for x in arrival_time_str.split(":")]
    return h * 3600 + m * 60 + s

def pick_best_stop_for_timestamp(stops_df, gps_dt):
    tod_seconds = gps_dt.hour * 3600 + gps_dt.minute * 60 + gps_dt.second
    sched_secs = stops_df["arrival_time"].apply(parse_gtfs_time_to_seconds)
    diffs = sched_secs - tod_seconds

    # Only consider stops within ±30 minutes
    mask = diffs.abs() < 1800
    if mask.any():
        idx = diffs[mask].abs().idxmin()
    else:
        # fallback: closest overall
        idx = diffs.abs().idxmin()

    return stops_df.loc[idx]


def map_gps_to_stop(lat, lon, trip_id, route_id, trip_cache, route_cache, gps_dt):
    if trip_id and trip_id in trip_cache and not trip_cache[trip_id].empty:
        stops_df = trip_cache[trip_id]
    else:
        stops_df = route_cache.get(str(route_id))
        if stops_df is None or stops_df.empty:
            return None

    # Compute distance to all stops
    stops_df = stops_df.copy()
    stops_df["dist"] = haversine_np(lat, lon, stops_df["stop_lat"], stops_df["stop_lon"])

    # Pick the closest stop in time ±30 min
    nearest = pick_best_stop_for_timestamp(stops_df.sort_values("dist"), gps_dt)
    return nearest


# -------------------------
# DELAY COMPUTATION
# -------------------------
def compute_delay(gps_dt, arrival_time_str):
    try:
        h, m, s = [int(x) for x in arrival_time_str.split(":")]
    except Exception:
        return float("nan")

    # Handle 24+ hour times
    day_offset, h_norm = divmod(h, 24)
    scheduled_seconds = h_norm * 3600 + m * 60 + s
    base_midnight = datetime(gps_dt.year, gps_dt.month, gps_dt.day, tzinfo=gps_dt.tzinfo)
    scheduled_dt = base_midnight + timedelta(days=day_offset, seconds=scheduled_seconds)

    # Consider yesterday/today/tomorrow
    candidates = [
        scheduled_dt + timedelta(days=-1),
        scheduled_dt,
        scheduled_dt + timedelta(days=1),
    ]
    deltas = [(gps_dt - c).total_seconds() for c in candidates]
    best = min(deltas, key=lambda x: abs(x))

    # Keep delay in seconds (instead of minutes)
    best_seconds = round(best)

    # Ignore if outside ±1800 seconds (±30 minutes)
    if abs(best_seconds) > 1800:
        return float("nan")

    return best_seconds


def main():
    stop_times, routes = load_gtfs()
    route_cache = build_route_cache(stop_times)
    trip_cache = build_trip_cache(stop_times)

    vehicles = get_vehicle_positions(routes)
    print(f"{len(vehicles)} vehicles fetched")

    results = []
    for _, v in vehicles.iterrows():
        nearest = map_gps_to_stop(
            v["latitude"], v["longitude"],
            v.get("trip_id"), v["route_id"],
            trip_cache, route_cache,
            v["timestamp_dt"]
        )
        if nearest is not None:
            delay = compute_delay(v["timestamp_dt"], nearest["arrival_time"])
            results.append({
                "vehicle_id": v["vehicle_id"],
                "trip_id": v.get("trip_id"),
                "line_number": v["line_number"],
                "route_id": v["route_id"],
                "latitude": v["latitude"],
                "longitude": v["longitude"],
                "timestamp": v["timestamp_dt"].strftime("%Y-%m-%d %H:%M:%S"),
                "nearest_stop": nearest["stop_id"],
                "scheduled_arrival": nearest["arrival_time"],
                "delay_seconds": delay
            })


    df = pd.DataFrame(results)
    df.to_csv(CSV_OUTPUT, index=False)
    print(f"Saved {len(df)} delay records to {CSV_OUTPUT}")
    print(df.head())

if __name__ == "__main__":
    main()
    print(stop_times["arrival_time"].sample(10))