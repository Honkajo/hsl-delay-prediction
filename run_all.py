import os
import io
import time
import zipfile
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from flask import Flask, render_template, jsonify
from google.transit import gtfs_realtime_pb2

# -----------------------------
# CONFIG
# -----------------------------
TZ = pytz.timezone("Europe/Helsinki")
CACHE_DIR = "gtfs_cache"
os.makedirs(CACHE_DIR, exist_ok=True)
ROUTES_CSV = os.path.join(CACHE_DIR, "routes.csv")
CSV_PATH = os.path.join(os.path.expanduser("~"), "Desktop", "hsl_vehicle_delays.csv")
GTFS_ZIP = "https://transitfeeds.com/p/helsinki-regional-transport/735/latest/download"

app = Flask(__name__)

# ==========================================================
# STATIC + REALTIME DATA HELPERS
# ==========================================================

def load_routes():
    """Load static GTFS routes (cached)."""
    if os.path.exists(ROUTES_CSV):
        return pd.read_csv(ROUTES_CSV)

    print("📦 Downloading static GTFS ZIP (first run only)…")
    r = requests.get(GTFS_ZIP)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        with z.open("routes.txt") as f:
            routes = pd.read_csv(f)

    routes.to_csv(ROUTES_CSV, index=False)
    print("✅ Cached routes.csv")
    return routes


def get_vehicle_positions(routes):
    """Fetch live vehicle positions."""
    route_map = dict(zip(routes["route_id"].astype(str), routes["route_short_name"]))
    rt_url = "https://realtime.hsl.fi/realtime/vehicle-positions/v2/hsl"

    try:
        resp = requests.get(rt_url, timeout=15)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(resp.content)
    except Exception as e:
        print(f"⚠️ Failed to fetch realtime vehicles: {e}")
        return pd.DataFrame([])

    vehicles = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        route_id = v.trip.route_id if v.trip.HasField("route_id") else None
        if not route_id or str(route_id) not in route_map:
            continue

        vehicles.append({
            "vehicle_id": v.vehicle.id,
            "trip_id": v.trip.trip_id if v.trip.HasField("trip_id") else None,
            "route_id": route_id,
            "line_number": route_map.get(str(route_id), "Unknown"),
            "direction": v.trip.direction_id if v.trip.HasField("direction_id") else None,
            "latitude": v.position.latitude if v.HasField("position") else None,
            "longitude": v.position.longitude if v.HasField("position") else None
        })

    df = pd.DataFrame(vehicles)
    df = df[df["line_number"] != "Unknown"]
    print(f"✅ {len(df)} vehicles fetched")
    return df


# ==========================================================
# DATA COLLECTION FOR TABLEAU
# ==========================================================

def create_clean_data(rounds=1, delay_between=0):
    print(f"🧹 Collecting realtime HSL data ({rounds} round)…")

    # Load GTFS static files
    r = requests.get(GTFS_ZIP)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    with z.open("stop_times.txt") as f: stop_times = pd.read_csv(f)
    with z.open("stops.txt") as f: stops = pd.read_csv(f)
    with z.open("trips.txt") as f: trips = pd.read_csv(f)
    with z.open("routes.txt") as f: routes = pd.read_csv(f)

    stop_times = stop_times.merge(stops[["stop_id","stop_lat","stop_lon"]], on="stop_id", how="left")
    stop_times = stop_times.merge(trips[["trip_id","route_id"]], on="trip_id", how="left")
    route_map = dict(zip(routes["route_id"].astype(str), routes["route_short_name"]))

    def haversine(lat1, lon1, lat2, lon2):
        R = 6371000
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
        return 2 * R * np.arcsin(np.sqrt(a))

    def parse_gtfs_time(t):
        try:
            h, m, s = map(int, t.split(":"))
            return h * 3600 + m * 60 + s
        except:
            return np.nan

    all_rows = []

    print("⏱️ Fetching realtime positions (1 round)…")
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        resp = requests.get("https://realtime.hsl.fi/realtime/vehicle-positions/v2/hsl", timeout=15)
        feed.ParseFromString(resp.content)
    except Exception as e:
        print("⚠️ Realtime fetch failed:", e)
        return pd.DataFrame([])

    now = datetime.now(TZ)
    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue
        v = e.vehicle
        route_id = v.trip.route_id if v.trip.HasField("route_id") else None
        trip_id = v.trip.trip_id if v.trip.HasField("trip_id") else None
        if not route_id or str(route_id) not in route_map:
            continue
        line_number = route_map[str(route_id)]
        lat = v.position.latitude if v.HasField("position") else None
        lon = v.position.longitude if v.HasField("position") else None
        ts_dt = datetime.fromtimestamp(v.timestamp, pytz.utc).astimezone(TZ) if v.HasField("timestamp") else now

        # ✅ Prefer trip-based stop times if available
        if trip_id and trip_id in stop_times["trip_id"].values:
            stops_df = stop_times[stop_times["trip_id"] == trip_id].copy()
        else:
            stops_df = stop_times[stop_times["route_id"] == route_id].copy()

        if stops_df.empty or lat is None or lon is None:
            continue

        # --- nearest stop by space + past time ---
        stops_df["dist"] = haversine(lat, lon, stops_df["stop_lat"], stops_df["stop_lon"])
        stops_df["sched_sec"] = stops_df["arrival_time"].apply(parse_gtfs_time)
        current_sec = ts_dt.hour * 3600 + ts_dt.minute * 60 + ts_dt.second
        stops_df = stops_df[stops_df["sched_sec"] <= current_sec + 300]
        if stops_df.empty:
            continue
        stops_df["score"] = stops_df["dist"] + (current_sec - stops_df["sched_sec"]) * 5
        nearest = stops_df.loc[stops_df["score"].idxmin()]
        # -----------------------------------------

        try:
            h, m, s = [int(x) for x in nearest["arrival_time"].split(":")]
        except Exception:
            continue
        day_offset, h_norm = divmod(h, 24)

        # ✅ Proper local timezone alignment
        scheduled = TZ.localize(datetime(ts_dt.year, ts_dt.month, ts_dt.day, h_norm, m, s)) + timedelta(days=day_offset)

        delay = (ts_dt - scheduled).total_seconds()
        if abs(delay) > 7200:
            continue

        all_rows.append({
            "timestamp_local": ts_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "date": ts_dt.date().isoformat(),
            "hour": ts_dt.hour,
            "dow": ts_dt.weekday(),
            "is_weekend": int(ts_dt.weekday() >= 5),
            "season": ["winter","winter","spring","spring","spring","summer","summer","summer","autumn","autumn","autumn","winter"][ts_dt.month-1],
            "line_number": line_number,
            "route_id": route_id,
            "trip_id": trip_id,
            "vehicle_id": v.vehicle.id,
            "nearest_stop": nearest["stop_id"],
            "latitude": lat,
            "longitude": lon,
            "delay_seconds": int(delay),
            "delay_minutes": round(delay / 60, 2),
            "delay_minutes_abs": round(abs(delay) / 60, 2)
        })

    df_new = pd.DataFrame(all_rows)
    print(f"✅ Collected {len(df_new)} rows from realtime feed")

    if os.path.exists(CSV_PATH):
        df_old = pd.read_csv(CSV_PATH)
        df = pd.concat([df_old, df_new]).drop_duplicates().reset_index(drop=True)
    else:
        df = df_new

    df.to_csv(CSV_PATH, index=False)
    print(f"✅ Saved {len(df)} total rows → {CSV_PATH}")

    if len(df) > 0:
        top_lines = df["line_number"].value_counts().head(5)
        print("📊 Top lines:")
        print(top_lines.to_string())

    return df


# ==========================================================
# FLASK ROUTES
# ==========================================================

@app.route("/")
def home():
    return render_template("index.html")


@app.route("/vehicle_positions.json")
def vehicle_positions():
    """Serve live data for frontend dropdown"""
    routes = load_routes()
    df = get_vehicle_positions(routes)
    return jsonify(df.to_dict(orient="records"))


# ==========================================================
# MAIN ENTRY POINT
# ==========================================================

if __name__ == "__main__":
    print("🚀 Starting data collection and Flask server...")
    create_clean_data(rounds=1, delay_between=0)
    print("🌐 Starting Flask at http://127.0.0.1:5000")
    app.run(debug=False)




