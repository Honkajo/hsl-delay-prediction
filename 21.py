import os, io, zipfile, requests, time
import pandas as pd
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt
from google.transit import gtfs_realtime_pb2

# --------------------------------
# CONFIG
# --------------------------------
CACHE_DIR = "gtfs_cache"
os.makedirs(CACHE_DIR, exist_ok=True)
STATIC_GTFS_URL = "https://transitfeeds.com/p/helsinki-regional-transport/735/latest/download"
ROUTES_CSV = os.path.join(CACHE_DIR, "routes.csv")
DATASET_CSV = os.path.join(CACHE_DIR, "delay_training_data.csv")

# --------------------------------
# UTILITIES
# --------------------------------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat, dlon = radians(lat2-lat1), radians(lon2-lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))

def compute_delay(gps_timestamp, arrival_time_str):
    gps_time = datetime.strptime(gps_timestamp, "%Y-%m-%d %H:%M:%S")
    h, m, s = map(int, arrival_time_str.split(":"))
    sched = datetime(gps_time.year, gps_time.month, gps_time.day, h % 24, m, s)
    if h >= 24:
        sched += timedelta(days=h // 24)
    return (gps_time - sched).total_seconds()

# --------------------------------
# LOAD STATIC GTFS
# --------------------------------
def load_gtfs():
    """Download/cached static GTFS and merge stop_times, stops, trips."""
    r = requests.get(STATIC_GTFS_URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))

    stops = pd.read_csv(z.open("stops.txt"))[["stop_id", "stop_lat", "stop_lon"]]
    stop_times = pd.read_csv(z.open("stop_times.txt"))[["trip_id", "arrival_time", "stop_id"]]
    trips = pd.read_csv(z.open("trips.txt"))[["trip_id", "route_id"]]
    routes = pd.read_csv(z.open("routes.txt"))[["route_id", "route_short_name"]]

    df = stop_times.merge(stops, on="stop_id", how="left").merge(trips, on="trip_id", how="left")
    df["route_id"] = df["route_id"].astype(str)
    routes["route_id"] = routes["route_id"].astype(str)
    return df, routes

# --------------------------------
# MAP GPS → NEAREST STOP
# --------------------------------
def map_gps_to_stop(vehicle_lat, vehicle_lon, route_id, stop_times_df):
    route_stops = stop_times_df[stop_times_df["route_id"] == str(route_id)].copy()
    if route_stops.empty:
        return None
    route_stops["dist_m"] = route_stops.apply(
        lambda row: haversine(vehicle_lat, vehicle_lon, row.stop_lat, row.stop_lon),
        axis=1
    )
    return route_stops.loc[route_stops["dist_m"].idxmin()]

# --------------------------------
# FETCH REAL-TIME VEHICLES
# --------------------------------
def get_vehicle_positions(routes):
    route_map = dict(zip(routes["route_id"], routes["route_short_name"]))
    url = "https://realtime.hsl.fi/realtime/vehicle-positions/v2/hsl"

    resp = requests.get(url)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)

    rows = []
    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue
        v = e.vehicle
        rid = v.trip.route_id if v.trip.HasField("route_id") else None
        if rid not in route_map:
            continue
        rows.append({
            "vehicle_id": v.vehicle.id,
            "route_id": rid,
            "line_number": route_map[rid],
            "latitude": v.position.latitude,
            "longitude": v.position.longitude,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    return pd.DataFrame(rows)

# --------------------------------
# MAIN LOOP
# --------------------------------
if __name__ == "__main__":
    stop_times, routes = load_gtfs()

    print("Starting data collection...")
    all_records = []

    print("Starting data collection...")

    while True:
        vehicles = get_vehicle_positions(routes)
        fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{len(vehicles)} live vehicles fetched at {fetch_time}")

        new_records = []

        for _, v in vehicles.iterrows():
            nearest = map_gps_to_stop(v.latitude, v.longitude, v.route_id, stop_times)
            if nearest is None:
                print(f"No stop match for vehicle {v.vehicle_id} on route {v.route_id}")
                continue

            try:
                delay_sec = compute_delay(v.timestamp, nearest["arrival_time"])
            except Exception as e:
                print("Delay computation failed:", e)
                continue
            new_records.append({
                **v.to_dict(),
                "nearest_stop": nearest["stop_id"],
                "scheduled_arrival": nearest["arrival_time"],
                "delay_seconds": delay_sec,
                "distance_m": nearest["dist_m"],
                "fetch_time": fetch_time
            })

        if new_records:
            new_df = pd.DataFrame(new_records)
            if not os.path.exists(DATASET_CSV):
                new_df.to_csv(DATASET_CSV, index=False)
            else:
                new_df.to_csv(DATASET_CSV, mode="a", header=False, index=False)

            print(f"Saved {len(new_records)} samples -> {DATASET_CSV}")

        else:
            print("No new records found this cycle.")

        time.sleep(300)  # 5 minutes

