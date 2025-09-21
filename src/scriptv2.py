#pip install pandas
#pip install gtfs-realtime-bindings

import requests, zipfile, io, os, pandas as pd
from google.transit import gtfs_realtime_pb2

# --------------------------
# Static GTFS caching
# --------------------------
CACHE_DIR = "gtfs_cache"
os.makedirs(CACHE_DIR, exist_ok=True)
ROUTES_CSV = os.path.join(CACHE_DIR, "routes.csv")

STATIC_GTFS_URL = "https://transitfeeds.com/p/helsinki-regional-transport/735/latest/download"

def load_routes():
    """Load routes from cache, or download if not cached yet"""
    if os.path.exists(ROUTES_CSV):
        return pd.read_csv(ROUTES_CSV)

    print("Downloading static GTFS ZIP (first run only)…")
    r = requests.get(STATIC_GTFS_URL)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        with z.open("routes.txt") as routes_file:
            routes = pd.read_csv(routes_file)

    routes.to_csv(ROUTES_CSV, index=False)  # cache for next time
    return routes

# --------------------------
# Real-time vehicle fetcher
# --------------------------
def get_vehicle_positions(routes):
    """Fetch HSL real-time vehicle positions and join with routes"""
    route_map = dict(zip(routes["route_id"].astype(str), routes["route_short_name"]))

    rt_url = "https://realtime.hsl.fi/realtime/vehicle-positions/v2/hsl"
    resp = requests.get(rt_url)

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)

    vehicles = []
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            vehicles.append({
                "vehicle_id": v.vehicle.id,
                "route_id": v.trip.route_id if v.trip.HasField("route_id") else None,
                "line_number": route_map.get(v.trip.route_id, "Unknown"),
                "direction": v.trip.direction_id if v.trip.HasField("direction_id") else None,
                "latitude": v.position.latitude if v.position else None,
                "longitude": v.position.longitude if v.position else None
            })

    df = pd.DataFrame(vehicles)
    # Filter out unknown line numbers
    df = df[df["line_number"] != "Unknown"]
    return df

# --------------------------
# Main
# --------------------------
if __name__ == "__main__":
    routes = load_routes()
    df = get_vehicle_positions(routes)

    # Example: show first 5 vehicles
    print(df.head(5))

    # Example: show all vehicles on line 506
    print(df[df["line_number"] == "506"])

