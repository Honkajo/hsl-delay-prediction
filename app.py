from flask import Flask, render_template, jsonify
import requests, zipfile, io, os, pandas as pd
from google.transit import gtfs_realtime_pb2

app = Flask(__name__)

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

    routes.to_csv(ROUTES_CSV, index=False)
    return routes

def get_vehicle_positions(routes):

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
    df = df[df["line_number"] != "Unknown"]
    return df

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/vehicle_positions.json")
def vehicle_positions():
    routes = load_routes()
    df = get_vehicle_positions(routes)
    return jsonify(df.to_dict(orient="records"))

if __name__ == "__main__":
    print("Starting Flask app at http://127.0.0.1:5000")
    app.run(debug=True)