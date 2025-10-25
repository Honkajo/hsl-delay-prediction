import os
import time
import csv
import io
import zipfile
import requests
from google.transit import gtfs_realtime_pb2

# --- URLs ---
GTFS_RT_TRIP_UPDATES = "https://realtime.hsl.fi/realtime/trip-updates/v2/hsl"
GTFS_STATIC_ZIP = "https://infopalvelut.storage.hsldev.com/gtfs/hsl.zip"

API_KEY = os.getenv("DIGITRANSIT_KEY") or "YOUR_API_KEY_HERE"
HEADERS = {"digitransit-subscription-key": API_KEY} if API_KEY else {}

# --- 1. Fetch realtime updates ---
def get_trip_updates():
    feed = gtfs_realtime_pb2.FeedMessage()
    resp = requests.get(GTFS_RT_TRIP_UPDATES, headers=HEADERS)
    resp.raise_for_status()
    print("Response content-type:", resp.headers.get("Content-Type"))
    feed.ParseFromString(resp.content)
    return feed.entity

# --- 2. Load route_id → route_short_name map ---
def load_route_map():
    print("Downloading GTFS static routes (routes.txt)...")
    r = requests.get(GTFS_STATIC_ZIP)
    r.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(r.content))
    routes_file = None
    for name in z.namelist():
        if "routes" in name.lower() and name.lower().endswith(".txt"):
            routes_file = name
            break

    if not routes_file:
        print("⚠️ No routes.txt found in GTFS ZIP.")
        return {}

    route_map = {}
    with z.open(routes_file) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
        for row in reader:
            route_id = row.get("route_id")
            short_name = row.get("route_short_name")
            if route_id and short_name:
                route_map[route_id] = short_name

    print(f"Loaded {len(route_map)} routes from GTFS static data.")
    return route_map

# --- 3. Calculate delays ---
def calculate_delays(trip_updates, route_map):
    delays = []
    current_time = int(time.time())

    for entity in trip_updates:
        if not entity.HasField("trip_update"):
            continue
        trip = entity.trip_update
        trip_id = trip.trip.trip_id
        route_id = trip.trip.route_id.replace("HSL:", "")
        line_number = route_map.get(route_id, route_id)  # Map to public line name

        for stu in trip.stop_time_update:
            delay = 0
            if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                delay = stu.arrival.delay
            elif stu.HasField("departure") and stu.departure.HasField("delay"):
                delay = stu.departure.delay
            elif stu.HasField("arrival") and stu.arrival.HasField("time"):
                delay = stu.arrival.time - current_time  # fallback

            # ✅ Only include delays between -3 min and +5 min
            if -180 <= delay <= 300 and delay != 0:
                delays.append({
                    "trip_id": trip_id,
                    "route_id": route_id,
                    "line_number": line_number,
                    "stop_id": stu.stop_id,
                    "delay_seconds": delay
                })
    return delays

# --- 4. Save to CSV ---
def save_to_csv(delays, filename="gtfs_delays.csv"):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["trip_id", "route_id", "line_number", "stop_id", "delay_seconds"]
        )
        writer.writeheader()
        writer.writerows(delays)
    print(f"Saved {len(delays)} rows to {filename}")

# --- Main ---
if __name__ == "__main__":
    print("Fetching realtime trip updates...")
    trip_updates = get_trip_updates()
    print(f"Fetched {len(trip_updates)} trip updates")

    route_map = load_route_map()
    delays = calculate_delays(trip_updates, route_map)
    save_to_csv(delays)
