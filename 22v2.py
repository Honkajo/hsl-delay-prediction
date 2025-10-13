import os, requests, zipfile, io, pandas as pd
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from math import radians, cos, sin, asin, sqrt
from datetime import timedelta

# ---------------------------
# Load GTFS static data
# ---------------------------
def load_gtfs():
    stops = pd.read_csv("stops.txt")[["stop_id", "stop_lat", "stop_lon"]]
    stop_times = pd.read_csv("stop_times.txt")[["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"]]
    trips = pd.read_csv("trips.txt")[["route_id", "trip_id"]]

    stops["stop_id"] = stops["stop_id"].astype(str)
    stop_times["stop_id"] = stop_times["stop_id"].astype(str)
    stop_times["trip_id"] = stop_times["trip_id"].astype(str)
    trips["trip_id"] = trips["trip_id"].astype(str)
    trips["route_id"] = trips["route_id"].astype(str)

    stop_times = (
        stop_times
        .merge(stops, on="stop_id", how="left")
        .merge(trips, on="trip_id", how="left")
    )
    return stop_times

# ---------------------------
# Haversine distance
# ---------------------------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return R * 2 * asin(sqrt(a))

# ---------------------------
# Map GPS → nearest stop
# ---------------------------
def map_gps_to_stop(vehicle_lat, vehicle_lon, route_id, stop_times_df):
    route_stops = stop_times_df[stop_times_df["route_id"] == str(route_id)].copy()
    if route_stops.empty:
        return None
    route_stops["dist_m"] = route_stops.apply(
        lambda row: haversine(vehicle_lat, vehicle_lon, row.stop_lat, row.stop_lon), axis=1
    )
    return route_stops.loc[route_stops["dist_m"].idxmin()]

# ---------------------------
# Compute delay
# ---------------------------
def compute_delay(gps_timestamp, arrival_time_str):
    gps_time = datetime.strptime(gps_timestamp, "%Y-%m-%d %H:%M:%S")
    h, m, s = map(int, arrival_time_str.split(":"))
    scheduled_time = datetime(
        year=gps_time.year, month=gps_time.month, day=gps_time.day,
        hour=h % 24, minute=m, second=s
    )
    if h >= 24:
        scheduled_time += timedelta(days=h // 24)
    return (gps_time - scheduled_time).total_seconds()

# ---------------------------
# Fetch real-time vehicles
# ---------------------------
def get_vehicle_positions(routes):
    from google.transit import gtfs_realtime_pb2
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
                "latitude": v.position.latitude if v.position else None,
                "longitude": v.position.longitude if v.position else None,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
    return pd.DataFrame(vehicles)

# ---------------------------
# Main pipeline
# ---------------------------
if __name__ == "__main__":
    stop_times = load_gtfs()
    routes = pd.read_csv("routes.txt")  # already extracted
    vehicles = get_vehicle_positions(routes)

    records = []
    for _, v in vehicles.iterrows():
        nearest = map_gps_to_stop(v.latitude, v.longitude, v.route_id, stop_times)
        if nearest is not None:
            delay = compute_delay(v.timestamp, nearest["arrival_time"])
            records.append({
                "vehicle_id": v.vehicle_id,
                "route_id": v.route_id,
                "timestamp": v.timestamp,
                "nearest_stop_id": nearest["stop_id"],
                "scheduled_arrival": nearest["arrival_time"],
                "delay_seconds": delay
            })

    df = pd.DataFrame(records)
    print(df.head())
    df.to_csv("training_data.csv", index=False)
