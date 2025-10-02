import pandas as pd
from math import radians, cos, sin, asin, sqrt
from datetime import datetime, timedelta


# ---------------------------
# Load GTFS data
# ---------------------------
def load_gtfs():
    # Load required GTFS files
    stops = pd.read_csv("stops.txt")[["stop_id", "stop_lat", "stop_lon"]]
    stop_times = pd.read_csv("stop_times.txt")[["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"]]
    trips = pd.read_csv("trips.txt")[["route_id", "trip_id"]]

    # --- Fix types: force all IDs to string ---
    stops["stop_id"] = stops["stop_id"].astype(str)
    stop_times["stop_id"] = stop_times["stop_id"].astype(str)
    stop_times["trip_id"] = stop_times["trip_id"].astype(str)
    trips["trip_id"] = trips["trip_id"].astype(str)
    trips["route_id"] = trips["route_id"].astype(str)

    # Merge stop_times with stops and trips
    stop_times = (
        stop_times
        .merge(stops, on="stop_id", how="left")
        .merge(trips, on="trip_id", how="left")
    )

    return stop_times


# ---------------------------
# Utility: Haversine distance
# ---------------------------
def haversine(lat1, lon1, lat2, lon2):
    """Return distance in meters between two lat/lon points."""
    R = 6371000  # Earth radius in meters
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return R * c


# ---------------------------
# Core: Map GPS → nearest stop
# ---------------------------
def map_gps_to_stop(vehicle_lat, vehicle_lon, route_id, stop_times_df):
    """
    Given a GPS point (lat, lon) and a route_id,
    return the nearest scheduled stop from GTFS stop_times.
    """
    route_stops = stop_times_df[stop_times_df["route_id"] == str(route_id)].copy()

    if route_stops.empty:
        print(f"No stops found for route_id={route_id}")
        return None

    # Compute distance to each stop
    route_stops["dist_m"] = route_stops.apply(
        lambda row: haversine(vehicle_lat, vehicle_lon, row.stop_lat, row.stop_lon),
        axis=1
    )

    # Pick the closest stop
    nearest = route_stops.loc[route_stops["dist_m"].idxmin()]
    return nearest


def compute_delay(gps_timestamp, arrival_time_str):
    """
    gps_timestamp: string "YYYY-MM-DD HH:MM:SS"
    arrival_time_str: string "HH:MM:SS" from GTFS
    Returns delay in seconds (positive = late, negative = early)
    """
    # Parse GPS timestamp
    gps_time = datetime.strptime(gps_timestamp, "%Y-%m-%d %H:%M:%S")
    
    # Parse GTFS time (HH:MM:SS). GTFS may have hours >24 for next-day trips
    h, m, s = map(int, arrival_time_str.split(":"))
    scheduled_time = datetime(
        year=gps_time.year, month=gps_time.month, day=gps_time.day,
        hour=h % 24, minute=m, second=s
    )
    # If GTFS hour >=24, increment day
    if h >= 24:
        scheduled_time += timedelta(days=h // 24)
    
    # Compute delay in seconds
    delay_sec = (gps_time - scheduled_time).total_seconds()
    return delay_sec



# ---------------------------
# Example usage
# ---------------------------
if __name__ == "__main__":
    stop_times = load_gtfs()

    print("Available route_ids:", stop_times["route_id"].unique()[:10])  # debug: show first few route_ids

    # Example live GPS record
    gps_point = {
        "vehicle_id": "40/417",
        "route_id": "1002",   # string now!
        "lat": 60.181557,
        "lon": 24.926863,
        "timestamp": "2025-10-02 08:15:00"
    }

    nearest_stop = map_gps_to_stop(
        gps_point["lat"],
        gps_point["lon"],
        gps_point["route_id"],
        stop_times
    )

    if nearest_stop is not None:
        print(f"\nVehicle {gps_point['vehicle_id']} nearest stop:")
        print(nearest_stop)

    delay_seconds = compute_delay(gps_point["timestamp"], nearest_stop["arrival_time"])
    print(f"Estimated delay for vehicle {gps_point['vehicle_id']}: {delay_seconds:.0f} seconds")

