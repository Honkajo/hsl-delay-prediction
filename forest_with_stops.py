import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt

df = pd.read_csv("combined_delays.csv")
df = df.dropna(subset=["delay_seconds", "route_id", "stop_id", "latitude", "longitude"])

stops = pd.read_csv("stops.csv")
stops = stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]]
df = df.merge(stops, on="stop_id", how="left")

df = df.dropna(subset=["stop_name"])

le_route = LabelEncoder()
le_stop = LabelEncoder()
df["route_encoded"] = le_route.fit_transform(df["route_id"])
df["stop_encoded"] = le_stop.fit_transform(df["stop_id"])


X = df[["route_encoded", "stop_encoded", "latitude", "longitude"]]
y = df["delay_seconds"]


X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

rf = RandomForestRegressor(
    n_estimators=1000,
    max_depth=None,
    min_samples_split=2,
    random_state=42,
    n_jobs=-1
)
rf.fit(X_train, y_train)

y_pred = rf.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print(f"Random Forest Performance:")
print(f"MAE = {mae:.2f}")
print(f"RMSE = {rmse:.2f}")
print(f"R^2 = {r2:.3f}")

plt.figure(figsize=(6,6))
plt.scatter(y_test, y_pred, alpha=0.5, color="blue", label="Random Forest")
plt.plot([y.min(), y.max()], [y.min(), y.max()], "r--")
plt.xlabel("Actual Delay (s)")
plt.ylabel("Predicted Delay (s)")
plt.title("Random Forest: Predicted vs Actual Delays")
plt.legend()
plt.tight_layout()
plt.show()

df["predicted_delay"] = rf.predict(X)

avg_delays = (
    df.groupby("route_id")["predicted_delay"]
    .mean()
    .reset_index()
    .rename(columns={"predicted_delay": "average_predicted_delay"})
)

df.to_csv("predicted_delays_with_stops.csv", index=False)
avg_delays.to_csv("predicted_delays_by_route.csv", index=False)

print("\nCSV files saved:")
print(" - predicted_delays_with_stops.csv (includes stop names, missing names dropped)")
print(" - predicted_delays_by_route.csv (average predicted delay per route)")
