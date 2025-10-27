import os
import pandas as pd
import streamlit as st
import pydeck as pdk
import altair as alt

st.set_page_config(page_title="HSL Delay Dashboard", layout="wide")
st.title("HSL Delay Dashboard")

df = pd.read_csv("combined_delays.csv")

df["delay_minutes"] = df["delay_seconds"] / 60

st.sidebar.header("Filters")

lines = sorted(df["line_number"].dropna().unique())
selected_lines = st.sidebar.multiselect(
    "Select line(s):", lines, default=lines
)

df_filtered = df[df["line_number"].isin(selected_lines)]

stops = sorted(df_filtered["stop_name"].dropna().unique())
selected_stops = st.sidebar.multiselect(
    "Select stop(s):", stops, default=[]
)

if selected_stops:
    df_filtered = df_filtered[df_filtered["stop_name"].isin(selected_stops)]

st.subheader("Interactive Delay Map")

df_map = (
    df_filtered.groupby(["stop_name", "stop_lat", "stop_lon"])["delay_minutes"]
    .mean()
    .reset_index()
    .dropna(subset=["stop_lat", "stop_lon"])
)
df_map["delay_minutes"] = df_map["delay_minutes"].round(2)

def delay_to_color(delay):
    delay = max(min(delay, 5), -3)  
    if delay <= 0:
        return [0, 180, 100, 180]   # Green
    elif delay < 2:
        return [255, 200, 50, 180]  # Yellow
    else:
        return [255, 50, 50, 200]   # Red

df_map["color"] = df_map["delay_minutes"].apply(delay_to_color)

view_state = pdk.ViewState(
    latitude=df_map["stop_lat"].astype(float).mean(),
    longitude=df_map["stop_lon"].astype(float).mean(),
    zoom=11,
    pitch=0,
)

layer = pdk.Layer(
    "ScatterplotLayer",
    data=df_map,
    get_position=["stop_lon", "stop_lat"],
    get_color="color",
    get_radius=30,
    pickable=True,
)

tooltip = {
    "html": "<b>Stop:</b> {stop_name}<br/><b>Avg Delay:</b> {delay_minutes} min",
    "style": {"backgroundColor": "steelblue", "color": "white"},
}

st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))

st.subheader("Delay Statistics")
avg_delay = df_filtered["delay_minutes"].mean()
st.metric("Average Delay (minutes)", f"{avg_delay:.2f}")

st.subheader("Stops with Highest Average Delay")
top_stops = (
    df_filtered.groupby("stop_name")["delay_minutes"]
    .mean()
    .sort_values(ascending=False)
    .reset_index()
    .head(15)
)
chart = (
    alt.Chart(top_stops)
    .mark_bar(color="tomato")
    .encode(
        x=alt.X("delay_minutes:Q", title="Average Delay (minutes)"),
        y=alt.Y("stop_name:N", sort="-x", title="Stop ID"),
        tooltip=["stop_name", "delay_minutes"]
    )
    .properties(width="container", height=450)
)
st.altair_chart(chart, use_container_width=True)

with st.expander("View all stops by average delay"):
    st.dataframe(
        df_filtered.groupby("stop_name")["delay_minutes"]
        .mean()
        .sort_values(ascending=False)
        .reset_index(),
        use_container_width=True,
    )

st.subheader("Predicted vs Actual Delays")

df_pred = df_filtered.dropna(subset=["delay_seconds", "predicted_delay"]).copy()

df_pred["delay_seconds"] = pd.to_numeric(df_pred["delay_seconds"], errors="coerce")
df_pred["predicted_delay"] = pd.to_numeric(df_pred["predicted_delay"], errors="coerce")

min_val = float(df_pred[["delay_seconds", "predicted_delay"]].min().min())
max_val = float(df_pred[["delay_seconds", "predicted_delay"]].max().max())

scatter = (
    alt.Chart(df_pred)
    .mark_point(opacity=0.5, color="steelblue")
    .encode(
        x=alt.X("delay_seconds:Q", title="Actual Delay (s)"),
        y=alt.Y("predicted_delay:Q", title="Predicted Delay (s)"),
        tooltip=["line_number", "stop_name", "delay_seconds", "predicted_delay"]
    )
    .properties(height=420)
    .interactive()
)

diagonal = (
    alt.Chart(pd.DataFrame({"x": [min_val, max_val], "y": [min_val, max_val]}))
    .mark_line(strokeDash=[6, 6], color="red")
    .encode(x="x", y="y")
)

st.altair_chart(scatter + diagonal, use_container_width=True)





