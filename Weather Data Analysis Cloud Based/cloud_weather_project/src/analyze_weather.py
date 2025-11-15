from __future__ import annotations
import os
import pandas as pd
import plotly.express as px
from pathlib import Path

def save_csv(df: pd.DataFrame, out_path: str) -> None:
    Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

def plot_timeseries(hourly: pd.DataFrame, city: str, out_dir: str) -> list[str]:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    outputs = []
    # Temp
    df_city = hourly[hourly["city"] == city]
    if df_city.empty:
        return outputs
    for col, title in [
        ("temperature_2m", "Hourly Temperature (°C)"),
        ("relative_humidity_2m", "Hourly Relative Humidity (%)"),
        ("precipitation", "Hourly Precipitation (mm)"),
        ("windspeed_10m", "Hourly Wind Speed (m/s)"),
    ]:
        if col in df_city.columns:
            fig = px.line(df_city, x="time", y=col, title=f"{title} — {city}")
            out_path = os.path.join(out_dir, f"{city.replace(' ', '_').lower()}_{col}.png")
            fig.write_image(out_path, engine="kaleido")
            outputs.append(out_path)
    return outputs
