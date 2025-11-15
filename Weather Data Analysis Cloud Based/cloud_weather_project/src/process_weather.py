from __future__ import annotations
import os
from typing import Dict, Any, List
import pandas as pd
from pathlib import Path

def _hourly_to_df(raw: Dict[str, Any], city: str) -> pd.DataFrame:
    hourly = raw.get("hourly", {})
    if not hourly:
        return pd.DataFrame()

    time_col = hourly.get("time", [])
    df = pd.DataFrame({"time": pd.to_datetime(time_col, errors="coerce", utc=True)})
    for key, values in hourly.items():
        if key == "time":
            continue
        df[key] = values

    df["city"] = city
    return df

def combine_hourly(raw_archive: Dict[str, Any] | None, raw_forecast: Dict[str, Any] | None, city: str) -> pd.DataFrame:
    dfs = []
    if raw_archive:
        dfs.append(_hourly_to_df(raw_archive, city))
    if raw_forecast:
        dfs.append(_hourly_to_df(raw_forecast, city))
    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    out = out.drop_duplicates(subset=["city", "time"]).sort_values("time").reset_index(drop=True)
    return out

def save_parquet(df: pd.DataFrame, out_path: str) -> None:
    Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

def summarize_daily(hourly_df: pd.DataFrame) -> pd.DataFrame:
    if hourly_df.empty:
        return hourly_df
    df = hourly_df.copy()
    df["date"] = df["time"].dt.date
    group_cols = ["city", "date"]
    agg_map = {
        "temperature_2m": ["mean", "min", "max"],
        "relative_humidity_2m": "mean",
        "precipitation": "sum",
        "rain": "sum",
        "snowfall": "sum",
        "windspeed_10m": "mean",
        "cloudcover": "mean",
        "pressure_msl": "mean",
    }
    daily = df.groupby(group_cols).agg(agg_map)
    # flatten columns
    daily.columns = ["_".join([c for c in col if c]) for col in daily.columns.values]
    daily = daily.reset_index()
    return daily
