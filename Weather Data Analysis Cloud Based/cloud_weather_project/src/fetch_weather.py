# src/fetch_weather.py
from __future__ import annotations
import os
import json
import time
import random
from typing import Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "cloudcover",
    "pressure_msl",
    "windspeed_10m",
    "winddirection_10m",
]

def _build_session() -> requests.Session:
    """Requests session with retry/backoff for 429/5xx."""
    session = requests.Session()
    retry = Retry(
        total=6,
        connect=3,
        read=3,
        status=6,
        backoff_factor=1.2,  # 1.2s, 2.4s, 4.8s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

_SESSION = _build_session()

def _get_json(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """GET JSON with retries + small jitter to avoid thundering herd."""
    for attempt in range(1, 8):
        resp = _SESSION.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            # Too many requests: backoff with jitter
            sleep_s = min(8, 0.8 * attempt) + random.uniform(0, 0.6)
            time.sleep(sleep_s)
            continue
        try:
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError:
            if attempt >= 7:
                raise
            time.sleep(min(6, 0.6 * attempt) + random.uniform(0, 0.4))
    resp.raise_for_status()
    return resp.json()

def fetch_for_location(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    timezone: str = "UTC",
) -> Dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": timezone,
    }
    return _get_json(OPEN_METEO_ARCHIVE, params)

def fetch_forecast(
    lat: float,
    lon: float,
    timezone: str = "UTC",
) -> Dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": timezone,
    }
    return _get_json(OPEN_METEO_FORECAST, params)

def save_raw_json(payload: Dict[str, Any], raw_dir: str, city: str,
                  kind: str, start: Optional[str], end: Optional[str]) -> str:
    os.makedirs(raw_dir, exist_ok=True)
    safe_city = city.lower().replace(" ", "_")
    if start and end:
        name = f"{safe_city}__{kind}__{start}__{end}.json"
    else:
        name = f"{safe_city}__{kind}.json"
    path = os.path.join(raw_dir, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return path
