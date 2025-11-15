# app/streamlit_app.py
import os
import datetime as dt
import pandas as pd
import streamlit as st
import plotly.express as px

DATA_DIR = os.getenv("DATA_DIR", "./data")
PROC_DIR = os.path.join(DATA_DIR, "processed")
HOURLY_PATH = os.path.join(PROC_DIR, "hourly.parquet")
DAILY_PATH  = os.path.join(PROC_DIR, "daily_summary.parquet")

st.set_page_config(page_title="Cloud Weather Analysis", layout="wide")
st.title("☁️ Cloud-based Weather Data Analysis")
st.markdown("Data from **Open-Meteo** (archive + forecast). Use the pipeline to refresh data.")

# ---------------- Helpers ----------------
def to_utc(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tz is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")

@st.cache_data
def load_data():
    hourly = pd.read_parquet(HOURLY_PATH) if os.path.exists(HOURLY_PATH) else pd.DataFrame()
    daily  = pd.read_parquet(DAILY_PATH)  if os.path.exists(DAILY_PATH)  else pd.DataFrame()
    if not hourly.empty:
        # New, non-deprecated dtype check
        if not isinstance(hourly["time"].dtype, pd.DatetimeTZDtype):
            hourly["time"] = pd.to_datetime(hourly["time"], utc=True, errors="coerce")
        hourly["year"]  = hourly["time"].dt.year
        hourly["month"] = hourly["time"].dt.month
        hourly["day"]   = hourly["time"].dt.day
        hourly["hour"]  = hourly["time"].dt.hour
    return hourly, daily

hourly, daily = load_data()
if hourly.empty:
    st.warning("No data found. Run the pipeline first: `python scripts/run_pipeline.py --start YYYY-MM-DD --end YYYY-MM-DD`")
    st.stop()

# ---------------- Sidebar controls ----------------
with st.sidebar:
    st.header("Filters")

    cities = sorted(hourly["city"].unique().tolist())
    city = st.selectbox("City", cities, index=0)

    # Use timezone-aware 'now' in UTC (no deprecation)
    current_year = dt.datetime.now(dt.UTC).year

    data_min_year = int(hourly["year"].min())
    data_max_year = int(hourly["year"].max())
    min_year_ui = min(2000, data_min_year) if data_min_year <= 2000 else data_min_year
    max_year_ui = min(current_year, data_max_year)

    year_range = st.select_slider(
        "Year range",
        options=list(range(min_year_ui, max_year_ui + 1)),
        value=(max(min_year_ui, data_min_year), max_year_ui),
        help="Pick start and end year to filter.",
    )

    now_utc = dt.datetime.now(dt.UTC)
    this_month = now_utc.month if year_range[1] >= current_year else 12
    st.caption(f"Showing up to month **{this_month:02d}** for year **{year_range[1]}** if selected.")

    hour_range = st.select_slider(
        "Hour of day (UTC)",
        options=list(range(0, 24)),
        value=(0, 23),
        help="Filter by hour of day in UTC.",
    )

    min_time = hourly["time"].min()
    max_time = hourly["time"].max()

    start_date_bound = max(
        to_utc(dt.datetime(year_range[0], 1, 1)),
        min_time
    )
    try:
        if year_range[1] == current_year:
            end_cap = to_utc(pd.Timestamp(year_range[1], this_month, 1) + pd.offsets.MonthEnd(0))
        else:
            end_cap = to_utc(pd.Timestamp(year_range[1], 12, 31))
    except Exception:
        end_cap = max_time
    end_date_bound = min(end_cap, max_time)

    st.caption("Optional: refine by exact dates within the year range.")
    date_rng = st.slider(
        "Date range (UTC)",
        min_value=start_date_bound.to_pydatetime().replace(tzinfo=None),
        max_value=end_date_bound.to_pydatetime().replace(tzinfo=None),
        value=(
            start_date_bound.to_pydatetime().replace(tzinfo=None),
            end_date_bound.to_pydatetime().replace(tzinfo=None),
        ),
        key="date_slider",
    )

# ---------------- Filtering ----------------
start_ts = to_utc(date_rng[0])
end_ts   = to_utc(date_rng[1])

h = hourly[
    (hourly["city"] == city)
    & (hourly["time"] >= start_ts)
    & (hourly["time"] <= end_ts)
    & (hourly["year"] >= year_range[0])
    & (hourly["year"] <= year_range[1])
    & (hourly["hour"] >= hour_range[0])
    & (hourly["hour"] <= hour_range[1])
].copy()

# ---------------- Plots ----------------
st.subheader(f"Hourly Metrics — {city}")
if h.empty:
    st.info("No data for the selected filters.")
else:
    plots = [
        ("temperature_2m", "Temperature (°C)"),
        ("relative_humidity_2m", "Relative Humidity (%)"),
        ("precipitation", "Precipitation (mm)"),
        ("windspeed_10m", "Wind Speed (m/s)"),
    ]
    for col, title in plots:
        if col in h.columns:
            fig = px.line(h, x="time", y=col, title=title)
            st.plotly_chart(fig, use_container_width=True)

# ---------------- Daily Summary table (tz-safe) ----------------
st.subheader(f"Daily Summary — {city}")
if not daily.empty:
    d = daily[daily["city"] == city].copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.date  # naive dates
    start_date = start_ts.to_pydatetime().date()
    end_date   = end_ts.to_pydatetime().date()
    # Filter by naive dates and year range
    d = d[(d["date"] >= start_date) & (d["date"] <= end_date)]
    d = d[d["date"].apply(lambda x: year_range[0] <= x.year <= year_range[1])]
    st.dataframe(d, use_container_width=True)
else:
    st.info("No daily summary found.")
