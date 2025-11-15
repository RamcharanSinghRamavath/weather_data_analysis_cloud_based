from __future__ import annotations
import os
import sys
import argparse
import pandas as pd

# --- Make 'src' imports work no matter how you run this script ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# ----------------------------------------------------------------

from src.config import load_settings, load_locations
from src.process_weather import combine_hourly, save_parquet, summarize_daily
from src.analyze_weather import save_csv, plot_timeseries
from src.upload_cloud import upload_path_to_s3

# Dask for parallel city-level work
from dask import delayed, compute
from multiprocessing.pool import ThreadPool
import dask


def _worker_fetch_city(loc, start: str, end: str, raw_dir: str):
    """
    Per-city worker: fetch archive+forecast for a location, save raw JSON,
    combine hourly frames, return a pandas DataFrame (or None if empty).
    Imported modules are inside to keep the worker picklable.
    """
    from src.fetch_weather import fetch_for_location, fetch_forecast, save_raw_json

    archive = fetch_for_location(
        lat=loc.latitude,
        lon=loc.longitude,
        start_date=start,
        end_date=end,
        timezone=loc.timezone,
    )
    forecast = fetch_forecast(
        lat=loc.latitude,
        lon=loc.longitude,
        timezone=loc.timezone,
    )

    save_raw_json(archive, raw_dir, loc.name, "archive", start, end)
    save_raw_json(forecast, raw_dir, loc.name, "forecast", start, end)

    hourly = combine_hourly(archive, forecast, loc.name)
    if hourly is None or hourly.empty:
        return None
    return hourly


def run_pipeline(start: str | None, end: str | None, upload_s3: bool = False) -> None:
    """Run the full Cloud-based Weather Data pipeline (with Dask parallelism, throttled)."""
    settings = load_settings()
    locations = load_locations(settings.config_file)

    start = start or settings.default_start_date
    end = end or settings.default_end_date

    raw_dir = os.path.join(settings.data_dir, "raw")
    processed_dir = os.path.join(settings.data_dir, "processed")
    reports_dir = settings.reports_dir

    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)

    # -------- Parallel per-city fetch/process with Dask (throttled) --------
    tasks = []
    for loc in locations:
        print(f"[Dask task] {loc.name}: {start} \u2192 {end}")
        tasks.append(delayed(_worker_fetch_city)(loc, start, end, raw_dir))

    # Limit concurrency to avoid API rate limits (HTTP 429)
    pool = ThreadPool(processes=4)  # adjust to 3–4 if you still see 429
    with dask.config.set(pool=pool):
        results = compute(*tasks, scheduler="threads")
    all_hourly = [df for df in results if df is not None]
    # -----------------------------------------------------------------------

    if not all_hourly:
        print("No data fetched. Exiting.")
        sys.exit(1)

    hourly_df = (
        pd.concat(all_hourly, ignore_index=True)
        .sort_values(["city", "time"])
        .reset_index(drop=True)
    )
    hourly_path = os.path.join(processed_dir, "hourly.parquet")
    save_parquet(hourly_df, hourly_path)
    print(f"✅ Saved hourly parquet to {hourly_path}")

    daily_df = summarize_daily(hourly_df)
    daily_path = os.path.join(processed_dir, "daily_summary.parquet")
    save_parquet(daily_df, daily_path)
    print(f"✅ Saved daily summary parquet to {daily_path}")

    # Also save CSVs
    hourly_csv = os.path.join(processed_dir, "hourly.csv")
    daily_csv = os.path.join(processed_dir, "daily_summary.csv")
    save_csv(hourly_df, hourly_csv)
    save_csv(daily_df, daily_csv)
    print("✅ Saved CSVs.")

    # Plots per city
    plots_dir = os.path.join(reports_dir, "plots")
    os.makedirs(plots_dir, exist_ok=True)
    for city in hourly_df["city"].unique():
        plot_paths = plot_timeseries(hourly_df, city, plots_dir)
        for p in plot_paths:
            print(f"📈 Wrote plot: {p}")

    # Optional S3 upload
    if upload_s3:
        s = settings
        if not (s.aws_access_key_id and s.aws_secret_access_key and s.s3_bucket):
            print("⚠️ S3 upload requested but AWS credentials or bucket missing. Skipping upload.")
        else:
            print("☁️ Uploading data/ and reports/ to S3...")
            uploaded1 = upload_path_to_s3(s.data_dir, s.s3_bucket, s.s3_prefix, s.aws_region)
            uploaded2 = upload_path_to_s3(s.reports_dir, s.s3_bucket, s.s3_prefix + "reports/", s.aws_region)
            print(f"✅ Uploaded {len(uploaded1) + len(uploaded2)} files to S3.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cloud-based Weather Data Analysis pipeline (Dask parallel)")
    parser.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD) for archive")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD) for archive")
    parser.add_argument("--upload-s3", action="store_true", help="Upload outputs to S3 after run")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(start=args.start, end=args.end, upload_s3=args.upload_s3)
