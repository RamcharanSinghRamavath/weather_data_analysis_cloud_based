# Cloud-based Weather Data Analysis (End-to-End)

This is a complete, copy-paste friendly project you can open in VS Code and run end to end.  
It fetches weather data from the free **Open-Meteo** API (no API key required), processes and analyzes it,
optionally uploads outputs to **AWS S3**, and provides an interactive **Streamlit** dashboard.

---

## 🚀 Quick Start (Local)

1) **Unzip** this project and open the folder in **VS Code**.
2) Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   # Windows:
   .venv\Scripts\activate
   # Mac/Linux:
   source .venv/bin/activate
   ```
3) Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4) Copy env template and adjust if needed:
   ```bash
   cp .env.example .env
   ```
   - Edit dates/paths in `.env` if desired.
5) Run the **pipeline** (fetch → process → analyze):
   ```bash
   python scripts/run_pipeline.py --start 2024-10-01 --end 2024-10-07
   ```
   Outputs are saved to `data/` and `reports/`.
6) Launch the **dashboard**:
   ```bash
   streamlit run app/streamlit_app.py
   ```

---

## ☁️ Optional: Upload to AWS S3

1) In `.env`, set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `S3_BUCKET_NAME`, and `S3_PREFIX` (optional).  
2) Run:
   ```bash
   python scripts/run_pipeline.py --start 2024-10-01 --end 2024-10-07 --upload-s3
   ```

---

## 📂 Project Structure

```
cloud_weather_project/
├─ app/
│  └─ streamlit_app.py        # Interactive dashboard
├─ config/
│  └─ locations.yaml          # Cities (lat/lon) you want to fetch
├─ scripts/
│  └─ run_pipeline.py         # Orchestrates whole pipeline
├─ src/
│  ├─ __init__.py
│  ├─ config.py               # Loads env + YAML config
│  ├─ fetch_weather.py        # Calls Open-Meteo API
│  ├─ process_weather.py      # Cleans + transforms data
│  ├─ analyze_weather.py      # Analytics + plots
│  └─ upload_cloud.py         # Optional: upload to S3
├─ .env.example
├─ requirements.txt
├─ README.md
└─ .gitignore
```

---

## 🧠 What It Does (Step-by-Step)

1. **Configuration**: Reads `.env` and `config/locations.yaml` to know where to fetch, where to save, and date range.
2. **Fetch**: Uses Open-Meteo **Archive** and **Forecast** endpoints to get hourly temperature, humidity, wind, precipitation, etc.
3. **Process**: Converts raw JSON to tidy tables, standardizes timestamps (UTC), merges by city, and saves as efficient **Parquet**.
4. **Analyze**: Computes daily summaries, trends and creates plots (saved to `reports/`). Exposes clean CSVs + Parquet for further use.
5. **(Optional) Upload**: Pushes produced files to **S3** (bucket/prefix from `.env`).
6. **Visualize**: **Streamlit** app lets you pick a city/date range and interact with plots and stats.

---

## ⚙️ Customize

- Edit `config/locations.yaml` to add/remove cities (must include `name`, `latitude`, `longitude`).
- Change default date range via CLI args or `.env` (`DEFAULT_START_DATE`, `DEFAULT_END_DATE`).
- Add more metrics from [Open-Meteo docs] and extend fields in `src/fetch_weather.py`.

---

## 🧪 Sanity Check

Run a short sample after install:
```bash
python scripts/run_pipeline.py --start 2024-10-01 --end 2024-10-03
```

Then check:
- `data/raw/` has raw JSON snapshots
- `data/processed/hourly.parquet`
- `data/processed/daily_summary.parquet`
- `reports/` has PNG charts + CSVs

---

## 🗓 Scheduling (optional)

Use OS scheduler:
- **Windows Task Scheduler** → run `python scripts/run_pipeline.py --start YYYY-MM-DD --end YYYY-MM-DD --upload-s3`
- **cron (Linux/Mac)** → e.g., every morning at 7 AM:
  ```cron
  0 7 * * * /path/to/venv/bin/python /path/to/cloud_weather_project/scripts/run_pipeline.py --start $(date -d 'yesterday' +\%F) --end $(date +\%F) --upload-s3
  ```

---

## ✅ No API Keys Required

Data comes from **Open-Meteo** free endpoints. Internet connectivity is required to fetch data.

Enjoy building! 🚀
