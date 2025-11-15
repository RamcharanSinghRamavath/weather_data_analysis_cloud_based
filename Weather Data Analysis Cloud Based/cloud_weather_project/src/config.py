import os
from dataclasses import dataclass
from typing import List, Dict, Any
import yaml
from dotenv import load_dotenv

@dataclass
class Location:
    name: str
    latitude: float
    longitude: float
    timezone: str = "UTC"

@dataclass
class Settings:
    data_dir: str
    reports_dir: str
    config_file: str
    default_start_date: str
    default_end_date: str
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_region: str | None
    s3_bucket: str | None
    s3_prefix: str

def load_settings() -> Settings:
    load_dotenv(override=True)
    data_dir = os.getenv("DATA_DIR", "./data")
    reports_dir = os.getenv("REPORTS_DIR", "./reports")
    config_file = os.getenv("CONFIG_FILE", "./config/locations.yaml")
    default_start_date = os.getenv("DEFAULT_START_DATE", "2024-10-01")
    default_end_date = os.getenv("DEFAULT_END_DATE", "2024-10-07")
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION")
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    s3_prefix = os.getenv("S3_PREFIX", "cloud-weather-data/")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)
    return Settings(
        data_dir=data_dir,
        reports_dir=reports_dir,
        config_file=config_file,
        default_start_date=default_start_date,
        default_end_date=default_end_date,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
    )

def load_locations(config_file: str) -> List[Location]:
    with open(config_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    locs = []
    for item in data.get("locations", []):
        locs.append(Location(
            name=item["name"],
            latitude=float(item["latitude"]),
            longitude=float(item["longitude"]),
            timezone=item.get("timezone", "UTC"),
        ))
    return locs
