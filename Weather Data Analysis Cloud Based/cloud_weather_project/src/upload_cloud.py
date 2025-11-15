from __future__ import annotations
import os
import boto3
from botocore.exceptions import ClientError
from pathlib import Path

def upload_path_to_s3(local_path: str, bucket: str, prefix: str="", region: str|None=None) -> list[str]:
    if not os.path.exists(local_path):
        return []
    s3 = boto3.client("s3", region_name=region) if region else boto3.client("s3")
    uploaded = []
    if os.path.isdir(local_path):
        for root, _, files in os.walk(local_path):
            for fname in files:
                fpath = os.path.join(root, fname)
                rel = os.path.relpath(fpath, start=local_path).replace("\\", "/")
                key = f"{prefix.rstrip('/')}/{rel}".lstrip("/")
                s3.upload_file(fpath, bucket, key)
                uploaded.append(f"s3://{bucket}/{key}")
    else:
        fname = os.path.basename(local_path)
        key = f"{prefix.rstrip('/')}/{fname}".lstrip("/")
        s3.upload_file(local_path, bucket, key)
        uploaded.append(f"s3://{bucket}/{key}")
    return uploaded
