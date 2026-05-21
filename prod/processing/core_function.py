from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any
from pathlib import Path

import boto3
import polars as pl

from core_process import load_and_merge
from process_lambda_utils import ok_response, error_response

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

s3 = boto3.client("s3")

# ── Config ─────────────────────────────────────────────────────────────────────
S3_BUCKET  = os.environ["S3_BUCKET"]
PROCESSED_FOLDER = os.environ["S3_PROCESSED_FOLDER"]

PARQUET_KEY = f"{PROCESSED_FOLDER}/publication_processed.parquet"
PARQUET_URI = f"s3://{S3_BUCKET}/{PARQUET_KEY}"


# ── Lambda entry point ─────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Extract file from S3 event -----------------------------------------
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key    = record["object"]["key"]
    log.info("Triggered by s3://%s/%s", bucket, key)

    # -- 2. Load current master -----------------------------------------------
    tmp_path = f"/tmp/{Path(key).name}"
    try:
        log.info("Downloading s3://%s/%s → %s", bucket, key, tmp_path)
        s3.download_file(bucket, key, tmp_path)
    except Exception as exc:
        log.error("Failed to download source file: %s", exc)
        return error_response(500, f"Download failed: {exc}")

    try:
        merged, stats = load_and_merge(tmp_path, PARQUET_URI, now)
        log.info(
            "  %s → new=%d  updated=%d  source_only=%d  total=%d",
            key, stats["new"], stats["updated"], stats["source_only"], stats["total"],
        )
    except Exception as exc:
        log.error("Failed to process %s: %s", key, exc)
        return error_response(500, f"Processing failed for {key}: {exc}")

    # ── Persist final merged result ───────────────────────────────────────────
    if merged is None:
        return error_response(500, "No data was merged.")

    try:
        merged.write_parquet(PARQUET_URI, compression="zstd", use_pyarrow=False)
    except Exception as exc:
        log.error("Failed to write output parquet: %s", exc)
        return error_response(500, f"Output write failed: {exc}")

    return ok_response({
        "processed_at": now.isoformat(),
        "source_key": key,
        "rows_added": stats["new"],
        "rows_updated": stats["updated"],
        "total_rows": len(merged),
        "output_uri": PARQUET_URI,
    })