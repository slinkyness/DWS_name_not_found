"""
trials_function.py — AWS Lambda: Clinical Trials transformer (CTIS + CTUS)
===========================================================================
Triggered by an S3 PUT event when a new CTIS CSV (EU) or CTUS JSON
(ClinicalTrials.gov) lands in the source bucket.

Workflow:
  1. Extract source bucket + key from S3 event.
  2. Detect source type from file extension (.csv → ctis, .json → ctus).
  3. Stream file from S3 to /tmp/ (Lambda writable scratch).
  4. Call trials_process.transform(tmp_path, source) for all heavy logic.
  5. Upsert into the master Parquet via process_lambda_utils.upsert_by_date.
  6. Write back to S3 and return a summary response.

ICD lookup parquets (icd_lookup_fresh.parquet, 11_10_lookup_new.parquet,
icd_11.parquet) are loaded by trials_process at import time from:
  s3://{S3_BUCKET}/{MAPPING_PREFIX}/   (if S3_BUCKET + MAPPING_PREFIX are set)
  or local DATA_DIR                    (for local testing)

Upsert key : trial_id
Date col   : last_updated

Env vars (required):
  S3_BUCKET            – destination + lookup bucket
  S3_PROCESSED_FOLDER  – key prefix for output parquet, e.g. "processed"
  MAPPING_PREFIX       – key prefix for ICD lookup parquets (default = S3_PROCESSED_FOLDER)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3

from process_lambda_utils import (
    load_s3_parquet,
    upsert_by_date,
    ok_response,
    error_response,
)
from trials_process import TRIALS_DATA_URI, UPSERT_KEY, DATE_COL, transform

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

S3_BUCKET  = os.environ["S3_BUCKET"]
S3_FOLDER  = os.environ["S3_PROCESSED_FOLDER"]

s3 = boto3.client("s3")


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Extract S3 event metadata ------------------------------------------
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key    = record["object"]["key"]
    log.info("Triggered by s3://%s/%s", bucket, key)

    # -- 2. Detect source type -------------------------------------------------
    try:
        source = "ctis" if key.endswith(".csv") else "ctus"
        log.info("Detected source: %s", source)
    except ValueError as exc:
        log.error(str(exc))
        return error_response(400, str(exc))

    # -- 3. Stream file to /tmp/ -----------------------------------------------
    tmp_path = f"/tmp/{Path(key).name}"
    try:
        log.info("Downloading s3://%s/%s → %s", bucket, key, tmp_path)
        s3.download_file(bucket, key, tmp_path)
    except Exception as exc:
        log.error("Failed to download source file: %s", exc)
        return error_response(500, f"Download failed: {exc}")

    # -- 4. Transform ----------------------------------------------------------
    try:
        new_data = transform(tmp_path, source)
        log.info("Transformed %d rows from %s", len(new_data), key)
    except Exception as exc:
        log.error("Transform failed for %s: %s", key, exc)
        return error_response(500, f"Transform failed: {exc}")

    # -- 5. Load existing master (lazy — defers I/O until upsert needs it) -----
    existing = load_s3_parquet(TRIALS_DATA_URI, lazy=True)

    # -- 6. Upsert -------------------------------------------------------------
    try:
        merged, n_new, n_updated, n_src = upsert_by_date(
            existing   = existing,
            incoming   = new_data,
            upsert_key = UPSERT_KEY,
            date_col   = DATE_COL,
        )
    except Exception as exc:
        log.error("Upsert failed: %s", exc)
        return error_response(500, f"Upsert failed: {exc}")

    log.info(
        "Upsert complete — %d new, %d updated, %d source-only, %d total rows",
        n_new, n_updated, n_src, len(merged),
    )

    # -- 7. Write output -------------------------------------------------------
    try:
        merged.write_parquet(TRIALS_DATA_URI, compression="zstd", use_pyarrow=False)
    except Exception as exc:
        log.error("Failed to write output parquet: %s", exc)
        return error_response(500, f"Output write failed: {exc}")

    # -- 8. Summary ------------------------------------------------------------
    return ok_response({
        "processed_at":     now.isoformat(),
        "source_key":       key,
        "source_type":      source,
        "rows_new":         n_new,
        "rows_updated":     n_updated,
        "rows_source_only": n_src,
        "total_rows":       len(merged),
        "output_uri":       TRIALS_DATA_URI,
    })
