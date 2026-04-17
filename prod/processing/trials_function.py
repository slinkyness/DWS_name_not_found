"""
ctis_lambda.py — AWS Lambda entry point: CTIS Clinical Trials CSV transformer
==============================================================================
Triggered by an S3 PUT event when a new CTIS CSV lands in the source bucket.
Delegates all transform logic to ctis_process.transform(), then upserts into
the master parquet via lambda_utils.upsert_by_date().

Upsert key : trial_id
Date col   : last_updated  (an existing row is replaced only when the
             incoming row carries a strictly newer last_updated date)

Env vars (required):
    MAPPING_BUCKET      – bucket that holds the ICD lookup parquet
    MAPPING_KEY         – S3 key of icd10_to_icd11_lookup_flat.parquet
    S3_BUCKET           – destination bucket
    S3_PROCESSED_FOLDER – key prefix for output, e.g. "processed/ctis"
"""

from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import polars as pl

from lambda_utils import load_s3_parquet, upsert_by_date, ok_response, error_response
from trials_process import CTIS_DATA_URI, UPSERT_KEY, DATE_COL, transform

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

s3 = boto3.client("s3")

MAPPING_BUCKET = os.environ["MAPPING_BUCKET"]
MAPPING_KEY    = os.environ["MAPPING_KEY"]


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Extract file from S3 event ----------------------------------------
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key    = record["object"]["key"]
    log.info("Triggered by s3://%s/%s", bucket, key)

    # -- 2. Load ICD lookup ----------------------------------------------------
    try:
        log.info("Loading ICD lookup from s3://%s/%s", MAPPING_BUCKET, MAPPING_KEY)
        lookup_obj = s3.get_object(Bucket=MAPPING_BUCKET, Key=MAPPING_KEY)
        lookup     = pl.read_parquet(io.BytesIO(lookup_obj["Body"].read()))
    except Exception as exc:
        log.error("Failed to load ICD lookup: %s", exc)
        return error_response(500, f"ICD lookup load failed: {exc}")

    # -- 3. Read & transform ---------------------------------------------------
    try:
        log.info("Reading input CSV from s3://%s/%s", bucket, key)
        csv_bytes = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        new_data  = transform(csv_bytes, lookup)
        log.info("Transformed %d rows from source file", len(new_data))
    except Exception as exc:
        log.error("Failed to read/transform %s: %s", key, exc)
        return error_response(500, f"Transform failed: {exc}")

    # -- 4. Load existing data as LazyFrame (defers I/O until upsert needs it) -
    existing_lf: pl.LazyFrame | None = load_s3_parquet(CTIS_DATA_URI, lazy=True)

    # -- 5. Upsert -------------------------------------------------------------
    try:
        merged, n_new, n_updated, n_src = upsert_by_date(
            existing=existing_lf,
            incoming=new_data,
            upsert_key=UPSERT_KEY,
            date_col=DATE_COL,
        )
    except Exception as exc:
        log.error("Upsert failed: %s", exc)
        return error_response(500, f"Upsert failed: {exc}")

    log.info(
        "Upsert complete — %d new, %d updated, %d source-only, %d total rows",
        n_new, n_updated, n_src, len(merged),
    )

    # -- 6. Write output -------------------------------------------------------
    try:
        merged.write_parquet(CTIS_DATA_URI, compression="zstd", use_pyarrow=False)
    except Exception as exc:
        log.error("Failed to write output parquet: %s", exc)
        return error_response(500, f"Output write failed: {exc}")

    # -- 7. Summary ------------------------------------------------------------
    return ok_response({
        "processed_at":      now.isoformat(),
        "source_key":        key,
        "rows_new":          n_new,
        "rows_updated":      n_updated,
        "rows_source_only":  n_src,
        "total_rows":        len(merged),
        "output_uri":        CTIS_DATA_URI,
    })