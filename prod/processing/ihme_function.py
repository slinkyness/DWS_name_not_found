"""
ihme_lambda.py — AWS Lambda entry point: IHME GBD CSV transformer
==================================================================
Triggered by an S3 PUT event when a new IHME CSV lands in the source bucket.
Delegates all transform logic to ihme_process.transform(), then upserts
into the master parquet via lambda_utils.upsert_by_date().

Upsert key : row_key   (pipe-delimited composite of all dimension columns)
Date col   : ingested_at (UTC timestamp stamped at transform time)

Because IHME rows have no natural "updated" date, ingested_at acts as the
freshness signal: a re-delivery of the same row from a newer file will win
only if it arrives in a later invocation.

Env vars (required): S3_BUCKET, S3_PROCESSED_FOLDER
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import polars as pl

from process_lambda_utils import load_s3_parquet, upsert_by_date, ok_response, error_response
from ihme_process import transform, HEALTH_DATA_URI, UPSERT_KEY, DATE_COL

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Extract file from S3 event ----------------------------------------
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key    = record["object"]["key"]
    log.info("Triggered by s3://%s/%s", bucket, key)

    # -- 2. Read & transform ---------------------------------------------------
    try:
        raw = pl.read_csv(
            f"s3://{bucket}/{key}",
            schema_overrides={
                "val":   pl.Float64,
                "upper": pl.Float64,
                "lower": pl.Float64,
            },
        )
        new_data = transform(raw, now)
        log.info("Transformed %d rows from source file", len(new_data))
    except Exception as exc:
        log.error("Failed to read/transform %s: %s", key, exc)
        return error_response(500, f"Transform failed: {exc}")

    # -- 3. Load existing data as LazyFrame (defers I/O until upsert needs it) -
    existing_lf: pl.LazyFrame | None = load_s3_parquet(HEALTH_DATA_URI, lazy=True)

    # -- 4. Upsert -------------------------------------------------------------
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
        "Upsert complete — %d new, %d updated, %d total rows",
        n_new, n_updated, len(merged),
    )

    # -- 5. Write output -------------------------------------------------------
    try:
        merged.write_parquet(HEALTH_DATA_URI, compression="zstd", use_pyarrow=False)
    except Exception as exc:
        log.error("Failed to write output parquet: %s", exc)
        return error_response(500, f"Output write failed: {exc}")

    # -- 6. Summary ------------------------------------------------------------
    return ok_response({
        "processed_at":  now.isoformat(),
        "source_key":    key,
        "rows_new":      n_new,
        "rows_updated":  n_updated,
        "total_rows":    len(merged),
        "output_uri":    HEALTH_DATA_URI,
    })



