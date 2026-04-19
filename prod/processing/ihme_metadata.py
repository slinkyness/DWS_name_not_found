"""
ihme_metadata.py — Lambda: extract and persist IHME dimension metadata
=======================================================================
Triggered by the same S3 PUT event as ihme_function.py when a new IHME
CSV lands in the source bucket.

Responsibilities:
  1. Read the raw IHME CSV from S3.
  2. Extract id→name dimension rows (cause, location, measure, …).
  3. Load the existing dimension table from S3 (or start fresh).
  4. Merge new rows in, deduplicating by (dimension, id).
  5. Write the updated table back to S3.

This Lambda runs alongside ihme_function.py (data transform + upsert).
Keeping metadata separate means the process layer stays lean and the
dimension table can be refreshed independently.

Env vars (required): S3_BUCKET, S3_PROCESSED_FOLDER
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import polars as pl

from process_lambda_utils import load_s3_parquet, ok_response, error_response
from ihme_process import (
    extract_metadata,
    merge_metadata,
    empty_metadata,
    METADATA_URI,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Extract source file from S3 event ----------------------------------
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key    = record["object"]["key"]
    log.info("Triggered by s3://%s/%s", bucket, key)

    # -- 2. Read raw CSV -------------------------------------------------------
    try:
        raw = pl.read_csv(
            f"s3://{bucket}/{key}",
            schema_overrides={
                "val":   pl.Float64,
                "upper": pl.Float64,
                "lower": pl.Float64,
            },
        )
        log.info("Read %d rows from source file", len(raw))
    except Exception as exc:
        log.error("Failed to read %s: %s", key, exc)
        return error_response(500, f"CSV read failed: {exc}")

    # -- 3. Extract metadata rows from this file -------------------------------
    try:
        new_meta = extract_metadata(raw)
        log.info("Extracted %d dimension rows from source file", len(new_meta))
    except Exception as exc:
        log.error("extract_metadata failed: %s", exc)
        return error_response(500, f"Metadata extraction failed: {exc}")

    if len(new_meta) == 0:
        log.info("No id/name column pairs found — nothing to update.")
        return ok_response({
            "processed_at": now.isoformat(),
            "source_key":   key,
            "rows_added":   0,
            "output_uri":   METADATA_URI,
        })

    # -- 4. Load existing dimension table -------------------------------------
    try:
        result = load_s3_parquet(METADATA_URI)
        if result is None:
            log.info("No existing dimension table — starting fresh.")
            existing = empty_metadata()
        else:
            existing = result if isinstance(result, pl.DataFrame) else result.collect()
            log.info("Loaded %d existing dimension rows", len(existing))
    except Exception as exc:
        log.warning("Could not load existing metadata (%s) — starting fresh.", exc)
        existing = empty_metadata()

    # -- 5. Merge and save -----------------------------------------------------
    try:
        merged, n_added = merge_metadata(existing, new_meta)
    except Exception as exc:
        log.error("merge_metadata failed: %s", exc)
        return error_response(500, f"Metadata merge failed: {exc}")

    if n_added == 0:
        log.info("No new dimension entries — table unchanged.")
        return ok_response({
            "processed_at": now.isoformat(),
            "source_key":   key,
            "rows_added":   0,
            "total_rows":   len(existing),
            "output_uri":   METADATA_URI,
        })

    try:
        merged.write_parquet(METADATA_URI, compression="zstd", use_pyarrow=False)
        log.info(
            "Dimension table updated — %d new entries, %d total → %s",
            n_added, len(merged), METADATA_URI,
        )
    except Exception as exc:
        log.error("Failed to write dimension table: %s", exc)
        return error_response(500, f"Metadata write failed: {exc}")

    return ok_response({
        "processed_at": now.isoformat(),
        "source_key":   key,
        "rows_added":   n_added,
        "total_rows":   len(merged),
        "output_uri":   METADATA_URI,
    })