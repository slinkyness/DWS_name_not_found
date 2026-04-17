from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import polars as pl

from lambda_utils import load_s3_parquet, upsert_by_date, ok_response, error_response
from gho_process import transform, HEALTH_DATA_URI, UPSERT_KEY, DATE_COL

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
        raw = (
            pl.read_json(f"s3://{bucket}/{key}")
            .unnest("data")
            .select("records")
            .explode("records")
            .unnest("records")
        )
        new_data = transform(raw)
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