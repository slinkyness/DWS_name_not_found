"""
lambda_function.py
------------------
AWS Lambda: reads ICD-10→11 mapping files from an S3 input bucket,
builds a deduplicated lookup table with the ICD-10, ICD-11 Code(s) and ICD-11 Chapter
writes Parquet to the output bucket via save_to_s3_parquet

Environment variables:
    IN_BUCKET            – source bucket name
    S3_BUCKET            – destination bucket name
    S3_PROCESSED_FOLDER  – S3 key prefix for output, e.g. "processed/icd_mapping"
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import boto3
import polars as pl

from lambda_utils import (
    save_to_s3_parquet,
    ok_response,
    error_response,
    read_s3_csv,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

# ── Expected file name stems (case-insensitive) ────────────────────────────────
_TO_11_MULTI_STEM = "10to11maptomultiple"
_TO_11_SINGLE_STEM   = "10to11maptoone"
_TO_10_STEM = "11To10maptoone"

def build_lookup(in_bucket: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Build flat and nested lookup DataFrames, both with a `chapter` column.

    Returns:
        (lookup_nested, lookup_flat)
    """
    params_csv = {"schema_overrides":{"icd11Chapter": pl.String}}
    raw_multi = read_s3_csv(in_bucket, "raw", _TO_11_MULTI_STEM, sep="\t", **params_csv)
    raw_one   = read_s3_csv(in_bucket, "raw", _TO_11_SINGLE_STEM, sep="\t", **params_csv)
    raw_historic = read_s3_csv(in_bucket, "raw", _TO_10_STEM, sep="\t", **params_csv)
    old = (
        raw_historic
        .filter(pl.col("icd10Code").is_not_null() & (pl.col("icd10Code") != ""))
        .group_by("icd11Code")
        .agg(
            pl.col("icd10Code").unique().alias("icd11_codes"),
            pl.col("icd11Chapter").unique().alias("icd11_chapter")
        )
    )
    lookup_multi = (
        raw_multi
        .filter(pl.col("icd11Code").is_not_null() & (pl.col("icd11Code") != ""))
        .group_by("icd10Code")
        .agg(
            pl.col("icd11Code").unique().alias("icd11_codes"),
            pl.col("icd11Chapter").unique().alias("icd11_chapter")
        )
    )

    lookup_one = (
        raw_one
        .filter(pl.col("icd11Code").is_not_null() & (pl.col("icd11Code") != ""))
        .filter(~pl.col("icd10Code").is_in(pl.lit(lookup_multi["icd10Code"].implode())))
        .group_by("icd10Code")
        .agg(
            pl.col("icd11Code").unique().alias("icd11_codes"),
            pl.col("icd11Chapter").unique().alias("icd11_chapter")
        )
    )

    nested = pl.concat([lookup_multi, lookup_one])
    flat   = (
        nested
        .explode("icd11_codes")
        .rename({"icd11_codes": "icd11Code"})
    )

    logger.info(
        "Lookup built: %d unique ICD-10 codes (%d multi, %d one-to-one)",
        len(nested), len(lookup_multi), len(lookup_one),
    )
    return nested, flat, old


# ── Lambda entry point ─────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    try:
        in_bucket  = event.get("in_bucket",  os.environ["IN_BUCKET"])
        out_bucket = event.get("out_bucket", os.environ["S3_BUCKET"])
        s3_folder  = event.get("s3_folder",  os.environ["S3_PROCESSED_FOLDER"])

        logger.info(
            "IN_BUCKET=%s  S3_BUCKET=%s  S3_PROCESSED_FOLDER=%s",
            in_bucket, out_bucket, s3_folder,
        )

        now = datetime.now(timezone.utc)
        nested, flat, old = build_lookup(in_bucket)

        uri_nested = save_to_s3_parquet(nested, out_bucket, s3_folder, "icd10_to_icd11_lookup_nested", now)
        uri_flat   = save_to_s3_parquet(flat,   out_bucket, s3_folder, "icd10_to_icd11_lookup_flat",   now)
        uri_old    = save_to_s3_parquet(flat,   out_bucket, s3_folder, "icd11_to_icd10_lookup_nested",   now)
        return ok_response({
            "rows_nested": len(nested),
            "rows_flat":   len(flat),
            "outputs": [uri_nested, uri_flat, uri_old],
        })

    except Exception as exc:
        logger.exception("Lambda failed: %s", exc)
        return error_response(500, str(exc))