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

from process_lambda_utils import (
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
_TO_10_STEM = "11to10maptoone"

# ── Output Names stems ────────────────────────────────
NESTED_10_11 = "icd_10_to_11_nested"
FLAT_10_11 = "icd_10_to_11_flat"
NESTED_11_10 = "icd_11_to_10_nested"

def build_lookup(in_bucket: str, folder: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Build flat and nested lookup DataFrames, both with a `chapter` column.

    Returns:
        (lookup_nested, lookup_flat)
    """
    params_csv = {"schema_overrides":{"icd11Chapter": pl.String}}
    raw_multi = read_s3_csv(in_bucket, folder, _TO_11_MULTI_STEM, sep="\t", **params_csv)
    raw_one   = read_s3_csv(in_bucket, folder, _TO_11_SINGLE_STEM, sep="\t", **params_csv)
    raw_historic = read_s3_csv(in_bucket, folder, _TO_10_STEM, sep="\t", **params_csv)
    old = (
        raw_historic
        .filter(pl.col("icd10Code").is_not_null() & (pl.col("icd10Code") != ""))
        .group_by("icd11Code")
        .agg(
            pl.col("icd10Code").unique().alias("icd10_codes"),
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
        s3_bucket = event.get("s3_bucket", os.environ["S3_BUCKET"])
        in_folder = event.get("in_folder", os.environ["S3_SOURCE_FOLDER"])
        out_folder  = event.get("out_folder",  os.environ["S3_PROCESSED_FOLDER"])

        logger.info(
            "S3_BUCKET=%s  S3_PROCESSED_FOLDER=%s",
            s3_bucket, out_folder,
        )

        nested, flat, old = build_lookup(s3_bucket, in_folder)

        nested.write_parquet(f"s3://{s3_bucket}/{out_folder}/{NESTED_11_10}.parquet", compression="zstd", use_pyarrow=False)
        flat.write_parquet(f"s3://{s3_bucket}/{out_folder}/{FLAT_10_11}.parquet", compression="zstd", use_pyarrow=False)
        old.write_parquet(f"s3://{s3_bucket}/{out_folder}/{NESTED_10_11}.parquet", compression="zstd", use_pyarrow=False)
        return ok_response({
            "rows_nested": len(nested),
            "rows_flat":   len(flat),
            "rows_old": len(old),
            "outputs": [NESTED_11_10, FLAT_10_11, NESTED_10_11],
        })

    except Exception as exc:
        logger.exception("Lambda failed: %s", exc)
        return error_response(500, str(exc))