"""
lambda_function.py
------------------
AWS Lambda: reads gho-catalogue files from an S3 input bucket,
builds a deduplicated lookup table with the ICD-10, ICD-11 Code(s) and ICD-11 Chapter
writes Parquet to the output bucket via save_to_s3_parquet

Environment variables:
    IN_BUCKET            – source bucket name
    S3_BUCKET            – destination bucket name
    S3_PROCESSED_FOLDER  – S3 key prefix for output, e.g. "processed/icd_mapping"
"""

from __future__ import annotations

import logging
import json
import os
from datetime import datetime, timezone
from typing import Any
from pathlib import Path

import boto3
import polars as pl

from process_lambda_utils import (
    save_to_s3_parquet,
    ok_response,
    error_response,
    find_s3_key,
)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
S3_BUCKET = os.environ["S3_BUCKET"]
OUT_FOLDER = os.environ["S3_PROCESSED_FOLDER"]
SOURCE_FOLDER = os.environ["S3_SOURCE_FOLDER"]

# ── Expected file name stems ────────────────────────────────
_GHO_CATALOGUE = "gho_catalogue"
GHO_META_URI   = f"s3://{S3_BUCKET}/{OUT_FOLDER}/gho_metadata.parquet"

GHO_TO_IHME_CAUSE: dict[str, int] = {
    # ── Direct disease matches ─────────────────────────────────────────────
    "SA_0000001419": 429,   # breast cancer
    "SA_0000001420": 441,   # colon and rectum cancers
    "SA_0000001421": 976,   # diabetes mellitus         → type 2 (dominant burden)
    "SA_0000001422": 698,   # drownings
    "SA_0000001423": 697,   # falls
    "SA_0000001424": 699,   # fires, heat, hot substances
    "SA_0000001425": 493,   # ischaemic heart disease
    "SA_0000001426": 421,   # liver cancer              → due to other causes (aggregate proxy)
    "SA_0000001427": 525,   # liver cirrhosis           → cirrhosis due to other causes (aggregate proxy)
    "SA_0000001429": 444,   # mouth and oropharynx cancer → lip and oral cavity cancer
    "SA_0000001430": 411,   # oesophagus cancer
    "SA_0000001431": 703,   # poisoning                 → poisoning by other means
    "SA_0000001432": 381,   # prematurity and low birth rate → neonatal preterm birth
    "SA_0000001433": 693,   # road traffic accidents    → motor vehicle road injuries
    "SA_0000001434": 721,   # self-inflicted injury     → self-harm by firearm (proxy; see note)
    "SA_0000001435": 716,   # other unintentional injuries
    "SA_0000001436": 945,   # violence                  → conflict and terrorism (best-effort)
    "SA_0000001689": 495,   # cerebrovascular disease   → ischaemic stroke (dominant; see note)
    "SA_0000001418": 560,   # alcohol use disorders
}

GHO_TO_IHME_SEX = {
    "SEX_BTSX": 3,
    "SEX_FMLE": 2,
    "SEX_MLE":  1,
}

def _extract_metadata(in_bucket: str, folder:str) -> pl.DataFrame:
    """
    Build metadate lookup DataFrames

    Returns:
        (lookup_nested, lookup_flat)
    """
    key = find_s3_key(in_bucket, folder, _GHO_CATALOGUE)
    tmp_path = f"/tmp/{Path(key).name}"
    try:
        logger.info("Downloading s3://%s/%s → %s", in_bucket, key, tmp_path)
        s3.download_file(in_bucket, key, tmp_path)
    except Exception as exc:
        logger.error("Failed to download source file: %s", exc)
        return error_response(500, f"Download failed: {exc}")

    raw = pl.read_json(tmp_path).unnest("data")
    metadata = pl.concat([
        raw.select("indicator_catalogue")
        .explode("indicator_catalogue")
        .unnest("indicator_catalogue")
        .with_columns(
            pl.lit("indicator").alias("dimension"),
            pl.col("code").alias("id")
        )
        .select("id", "name", "dimension"),
        raw.select("geo_catalogue")
        .unnest("geo_catalogue")
        .unpivot()
        .unnest("value")
        .with_columns(
            pl.col("type").str.to_lowercase().alias("dimension"),
            pl.col("code").alias("id")
        )
        .select("id", "name", "dimension"),
        pl.DataFrame({
            "id": list(GHO_TO_IHME_CAUSE.keys()),
            "name": list(GHO_TO_IHME_CAUSE.values()),
            "dimension": "gho_ihme_cause"
        }).with_columns(pl.col("name").cast(pl.String)),
        pl.DataFrame({
            "id": list(GHO_TO_IHME_SEX.keys()),
            "name": list(GHO_TO_IHME_SEX.values()),
            "dimension": "gho_ihme_sex"
        }).with_columns(pl.col("name").cast(pl.String)),
    ], how="vertical")
    logger.info(
        "Lookup built: %d mapped codes (%d dimensions)",
        len(metadata), metadata["dimension"].n_unique()
    )
    return metadata

def lambda_handler(event: dict, context: Any) -> dict:
    try:
        s3_bucket = event.get("s3_bucket", os.environ["S3_BUCKET"])
        in_folder = event.get("in_folder", os.environ["S3_SOURCE_FOLDER"])
        out_folder  = event.get("out_folder",  os.environ["S3_PROCESSED_FOLDER"])

        logger.info(
            "S3_BUCKET=%s IN_FOLDER=%s S3_PROCESSED_FOLDER=%s",
            s3_bucket, in_folder, out_folder,
        )

        metadata = _extract_metadata(s3_bucket, in_folder)
    except Exception as exc:
        logger.error("Failed to transform %s: %s", _GHO_CATALOGUE, exc)
        return error_response(500, f"Transform failed: {exc}")

    # -- 2. Write output -------------------------------------------------------
    try:
        metadata.write_parquet(GHO_META_URI, compression="zstd", use_pyarrow=False)
    except Exception as exc:
        logger.error("Failed to write output parquet: %s", exc)
        return error_response(500, f"Output write failed: {exc}")


