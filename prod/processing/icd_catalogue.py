
import logging
import os
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3
import polars as pl

from process_lambda_utils import ok_response, error_response

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)
s3 = boto3.client("s3")

# ---- Lambda Config ----
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_FOLDER = os.environ.get("S3_PROCESSED_FOLDER", "")
PARQUET_URI = f"s3://{S3_BUCKET}/{S3_FOLDER}/icd_11_catalogue.parquet"

NEEDED_COLS = [
    "icd_code", "icd_uri", "browser_url", "class_kind", "title",
    "definition", "long_definition", "fully_specified_name",
    "inclusion", "exclusion", "index_terms", "parent_uris",
    "child_uris", "foundation_uri",
]
CHAPTER_NAMING = {
    "certain_infectious": "1",
    "neoplasms": "2",
    "blood": "3",
    "endocrine": "4",
    "mental": "5",
    "nervous": "6",
    "eye": "7",
    "ear": "8",
    "circulatory": "9",
    "respiratory": "a",
    "digestive": "b",
    "skin": "c",
    "musculoskeletal": "d",
    "genitourinary": "e",
    "pregnancy": "f",
    "perinatal": "g",
    "congenital": "h",
    "symptoms": "j",
    "injury": "k",
    "external": "l",
    "factors_health": "m",
    "purposes": "n",
    "supplementary_factors": "p",
    "complementary_factors": "q",
    "supplementary_conditions": "r",
    "supplementary_populations": "s",
    "supplementary_contexts": "t",
    "supplementary_settings": "u",
}
LETTER_TO_CHAPTER = {v: k for k, v in CHAPTER_NAMING.items()}
ENTITY_ID_RE = r"/mms/(\d+)"
_NBSP = "\u00a0"

def transform(df: pl.DataFrame) -> pl.DataFrame:
    id_lut = (
        df.select(
            pl.col("icd_uri").str.extract(ENTITY_ID_RE).alias("entity_id"),
            pl.col("icd_code"),
        )
        .filter(pl.col("entity_id").is_not_null())
        .group_by("entity_id")
        .agg(pl.col("icd_code"))
        .with_columns(pl.col("icd_code").list.join("&"))
    )
    excl_resolved = (
        df.select("_row", "exclusion")
        .explode("exclusion")
        .filter(pl.col("exclusion").is_not_null())
        .unnest("exclusion")
        .with_columns(pl.col("linearization_uri").str.extract(ENTITY_ID_RE).alias("entity_id"))
        .join(id_lut, on="entity_id", how="left")
        .group_by("_row", maintain_order=True)
        .agg(
            pl.col("label").alias("exclusion_labels"),
            pl.col("icd_code").alias("exclusion_codes"),
        )
    )
    out_cols = (
            [c for c in NEEDED_COLS if c not in ("exclusion", "parent_uris", "child_uris")]
            + ["exclusion_labels", "exclusion_codes", "parent_codes", "child_codes", "chapter"]
    )
    return (
        df.drop("exclusion")
        .join(excl_resolved, on="_row", how="left")
        .with_columns(
            pl.col("title", "definition", "long_definition", "fully_specified_name")
            .str.replace_all(_NBSP, " "),
            pl.col("exclusion_labels", "index_terms")
            .fill_null([])
            .list.eval(pl.element().str.replace_all(_NBSP, " ")),
            pl.col("exclusion_codes").fill_null([]),
            pl.col("parent_uris", "child_uris")
            .list.eval(
                pl.element()
                .str.extract(ENTITY_ID_RE)
                .replace(
                    pl.Series(id_lut["entity_id"]),
                    pl.Series(id_lut["icd_code"]),
                )
                .str.split("&")
                .explode()
            )
            .name.map(lambda n: n.replace("_uris", "_codes")),
            pl.col("icd_code").str.slice(0, 1).str.to_lowercase()
            .replace(LETTER_TO_CHAPTER)
            .alias("chapter"),
        )
        .drop("_row")
        .select(out_cols)
    )

def lambda_handler(event: dict, context) -> dict:
    now = datetime.now(timezone.utc)
    # -- 1. Extract file from S3 event ----------------------------------------
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key    = record["object"]["key"]
    log.info("Triggered by s3://%s/%s", bucket, key)
    # -- 2. Read & transform ---------------------------------------------------
    tmp_path = f"/tmp/{Path(key).name}"
    try:
        log.info("Downloading s3://%s/%s → %s", bucket, key, tmp_path)
        s3.download_file(bucket, key, tmp_path)
    except Exception as exc:
        log.error("Failed to download source file: %s", exc)
        return error_response(500, f"Download failed: {exc}")

    try:
        clean = []
        with open(tmp_path, encoding="utf-8") as f:
            for i, line in enumerate(f):
                try:
                    json.loads(line)
                    clean.append(line)
                except json.JSONDecodeError as e:
                    log.error("Skipping malformed line %d: %s", i + 1, e)
        raw = (
             pl.read_ndjson("".join(clean).encode())
             .filter(~pl.all_horizontal(pl.all().is_null()))
             .select(NEEDED_COLS)
             .with_row_index("_row")
        )
        data = transform(raw)
        log.info("Transformed %d rows from source file", len(data))
    except Exception as exc:
        log.error("Failed to transform %s: %s", key, exc)
        return error_response(500, f"Transform failed: {exc}")
    try:
        data.write_parquet(PARQUET_URI, compression="zstd", use_pyarrow=False)
    except Exception as exc:
        log.error("Failed to write output parquet: %s", exc)
        return error_response(500, f"Output write failed: {exc}")
    return ok_response({
        "processed_at": now.isoformat(),
        "source_key": key,
        "output_uri": PARQUET_URI,
    })