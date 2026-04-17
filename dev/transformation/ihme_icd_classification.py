"""
ihme_icd_matcher.py — Match IHME cause/etiology names to ICD-11 via WHO API
=============================================================================
Reads cause + etiology dimensions from ihme_dimension_map.parquet,
invokes the icd_function Lambda once per name, saves results to
ihme_cause_icd_map.parquet.

Env vars (required): OUT_BUCKET, S3_PROCESSED_FOLDER, ICD_FUNCTION_NAME
"""

from __future__ import annotations

import json
import logging
import os

import boto3
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

OUT_BUCKET          = os.environ["OUT_BUCKET"]
S3_FOLDER           = os.environ["S3_PROCESSED_FOLDER"]
ICD_FUNCTION_NAME   = os.environ["ICD_FUNCTION_NAME"]

METADATA_URI        = f"s3://{OUT_BUCKET}/{S3_FOLDER}/ihme_dimension_map.parquet"
OUTPUT_URI          = f"s3://{OUT_BUCKET}/{S3_FOLDER}/ihme_cause_icd_map.parquet"

def _invoke_icd_lambda(client, name: str) -> dict | None:
    """Invoke icd_function synchronously for a single query term."""
    response = client.invoke(
        FunctionName   = ICD_FUNCTION_NAME,
        InvocationType = "RequestResponse",
        Payload        = json.dumps({"action": "query_details", "query": name}),
    )
    body = json.loads(response["Payload"].read())
    if body.get("statusCode", 200) != 200:
        log.warning("ICD Lambda returned error for '%s': %s", name, body)
        return None
    return body


def _parse_result(dimension: str, dim_id: int, name: str, body: dict | None) -> dict:
    """Flatten the top ICD-11 match from the Lambda response into a record."""
    if body is None or not body.get("data"):
        return {
            "dimension": dimension, "id": dim_id, "cause_name": name,
            "icd_code": None, "icd_title": None, "chapter": None,
            "score": None, "matched": False,
        }

    # icd_fetch.get_query_details returns a list of matches ranked by score
    top = body["data"][0]
    return {
        "dimension":  dimension,
        "id":         dim_id,
        "cause_name": name,
        "icd_code":   top.get("icd_code"),
        "icd_title":  top.get("title"),
        "chapter":    top.get("chapter"),
        "score":      top.get("score"),
        "matched":    True,
    }


def run_matching() -> pl.DataFrame:
    terms  = (
        pl.read_parquet(METADATA_URI)
        .filter(pl.col("dimension").is_in(["cause", "etiology"]))
        .select("dimension", "id", "name")
        .sort(["dimension", "id"])
    )
    try:
        existing = pl.read_parquet(OUTPUT_URI)
    except Exception:
        log.info("No existing cause→ICD map — starting fresh")
        existing = None

    if existing is not None:
        already_mapped = existing.filter(pl.col("matched")).select("dimension", "id")
        terms = terms.join(already_mapped, on=["dimension", "id"], how="anti")
        log.info("%d terms already mapped, %d remaining", len(already_mapped), len(terms))

    if terms.is_empty():
        log.info("All terms already mapped — nothing to do")
        return existing

    client = boto3.client("lambda")
    records = [
        _parse_result(dimension, dim_id, name, _invoke_icd_lambda(client, name))
        for dimension, dim_id, name in terms.iter_rows()
        if log.info("Querying ICD-11 for [%s] '%s'", dimension, name) or True
    ]

    searched = pl.DataFrame(records, schema={
        "dimension":  pl.String,
        "id":         pl.Int64,
        "cause_name": pl.String,
        "icd_code":   pl.String,
        "icd_title":  pl.String,
        "chapter":    pl.String,
        "score":      pl.Float64,
        "matched":    pl.Boolean,
    })
    merged = pl.concat(
        [f for f in [existing, searched] if f is not None]
    ).sort(["dimension", "id"])
    merged.write_parquet(OUTPUT_URI, compression="zstd", use_pyarrow=False)
    log.info(
        "Saved %d mappings (%d matched) to %s",
        len(merged), merged["matched"].sum(), OUTPUT_URI,
    )
    return merged