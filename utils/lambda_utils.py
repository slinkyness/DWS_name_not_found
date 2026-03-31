"""
lambda_utils.py — Shared helpers for all fetch Lambdas
=======================================================
Provides:
  - get_secret(secret_name, region, key)  → str
  - save_to_s3(payload, bucket, s3_folder, s3_key, region, timestamp) → str
  - save_to_s3_parquet(payload, bucket, s3_folder, s3_key, timestamp) → str
  - error_response(status_code, message)  → dict
  - ok_response(body)                     → dict

All Lambdas import from this module instead of duplicating the logic.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

import polars as pl

log = logging.getLogger(__name__)


# ── Secrets Manager ────────────────────────────────────────────────────────────

def get_secret(secret_name: str, region: str, key: str) -> str:
    """
    Retrieve a single string value from an AWS Secrets Manager JSON secret.

    Args:
        secret_name: SecretId, e.g. "prod/App/fetch"
        region:      AWS region name, e.g. "eu-central-1"
        key:         JSON key inside the secret, e.g. "NEWS_API"

    Returns:
        The secret string value.

    Raises:
        ClientError: on AWS access failure.
        KeyError:    if the key is absent from the secret JSON.
        ValueError:  if the resolved value is empty.
    """
    session = boto3.session.Session()
    client  = session.client(service_name="secretsmanager", region_name=region)

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        log.error("Failed to retrieve secret %s: %s", secret_name, exc)
        raise

    raw    = response["SecretString"]
    secret = json.loads(raw)

    if key not in secret:
        raise KeyError(f"Key '{key}' not found in secret '{secret_name}'")

    value = secret[key]
    if not value:
        raise ValueError(f"Key '{key}' is empty in secret '{secret_name}'")

    return value


# ── S3 ─────────────────────────────────────────────────────────────────────────

def save_to_s3(
    payload:   dict,
    bucket:    str,
    s3_folder:    str,
    s3_key:    str,
    region:    str,
    timestamp: datetime,
) -> str:
    """
    Serialise payload to JSON and upload to S3.

    Key format: {prefix}/YYYY-MM-DD/{s3_key}_<YYYYMMDDTHHMMSSZ>.json
    Returns the full S3 URI of the saved object.
    """
    date_part = timestamp.strftime("%Y-%m-%d")
    ts_part   = timestamp.strftime("%Y%m%dT%H%M%SZ")
    key       = f"{s3_folder}/{date_part}/{s3_key}_{ts_part}.json"

    body = json.dumps(payload, ensure_ascii=False, default=str, indent=2)

    boto3.client("s3", region_name=region).put_object(
        Bucket      = bucket,
        Key         = key,
        Body        = body.encode("utf-8"),
        ContentType = "application/json",
    )

    uri = f"s3://{bucket}/{key}"
    log.info("Saved to %s  (%d bytes)", uri, len(body))
    return uri


def save_to_s3_parquet(
    payload:   pl.DataFrame,
    bucket:    str,
    s3_folder:     str,
    s3_key: str,
    timestamp: datetime,
) -> str:
    """
    Serialise payload to JSON and upload to S3.

    Key format: {phase}/{s3_key}.parquet
    Returns the full S3 URI of the saved object.
    """
    ts_part = timestamp.strftime("%Y-%m-%d")
    uri       = f"s3://{bucket}/{s3_folder}/{s3_key}_{ts_part}.parquet"
    payload.write_parquet(
        uri,
        compression="zstd",
        use_pyarrow=False,
    )
    log.info("Saved to %s  (%d rows)", uri, len(payload))
    return uri

# ── HTTP responses ─────────────────────────────────────────────────────────────

def ok_response(body: Any) -> dict:
    return {
        "statusCode": 200,
        "headers":    {"Content-Type": "application/json"},
        "body":       json.dumps(body, ensure_ascii=False, default=str),
    }


def error_response(status_code: int, message: str) -> dict:
    return {
        "statusCode": status_code,
        "headers":    {"Content-Type": "application/json"},
        "body":       json.dumps({"error": message}),
    }
